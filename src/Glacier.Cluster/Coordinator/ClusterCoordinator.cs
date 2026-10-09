namespace Glacier.Cluster.Coordinator;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Glacier.Cluster.Shuffle;

public sealed class ClusterCoordinatorOptions
{
    public ushort Port { get; init; } = 9000;
    public IPAddress BindAddress { get; init; } = IPAddress.Any;
    public string ClusterSecret { get; init; } = string.Empty;
    public TimeSpan HeartbeatAuditInterval { get; init; } = TimeSpan.FromMilliseconds(500);
    public int MaxTaskRetries { get; init; } = 3;
    public bool EnableConsensusReplication { get; init; } = false;
}

/// <summary>
/// Authoritative master daemon managing node join requests, heartbeat pings/pongs,
/// dynamic DAG query submissions, two-phase task scheduling, stage dispatch, and lineage fault recovery.
/// </summary>
public sealed class ClusterCoordinator : IAsyncDisposable, IDisposable
{
    private readonly ClusterCoordinatorOptions _options;
    private readonly MembershipManager _membershipManager;
    private readonly TwoPhaseScheduler _scheduler;
    private readonly StageCompiler _stageCompiler;
    private readonly ActiveStandbyConsensusLedger? _consensusLedger;

    private readonly ConcurrentDictionary<Guid, ActiveJobContext> _activeJobs = new();
    private readonly ConcurrentDictionary<Guid, DuplexConnection> _workerConnections = new();
    private readonly ConcurrentDictionary<Guid, UpstreamPartitionRegistry> _jobPartitionLocations = new();

    private TcpListener? _listener;
    private Task? _acceptLoopTask;
    private Task? _healthAuditLoopTask;
    private readonly CancellationTokenSource _cts = new();
    private int _isDisposed;
    private ulong _correlationIdCounter;

    public Guid CoordinatorId { get; } = Guid.NewGuid();
    public ushort Port { get; private set; }
    public MembershipManager Membership => _membershipManager;
    public ActiveStandbyConsensusLedger? ConsensusLedger => _consensusLedger;
    public bool IsActiveLeader => _consensusLedger == null || _consensusLedger.CurrentRole == ConsensusRole.Active;

    public ClusterCoordinator(
        ClusterCoordinatorOptions? options = null,
        MembershipManager? membershipManager = null,
        TwoPhaseScheduler? scheduler = null,
        StageCompiler? stageCompiler = null,
        ActiveStandbyConsensusLedger? consensusLedger = null)
    {
        _options = options ?? new ClusterCoordinatorOptions();
        _membershipManager = membershipManager ?? new MembershipManager();
        _scheduler = scheduler ?? new TwoPhaseScheduler();
        _stageCompiler = stageCompiler ?? new StageCompiler();
        _consensusLedger = consensusLedger;

        _membershipManager.WorkerLeft += OnWorkerLeft;
        _membershipManager.WorkerHealthChanged += OnWorkerHealthChanged;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_consensusLedger != null)
        {
            await _consensusLedger.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            _listener = new TcpListener(_options.BindAddress, _options.Port);
            _listener.Server.NoDelay = true;
            _listener.Start();
            Port = (ushort)((IPEndPoint)_listener.LocalEndpoint).Port;
            _acceptLoopTask = Task.Run(() => AcceptLoopAsync(_cts.Token));
        }
        catch (SocketException)
        {
            // If socket binding fails (e.g. port conflict or restricted environment), fallback to in-process only
            Port = _options.Port;
        }

        _healthAuditLoopTask = Task.Run(() => HealthAuditLoopAsync(_cts.Token));
    }

    /// <summary>
    /// Registers an in-process worker pipe, supporting loopback unit tests and simulators without TCP sockets.
    /// </summary>
    public ValueTask RegisterInProcessWorkerAsync(
        Guid workerNodeId,
        IDuplexPipe coordinatorSidePipe,
        CancellationToken cancellationToken = default)
    {
        var connection = new DuplexConnection(
            workerNodeId,
            coordinatorSidePipe,
            unsolicitedFrameHandler: frame => HandleInboundFrameAsync(workerNodeId, frame));

        connection.Start();
        _workerConnections[workerNodeId] = connection;
        return ValueTask.CompletedTask;
    }

    public async Task<JobCompletionMessage> SubmitJobAsync(
        ExecutionDag dag,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dag);
        if (!IsActiveLeader)
            throw new InvalidOperationException("This coordinator instance is not the active leader.");

        var jobContext = new ActiveJobContext(dag);
        _activeJobs[dag.JobId] = jobContext;
        _jobPartitionLocations[dag.JobId] = new UpstreamPartitionRegistry();

        // Schedule root stages
        foreach (var rootStage in dag.RootStages)
        {
            await ScheduleStageTasksAsync(jobContext, rootStage, cancellationToken).ConfigureAwait(false);
        }

        return await jobContext.CompletionSource.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task<JobCompletionMessage> SubmitJobAsync(
        LogicalPlanNode logicalPlan,
        string jobName = "DistributedQuery",
        int defaultPartitions = 8,
        CancellationToken cancellationToken = default)
    {
        Guid jobId = Guid.NewGuid();
        ExecutionDag dag = _stageCompiler.Compile(jobId, jobName, logicalPlan, defaultPartitions);
        return SubmitJobAsync(dag, cancellationToken);
    }

    private async Task ScheduleStageTasksAsync(
        ActiveJobContext jobContext,
        ExecutionStage stage,
        CancellationToken cancellationToken)
    {
        var candidateProfiles = _membershipManager.GetHealthyHardwareProfiles();
        var activeTaskCounts = _membershipManager.GetActiveTaskCounts();

        if (candidateProfiles.Count == 0)
        {
            throw new InvalidOperationException("Cannot schedule stage: Zero healthy worker nodes available in cluster.");
        }

        foreach (var task in stage.Tasks)
        {
            if (task.State != TaskState.Pending && task.State != TaskState.Retrying)
                continue;

            // Populate upstream partition locations from parent stages
            if (stage.ParentStageIds.Count > 0)
            {
                var registry = _jobPartitionLocations[jobContext.JobId];
                var upstreamsList = new List<UpstreamPartitionLocation>();
                foreach (int parentId in stage.ParentStageIds)
                {
                    upstreamsList.AddRange(registry.GetLocationsForStage(parentId));
                }
                task.UpstreamPartitions = upstreamsList.ToArray();
            }

            // Two-Phase Placement
            var decision = _scheduler.ScheduleTask(task, candidateProfiles, activeTaskCounts);
            if (!decision.IsSuccess || decision.SelectedNode == null)
            {
                throw new InvalidOperationException($"Scheduling failed for Task {task.TaskId}: No feasible nodes.");
            }

            Guid assignedNodeId = decision.SelectedNode.Value.NodeId;
            task.MarkScheduled(assignedNodeId);

            if (_workerConnections.TryGetValue(assignedNodeId, out var connection))
            {
                var dispatchMsg = task.ToDispatchMessage();
                if (stage.PrimaryOperator == StageOperatorType.GroupByReduce && stage.ParentStageIds.Count > 0)
                {
                    int parentId = stage.ParentStageIds.First();
                    var parentStage = jobContext.Dag.Stages[parentId];
                    string parentPayload = System.Text.Encoding.UTF8.GetString(parentStage.OperatorPayload.Span);
                    string stagePayload = System.Text.Encoding.UTF8.GetString(stage.OperatorPayload.Span);
                    if (!string.IsNullOrEmpty(parentPayload) && !stagePayload.Contains(';'))
                    {
                        var payload = System.Text.Encoding.UTF8.GetBytes($"{parentPayload};{stagePayload}");
                        dispatchMsg = new StageDispatchMessage(
                            dispatchMsg.JobId,
                            dispatchMsg.StageId,
                            dispatchMsg.PartitionId,
                            dispatchMsg.OperatorType,
                            payload,
                            dispatchMsg.UpstreamPartitions);
                    }
                }

                ulong correlationId = Interlocked.Increment(ref _correlationIdCounter);

                byte[] frameBuffer = new byte[dispatchMsg.GetByteCount() + ClusterFrameHeader.HeaderSize];
                ClusterMessageSerializer.PackFrame(dispatchMsg, correlationId, ClusterFrameFlags.None, frameBuffer);

                await connection.SendRawFrameAsync(frameBuffer, cancellationToken).ConfigureAwait(false);
                task.MarkRunning();
            }
        }
    }

    private async ValueTask HandleInboundFrameAsync(Guid senderNodeId, ClusterFrame frame)
    {
        IClusterMessage message = ClusterMessageSerializer.Deserialize(frame.Header.MessageType, frame.Payload.Span);

        switch (message)
        {
            case NodeJoinRequestMessage joinRequest:
            {
                Guid nodeId = joinRequest.HardwareProfile.NodeId != Guid.Empty
                    ? joinRequest.HardwareProfile.NodeId
                    : senderNodeId;

                var ack = await _membershipManager.RegisterWorkerAsync(joinRequest, new IPEndPoint(IPAddress.Loopback, 0)).ConfigureAwait(false);

                // If connection was indexed by temporary ID, update to assigned ID
                if (senderNodeId != nodeId && _workerConnections.TryRemove(senderNodeId, out var existingConn))
                {
                    _workerConnections[nodeId] = existingConn;
                }

                if (_workerConnections.TryGetValue(nodeId, out var conn) || _workerConnections.TryGetValue(senderNodeId, out conn))
                {
                    byte[] ackBuf = new byte[ack.GetByteCount() + ClusterFrameHeader.HeaderSize];
                    ClusterMessageSerializer.PackFrame(ack, frame.Header.CorrelationId, ClusterFrameFlags.None, ackBuf);
                    await conn.SendRawFrameAsync(ackBuf).ConfigureAwait(false);
                }
                break;
            }

            case HeartbeatPingMessage ping:
            {
                _membershipManager.RecordHeartbeat(ping);
                var pong = new HeartbeatPongMessage(
                    CoordinatorId,
                    (ulong)Stopwatch.GetTimestamp(),
                    _membershipManager.ClusterEpoch,
                    coordinatorStatusFlags: 0,
                    activeNodeCount: (uint)_membershipManager.ActiveNodeCount);

                if (_workerConnections.TryGetValue(ping.NodeId, out var conn) || _workerConnections.TryGetValue(senderNodeId, out conn))
                {
                    byte[] pongBuf = new byte[HeartbeatPongMessage.PayloadSize + ClusterFrameHeader.HeaderSize];
                    ClusterMessageSerializer.PackFrame(pong, frame.Header.CorrelationId, ClusterFrameFlags.None, pongBuf);
                    await conn.SendRawFrameAsync(pongBuf).ConfigureAwait(false);
                }
                break;
            }

            case TaskStatusUpdateMessage statusUpdate:
            {
                await HandleTaskStatusUpdateAsync(statusUpdate).ConfigureAwait(false);
                break;
            }

            case NodeLeaveMessage leaveMsg:
            {
                await _membershipManager.HandleWorkerLeaveAsync(leaveMsg).ConfigureAwait(false);
                break;
            }
        }
    }

    private async ValueTask HandleTaskStatusUpdateAsync(TaskStatusUpdateMessage status)
    {
        if (!_activeJobs.TryGetValue(status.JobId, out var jobContext)) return;

        var stage = jobContext.Dag.Stages[status.StageId];
        var task = stage.Tasks.FirstOrDefault(t => t.PartitionId == status.PartitionId);
        if (task == null) return;

        if (status.Status == TaskExecutionStatus.Completed)
        {
            task.MarkCompleted(status.ElapsedMs, status.OutputBytes, status.OutputRowCount);

            if (_membershipManager.TryGetNode(task.AssignedNodeId ?? Guid.Empty, out var workerNode) && workerNode != null)
            {
                _jobPartitionLocations[status.JobId].RegisterPartitionLocation(
                    status.StageId,
                    status.PartitionId,
                    new UpstreamPartitionLocation(
                        workerNode.NodeId,
                        status.StageId,
                        status.PartitionId,
                        workerNode.ShufflePort,
                        workerNode.Hostname));
            }

            // Check if stage is complete
            if (stage.IsCompleted)
            {
                // Unlock child stages
                foreach (int childStageId in stage.ChildStageIds)
                {
                    var childStage = jobContext.Dag.Stages[childStageId];
                    if (childStage.ParentStageIds.All(pid => jobContext.Dag.Stages[pid].IsCompleted))
                    {
                        await ScheduleStageTasksAsync(jobContext, childStage, _cts.Token).ConfigureAwait(false);
                    }
                }

                // Check if entire job is complete
                if (jobContext.Dag.LeafStages.All(s => s.IsCompleted))
                {
                    var completion = new JobCompletionMessage(
                        status.JobId,
                        JobCompletionStatus.Succeeded,
                        jobContext.ElapsedMs,
                        (ulong)jobContext.Dag.Stages.Values.Sum(s => (long)s.Tasks.Sum(t => (long)t.OutputBytes)),
                        (ulong)jobContext.Dag.Stages.Values.Sum(s => (long)s.Tasks.Sum(t => (long)t.OutputRowCount)));

                    jobContext.CompletionSource.TrySetResult(completion);
                    _activeJobs.TryRemove(status.JobId, out _);
                }
            }
        }
        else if (status.Status == TaskExecutionStatus.Failed)
        {
            if (TryParseFetchFailedError(status.ErrorMessage, out int upstreamStageId, out int upstreamPartitionId, out Guid upstreamProducerNodeId))
            {
                // Upstream data was lost!
                if (upstreamProducerNodeId != Guid.Empty && _membershipManager.TryGetNode(upstreamProducerNodeId, out var deadNode) && deadNode != null)
                {
                    deadNode.HealthState = NodeHealthState.Evicted;
                    _membershipManager.FailureDetector.UnregisterNode(upstreamProducerNodeId);
                }

                // Invalidate upstream partition in DAG
                jobContext.Dag.InvalidateStagePartition(upstreamStageId, upstreamPartitionId);
                if (_jobPartitionLocations.TryGetValue(status.JobId, out var reg))
                {
                    reg.InvalidatePartition(upstreamStageId, upstreamPartitionId);
                }

                // Reset downstream task to Pending WITHOUT consuming application retry budget
                task.ResetForLineageRecompute();

                // Reschedule ready stages (upstream stage recomputation first)
                var readyStages = jobContext.Dag.GetReadyStages().ToList();
                foreach (var readyStage in readyStages)
                {
                    await ScheduleStageTasksAsync(jobContext, readyStage, _cts.Token).ConfigureAwait(false);
                }
            }
            else if (task.MarkFailed(status.ErrorMessage ?? "Task execution failed.", out bool canRetry))
            {
                // Retry task
                await ScheduleStageTasksAsync(jobContext, stage, _cts.Token).ConfigureAwait(false);
            }
            else
            {
                FailJob(jobContext, status.ErrorMessage);
            }
        }
    }

    private void FailJob(ActiveJobContext jobContext, string? errorMessage)
    {
        var completion = new JobCompletionMessage(
            jobContext.JobId,
            JobCompletionStatus.Failed,
            jobContext.ElapsedMs,
            0,
            0,
            errorMessage ?? "Job execution failed.");

        jobContext.CompletionSource.TrySetResult(completion);
        _activeJobs.TryRemove(jobContext.JobId, out _);
    }

    public static bool TryParseFetchFailedError(
        string? errorMessage,
        out int upstreamStageId,
        out int upstreamPartitionId,
        out Guid upstreamProducerNodeId)
    {
        upstreamStageId = 0;
        upstreamPartitionId = 0;
        upstreamProducerNodeId = Guid.Empty;

        if (string.IsNullOrEmpty(errorMessage))
            return false;

        int ffIdx = errorMessage.IndexOf("FetchFailed:", StringComparison.OrdinalIgnoreCase);
        if (ffIdx < 0)
            return false;

        errorMessage = errorMessage.Substring(ffIdx);

        try
        {
            int colonIdx = errorMessage.IndexOf(':');
            string metaPart = errorMessage.Substring(colonIdx + 1);
            int secondColon = metaPart.IndexOf(':');
            if (secondColon > 0)
                metaPart = metaPart.Substring(0, secondColon);

            string[] pairs = metaPart.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            foreach (var pair in pairs)
            {
                string[] kv = pair.Split('=', 2, StringSplitOptions.TrimEntries);
                if (kv.Length != 2) continue;

                if (kv[0].Equals("Stage", StringComparison.OrdinalIgnoreCase))
                    int.TryParse(kv[1], out upstreamStageId);
                else if (kv[0].Equals("Partition", StringComparison.OrdinalIgnoreCase))
                    int.TryParse(kv[1], out upstreamPartitionId);
                else if (kv[0].Equals("Producer", StringComparison.OrdinalIgnoreCase) || kv[0].Equals("ProducerNodeId", StringComparison.OrdinalIgnoreCase))
                    Guid.TryParse(kv[1], out upstreamProducerNodeId);
            }

            return upstreamStageId >= 0 && upstreamPartitionId >= 0;
        }
        catch
        {
            return false;
        }
    }

    private void OnWorkerLeft(ClusterWorkerNode node)
    {
        _workerConnections.TryRemove(node.NodeId, out _);
    }

    private void OnWorkerHealthChanged(ClusterWorkerNode node, NodeHealthState oldState, NodeHealthState newState)
    {
        if (newState == NodeHealthState.Evicted)
        {
            _workerConnections.TryRemove(node.NodeId, out _);
            _ = Task.Run(async () =>
            {
                try
                {
                    await TriggerLineageRecoveryForEvictedNodeAsync(node.NodeId, _cts.Token).ConfigureAwait(false);
                }
                catch { /* Ignore background cancellation during shutdown */ }
            });
        }
    }

    public async Task TriggerLineageRecoveryForEvictedNodeAsync(Guid evictedNodeId, CancellationToken ct = default)
    {
        foreach (var jobContext in _activeJobs.Values)
        {
            var dag = jobContext.Dag;

            // 1. Purge stale partition locations for the evicted node from coordinator registry
            if (_jobPartitionLocations.TryGetValue(jobContext.JobId, out var partitionRegistry))
            {
                partitionRegistry.RemoveLocationsForNode(evictedNodeId);
            }

            bool jobFailed = false;

            // 2. Identify in-flight tasks and completed tasks that were on the evicted node
            foreach (var stage in dag.Stages.Values)
            {
                foreach (var task in stage.Tasks.Where(t => t.AssignedNodeId == evictedNodeId))
                {
                    if (task.State == TaskState.Running || task.State == TaskState.Scheduled)
                    {
                        if (!task.MarkFailed($"Worker {evictedNodeId} evicted.", out bool canRetry) || !canRetry)
                        {
                            FailJob(jobContext, $"Task {task.TaskId} exceeded max retry limit after worker {evictedNodeId} eviction.");
                            jobFailed = true;
                            break;
                        }
                    }
                    else if (task.State == TaskState.Completed)
                    {
                        // Invalidate intermediate data resident on the dead node and propagate cascade invalidations down DAG
                        dag.InvalidateStagePartition(stage.StageId, task.PartitionId);
                        if (_jobPartitionLocations.TryGetValue(jobContext.JobId, out var reg))
                        {
                            reg.InvalidatePartition(stage.StageId, task.PartitionId);
                        }
                    }
                }

                if (jobFailed) break;
            }

            if (!jobFailed)
            {
                // 3. Reschedule ready stages (upstream stages first)
                var readyStages = dag.GetReadyStages().ToList();
                foreach (var readyStage in readyStages)
                {
                    await ScheduleStageTasksAsync(jobContext, readyStage, ct).ConfigureAwait(false);
                }
            }
        }
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_listener == null) break;
                Socket socket = await _listener.AcceptSocketAsync(ct).ConfigureAwait(false);
                socket.NoDelay = true;
                _ = Task.Run(async () =>
                {
                    await using var pipe = new SocketDuplexPipe(socket);
                    Guid tempId = Guid.NewGuid();
                    var connection = new DuplexConnection(
                        tempId,
                        pipe,
                        socket,
                        unsolicitedFrameHandler: frame => HandleInboundFrameAsync(tempId, frame));

                    _workerConnections[tempId] = connection;
                    connection.Start();
                }, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch { if (ct.IsCancellationRequested) break; }
        }
    }

    private async Task HealthAuditLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_options.HeartbeatAuditInterval);
        while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
        {
            ulong currentNs = PhiAccrualFailureDetector.GetCurrentTimestampNs();
            foreach (var node in _membershipManager.GetAllNodes())
            {
                _membershipManager.FailureDetector.GetHealthState(node.NodeId, currentNs);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _cts.Cancel();

        if (_listener != null)
        {
            try { _listener.Stop(); } catch { }
            _listener = null;
        }

        foreach (var conn in _workerConnections.Values)
        {
            await conn.DisposeAsync().ConfigureAwait(false);
        }
        _workerConnections.Clear();

        _membershipManager.Dispose();
        _cts.Dispose();
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}

internal sealed class ActiveJobContext
{
    public ExecutionDag Dag { get; }
    public Guid JobId => Dag.JobId;
    public TaskCompletionSource<JobCompletionMessage> CompletionSource { get; }
    private readonly long _startTimestamp;

    public double ElapsedMs => Stopwatch.GetElapsedTime(_startTimestamp).TotalMilliseconds;

    public ActiveJobContext(ExecutionDag dag)
    {
        Dag = dag;
        CompletionSource = new TaskCompletionSource<JobCompletionMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _startTimestamp = Stopwatch.GetTimestamp();
    }
}

internal sealed class UpstreamPartitionRegistry
{
    private readonly ConcurrentDictionary<int, List<UpstreamPartitionLocation>> _locations = new();

    public void RegisterPartitionLocation(int stageId, int partitionId, UpstreamPartitionLocation location)
    {
        var list = _locations.GetOrAdd(stageId, _ => new List<UpstreamPartitionLocation>());
        lock (list)
        {
            list.RemoveAll(loc => loc.PartitionId == partitionId);
            list.Add(location);
        }
    }

    public void RemoveLocationsForNode(Guid nodeId)
    {
        foreach (var list in _locations.Values)
        {
            lock (list)
            {
                list.RemoveAll(loc => loc.ProducerNodeId == nodeId);
            }
        }
    }

    public void InvalidatePartition(int stageId, int partitionId)
    {
        if (_locations.TryGetValue(stageId, out var list))
        {
            lock (list)
            {
                list.RemoveAll(loc => loc.PartitionId == partitionId);
            }
        }
    }

    public UpstreamPartitionLocation[] GetLocationsForStage(int stageId)
    {
        if (_locations.TryGetValue(stageId, out var list))
        {
            lock (list) return list.ToArray();
        }
        return Array.Empty<UpstreamPartitionLocation>();
    }
}
