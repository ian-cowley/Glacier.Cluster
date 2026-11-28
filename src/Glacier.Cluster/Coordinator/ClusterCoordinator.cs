namespace Glacier.Cluster.Coordinator;

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Glacier.Cluster.Shuffle;

public sealed class ClusterCoordinatorOptions
{
    public ushort Port { get; init; } = 9000;
    [JsonIgnore]
    public IPAddress BindAddress { get; init; } = IPAddress.Any;
    public string ClusterSecret { get; init; } = string.Empty;
    public TimeSpan HeartbeatAuditInterval { get; init; } = TimeSpan.FromMilliseconds(500);
    public int MaxTaskRetries { get; init; } = 3;
    public bool EnableConsensusReplication { get; init; } = false;
    public bool DisableSocketListener { get; init; } = false;
    [JsonIgnore]
    public IReadOnlyList<EndPoint> PeerCoordinatorEndPoints { get; init; } = Array.Empty<EndPoint>();
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
    private readonly byte[]? _expectedSecretHash;

    private readonly ConcurrentDictionary<Guid, ActiveJobContext> _activeJobs = new();
    private readonly ConcurrentDictionary<Guid, DuplexConnection> _workerConnections = new();
    private readonly ConcurrentDictionary<Guid, EndPoint> _workerEndPoints = new();
    private readonly ConcurrentDictionary<Guid, UpstreamPartitionRegistry> _jobPartitionLocations = new();
    private readonly ConcurrentDictionary<EndPoint, DuplexConnection> _peerConnections = new();
    private readonly ConcurrentDictionary<Guid, DuplexConnection> _inProcessPeerConnections = new();

    private TcpListener? _listener;
    private Task? _acceptLoopTask;
    private Task? _healthAuditLoopTask;
    private readonly CancellationTokenSource _cts = new();
    private int _isDisposed;
    private ulong _correlationIdCounter;

    private sealed class ConnectionAuthState
    {
        public Guid ConnectionKey;
        public Guid? AuthenticatedNodeId;
        public EndPoint? RemoteEndPoint;
    }

    public Guid CoordinatorId { get; } = Guid.NewGuid();
    public ushort Port { get; private set; }
    public MembershipManager Membership => _membershipManager;
    public ActiveStandbyConsensusLedger? ConsensusLedger => _consensusLedger;
    public bool IsActiveLeader => _consensusLedger == null || _consensusLedger.CurrentRole == ConsensusRole.Active || _consensusLedger.CurrentRole == ConsensusRole.Standalone;

    public ClusterCoordinator(ushort port)
        : this(new ClusterCoordinatorOptions { Port = port })
    {
    }

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
        _expectedSecretHash = string.IsNullOrEmpty(_options.ClusterSecret)
            ? null
            : SHA256.HashData(Encoding.UTF8.GetBytes(_options.ClusterSecret));

        _membershipManager.WorkerLeft += OnWorkerLeft;
        _membershipManager.WorkerHealthChanged += OnWorkerHealthChanged;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_consensusLedger != null)
        {
            if (_options.EnableConsensusReplication)
            {
                _consensusLedger.LeaseRenewed += OnLocalConsensusLeaseRenewed;
            }
            await _consensusLedger.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        if (_options.DisableSocketListener)
        {
            Port = _options.Port;
        }
        else
        {
            try
            {
                _listener = new TcpListener(_options.BindAddress, _options.Port);
                _listener.Server.NoDelay = true;
                _listener.Start();
                Port = (ushort)((IPEndPoint)_listener.LocalEndpoint).Port;
                _acceptLoopTask = Task.Run(() => AcceptLoopAsync(_cts.Token));
            }
            catch (SocketException) when (_options.Port == 0)
            {
                Port = 0;
            }
        }

        _healthAuditLoopTask = Task.Run(() => HealthAuditLoopAsync(_cts.Token));

        if (_options.EnableConsensusReplication && _consensusLedger != null && _consensusLedger.CurrentRole == ConsensusRole.Active)
        {
            await ReplicateLeaderLeaseAsync(_consensusLedger.CurrentLease, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Registers an in-process worker pipe, supporting loopback unit tests and simulators without TCP sockets.
    /// </summary>
    public ValueTask RegisterInProcessWorkerAsync(
        Guid workerNodeId,
        IDuplexPipe coordinatorSidePipe,
        CancellationToken cancellationToken = default)
    {
        var authState = new ConnectionAuthState { ConnectionKey = workerNodeId };
        DuplexConnection? connection = null;
        connection = new DuplexConnection(
            workerNodeId,
            coordinatorSidePipe,
            unsolicitedFrameHandler: frame => HandleInboundFrameAsync(connection!, authState, frame));

        connection.Start();
        _workerConnections[workerNodeId] = connection;
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Registers an in-process peer coordinator pipe for consensus lease replication testing without TCP sockets.
    /// </summary>
    public ValueTask RegisterInProcessPeerCoordinatorAsync(
        Guid peerCoordinatorId,
        IDuplexPipe peerPipe)
    {
        ArgumentNullException.ThrowIfNull(peerPipe);
        var authState = new ConnectionAuthState { ConnectionKey = peerCoordinatorId };
        DuplexConnection? connection = null;
        connection = new DuplexConnection(
            peerCoordinatorId,
            peerPipe,
            unsolicitedFrameHandler: frame => HandleInboundFrameAsync(connection!, authState, frame));
        connection.Start();
        _inProcessPeerConnections[peerCoordinatorId] = connection;
        return ValueTask.CompletedTask;
    }

    private void OnLocalConsensusLeaseRenewed(LeaderLease lease)
    {
        if (!_options.EnableConsensusReplication || Volatile.Read(ref _isDisposed) != 0)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                await ReplicateLeaderLeaseAsync(lease, _cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested) { }
            catch (Exception ex)
            {
                Trace.TraceWarning($"[ClusterCoordinator] Consensus lease replication warning: {ex.Message}");
            }
        });
    }

    public async Task ReplicateLeaderLeaseAsync(LeaderLease lease, CancellationToken ct = default)
    {
        if (_consensusLedger == null || _consensusLedger.CurrentRole != ConsensusRole.Active)
            return;

        long remainingMs = Math.Max(0L, (long)(lease.ExpiryUtc - DateTimeOffset.UtcNow).TotalMilliseconds);
        var msg = new LeaderLeaseMessage(lease.LeaderId, lease.Epoch, remainingMs, _options.ClusterSecret);
        ulong corrId = Interlocked.Increment(ref _correlationIdCounter);

        foreach (var peerEp in _options.PeerCoordinatorEndPoints)
        {
            try
            {
                if (!_peerConnections.TryGetValue(peerEp, out var peerConn) || !peerConn.IsConnected)
                {
                    var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                    await socket.ConnectAsync(peerEp, ct).ConfigureAwait(false);
                    var pipe = new SocketDuplexPipe(socket);
                    var authState = new ConnectionAuthState { ConnectionKey = Guid.NewGuid() };
                    DuplexConnection? newConn = null;
                    newConn = new DuplexConnection(
                        authState.ConnectionKey,
                        pipe,
                        socket,
                        unsolicitedFrameHandler: frame => HandleInboundFrameAsync(newConn!, authState, frame));
                    newConn.Start();
                    _peerConnections[peerEp] = newConn;
                    peerConn = newConn;
                }

                await peerConn.SendFrameAsync(msg, corrId, ct: ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (_peerConnections.TryRemove(peerEp, out var deadConn))
                {
                    await deadConn.DisposeAsync().ConfigureAwait(false);
                }
                Trace.TraceWarning($"[ClusterCoordinator] Failed replicating lease to peer {peerEp}: {ex.Message}");
            }
        }

        foreach (var kvp in _inProcessPeerConnections)
        {
            if (kvp.Value.IsConnected)
            {
                try
                {
                    await kvp.Value.SendFrameAsync(msg, corrId, ct: ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Trace.TraceWarning($"[ClusterCoordinator] Failed replicating lease to in-process peer {kvp.Key}: {ex.Message}");
                }
            }
        }
    }

    public async Task<JobCompletionMessage> SubmitJobAsync(
        ExecutionDag dag,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dag);
        if (!IsActiveLeader)
            throw new InvalidOperationException("This coordinator instance is not the active leader.");

        ulong epoch = _consensusLedger?.CurrentEpoch ?? 0;
        var jobContext = new ActiveJobContext(dag, epoch);
        _activeJobs[dag.JobId] = jobContext;
        _jobPartitionLocations[dag.JobId] = new UpstreamPartitionRegistry();

        bool anyRootScheduled = false;
        try
        {
            // Schedule root stages
            foreach (var rootStage in dag.RootStages)
            {
                if (jobContext.CompletionSource.Task.IsCompleted)
                    break;

                try
                {
                    await ScheduleStageTasksAsync(jobContext, rootStage, cancellationToken).ConfigureAwait(false);
                    anyRootScheduled = true;
                }
                catch (Exception ex) when (anyRootScheduled && ex is not OperationCanceledException)
                {
                    FailJob(jobContext, ex.Message);
                    break;
                }
            }

            return await jobContext.CompletionSource.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _activeJobs.TryRemove(dag.JobId, out _);
            _jobPartitionLocations.TryRemove(dag.JobId, out _);
        }
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
        if (!IsActiveLeader || (_consensusLedger != null && _consensusLedger.CurrentEpoch != jobContext.LeaderEpoch))
        {
            throw new InvalidOperationException("Coordinator lost active leadership or epoch fenced before stage dispatch.");
        }

        var candidateProfiles = _membershipManager.GetHealthyHardwareProfiles();
        var activeTaskCounts = _membershipManager.GetActiveTaskCounts();

        if (candidateProfiles.Count == 0)
        {
            throw new InvalidOperationException("Cannot schedule stage: Zero healthy worker nodes available in cluster.");
        }

        // Hoist stage-level payload adjustments out of the per-task dispatch loop.
        byte[]? hoistedPayload = null;
        bool coPartitionedJoin = false;
        if (stage.PrimaryOperator == StageOperatorType.GroupByReduce && stage.ParentStageIds.Count > 0)
        {
            int parentId = stage.ParentStageIds.First();
            var parentStage = jobContext.Dag.Stages[parentId];
            string parentPayload = Encoding.UTF8.GetString(parentStage.OperatorPayload.Span);
            int gbSep = parentPayload.IndexOf("\u001fGROUPBY:", StringComparison.Ordinal);
            string groupKeysStr = gbSep >= 0 ? parentPayload[(gbSep + "\u001fGROUPBY:".Length)..] : parentPayload;
            string stagePayload = Encoding.UTF8.GetString(stage.OperatorPayload.Span);
            string combined = !string.IsNullOrEmpty(groupKeysStr) && !stagePayload.Contains(';')
                ? $"{groupKeysStr};{stagePayload}"
                : stagePayload;
            if (!combined.Contains("\u001fP:", StringComparison.Ordinal))
            {
                combined = $"{combined}\u001fP:{stage.PartitionsCount}";
            }
            hoistedPayload = Encoding.UTF8.GetBytes(combined);
        }
        else if ((stage.PrimaryOperator == StageOperatorType.Scan || stage.PrimaryOperator == StageOperatorType.GroupByMap) &&
                 stage.ParentStageIds.Count == 0 &&
                 stage.PartitionsCount > 1)
        {
            string rawPayload = Encoding.UTF8.GetString(stage.OperatorPayload.Span);
            if (!string.IsNullOrEmpty(rawPayload) && !rawPayload.Contains("\u001fP:", StringComparison.Ordinal))
            {
                hoistedPayload = Encoding.UTF8.GetBytes($"{rawPayload}\u001fP:{stage.PartitionsCount}");
            }
        }
        else if (stage.PrimaryOperator == StageOperatorType.HashJoinProbe && stage.ParentStageIds.Count == 2)
        {
            string joinKey = Encoding.UTF8.GetString(stage.OperatorPayload.Span).Trim();
            int leftStageId = stage.ParentStageIds.First();
            int rightStageId = stage.ParentStageIds.Skip(1).First();
            var leftStage = jobContext.Dag.Stages[leftStageId];
            var rightStage = jobContext.Dag.Stages[rightStageId];

            coPartitionedJoin =
                leftStage.PartitionsCount == stage.PartitionsCount &&
                rightStage.PartitionsCount == stage.PartitionsCount &&
                IsStagePartitionedByKey(jobContext.Dag, leftStage, joinKey) &&
                IsStagePartitionedByKey(jobContext.Dag, rightStage, joinKey);

            string tag = coPartitionedJoin ? $"\u001fCO_PART:{stage.PartitionsCount}" : $"\u001fP:{stage.PartitionsCount}";
            if (!joinKey.Contains("\u001f", StringComparison.Ordinal))
            {
                hoistedPayload = Encoding.UTF8.GetBytes($"{joinKey}{tag}");
            }
        }

        foreach (var task in stage.Tasks)
        {
            if (!IsActiveLeader || (_consensusLedger != null && _consensusLedger.CurrentEpoch != jobContext.LeaderEpoch))
            {
                throw new InvalidOperationException("Coordinator lost active leadership during stage dispatch.");
            }

            if (task.State != TaskState.Pending && task.State != TaskState.Retrying)
                continue;

            // Populate upstream partition locations from parent stages
            if (stage.ParentStageIds.Count > 0)
            {
                var registry = _jobPartitionLocations[jobContext.JobId];
                var upstreamsList = new List<UpstreamPartitionLocation>();
                foreach (int parentId in stage.ParentStageIds)
                {
                    var locs = registry.GetLocationsForStage(parentId);
                    if (coPartitionedJoin)
                    {
                        foreach (var loc in locs)
                        {
                            if (loc.PartitionId == task.PartitionId)
                            {
                                upstreamsList.Add(loc);
                            }
                        }
                    }
                    else
                    {
                        upstreamsList.AddRange(locs);
                    }
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

            if (_workerConnections.TryGetValue(assignedNodeId, out var connection) && connection.IsConnected)
            {
                var dispatchMsg = task.ToDispatchMessage();
                if (hoistedPayload != null)
                {
                    dispatchMsg = new StageDispatchMessage(
                        dispatchMsg.JobId,
                        dispatchMsg.StageId,
                        dispatchMsg.PartitionId,
                        dispatchMsg.OperatorType,
                        hoistedPayload,
                        dispatchMsg.UpstreamPartitions);
                }

                ulong correlationId = Interlocked.Increment(ref _correlationIdCounter);
                task.MarkRunning();
                try
                {
                    await connection.SendFrameAsync(dispatchMsg, correlationId, flags: 0, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    if (_workerConnections.TryRemove(KeyValuePair.Create(assignedNodeId, connection)))
                    {
                        _workerEndPoints.TryRemove(assignedNodeId, out _);
                    }
                    string errorMsg = $"Failed dispatching Task {task.TaskId} to worker {assignedNodeId}: {ex.Message}";
                    task.MarkFailed(errorMsg, out _);
                    FailJob(jobContext, errorMsg);
                    return;
                }
            }
            else
            {
                if (connection != null && _workerConnections.TryRemove(KeyValuePair.Create(assignedNodeId, connection)))
                {
                    _workerEndPoints.TryRemove(assignedNodeId, out _);
                }
                string errorMsg = $"Cannot dispatch Task {task.TaskId}: No active control connection for assigned worker {assignedNodeId}.";
                task.MarkFailed(errorMsg, out _);
                FailJob(jobContext, errorMsg);
                return;
            }
        }
    }

    private static bool IsStagePartitionedByKey(ExecutionDag dag, ExecutionStage stage, string requiredKey)
    {
        if (string.IsNullOrEmpty(requiredKey))
            return false;

        if (stage.PrimaryOperator == StageOperatorType.Custom)
        {
            string p = Encoding.UTF8.GetString(stage.OperatorPayload.Span);
            if (p.StartsWith("REPARTITION:", StringComparison.Ordinal))
            {
                string[] parts = p.Split(':', 3);
                if (parts.Length == 3 && string.Equals(parts[2].Trim(), requiredKey, StringComparison.Ordinal))
                {
                    return true;
                }
            }
        }
        else if (stage.PrimaryOperator == StageOperatorType.GroupByReduce && stage.ParentStageIds.Count > 0)
        {
            var mapStage = dag.Stages[stage.ParentStageIds.First()];
            string mapPayload = Encoding.UTF8.GetString(mapStage.OperatorPayload.Span);
            int gbSep = mapPayload.IndexOf("\u001fGROUPBY:", StringComparison.Ordinal);
            string keys = gbSep >= 0 ? mapPayload[(gbSep + "\u001fGROUPBY:".Length)..] : mapPayload;
            if (string.Equals(keys.Trim(), requiredKey, StringComparison.Ordinal))
            {
                return true;
            }
        }
        else if (stage.PrimaryOperator == StageOperatorType.HashJoinProbe)
        {
            string payload = Encoding.UTF8.GetString(stage.OperatorPayload.Span);
            int sep = payload.IndexOf('\u001f');
            string key = (sep >= 0 ? payload[..sep] : payload).Trim();
            if (string.Equals(key, requiredKey, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private bool VerifyClusterSecret(string? providedSecret)
    {
        if (_expectedSecretHash == null) return true;
        string secret = providedSecret ?? string.Empty;
        int maxByteLen = Encoding.UTF8.GetMaxByteCount(secret.Length);
        byte[]? rented = null;
        Span<byte> utf8Buf = maxByteLen <= 512
            ? stackalloc byte[maxByteLen]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteLen));
        try
        {
            int written = Encoding.UTF8.GetBytes(secret, utf8Buf);
            Span<byte> providedHash = stackalloc byte[32];
            SHA256.HashData(utf8Buf.Slice(0, written), providedHash);
            return CryptographicOperations.FixedTimeEquals(_expectedSecretHash, providedHash);
        }
        finally
        {
            if (rented != null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private async ValueTask CloseRejectedConnectionAsync(DuplexConnection conn, ConnectionAuthState authState)
    {
        if (_workerConnections.TryRemove(KeyValuePair.Create(authState.ConnectionKey, conn)))
        {
            _workerEndPoints.TryRemove(authState.ConnectionKey, out _);
        }
        if (authState.AuthenticatedNodeId is Guid authedId &&
            authedId != authState.ConnectionKey &&
            _workerConnections.TryRemove(KeyValuePair.Create(authedId, conn)))
        {
            _workerEndPoints.TryRemove(authedId, out _);
        }
        await conn.DisposeAsync().ConfigureAwait(false);
    }

    private async ValueTask HandleInboundFrameAsync(DuplexConnection conn, ConnectionAuthState authState, ClusterFrame frame)
    {
        IClusterMessage message = ClusterMessageSerializer.Deserialize(frame.Header.MessageType, frame.Payload.Span);

        switch (message)
        {
            case NodeJoinRequestMessage joinRequest:
            {
                if (!VerifyClusterSecret(joinRequest.ClusterSecret))
                {
                    var rejected = new NodeJoinResponseMessage(
                        status: NodeJoinStatus.Unauthorized,
                        assignedNodeId: Guid.Empty,
                        assignedRank: -1,
                        activeClusterSize: _membershipManager.ActiveNodeCount,
                        clusterEpoch: _membershipManager.ClusterEpoch,
                        rejectionReason: "Invalid cluster authentication secret.");

                    try
                    {
                        await conn.SendFrameAsync(rejected, frame.Header.CorrelationId).ConfigureAwait(false);
                        // Allow the peer TCP stack to receive the Unauthorized rejection frame before tearing down the socket.
                        await Task.Delay(25).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceWarning($"[ClusterCoordinator] Failed sending unauthorized response: {ex.Message}");
                    }

                    await CloseRejectedConnectionAsync(conn, authState).ConfigureAwait(false);
                    break;
                }

                if (authState.AuthenticatedNodeId is Guid existingId &&
                    joinRequest.HardwareProfile.NodeId != existingId)
                {
                    var rejected = new NodeJoinResponseMessage(
                        status: NodeJoinStatus.Unauthorized,
                        assignedNodeId: Guid.Empty,
                        assignedRank: -1,
                        activeClusterSize: _membershipManager.ActiveNodeCount,
                        clusterEpoch: _membershipManager.ClusterEpoch,
                        rejectionReason: "Connection is already authenticated to a different NodeId.");

                    try
                    {
                        await conn.SendFrameAsync(rejected, frame.Header.CorrelationId).ConfigureAwait(false);
                        // Allow the peer TCP stack to receive the Unauthorized rejection frame before tearing down the socket.
                        await Task.Delay(25).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceWarning($"[ClusterCoordinator] Failed sending rebind rejection response: {ex.Message}");
                    }

                    await CloseRejectedConnectionAsync(conn, authState).ConfigureAwait(false);
                    break;
                }

                EndPoint remoteEndPoint = authState.RemoteEndPoint
                    ?? (_workerEndPoints.TryGetValue(authState.ConnectionKey, out var trackedEp)
                        ? trackedEp
                        : new IPEndPoint(IPAddress.Loopback, 0));

                var ack = await _membershipManager.RegisterWorkerAsync(
                    joinRequest,
                    remoteEndPoint,
                    joinRequest.ShufflePort).ConfigureAwait(false);

                Guid nodeId = ack.AssignedNodeId != Guid.Empty
                    ? ack.AssignedNodeId
                    : (joinRequest.HardwareProfile.NodeId != Guid.Empty
                        ? joinRequest.HardwareProfile.NodeId
                        : authState.ConnectionKey);

                authState.AuthenticatedNodeId = nodeId;

                if (authState.ConnectionKey != nodeId)
                {
                    if (_workerConnections.TryRemove(KeyValuePair.Create(authState.ConnectionKey, conn)) &&
                        _workerEndPoints.TryRemove(authState.ConnectionKey, out var existingEp))
                    {
                        _workerEndPoints[nodeId] = existingEp;
                    }
                    authState.ConnectionKey = nodeId;
                }

                _workerConnections[nodeId] = conn;
                if (authState.RemoteEndPoint != null)
                {
                    _workerEndPoints[nodeId] = authState.RemoteEndPoint;
                }

                await conn.SendFrameAsync(ack, frame.Header.CorrelationId).ConfigureAwait(false);
                break;
            }

            case LeaderLeaseMessage leaseMsg:
            {
                if (!VerifyClusterSecret(leaseMsg.ClusterSecret))
                {
                    await CloseRejectedConnectionAsync(conn, authState).ConfigureAwait(false);
                    break;
                }

                _consensusLedger?.UpdateLeaderLeaseFromRemote(
                    leaseMsg.LeaderId,
                    leaseMsg.Epoch,
                    leaseMsg.RemainingLeaseMs);
                break;
            }

            case HeartbeatPingMessage ping:
            {
                if (authState.AuthenticatedNodeId is not Guid authedNodeId || ping.NodeId != authedNodeId)
                {
                    await CloseRejectedConnectionAsync(conn, authState).ConfigureAwait(false);
                    break;
                }

                ulong receiptTimestampNs = PhiAccrualFailureDetector.GetCurrentTimestampNs();
                _membershipManager.RecordHeartbeat(ping, receiptTimestampNs);

                var pong = new HeartbeatPongMessage(
                    CoordinatorId,
                    receiptTimestampNs,
                    _membershipManager.ClusterEpoch,
                    coordinatorStatusFlags: 0,
                    activeNodeCount: (uint)_membershipManager.ActiveNodeCount);

                await conn.SendFrameAsync(pong, frame.Header.CorrelationId).ConfigureAwait(false);
                break;
            }

            case TaskStatusUpdateMessage statusUpdate:
            {
                if (authState.AuthenticatedNodeId is not Guid authedNodeId)
                {
                    await CloseRejectedConnectionAsync(conn, authState).ConfigureAwait(false);
                    break;
                }

                await HandleTaskStatusUpdateAsync(statusUpdate, authedNodeId).ConfigureAwait(false);
                break;
            }

            case NodeLeaveMessage leaveMsg:
            {
                if (authState.AuthenticatedNodeId is not Guid authedNodeId || leaveMsg.NodeId != authedNodeId)
                {
                    await CloseRejectedConnectionAsync(conn, authState).ConfigureAwait(false);
                    break;
                }

                await _membershipManager.HandleWorkerLeaveAsync(leaveMsg).ConfigureAwait(false);
                break;
            }
        }
    }

    private async ValueTask HandleTaskStatusUpdateAsync(TaskStatusUpdateMessage status, Guid authenticatedNodeId)
    {
        if (!IsActiveLeader) return;
        if (!_activeJobs.TryGetValue(status.JobId, out var jobContext)) return;
        if (_consensusLedger != null && _consensusLedger.CurrentEpoch != jobContext.LeaderEpoch) return;

        var stage = jobContext.Dag.Stages[status.StageId];
        var task = stage.Tasks.FirstOrDefault(t => t.PartitionId == status.PartitionId);
        if (task == null) return;
        if (task.AssignedNodeId != authenticatedNodeId) return;

        if (status.Status == TaskExecutionStatus.Completed)
        {
            task.MarkCompleted(status.ElapsedMs, status.OutputBytes, status.OutputRowCount);

            if (_membershipManager.TryGetNode(task.AssignedNodeId ?? Guid.Empty, out var workerNode) && workerNode != null)
            {
                if (_jobPartitionLocations.TryGetValue(status.JobId, out var registry))
                {
                    registry.RegisterPartitionLocation(
                        status.StageId,
                        status.PartitionId,
                        new UpstreamPartitionLocation(
                            workerNode.NodeId,
                            status.StageId,
                            status.PartitionId,
                            workerNode.ShufflePort,
                            workerNode.Hostname));
                }
            }

            // Check if stage is complete
            if (stage.IsCompleted)
            {
                // Unlock child stages
                foreach (int childStageId in stage.ChildStageIds)
                {
                    if (jobContext.CompletionSource.Task.IsCompleted)
                        break;

                    var childStage = jobContext.Dag.Stages[childStageId];
                    if (childStage.ParentStageIds.All(pid => jobContext.Dag.Stages[pid].IsCompleted))
                    {
                        try
                        {
                            await ScheduleStageTasksAsync(jobContext, childStage, _cts.Token).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            FailJob(jobContext, ex.Message);
                            return;
                        }

                        if (jobContext.CompletionSource.Task.IsCompleted)
                            return;
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
                    _jobPartitionLocations.TryRemove(status.JobId, out _);
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
                    if (jobContext.CompletionSource.Task.IsCompleted)
                        break;

                    try
                    {
                        await ScheduleStageTasksAsync(jobContext, readyStage, _cts.Token).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        FailJob(jobContext, ex.Message);
                        return;
                    }

                    if (jobContext.CompletionSource.Task.IsCompleted)
                        return;
                }
            }
            else if (task.MarkFailed(status.ErrorMessage ?? "Task execution failed.", out bool canRetry))
            {
                // Retry task
                try
                {
                    await ScheduleStageTasksAsync(jobContext, stage, _cts.Token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    FailJob(jobContext, ex.Message);
                    return;
                }
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
        _jobPartitionLocations.TryRemove(jobContext.JobId, out _);
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
        _workerEndPoints.TryRemove(node.NodeId, out _);
    }

    private void OnWorkerHealthChanged(ClusterWorkerNode node, NodeHealthState oldState, NodeHealthState newState)
    {
        if (newState == NodeHealthState.Evicted)
        {
            _workerConnections.TryRemove(node.NodeId, out _);
            _workerEndPoints.TryRemove(node.NodeId, out _);
            _ = Task.Run(async () =>
            {
                try
                {
                    await TriggerLineageRecoveryForEvictedNodeAsync(node.NodeId, _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    Trace.TraceWarning($"[ClusterCoordinator] Evicted node lineage recovery warning for {node.NodeId}: {ex.Message}");
                }
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
                    if (jobContext.CompletionSource.Task.IsCompleted)
                        break;

                    try
                    {
                        await ScheduleStageTasksAsync(jobContext, readyStage, ct).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        FailJob(jobContext, ex.Message);
                        break;
                    }

                    if (jobContext.CompletionSource.Task.IsCompleted)
                        break;
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
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
                {
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 5);
                }
                socket.ReceiveTimeout = 120_000;
                socket.SendTimeout = 120_000;

                var pipe = new SocketDuplexPipe(socket);
                Guid tempId = Guid.NewGuid();
                if (socket.RemoteEndPoint != null)
                {
                    _workerEndPoints[tempId] = socket.RemoteEndPoint;
                }

                var authState = new ConnectionAuthState
                {
                    ConnectionKey = tempId,
                    RemoteEndPoint = socket.RemoteEndPoint
                };
                DuplexConnection? connection = null;
                connection = new DuplexConnection(
                    tempId,
                    pipe,
                    socket,
                    unsolicitedFrameHandler: frame => HandleInboundFrameAsync(connection!, authState, frame));

                _workerConnections[tempId] = connection;
                connection.Start();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                if (ct.IsCancellationRequested) break;
                Trace.TraceWarning($"[ClusterCoordinator] AcceptLoop warning: {ex.Message}");
            }
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

        if (_consensusLedger != null && _options.EnableConsensusReplication)
        {
            _consensusLedger.LeaseRenewed -= OnLocalConsensusLeaseRenewed;
        }

        if (_listener != null)
        {
            try { _listener.Stop(); } catch (Exception ex) { Trace.TraceWarning($"[ClusterCoordinator] Listener stop warning: {ex.Message}"); }
            _listener = null;
        }

        foreach (var conn in _workerConnections.Values)
        {
            await conn.DisposeAsync().ConfigureAwait(false);
        }
        _workerConnections.Clear();
        _workerEndPoints.Clear();

        foreach (var peerConn in _peerConnections.Values)
        {
            await peerConn.DisposeAsync().ConfigureAwait(false);
        }
        _peerConnections.Clear();

        foreach (var inProcPeer in _inProcessPeerConnections.Values)
        {
            await inProcPeer.DisposeAsync().ConfigureAwait(false);
        }
        _inProcessPeerConnections.Clear();

        _activeJobs.Clear();
        _jobPartitionLocations.Clear();

        _membershipManager.Dispose();
        _cts.Dispose();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _cts.Cancel();

        if (_consensusLedger != null && _options.EnableConsensusReplication)
        {
            _consensusLedger.LeaseRenewed -= OnLocalConsensusLeaseRenewed;
        }

        if (_listener != null)
        {
            try { _listener.Stop(); } catch (Exception ex) { Trace.TraceWarning($"[ClusterCoordinator] Listener stop warning: {ex.Message}"); }
            _listener = null;
        }

        foreach (var conn in _workerConnections.Values)
        {
            conn.Dispose();
        }
        _workerConnections.Clear();
        _workerEndPoints.Clear();

        foreach (var peerConn in _peerConnections.Values)
        {
            peerConn.Dispose();
        }
        _peerConnections.Clear();

        foreach (var inProcPeer in _inProcessPeerConnections.Values)
        {
            inProcPeer.Dispose();
        }
        _inProcessPeerConnections.Clear();

        _activeJobs.Clear();
        _jobPartitionLocations.Clear();

        _membershipManager.Dispose();
        _cts.Dispose();
    }
}

internal sealed class ActiveJobContext
{
    public ExecutionDag Dag { get; }
    public Guid JobId => Dag.JobId;
    public ulong LeaderEpoch { get; }
    public TaskCompletionSource<JobCompletionMessage> CompletionSource { get; }
    private readonly long _startTimestamp;

    public double ElapsedMs => Stopwatch.GetElapsedTime(_startTimestamp).TotalMilliseconds;

    public ActiveJobContext(ExecutionDag dag, ulong leaderEpoch = 0)
    {
        Dag = dag;
        LeaderEpoch = leaderEpoch;
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
