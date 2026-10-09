namespace Glacier.Cluster.Worker;

using System;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;

/// <summary>
/// Worker node daemon hosting embedded ArrowShuffleServer, registering with ClusterCoordinator,
/// heartbeating telemetry, and executing StageDispatchMessage tasks via TaskExecutor.
/// </summary>
public sealed class ClusterWorker : IAsyncDisposable, IDisposable
{
    private readonly ClusterWorkerOptions _options;
    private readonly LocalPartitionStore _partitionStore;
    private readonly ArrowShuffleServer _shuffleServer;
    private readonly IArrowShuffleClient _shuffleClient;
    private readonly TaskExecutor _taskExecutor;
    private readonly CancellationTokenSource _cts = new();

    private NodeHardwareProfile _profile;
    private DuplexConnection? _coordinatorConnection;
    private Task? _heartbeatLoopTask;
    private int _assignedRank = -1;
    private ulong _clusterEpoch;
    private int _isDisposed;
    private ulong _correlationIdCounter;

    public Guid NodeId => _profile.NodeId;
    public int AssignedRank => _assignedRank;
    public ulong ClusterEpoch => _clusterEpoch;
    public ushort ShufflePort => _options.DisableSocketListener ? _options.ShufflePort : _shuffleServer.Port;
    public NodeHardwareProfile HardwareProfile => _profile;
    public LocalPartitionStore PartitionStore => _partitionStore;
    public TaskExecutor Executor => _taskExecutor;
    public ArrowShuffleServer ShuffleServer => _shuffleServer;
    public bool IsRunning => Volatile.Read(ref _isDisposed) == 0 && (_coordinatorConnection?.IsConnected ?? false);

    public ClusterWorker(
        ClusterWorkerOptions? options = null,
        LocalPartitionStore? partitionStore = null,
        IDiskSpillManager? spillManager = null)
    {
        _options = options ?? new ClusterWorkerOptions();
        _profile = _options.HardwareProfile ?? NodeHardwareProfile.ProbeLocal(nodeId: _options.NodeId);

        _partitionStore = partitionStore ?? new LocalPartitionStore(_profile.NodeId);
        _shuffleServer = new ArrowShuffleServer(
            null,
            _partitionStore,
            spillManager,
            port: _options.ShufflePort);

        _shuffleClient = _options.ShuffleClient ?? new ArrowShuffleClient();
        _taskExecutor = new TaskExecutor(
            _profile.NodeId,
            _partitionStore,
            _shuffleClient,
            maxConcurrency: _options.MaxConcurrentTasks > 0 ? _options.MaxConcurrentTasks : (int)_profile.CpuLogicalCores);
    }

    public async Task StartAsync(
        IDuplexPipe? inProcessCoordinatorPipe = null,
        CancellationToken cancellationToken = default)
    {
        // 1. Start Shuffle Server only if socket listener is enabled
        if (!_options.DisableSocketListener)
        {
            _shuffleServer.Start();
        }

        // 2. Establish Connection to Coordinator
        if (inProcessCoordinatorPipe != null)
        {
            _coordinatorConnection = new DuplexConnection(
                Guid.Empty,
                inProcessCoordinatorPipe,
                unsolicitedFrameHandler: HandleInboundCoordinatorFrameAsync);
        }
        else
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            await socket.ConnectAsync(_options.CoordinatorEndPoint, cancellationToken).ConfigureAwait(false);
            var pipe = new SocketDuplexPipe(socket);
            _coordinatorConnection = new DuplexConnection(
                Guid.Empty,
                pipe,
                socket,
                unsolicitedFrameHandler: HandleInboundCoordinatorFrameAsync);
        }

        _coordinatorConnection.Start();

        // 3. Send Registration Request
        var joinRequest = new NodeJoinRequestMessage(_profile, _options.Hostname, _options.ClusterSecret);
        ulong correlationId = Interlocked.Increment(ref _correlationIdCounter);

        byte[] reqBuf = new byte[joinRequest.GetByteCount() + ClusterFrameHeader.HeaderSize];
        ClusterMessageSerializer.PackFrame(joinRequest, correlationId, ClusterFrameFlags.None, reqBuf);

        var responseFrame = await _coordinatorConnection.SendRequestAsync(joinRequest, correlationId, TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
        var joinResponse = (NodeJoinResponseMessage)ClusterMessageSerializer.Deserialize(
            ClusterMessageType.NodeJoinResponse,
            responseFrame.Payload.Span);

        if (joinResponse.Status != NodeJoinStatus.Accepted)
        {
            throw new InvalidOperationException($"Worker registration rejected by coordinator: {joinResponse.RejectionReason}");
        }

        _assignedRank = joinResponse.AssignedRank;
        _clusterEpoch = joinResponse.ClusterEpoch;

        // 4. Start Heartbeat Loop
        _heartbeatLoopTask = Task.Run(() => HeartbeatLoopAsync(_cts.Token));
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _cts.Cancel();

        // Send graceful leave
        if (_coordinatorConnection != null && _coordinatorConnection.IsConnected)
        {
            var leaveMsg = new NodeLeaveMessage(NodeId, 0, 5000, "GracefulShutdown");
            ulong correlationId = Interlocked.Increment(ref _correlationIdCounter);
            byte[] buf = new byte[leaveMsg.GetByteCount() + ClusterFrameHeader.HeaderSize];
            ClusterMessageSerializer.PackFrame(leaveMsg, correlationId, ClusterFrameFlags.None, buf);

            try
            {
                await _coordinatorConnection.SendRawFrameAsync(buf, cancellationToken).ConfigureAwait(false);
            }
            catch { /* Ignore disconnect errors during leave */ }
        }

        await DisposeAsync().ConfigureAwait(false);
    }

    private async Task HeartbeatLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_options.HeartbeatInterval);
        while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
        {
            try
            {
                var ping = new HeartbeatPingMessage(
                    NodeId,
                    PhiAccrualFailureDetector.GetCurrentTimestampNs(),
                    (uint)_taskExecutor.ActiveTaskCount,
                    cpuUsagePercentage: 0.0f,
                    availablePhysicalMemoryBytes: (ulong)GC.GetGCMemoryInfo().TotalAvailableMemoryBytes);

                ulong correlationId = Interlocked.Increment(ref _correlationIdCounter);
                byte[] pingBuf = new byte[HeartbeatPingMessage.PayloadSize + ClusterFrameHeader.HeaderSize];
                ClusterMessageSerializer.PackFrame(ping, correlationId, ClusterFrameFlags.None, pingBuf);

                var pongFrame = await _coordinatorConnection!.SendRequestAsync(ping, correlationId, TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
                var pong = (HeartbeatPongMessage)ClusterMessageSerializer.Deserialize(
                    ClusterMessageType.HeartbeatPong,
                    pongFrame.Payload.Span);

                _clusterEpoch = pong.ClusterEpoch;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch { /* Heartbeat will be retried on next tick */ }
        }
    }

    private async ValueTask HandleInboundCoordinatorFrameAsync(ClusterFrame frame)
    {
        if (frame.Header.MessageType == ClusterMessageType.StageDispatch)
        {
            var dispatch = (StageDispatchMessage)ClusterMessageSerializer.Deserialize(
                ClusterMessageType.StageDispatch,
                frame.Payload.Span);

            _ = Task.Run(async () =>
            {
                TaskStatusUpdateMessage status = await _taskExecutor.ExecuteTaskAsync(dispatch, _cts.Token).ConfigureAwait(false);

                ulong correlationId = Interlocked.Increment(ref _correlationIdCounter);
                byte[] statusBuf = new byte[status.GetByteCount() + ClusterFrameHeader.HeaderSize];
                ClusterMessageSerializer.PackFrame(status, correlationId, ClusterFrameFlags.None, statusBuf);

                if (_coordinatorConnection != null && _coordinatorConnection.IsConnected)
                {
                    await _coordinatorConnection.SendRawFrameAsync(statusBuf, _cts.Token).ConfigureAwait(false);
                }
            }, _cts.Token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _cts.Cancel();

        if (_coordinatorConnection != null)
        {
            await _coordinatorConnection.DisposeAsync().ConfigureAwait(false);
        }

        await _shuffleServer.DisposeAsync().ConfigureAwait(false);
        await _shuffleClient.DisposeAsync().ConfigureAwait(false);
        _taskExecutor.Dispose();
        _partitionStore.Dispose();
        _cts.Dispose();
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
