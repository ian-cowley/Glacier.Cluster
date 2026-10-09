namespace Glacier.Cluster.Simulator;

using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;
using Glacier.Cluster.Worker;

/// <summary>
/// Virtual simulated worker node encapsulating ClusterWorker with in-memory routing,
/// configurable hardware profile, and deterministic kill/recovery support.
/// </summary>
public sealed class SimulatedWorkerNode : IAsyncDisposable, IDisposable
{
    private readonly ClusterWorker _worker;
    private readonly InMemoryShuffleBus _bus;
    private readonly int _nodeIndex;
    private int _isDisposed;

    public Guid NodeId => _worker.NodeId;
    public int NodeIndex => _nodeIndex;
    public int AssignedRank => _worker.AssignedRank;
    public ulong ClusterEpoch => _worker.ClusterEpoch;
    public ushort VirtualShufflePort { get; }
    public string Hostname { get; }
    public NodeHardwareProfile HardwareProfile => _worker.HardwareProfile;
    public LocalPartitionStore PartitionStore => _worker.PartitionStore;
    public TaskExecutor Executor => _worker.Executor;
    public ArrowShuffleServer ShuffleServer => _worker.ShuffleServer;
    public ClusterWorker Worker => _worker;
    public bool IsRunning => _worker.IsRunning && !IsAbruptlyKilled;
    public bool IsAbruptlyKilled { get; private set; }

    public SimulatedWorkerNode(
        int nodeIndex,
        NodeHardwareProfile profile,
        InMemoryShuffleBus bus,
        SimulatorShuffleMode shuffleMode,
        TimeSpan? heartbeatInterval = null)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _nodeIndex = nodeIndex;
        VirtualShufflePort = (ushort)(10000 + nodeIndex);
        Hostname = $"sim-node-{nodeIndex:D2}";

        var options = new ClusterWorkerOptions
        {
            NodeId = profile.NodeId,
            Hostname = Hostname,
            ShufflePort = VirtualShufflePort,
            HeartbeatInterval = heartbeatInterval ?? TimeSpan.FromMilliseconds(100),
            HardwareProfile = profile,
            DisableSocketListener = true,
            ShuffleClient = bus.CreateShuffleClientForNode(this, shuffleMode)
        };

        _worker = new ClusterWorker(options);
    }

    public async Task StartAsync(IDuplexPipe workerSidePipe, CancellationToken ct = default)
    {
        _bus.RegisterNode(this);
        await _worker.StartAsync(workerSidePipe, ct).ConfigureAwait(false);
    }

    public void KillAbruptly()
    {
        IsAbruptlyKilled = true;
        _bus.UnregisterNode(this);
        _worker.Dispose();
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _bus.UnregisterNode(this);
        await _worker.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _bus.UnregisterNode(this);
        await _worker.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _bus.UnregisterNode(this);
        _worker.Dispose();
    }
}
