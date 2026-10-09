namespace Glacier.Cluster.Coordinator;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;

/// <summary>
/// State and capability descriptor for a registered cluster worker node.
/// </summary>
public sealed class ClusterWorkerNode
{
    public Guid NodeId { get; }
    public int Rank { get; }
    public string Hostname { get; }
    public EndPoint ControlEndPoint { get; }
    public ushort ShufflePort { get; set; }
    public NodeHardwareProfile HardwareProfile { get; set; }
    public DuplexConnection? Connection { get; set; }
    public NodeHealthState HealthState { get; set; }
    public ulong LastHeartbeatTimestampNs { get; set; }
    public uint ActiveTaskCount { get; set; }
    public float CpuUsagePercentage { get; set; }
    public ulong AvailablePhysicalMemoryBytes { get; set; }
    public DateTimeOffset RegisteredAtUtc { get; }

    public ClusterWorkerNode(
        Guid nodeId,
        int rank,
        string hostname,
        EndPoint controlEndPoint,
        ushort shufflePort,
        NodeHardwareProfile hardwareProfile,
        DuplexConnection? connection = null)
    {
        NodeId = nodeId;
        Rank = rank;
        Hostname = hostname;
        ControlEndPoint = controlEndPoint;
        ShufflePort = shufflePort;
        HardwareProfile = hardwareProfile;
        Connection = connection;
        HealthState = NodeHealthState.Healthy;
        RegisteredAtUtc = DateTimeOffset.UtcNow;
    }
}

/// <summary>
/// Authoritative cluster membership manager allocating contiguous cluster ranks (0..N-1),
/// tracking epochs, maintaining worker hardware directories, and integrating with Phi-Accrual Failure Detection.
/// </summary>
public sealed class MembershipManager : IDisposable
{
    private readonly ConcurrentDictionary<Guid, ClusterWorkerNode> _nodes = new();
    private readonly ConcurrentStack<int> _recycledRanks = new();
    private readonly PhiAccrualFailureDetector _failureDetector;
    private readonly object _rankLock = new();
    private int _highestRankAssigned = -1;
    private ulong _clusterEpoch = 1;
    private int _isDisposed;

    public ulong ClusterEpoch => Volatile.Read(ref _clusterEpoch);
    public int ActiveNodeCount => _nodes.Count(n => n.Value.HealthState != NodeHealthState.Evicted);
    public PhiAccrualFailureDetector FailureDetector => _failureDetector;

    public event Action<ClusterWorkerNode>? WorkerRegistered;
    public event Action<ClusterWorkerNode>? WorkerLeft;
    public event Action<ClusterWorkerNode, NodeHealthState, NodeHealthState>? WorkerHealthChanged;

    public MembershipManager(PhiAccrualFailureDetector? failureDetector = null)
    {
        _failureDetector = failureDetector ?? new PhiAccrualFailureDetector(
            suspectThreshold: 8.0,
            evictionThreshold: 12.0);

        _failureDetector.NodeHealthChanged += OnDetectorHealthChanged;
    }

    /// <summary>
    /// Registers or updates a worker node in the cluster, assigning a contiguous rank and epoch.
    /// </summary>
    public ValueTask<NodeJoinResponseMessage> RegisterWorkerAsync(
        NodeJoinRequestMessage request,
        EndPoint remoteEndPoint,
        ushort shufflePort = 0)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);
        ArgumentNullException.ThrowIfNull(request);

        Guid nodeId = request.HardwareProfile.NodeId != Guid.Empty
            ? request.HardwareProfile.NodeId
            : Guid.NewGuid();

        int rank;
        ulong epoch;

        lock (_rankLock)
        {
            if (_nodes.TryGetValue(nodeId, out var existing) && existing.HealthState != NodeHealthState.Evicted)
            {
                rank = existing.Rank;
                existing.HardwareProfile = request.HardwareProfile;
                existing.HealthState = NodeHealthState.Healthy;
                if (shufflePort > 0) existing.ShufflePort = shufflePort;
            }
            else
            {
                if (!_recycledRanks.TryPop(out rank))
                {
                    rank = ++_highestRankAssigned;
                }

                var workerNode = new ClusterWorkerNode(
                    nodeId,
                    rank,
                    request.Hostname,
                    remoteEndPoint,
                    shufflePort,
                    request.HardwareProfile);

                _nodes[nodeId] = workerNode;
                WorkerRegistered?.Invoke(workerNode);
            }

            epoch = Interlocked.Increment(ref _clusterEpoch);
        }

        _failureDetector.RegisterNode(nodeId);

        var response = new NodeJoinResponseMessage(
            status: NodeJoinStatus.Accepted,
            assignedNodeId: nodeId,
            assignedRank: rank,
            activeClusterSize: ActiveNodeCount,
            clusterEpoch: epoch);

        return ValueTask.FromResult(response);
    }

    /// <summary>
    /// Processes a worker node departure, releasing its rank for recycling.
    /// </summary>
    public ValueTask HandleWorkerLeaveAsync(NodeLeaveMessage message)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);
        ArgumentNullException.ThrowIfNull(message);

        if (_nodes.TryGetValue(message.NodeId, out var node))
        {
            node.HealthState = NodeHealthState.Evicted;
            _failureDetector.UnregisterNode(message.NodeId);
            _recycledRanks.Push(node.Rank);
            Interlocked.Increment(ref _clusterEpoch);
            WorkerLeft?.Invoke(node);
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Records a received heartbeat ping, updating load metrics and failure history.
    /// </summary>
    public void RecordHeartbeat(HeartbeatPingMessage ping)
    {
        if (_nodes.TryGetValue(ping.NodeId, out var node))
        {
            node.LastHeartbeatTimestampNs = ping.TimestampNs;
            node.ActiveTaskCount = ping.ActiveTaskCount;
            node.CpuUsagePercentage = ping.CpuUsagePercentage;
            node.AvailablePhysicalMemoryBytes = ping.AvailablePhysicalMemoryBytes;
            _failureDetector.RecordHeartbeat(ping.NodeId, ping.TimestampNs);
        }
    }

    /// <summary>
    /// Returns hardware profiles for all currently healthy cluster nodes.
    /// </summary>
    public IReadOnlyList<NodeHardwareProfile> GetHealthyHardwareProfiles()
    {
        return _nodes.Values
            .Where(n => n.HealthState == NodeHealthState.Healthy)
            .Select(n => n.HardwareProfile)
            .ToList();
    }

    /// <summary>
    /// Returns active task counts for all currently healthy nodes.
    /// </summary>
    public IReadOnlyDictionary<Guid, uint> GetActiveTaskCounts()
    {
        return _nodes.Values
            .Where(n => n.HealthState == NodeHealthState.Healthy)
            .ToDictionary(n => n.NodeId, n => n.ActiveTaskCount);
    }

    public bool TryGetNode(Guid nodeId, out ClusterWorkerNode? node)
    {
        return _nodes.TryGetValue(nodeId, out node);
    }

    public IReadOnlyList<ClusterWorkerNode> GetAllNodes() => _nodes.Values.ToList();

    private void OnDetectorHealthChanged(Guid nodeId, NodeHealthState oldState, NodeHealthState newState)
    {
        if (_nodes.TryGetValue(nodeId, out var node))
        {
            node.HealthState = newState;
            WorkerHealthChanged?.Invoke(node, oldState, newState);

            if (newState == NodeHealthState.Evicted)
            {
                _recycledRanks.Push(node.Rank);
                Interlocked.Increment(ref _clusterEpoch);
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _failureDetector.NodeHealthChanged -= OnDetectorHealthChanged;
        _nodes.Clear();
    }
}
