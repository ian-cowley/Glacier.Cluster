namespace Glacier.Cluster.Worker;

using System;
using System.Net;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Shuffle;

public sealed class ClusterWorkerOptions
{
    public EndPoint CoordinatorEndPoint { get; init; } = new IPEndPoint(IPAddress.Loopback, 9000);
    public Guid? NodeId { get; init; }
    public ushort ShufflePort { get; init; } = 0; // 0 = dynamic ephemeral port
    public string Hostname { get; init; } = Environment.MachineName;
    public string ClusterSecret { get; init; } = string.Empty;
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(500);
    public int MaxConcurrentTasks { get; init; } = 0; // 0 = logical core count
    public string SpillDirectory { get; init; } = string.Empty;

    /// <summary>
    /// Optional explicit hardware profile override. If null, probes local machine hardware.
    /// </summary>
    public NodeHardwareProfile? HardwareProfile { get; init; }

    /// <summary>
    /// If true, does not bind physical TCP socket listener for ArrowShuffleServer.
    /// Enables in-process duplex pipe operation for 4-to-64 node simulator.
    /// </summary>
    public bool DisableSocketListener { get; init; }

    /// <summary>
    /// Optional custom shuffle client (e.g. backed by InMemoryShuffleBus or FaultInjectingShuffleClient).
    /// </summary>
    public IArrowShuffleClient? ShuffleClient { get; init; }
}
