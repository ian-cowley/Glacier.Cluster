namespace Glacier.Cluster.Simulator;

using System;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.Protocol;

/// <summary>
/// Transport strategy for in-memory virtual shuffle exchanges.
/// </summary>
public enum SimulatorShuffleMode
{
    /// <summary>
    /// Full wire protocol emulation over System.IO.Pipelines.
    /// Encodes frames, CRC32, chunking, credit flow control, and reassembly.
    /// </summary>
    WirePipes = 0,

    /// <summary>
    /// Zero-copy pointer bypass: directly shares RefCountedPartitionBuffers across stores.
    /// Maximum speed for 64-node scale tests.
    /// </summary>
    DirectZeroCopy = 1
}

/// <summary>
/// Strategy for assigning hardware profiles across the simulated fleet.
/// </summary>
public enum TopologyAssignmentStrategy
{
    /// <summary>
    /// Repeats the 4 physical fleet hardware profiles cyclically:
    /// Host Laptop (Zen5, AVX-512, CUDA), Beast (Zen3, AVX2, CUDA),
    /// Fedora Linux (Zen5, AVX-512, Vulkan/ROCm), MinisForum64 (Zen3+, AVX2, Vulkan).
    /// </summary>
    PhysicalFleetCyclic,

    /// <summary>All nodes have identical baseline hardware (8 cores, AVX2, 32GB RAM).</summary>
    Homogeneous,

    /// <summary>25% CUDA GPU, 25% Vulkan APU, 25% AVX-512 high-core CPU, 25% AVX2 low-core CPU.</summary>
    TieredHpc,

    /// <summary>First K nodes have CUDA GPU; remaining nodes are CPU-only.</summary>
    GpuAccelerated,

    /// <summary>Configured via user-supplied lambda.</summary>
    Custom
}

/// <summary>
/// Configuration options for ClusterSimulator.
/// </summary>
public sealed class ClusterSimulatorOptions
{
    /// <summary>Number of virtual worker nodes (4 to 64).</summary>
    public int NodeCount { get; set; } = 8;

    /// <summary>Shuffle transport mode.</summary>
    public SimulatorShuffleMode ShuffleMode { get; set; } = SimulatorShuffleMode.WirePipes;

    /// <summary>Hardware topology assignment strategy.</summary>
    public TopologyAssignmentStrategy TopologyStrategy { get; set; } = TopologyAssignmentStrategy.PhysicalFleetCyclic;

    /// <summary>Custom hardware profile factory when TopologyStrategy is Custom.</summary>
    public Func<int, Guid, NodeHardwareProfile>? CustomHardwareProfileFactory { get; set; }

    /// <summary>Worker heartbeat broadcast interval (low latency for testing).</summary>
    public TimeSpan WorkerHeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Coordinator heartbeat audit check interval.</summary>
    public TimeSpan CoordinatorHeartbeatAuditInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Chunk size for WirePipes shuffle streaming.</summary>
    public uint ShuffleChunkSizeBytes { get; set; } = 65536;

    /// <summary>Coordinator daemon options.</summary>
    public ClusterCoordinatorOptions CoordinatorOptions { get; set; } = new()
    {
        Port = 0, // In-process only, no TCP listen
        HeartbeatAuditInterval = TimeSpan.FromMilliseconds(100)
    };
}
