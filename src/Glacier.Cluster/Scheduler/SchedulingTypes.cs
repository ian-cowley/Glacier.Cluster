namespace Glacier.Cluster.Scheduler;

using System;
using System.Collections.Generic;
using Glacier.Cluster.Protocol;

/// <summary>
/// Health state classification produced by the Adaptive Phi-Accrual Failure Detector.
/// </summary>
public enum NodeHealthState : byte
{
    /// <summary>Node is actively communicating; Phi &lt; 8.0. Eligible for task placement.</summary>
    Healthy = 0,

    /// <summary>Heartbeat delayed; 8.0 &lt;= Phi &lt; 12.0. Deprioritized and disqualified from new task placement.</summary>
    Suspect = 1,

    /// <summary>Heartbeat deadline exceeded; Phi &gt;= 12.0. Node evicted from active cluster membership.</summary>
    Evicted = 2
}

/// <summary>
/// Specific reason a candidate node was disqualified during Phase 1 Hard Predicate Filtering.
/// </summary>
public enum FilterDisqualificationReason : byte
{
    None = 0,
    NodeNotHealthy = 1,
    MissingCudaCapability = 2,
    IncompatibleGpuBackend = 3,
    InsufficientVram = 4,
    InsufficientRam = 5,
    ZeroLogicalCores = 6
}

/// <summary>
/// Abstraction representing a task requesting cluster scheduling.
/// </summary>
public interface ITaskPlacementDescriptor
{
    Guid JobId { get; }
    int StageId { get; }
    int PartitionId { get; }
    Guid TaskId { get; }

    bool RequiresGpu { get; }
    GpuBackendFlags RequiredGpu { get; }
    ulong MinRamBytes { get; }
    ulong MinVramBytes { get; }

    Guid? PreferredNodeId { get; }
    IReadOnlySet<Guid>? LocalDataNodes { get; }
    bool IsPartitionLocalOnNode(Guid nodeId);
}

/// <summary>
/// Default implementation of ITaskPlacementDescriptor.
/// </summary>
public sealed record TaskPlacementRequest : ITaskPlacementDescriptor
{
    public Guid JobId { get; init; }
    public int StageId { get; init; }
    public int PartitionId { get; init; }
    public Guid TaskId { get; init; } = Guid.NewGuid();

    public bool RequiresGpu { get; init; }
    public GpuBackendFlags RequiredGpu { get; init; } = GpuBackendFlags.None;
    public ulong MinRamBytes { get; init; }
    public ulong MinVramBytes { get; init; }

    public Guid? PreferredNodeId { get; init; }
    public IReadOnlySet<Guid>? LocalDataNodes { get; init; }

    public bool IsPartitionLocalOnNode(Guid nodeId)
    {
        if (PreferredNodeId.HasValue && PreferredNodeId.Value == nodeId)
            return true;
        return LocalDataNodes is not null && LocalDataNodes.Contains(nodeId);
    }
}

/// <summary>
/// Configurable weights for Phase 2 Soft Optimization Affinity Scoring.
/// </summary>
public sealed record SchedulerAffinityWeights
{
    public double SimdAvx512Weight { get; init; } = 100.0;
    public double SimdAvx2Weight { get; init; } = 50.0;
    public double SimdAdvSimdWeight { get; init; } = 40.0;

    public double DataLocalityWeight { get; init; } = 200.0;
    public double CpuLoadWeight { get; init; } = 50.0;
    public double RamHeadroomWeight { get; init; } = 20.0;

    public static SchedulerAffinityWeights Default { get; } = new();
}

/// <summary>
/// Outcome of a task scheduling operation.
/// </summary>
public sealed class PlacementDecision
{
    public bool IsSuccess { get; }
    public Guid TaskId { get; }
    public NodeHardwareProfile? SelectedNode { get; }
    public double CompositeScore { get; }
    public IReadOnlyDictionary<Guid, FilterDisqualificationReason> DisqualifiedCandidates { get; }

    private PlacementDecision(
        bool isSuccess,
        Guid taskId,
        NodeHardwareProfile? selectedNode,
        double compositeScore,
        IReadOnlyDictionary<Guid, FilterDisqualificationReason> disqualified)
    {
        IsSuccess = isSuccess;
        TaskId = taskId;
        SelectedNode = selectedNode;
        CompositeScore = compositeScore;
        DisqualifiedCandidates = disqualified;
    }

    public static PlacementDecision Success(
        Guid taskId,
        NodeHardwareProfile selectedNode,
        double score,
        IReadOnlyDictionary<Guid, FilterDisqualificationReason>? disqualified = null) =>
        new(true, taskId, selectedNode, score, disqualified ?? new Dictionary<Guid, FilterDisqualificationReason>());

    public static PlacementDecision Failed(Guid taskId, Dictionary<Guid, FilterDisqualificationReason> disqualified) =>
        new(false, taskId, null, 0.0, disqualified);
}

/// <summary>
/// Contract for failure detection query in scheduling.
/// </summary>
public interface IFailureDetector
{
    void RecordHeartbeat(Guid nodeId, ulong timestampNs);
    double ComputePhi(Guid nodeId, ulong currentTimestampNs = 0);
    NodeHealthState GetHealthState(Guid nodeId, ulong currentTimestampNs = 0);
    bool IsHealthy(Guid nodeId, ulong currentTimestampNs = 0);
    void RegisterNode(Guid nodeId, ulong initialTimestampNs = 0);
    void UnregisterNode(Guid nodeId);
}
