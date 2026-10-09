namespace Glacier.Cluster.Scheduler;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Glacier.Cluster.Protocol;

/// <summary>
/// Two-Phase Heterogeneous Task Placement Scheduler.
/// Phase 1: Hard Predicate Constraint Filtering (CUDA, RAM, VRAM, cores, failure detector health).
/// Phase 2: Soft Optimization Affinity Scoring (SIMD capabilities, data locality, idle cores, headroom, tie-breaking).
/// </summary>
public sealed class TwoPhaseScheduler
{
    private readonly SchedulerAffinityWeights _weights;
    private readonly IFailureDetector? _failureDetector;

    public TwoPhaseScheduler(
        SchedulerAffinityWeights? weights = null,
        IFailureDetector? failureDetector = null)
    {
        _weights = weights ?? SchedulerAffinityWeights.Default;
        _failureDetector = failureDetector;
    }

    /// <summary>
    /// Schedules a single task against a set of candidate cluster nodes.
    /// </summary>
    public PlacementDecision ScheduleTask(
        ITaskPlacementDescriptor task,
        IReadOnlyList<NodeHardwareProfile> candidateNodes,
        IReadOnlyDictionary<Guid, uint>? nodeActiveTasks = null,
        ulong currentTimestampNs = 0)
    {
        ArgumentNullException.ThrowIfNull(task);
        ArgumentNullException.ThrowIfNull(candidateNodes);

        if (candidateNodes.Count == 0)
        {
            return PlacementDecision.Failed(task.TaskId, new Dictionary<Guid, FilterDisqualificationReason>());
        }

        var disqualified = new Dictionary<Guid, FilterDisqualificationReason>();
        Span<int> feasibleIndices = stackalloc int[candidateNodes.Count];
        int feasibleCount = 0;

        // =====================================================================
        // Phase 1: Hard Predicate Constraint Filtering
        // =====================================================================
        for (int i = 0; i < candidateNodes.Count; i++)
        {
            NodeHardwareProfile node = candidateNodes[i];
            if (EvaluateHardPredicates(task, in node, currentTimestampNs, out FilterDisqualificationReason reason))
            {
                feasibleIndices[feasibleCount++] = i;
            }
            else
            {
                disqualified[node.NodeId] = reason;
            }
        }

        if (feasibleCount == 0)
        {
            return PlacementDecision.Failed(task.TaskId, disqualified);
        }

        // =====================================================================
        // Phase 2: Soft Optimization Affinity Scoring
        // =====================================================================
        int bestIndex = feasibleIndices[0];
        double bestScore = double.NegativeInfinity;

        for (int k = 0; k < feasibleCount; k++)
        {
            int idx = feasibleIndices[k];
            NodeHardwareProfile candidate = candidateNodes[idx];

            uint activeTasks = 0;
            if (nodeActiveTasks is not null && nodeActiveTasks.TryGetValue(candidate.NodeId, out uint count))
            {
                activeTasks = count;
            }

            double score = ComputeAffinityScore(task, in candidate, activeTasks);

            if (score > bestScore)
            {
                bestScore = score;
                bestIndex = idx;
            }
            else if (Math.Abs(score - bestScore) < 1e-9)
            {
                // Deterministic Tie-Breaking
                NodeHardwareProfile currentBest = candidateNodes[bestIndex];
                if (candidate.RamAvailableBytes > currentBest.RamAvailableBytes)
                {
                    bestScore = score;
                    bestIndex = idx;
                }
                else if (candidate.RamAvailableBytes == currentBest.RamAvailableBytes &&
                         candidate.NodeId.CompareTo(currentBest.NodeId) < 0)
                {
                    bestScore = score;
                    bestIndex = idx;
                }
            }
        }

        return PlacementDecision.Success(task.TaskId, candidateNodes[bestIndex], bestScore, disqualified);
    }

    /// <summary>
    /// Schedules an entire batch of stage tasks across candidate nodes, dynamically tracking virtual task load.
    /// </summary>
    public IReadOnlyList<PlacementDecision> ScheduleStageTasks(
        IReadOnlyList<ITaskPlacementDescriptor> tasks,
        IReadOnlyList<NodeHardwareProfile> candidateNodes,
        ulong currentTimestampNs = 0)
    {
        ArgumentNullException.ThrowIfNull(tasks);
        ArgumentNullException.ThrowIfNull(candidateNodes);

        var decisions = new PlacementDecision[tasks.Count];
        var virtualLoads = new Dictionary<Guid, uint>();

        for (int i = 0; i < tasks.Count; i++)
        {
            PlacementDecision decision = ScheduleTask(tasks[i], candidateNodes, virtualLoads, currentTimestampNs);
            decisions[i] = decision;

            if (decision.IsSuccess && decision.SelectedNode.HasValue)
            {
                Guid nid = decision.SelectedNode.Value.NodeId;
                virtualLoads[nid] = virtualLoads.GetValueOrDefault(nid) + 1;
            }
        }

        return decisions;
    }

    // =========================================================================
    // Phase 1 Hard Predicate Evaluation
    // =========================================================================

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool EvaluateHardPredicates(
        ITaskPlacementDescriptor task,
        in NodeHardwareProfile node,
        ulong currentTimestampNs,
        out FilterDisqualificationReason reason)
    {
        // 1. Failure Detector Health Predicate
        if (_failureDetector is not null)
        {
            NodeHealthState health = _failureDetector.GetHealthState(node.NodeId, currentTimestampNs);
            if (health != NodeHealthState.Healthy)
            {
                reason = FilterDisqualificationReason.NodeNotHealthy;
                return false;
            }
        }

        // 2. Hardware Acceleration / GPU Backend Predicate
        if (task.RequiresGpu)
        {
            // If task requires CUDA
            if (task.RequiredGpu == GpuBackendFlags.None || (task.RequiredGpu & GpuBackendFlags.Cuda) != 0)
            {
                if (!node.HasCuda)
                {
                    reason = FilterDisqualificationReason.MissingCudaCapability;
                    return false;
                }
            }
            else if ((node.GpuFlags & task.RequiredGpu) == 0)
            {
                reason = FilterDisqualificationReason.IncompatibleGpuBackend;
                return false;
            }

            // VRAM Check
            if (task.MinVramBytes > 0 && node.VramAvailableBytes < task.MinVramBytes)
            {
                reason = FilterDisqualificationReason.InsufficientVram;
                return false;
            }
        }

        // 3. RAM Capacity Predicate
        if (task.MinRamBytes > 0 && node.RamAvailableBytes < task.MinRamBytes)
        {
            reason = FilterDisqualificationReason.InsufficientRam;
            return false;
        }

        // 4. Logical CPU Cores Predicate
        if (node.CpuLogicalCores == 0)
        {
            reason = FilterDisqualificationReason.ZeroLogicalCores;
            return false;
        }

        reason = FilterDisqualificationReason.None;
        return true;
    }

    // =========================================================================
    // Phase 2 Soft Optimization Affinity Scoring
    // =========================================================================

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public double ComputeAffinityScore(
        ITaskPlacementDescriptor task,
        in NodeHardwareProfile node,
        uint activeTasks)
    {
        double score = 0.0;

        // 1. SIMD Vector Capability Weighting
        if (node.HasAvx512)
        {
            score += _weights.SimdAvx512Weight;
        }
        else if (node.HasAvx2)
        {
            score += _weights.SimdAvx2Weight;
        }
        else if (node.SimdFlags.HasAdvSimd())
        {
            score += _weights.SimdAdvSimdWeight;
        }

        // 2. Data Locality Weighting (+200 if partition resident on node)
        if (task.IsPartitionLocalOnNode(node.NodeId))
        {
            score += _weights.DataLocalityWeight;
        }

        // 3. CPU Core Availability Weighting (prioritizes idle cores)
        if (node.CpuLogicalCores > 0)
        {
            double idleRatio = Math.Max(0.0, 1.0 - ((double)activeTasks / node.CpuLogicalCores));
            score += _weights.CpuLoadWeight * idleRatio;
        }

        // 4. RAM Headroom Weighting
        if (node.RamTotalBytes > 0)
        {
            double ramHeadroom = Math.Clamp((double)node.RamAvailableBytes / node.RamTotalBytes, 0.0, 1.0);
            score += _weights.RamHeadroomWeight * ramHeadroom;
        }

        return score;
    }
}
