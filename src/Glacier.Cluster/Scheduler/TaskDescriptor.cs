namespace Glacier.Cluster.Scheduler;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Glacier.Cluster.Protocol;

/// <summary>
/// Immutable specification and mutable runtime tracking for an individual partition task.
/// </summary>
public sealed class TaskDescriptor : ITaskPlacementDescriptor
{
    // --- Identification ---
    public Guid JobId { get; }
    public int StageId { get; }
    public int PartitionId { get; }

    /// <summary>Bit-packed 64-bit integer identifier: ((long)StageId &lt;&lt; 32) | (uint)PartitionId.</summary>
    public long TaskId { get; }

    /// <summary>Globally unique 128-bit GUID for cluster placement tracking.</summary>
    public Guid TaskGuid { get; } = Guid.NewGuid();

    Guid ITaskPlacementDescriptor.TaskId => TaskGuid;

    // --- Hardware & Placement Constraints ---
    public GpuBackendFlags RequiredGpu { get; init; }
    public bool RequiresGpu => RequiredGpu != GpuBackendFlags.None;
    public ulong MinRamBytes { get; init; }
    public ulong MinVramBytes { get; init; }
    public Guid? PreferredNodeId { get; set; }
    public IReadOnlySet<Guid>? LocalDataNodes { get; set; }

    public bool IsPartitionLocalOnNode(Guid nodeId)
    {
        if (PreferredNodeId.HasValue && PreferredNodeId.Value == nodeId)
            return true;
        return LocalDataNodes is not null && LocalDataNodes.Contains(nodeId);
    }

    // --- Execution Operator & Payload ---
    public StageOperatorType TargetOperator { get; }
    public ReadOnlyMemory<byte> OperatorPayload { get; }
    public IReadOnlyList<UpstreamPartitionLocation> UpstreamPartitions { get; set; }

    // --- Runtime Lifecycle State (Thread-Safe) ---
    private volatile int _state;
    public TaskState State => (TaskState)_state;

    public int AttemptCount { get; private set; }
    public const int DefaultMaxAttempts = 3;
    public int MaxAttempts { get; init; } = DefaultMaxAttempts;

    // --- Telemetry & Node Assignment ---
    public Guid? AssignedNodeId { get; private set; }
    public DateTimeOffset? ScheduledAt { get; private set; }
    public DateTimeOffset? StartedAt { get; private set; }
    public DateTimeOffset? CompletedAt { get; private set; }
    public double ElapsedMs { get; private set; }
    public ulong OutputBytes { get; private set; }
    public ulong OutputRowCount { get; private set; }
    public string? ErrorMessage { get; private set; }

    public TaskDescriptor(
        Guid jobId,
        int stageId,
        int partitionId,
        StageOperatorType targetOperator,
        ReadOnlyMemory<byte> operatorPayload,
        GpuBackendFlags requiredGpu = GpuBackendFlags.None,
        ulong minRamBytes = 0,
        ulong minVramBytes = 0,
        Guid? preferredNodeId = null,
        IReadOnlyList<UpstreamPartitionLocation>? upstreamPartitions = null)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
        TaskId = ((long)stageId << 32) | (uint)partitionId;
        TargetOperator = targetOperator;
        OperatorPayload = operatorPayload;
        RequiredGpu = requiredGpu;
        MinRamBytes = minRamBytes;
        MinVramBytes = minVramBytes;
        PreferredNodeId = preferredNodeId;
        UpstreamPartitions = upstreamPartitions ?? Array.Empty<UpstreamPartitionLocation>();
        _state = (int)TaskState.Pending;
        AttemptCount = 0;
    }

    /// <summary>
    /// Atomically transitions task state, returning true if successful.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryTransition(TaskState expectedState, TaskState newState)
    {
        return Interlocked.CompareExchange(ref _state, (int)newState, (int)expectedState) == (int)expectedState;
    }

    public void MarkScheduled(Guid targetNodeId)
    {
        var current = State;
        if (current != TaskState.Pending && current != TaskState.Retrying)
            throw new InvalidOperationException($"Cannot schedule task {TaskId} from state {current}.");

        AssignedNodeId = targetNodeId;
        ScheduledAt = DateTimeOffset.UtcNow;
        _state = (int)TaskState.Scheduled;
    }

    public void MarkRunning()
    {
        var current = State;
        if (current != TaskState.Scheduled)
            throw new InvalidOperationException($"Cannot mark task {TaskId} running from state {current}.");

        StartedAt = DateTimeOffset.UtcNow;
        _state = (int)TaskState.Running;
    }

    public void MarkCompleted(double elapsedMs, ulong outputBytes, ulong outputRowCount)
    {
        var current = State;
        if (current != TaskState.Running && current != TaskState.Scheduled)
            throw new InvalidOperationException($"Cannot complete task {TaskId} from state {current}.");

        CompletedAt = DateTimeOffset.UtcNow;
        ElapsedMs = elapsedMs;
        OutputBytes = outputBytes;
        OutputRowCount = outputRowCount;
        _state = (int)TaskState.Completed;
    }

    public bool MarkFailed(string errorMessage, out bool canRetry)
    {
        ErrorMessage = errorMessage;
        AttemptCount++;
        canRetry = AttemptCount < MaxAttempts;

        if (canRetry)
        {
            _state = (int)TaskState.Retrying;
            AssignedNodeId = null;
        }
        else
        {
            _state = (int)TaskState.Failed;
        }

        return canRetry;
    }

    public void MarkCancelled()
    {
        _state = (int)TaskState.Cancelled;
    }

    /// <summary>
    /// Resets a completed task back to Pending for lineage recomputation when a worker fails.
    /// </summary>
    public void ResetForLineageRecompute()
    {
        _state = (int)TaskState.Pending;
        AssignedNodeId = null;
        ScheduledAt = null;
        StartedAt = null;
        CompletedAt = null;
        ElapsedMs = 0;
        OutputBytes = 0;
        OutputRowCount = 0;
        ErrorMessage = null;
    }

    /// <summary>
    /// Blits this descriptor into a wire-protocol StageDispatchMessage (Opcode 0x10).
    /// </summary>
    public StageDispatchMessage ToDispatchMessage()
    {
        UpstreamPartitionLocation[] upstreams = UpstreamPartitions switch
        {
            UpstreamPartitionLocation[] arr => arr,
            List<UpstreamPartitionLocation> list => list.ToArray(),
            _ => UpstreamPartitions != null ? new List<UpstreamPartitionLocation>(UpstreamPartitions).ToArray() : Array.Empty<UpstreamPartitionLocation>()
        };

        return new StageDispatchMessage(
            JobId,
            StageId,
            PartitionId,
            TargetOperator,
            OperatorPayload,
            upstreams);
    }
}
