namespace Glacier.Cluster.Scheduler;

using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Cluster.Protocol;

/// <summary>
/// Status lifecycle of a distributed execution stage.
/// </summary>
public enum StageState : byte
{
    /// <summary>Waiting for parent stages to complete.</summary>
    Pending = 0,

    /// <summary>All parent stages completed; tasks are schedulable.</summary>
    Ready = 1,

    /// <summary>One or more tasks currently executing.</summary>
    Running = 2,

    /// <summary>All partition tasks finished successfully.</summary>
    Completed = 3,

    /// <summary>One or more partition tasks failed and exhausted retries.</summary>
    Failed = 4,

    /// <summary>Stage execution was cancelled.</summary>
    Cancelled = 5
}

/// <summary>
/// A collection of homogeneous partition tasks bounded by shuffle boundaries.
/// </summary>
public sealed class ExecutionStage
{
    public int StageId { get; }
    public Guid JobId { get; }
    public string StageName { get; }
    public bool IsShuffleBoundary { get; }
    public bool IsWideDependency => IsShuffleBoundary;
    public int PartitionsCount { get; }

    // Graph Topology
    public HashSet<int> ParentStageIds { get; }
    public HashSet<int> ChildStageIds { get; }

    // Partition Tasks
    public TaskDescriptor[] Tasks { get; }
    public StageState StageStatus { get; private set; }

    // Operator Pipeline Metadata
    public StageOperatorType PrimaryOperator { get; }
    public ReadOnlyMemory<byte> OperatorPayload { get; }

    public ExecutionStage(
        int stageId,
        Guid jobId,
        string stageName,
        bool isShuffleBoundary,
        int partitionsCount,
        StageOperatorType primaryOperator,
        ReadOnlyMemory<byte> operatorPayload,
        IEnumerable<int>? parentStageIds = null,
        GpuBackendFlags requiredGpu = GpuBackendFlags.None,
        ulong minRamBytesPerPartition = 0,
        ulong minVramBytesPerPartition = 0)
    {
        if (partitionsCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(partitionsCount), "Partition count must be positive.");

        StageId = stageId;
        JobId = jobId;
        StageName = stageName;
        IsShuffleBoundary = isShuffleBoundary;
        PartitionsCount = partitionsCount;
        PrimaryOperator = primaryOperator;
        OperatorPayload = operatorPayload;

        ParentStageIds = parentStageIds != null ? new HashSet<int>(parentStageIds) : new HashSet<int>();
        ChildStageIds = new HashSet<int>();
        StageStatus = ParentStageIds.Count == 0 ? StageState.Ready : StageState.Pending;

        Tasks = new TaskDescriptor[partitionsCount];
        for (int p = 0; p < partitionsCount; p++)
        {
            Tasks[p] = new TaskDescriptor(
                jobId,
                stageId,
                p,
                primaryOperator,
                operatorPayload,
                requiredGpu,
                minRamBytesPerPartition,
                minVramBytesPerPartition);
        }
    }

    public bool IsCompleted => Tasks.All(t => t.State == TaskState.Completed);

    public int CompletedTasksCount => Tasks.Count(t => t.State == TaskState.Completed);
    public int RunningTasksCount => Tasks.Count(t => t.State == TaskState.Running);
    public int FailedTasksCount => Tasks.Count(t => t.State == TaskState.Failed);

    public void MarkReady()
    {
        if (StageStatus == StageState.Pending)
        {
            StageStatus = StageState.Ready;
        }
    }

    public void MarkRunning()
    {
        if (StageStatus == StageState.Ready || StageStatus == StageState.Pending)
        {
            StageStatus = StageState.Running;
        }
    }

    public void CheckCompletion()
    {
        if (Tasks.Any(t => t.State == TaskState.Failed))
        {
            StageStatus = StageState.Failed;
            return;
        }

        if (Tasks.All(t => t.State == TaskState.Completed))
        {
            StageStatus = StageState.Completed;
            return;
        }

        if (Tasks.Any(t => t.State == TaskState.Running || t.State == TaskState.Scheduled))
        {
            StageStatus = StageState.Running;
        }
    }

    public void InvalidatePartition(int partitionId)
    {
        if (partitionId >= 0 && partitionId < Tasks.Length)
        {
            Tasks[partitionId].ResetForLineageRecompute();
            if (StageStatus == StageState.Completed)
            {
                StageStatus = StageState.Ready;
            }
        }
    }

    public void Cancel()
    {
        StageStatus = StageState.Cancelled;
        foreach (var task in Tasks)
        {
            if (task.State != TaskState.Completed)
            {
                task.MarkCancelled();
            }
        }
    }
}
