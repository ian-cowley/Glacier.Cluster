namespace Glacier.Cluster.Scheduler;

using System;
using Glacier.Cluster.Protocol;

/// <summary>
/// Authoritative state lifecycle of an individual partition execution task within the cluster coordinator.
/// </summary>
public enum TaskState : byte
{
    /// <summary>Task is created and waiting for upstream stage dependencies to satisfy.</summary>
    Pending = 0,

    /// <summary>Task dependencies satisfied; selected by TwoPhaseScheduler and assigned to a node.</summary>
    Scheduled = 1,

    /// <summary>Worker node has acknowledged task dispatch and initiated execution.</summary>
    Running = 2,

    /// <summary>Task has completed successfully and generated valid output partitions.</summary>
    Completed = 3,

    /// <summary>Task execution failed due to an exception, OOM, or network transport error.</summary>
    Failed = 4,

    /// <summary>Task failed but is eligible for retry; awaiting rescheduling by the coordinator.</summary>
    Retrying = 5,

    /// <summary>Task was cancelled due to job abort, downstream stage failure, or timeout.</summary>
    Cancelled = 6
}

/// <summary>
/// Extension methods for converting between internal TaskState and protocol TaskExecutionStatus.
/// </summary>
public static class TaskStateExtensions
{
    /// <summary>
    /// Maps internal scheduler TaskState to wire protocol TaskExecutionStatus.
    /// </summary>
    public static TaskExecutionStatus ToExecutionStatus(this TaskState state) => state switch
    {
        TaskState.Pending => TaskExecutionStatus.Pending,
        TaskState.Scheduled => TaskExecutionStatus.Pending,
        TaskState.Running => TaskExecutionStatus.Running,
        TaskState.Completed => TaskExecutionStatus.Completed,
        TaskState.Failed => TaskExecutionStatus.Failed,
        TaskState.Retrying => TaskExecutionStatus.Pending,
        TaskState.Cancelled => TaskExecutionStatus.Cancelled,
        _ => TaskExecutionStatus.Failed
    };

    /// <summary>
    /// Maps wire protocol TaskExecutionStatus to internal scheduler TaskState.
    /// </summary>
    public static TaskState FromExecutionStatus(TaskExecutionStatus status) => status switch
    {
        TaskExecutionStatus.Pending => TaskState.Pending,
        TaskExecutionStatus.Running => TaskState.Running,
        TaskExecutionStatus.Completed => TaskState.Completed,
        TaskExecutionStatus.Failed => TaskState.Failed,
        TaskExecutionStatus.Cancelled => TaskState.Cancelled,
        _ => TaskState.Failed
    };
}
