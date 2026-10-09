namespace Glacier.Cluster.Scheduler;

using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Cluster.Protocol;

/// <summary>
/// Directed Acyclic Graph (DAG) of execution stages for a distributed job.
/// Implements Kahn's algorithm for topological sorting, cycle detection, and lineage fault recovery.
/// </summary>
public sealed class ExecutionDag
{
    public Guid JobId { get; }
    public string JobName { get; }

    private readonly Dictionary<int, ExecutionStage> _stages;
    public IReadOnlyDictionary<int, ExecutionStage> Stages => _stages;

    public IReadOnlyList<ExecutionStage> RootStages { get; }
    public IReadOnlyList<ExecutionStage> LeafStages { get; }
    public IReadOnlyList<ExecutionStage> TopologicalSortOrder { get; }

    public ExecutionDag(Guid jobId, string jobName, IEnumerable<ExecutionStage> stages)
    {
        JobId = jobId;
        JobName = jobName ?? string.Empty;
        _stages = stages.ToDictionary(s => s.StageId);

        if (_stages.Count == 0)
            throw new ArgumentException("Execution DAG must contain at least one stage.", nameof(stages));

        // Connect child stage links from parent links
        foreach (var stage in _stages.Values)
        {
            foreach (var parentId in stage.ParentStageIds)
            {
                if (!_stages.TryGetValue(parentId, out var parentStage))
                {
                    throw new InvalidOperationException(
                        $"Stage {stage.StageId} references non-existent parent stage {parentId}.");
                }
                parentStage.ChildStageIds.Add(stage.StageId);
            }
        }

        // Validate acyclicity and compute topological sort using Kahn's algorithm
        TopologicalSortOrder = ComputeTopologicalSort();

        // Identify Root Stages (In-Degree = 0) and Leaf Stages (Out-Degree = 0)
        RootStages = _stages.Values.Where(s => s.ParentStageIds.Count == 0).ToList();
        LeafStages = _stages.Values.Where(s => s.ChildStageIds.Count == 0).ToList();
    }

    private List<ExecutionStage> ComputeTopologicalSort()
    {
        var inDegree = _stages.Values.ToDictionary(s => s.StageId, s => s.ParentStageIds.Count);
        var zeroInDegreeQueue = new Queue<int>(_stages.Values.Where(s => s.ParentStageIds.Count == 0).Select(s => s.StageId));
        var result = new List<ExecutionStage>(_stages.Count);

        while (zeroInDegreeQueue.Count > 0)
        {
            int currentId = zeroInDegreeQueue.Dequeue();
            var currentStage = _stages[currentId];
            result.Add(currentStage);

            foreach (var childId in currentStage.ChildStageIds)
            {
                inDegree[childId]--;
                if (inDegree[childId] == 0)
                {
                    zeroInDegreeQueue.Enqueue(childId);
                }
            }
        }

        if (result.Count != _stages.Count)
        {
            throw new InvalidOperationException("Cycle detected in Execution DAG. Execution graph must be strictly acyclic.");
        }

        return result;
    }

    /// <summary>
    /// Returns stages that are ready to execute (all parents Completed, stage currently Ready or Pending).
    /// </summary>
    public IEnumerable<ExecutionStage> GetReadyStages()
    {
        foreach (var stage in TopologicalSortOrder)
        {
            if (stage.StageStatus == StageState.Completed ||
                stage.StageStatus == StageState.Failed ||
                stage.StageStatus == StageState.Cancelled)
            {
                continue;
            }

            bool allParentsCompleted = stage.ParentStageIds.Count == 0 ||
                stage.ParentStageIds.All(pId => _stages[pId].StageStatus == StageState.Completed);

            if (allParentsCompleted)
            {
                stage.MarkReady();
                yield return stage;
            }
        }
    }

    /// <summary>
    /// Returns all tasks across ready stages that are currently in Pending or Retrying state.
    /// </summary>
    public IEnumerable<TaskDescriptor> GetSchedulableTasks()
    {
        foreach (var stage in GetReadyStages())
        {
            foreach (var task in stage.Tasks)
            {
                if (task.State == TaskState.Pending || task.State == TaskState.Retrying)
                {
                    yield return task;
                }
            }
        }
    }

    /// <summary>
    /// Updates task state from a received TaskStatusUpdateMessage, checks stage completion,
    /// and unlocks child stages.
    /// </summary>
    public void HandleTaskStatusUpdate(TaskStatusUpdateMessage update)
    {
        if (!_stages.TryGetValue(update.StageId, out var stage))
            return;

        if (update.PartitionId < 0 || update.PartitionId >= stage.Tasks.Length)
            return;

        var task = stage.Tasks[update.PartitionId];

        switch (update.Status)
        {
            case TaskExecutionStatus.Running:
                if (task.State == TaskState.Pending || task.State == TaskState.Retrying)
                {
                    task.MarkScheduled(Guid.Empty);
                }
                if (task.State == TaskState.Scheduled)
                {
                    task.MarkRunning();
                }
                stage.MarkRunning();
                break;

            case TaskExecutionStatus.Completed:
                if (task.State == TaskState.Pending || task.State == TaskState.Retrying)
                {
                    task.MarkScheduled(Guid.Empty);
                }
                if (task.State == TaskState.Scheduled)
                {
                    task.MarkRunning();
                }
                task.MarkCompleted(update.ElapsedMs, update.OutputBytes, update.OutputRowCount);
                stage.CheckCompletion();
                break;

            case TaskExecutionStatus.Failed:
                task.MarkFailed(update.ErrorMessage, out _);
                stage.CheckCompletion();
                break;

            case TaskExecutionStatus.Cancelled:
                task.MarkCancelled();
                stage.CheckCompletion();
                break;
        }
    }

    /// <summary>
    /// Resets failed partition and its upstream lineage if intermediate shuffle data was lost on node failure.
    /// </summary>
    public void InvalidateStagePartition(int stageId, int partitionId)
    {
        if (_stages.TryGetValue(stageId, out var stage))
        {
            stage.InvalidatePartition(partitionId);

            // Recursively invalidate downstream child stages that depend on this partition
            foreach (var childId in stage.ChildStageIds)
            {
                if (_stages.TryGetValue(childId, out var childStage))
                {
                    if (!childStage.IsShuffleBoundary)
                    {
                        // For narrow dependency, invalidate identical partition
                        InvalidateStagePartition(childId, partitionId);
                    }
                    else
                    {
                        // For wide dependency, all downstream partitions consume this upstream partition
                        for (int p = 0; p < childStage.PartitionsCount; p++)
                        {
                            InvalidateStagePartition(childId, p);
                        }
                    }
                }
            }
        }
    }

    public bool IsJobCompleted => LeafStages.All(s => s.StageStatus == StageState.Completed);
    public bool IsJobFailed => _stages.Values.Any(s => s.StageStatus == StageState.Failed);
}
