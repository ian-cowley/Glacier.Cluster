namespace Glacier.Cluster.Tests.SchedulerTests;

using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Xunit;

public sealed class ExecutionDagTests
{
    [Fact]
    public void TaskState_Transitions_Follow_Strict_Lifecycle()
    {
        var jobId = Guid.NewGuid();
        var task = new TaskDescriptor(
            jobId,
            stageId: 1,
            partitionId: 0,
            StageOperatorType.Scan,
            operatorPayload: new byte[] { 1, 2, 3 });

        Assert.Equal(TaskState.Pending, task.State);

        var workerId = Guid.NewGuid();
        task.MarkScheduled(workerId);
        Assert.Equal(TaskState.Scheduled, task.State);
        Assert.Equal(workerId, task.AssignedNodeId);
        Assert.NotNull(task.ScheduledAt);

        task.MarkRunning();
        Assert.Equal(TaskState.Running, task.State);
        Assert.NotNull(task.StartedAt);

        task.MarkCompleted(elapsedMs: 42.5, outputBytes: 1024, outputRowCount: 100);
        Assert.Equal(TaskState.Completed, task.State);
        Assert.Equal(42.5, task.ElapsedMs);
        Assert.Equal(1024UL, task.OutputBytes);
        Assert.Equal(100UL, task.OutputRowCount);
        Assert.NotNull(task.CompletedAt);

        // Invalid transition from Completed throws
        Assert.Throws<InvalidOperationException>(() => task.MarkRunning());
    }

    [Fact]
    public void TaskState_Retry_Limit_Behavior()
    {
        var task = new TaskDescriptor(
            Guid.NewGuid(),
            stageId: 1,
            partitionId: 0,
            StageOperatorType.Filter,
            operatorPayload: Array.Empty<byte>(),
            requiredGpu: GpuBackendFlags.None)
        {
            MaxAttempts = 3
        };

        // Failure 1 -> Retrying
        bool canRetry1 = task.MarkFailed("Network timeout", out bool retry1);
        Assert.True(canRetry1);
        Assert.True(retry1);
        Assert.Equal(TaskState.Retrying, task.State);
        Assert.Equal(1, task.AttemptCount);
        Assert.Null(task.AssignedNodeId);

        // Reschedule and Failure 2 -> Retrying
        task.MarkScheduled(Guid.NewGuid());
        task.MarkRunning();
        bool canRetry2 = task.MarkFailed("OOM error", out bool retry2);
        Assert.True(canRetry2);
        Assert.True(retry2);
        Assert.Equal(TaskState.Retrying, task.State);
        Assert.Equal(2, task.AttemptCount);

        // Reschedule and Failure 3 -> Failed (exhausted)
        task.MarkScheduled(Guid.NewGuid());
        task.MarkRunning();
        bool canRetry3 = task.MarkFailed("Disk full", out bool retry3);
        Assert.False(canRetry3);
        Assert.False(retry3);
        Assert.Equal(TaskState.Failed, task.State);
        Assert.Equal(3, task.AttemptCount);
    }

    [Fact]
    public void TaskDescriptor_Blits_To_Wire_StageDispatchMessage()
    {
        var jobId = Guid.NewGuid();
        byte[] payload = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var upstreams = new[]
        {
            new UpstreamPartitionLocation(Guid.NewGuid(), 0, 0, (ushort)8080, "127.0.0.1")
        };

        var task = new TaskDescriptor(
            jobId,
            stageId: 2,
            partitionId: 5,
            StageOperatorType.GroupByReduce,
            payload,
            upstreamPartitions: upstreams);

        var msg = task.ToDispatchMessage();

        Assert.Equal(ClusterMessageType.StageDispatch, msg.MessageType);
        Assert.Equal(jobId, msg.JobId);
        Assert.Equal(2, msg.StageId);
        Assert.Equal(5, msg.PartitionId);
        Assert.Equal(StageOperatorType.GroupByReduce, msg.OperatorType);
        Assert.True(msg.OperatorPayload.Span.SequenceEqual(payload));
        Assert.Single(msg.UpstreamPartitions);
        Assert.Equal(upstreams[0], msg.UpstreamPartitions[0]);

        // Verify roundtrip serialization
        byte[] buffer = new byte[msg.GetByteCount()];
        int written = msg.Serialize(buffer);
        var deserialized = StageDispatchMessage.Deserialize(buffer.AsSpan(0, written));
        Assert.Equal(msg, deserialized);
    }

    [Fact]
    public void TopologicalSort_KahnAlgorithm_Linear_DAG()
    {
        var jobId = Guid.NewGuid();
        var s0 = new ExecutionStage(0, jobId, "Scan", false, 4, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "Filter", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s2 = new ExecutionStage(2, jobId, "Reduce", true, 2, StageOperatorType.GroupByReduce, Array.Empty<byte>(), parentStageIds: new[] { 1 });

        // Supply in reverse order to ensure Kahn sort orders properly
        var dag = new ExecutionDag(jobId, "LinearJob", new[] { s2, s0, s1 });

        Assert.Equal(3, dag.TopologicalSortOrder.Count);
        Assert.Equal(0, dag.TopologicalSortOrder[0].StageId);
        Assert.Equal(1, dag.TopologicalSortOrder[1].StageId);
        Assert.Equal(2, dag.TopologicalSortOrder[2].StageId);

        Assert.Single(dag.RootStages);
        Assert.Equal(0, dag.RootStages[0].StageId);
        Assert.Single(dag.LeafStages);
        Assert.Equal(2, dag.LeafStages[0].StageId);
    }

    [Fact]
    public void TopologicalSort_KahnAlgorithm_Diamond_DAG()
    {
        // S0 (Root) -> S1 & S2 -> S3 (Leaf)
        var jobId = Guid.NewGuid();
        var s0 = new ExecutionStage(0, jobId, "Scan", false, 4, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "BranchLeft", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s2 = new ExecutionStage(2, jobId, "BranchRight", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s3 = new ExecutionStage(3, jobId, "Join", true, 4, StageOperatorType.HashJoinProbe, Array.Empty<byte>(), parentStageIds: new[] { 1, 2 });

        var dag = new ExecutionDag(jobId, "DiamondJob", new[] { s3, s1, s0, s2 });

        Assert.Equal(4, dag.TopologicalSortOrder.Count);
        Assert.Equal(0, dag.TopologicalSortOrder[0].StageId);
        Assert.Equal(3, dag.TopologicalSortOrder[3].StageId);

        // Stages 1 and 2 must come after 0 and before 3
        int idx1 = dag.TopologicalSortOrder.ToList().FindIndex(s => s.StageId == 1);
        int idx2 = dag.TopologicalSortOrder.ToList().FindIndex(s => s.StageId == 2);
        Assert.True(idx1 > 0 && idx1 < 3);
        Assert.True(idx2 > 0 && idx2 < 3);

        Assert.Single(dag.RootStages);
        Assert.Equal(0, dag.RootStages[0].StageId);
        Assert.Single(dag.LeafStages);
        Assert.Equal(3, dag.LeafStages[0].StageId);
    }

    [Fact]
    public void CycleDetection_Throws_InvalidOperationException()
    {
        var jobId = Guid.NewGuid();
        // S0 -> S1 -> S2 -> S0 (Cycle!)
        var s0 = new ExecutionStage(0, jobId, "S0", false, 2, StageOperatorType.Scan, Array.Empty<byte>(), parentStageIds: new[] { 2 });
        var s1 = new ExecutionStage(1, jobId, "S1", false, 2, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s2 = new ExecutionStage(2, jobId, "S2", false, 2, StageOperatorType.Project, Array.Empty<byte>(), parentStageIds: new[] { 1 });

        var ex = Assert.Throws<InvalidOperationException>(() => new ExecutionDag(jobId, "CyclicJob", new[] { s0, s1, s2 }));
        Assert.Contains("Cycle detected", ex.Message);
    }

    [Fact]
    public void Reactive_Stage_Readiness_Unlocking()
    {
        var jobId = Guid.NewGuid();
        var s0 = new ExecutionStage(0, jobId, "Scan", false, 2, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "Filter", false, 2, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });

        var dag = new ExecutionDag(jobId, "ReactiveJob", new[] { s0, s1 });

        // Initially only s0 is ready
        var readyInitial = dag.GetReadyStages().ToList();
        Assert.Single(readyInitial);
        Assert.Equal(0, readyInitial[0].StageId);

        // Schedulable tasks belong to s0
        var tasks0 = dag.GetSchedulableTasks().ToList();
        Assert.Equal(2, tasks0.Count);
        Assert.All(tasks0, t => Assert.Equal(0, t.StageId));

        // Complete partition 0 of stage 0
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 0, TaskExecutionStatus.Running, 10, 50, 5));
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 0, TaskExecutionStatus.Completed, 20, 100, 10));

        // Stage 0 is still running (partition 1 not done yet); Stage 1 remains pending
        Assert.Single(dag.GetReadyStages());

        // Complete partition 1 of stage 0
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 1, TaskExecutionStatus.Running, 10, 50, 5));
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 1, TaskExecutionStatus.Completed, 25, 120, 12));

        // Stage 0 is completed! Now Stage 1 unlocks and becomes ready!
        var readyNext = dag.GetReadyStages().ToList();
        Assert.Single(readyNext);
        Assert.Equal(1, readyNext[0].StageId);

        var tasks1 = dag.GetSchedulableTasks().ToList();
        Assert.Equal(2, tasks1.Count);
        Assert.All(tasks1, t => Assert.Equal(1, t.StageId));

        // Complete stage 1
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, 0, TaskExecutionStatus.Completed, 5, 50, 10));
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, 1, TaskExecutionStatus.Completed, 5, 50, 10));

        Assert.True(dag.IsJobCompleted);
    }

    [Fact]
    public void Partition_Lineage_Invalidation_And_Recovery()
    {
        var jobId = Guid.NewGuid();
        // S0 (Narrow) -> S1 (Narrow) -> S2 (Wide Shuffle)
        var s0 = new ExecutionStage(0, jobId, "Scan", false, 4, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "Filter", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s2 = new ExecutionStage(2, jobId, "Reduce", true, 2, StageOperatorType.GroupByReduce, Array.Empty<byte>(), parentStageIds: new[] { 1 });

        var dag = new ExecutionDag(jobId, "LineageJob", new[] { s0, s1, s2 });

        // Complete all tasks in all stages
        for (int p = 0; p < 4; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, p, TaskExecutionStatus.Completed, 10, 100, 10));
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }
        for (int p = 0; p < 2; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 2, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }

        Assert.True(dag.IsJobCompleted);

        // Node hosting S0 partition 2 fails! Invalidate S0 partition 2:
        dag.InvalidateStagePartition(stageId: 0, partitionId: 2);

        // Stage 0 partition 2 was reset to Pending
        Assert.Equal(TaskState.Pending, s0.Tasks[2].State);
        Assert.Equal(TaskState.Completed, s0.Tasks[0].State); // other partitions remain Completed

        // In Stage 1 (Narrow), partition 2 was also reset to Pending
        Assert.Equal(TaskState.Pending, s1.Tasks[2].State);
        Assert.Equal(TaskState.Completed, s1.Tasks[0].State);

        // In Stage 2 (Wide), ALL partitions depend on S0 partition 2, so all partitions are reset!
        Assert.Equal(TaskState.Pending, s2.Tasks[0].State);
        Assert.Equal(TaskState.Pending, s2.Tasks[1].State);

        Assert.False(dag.IsJobCompleted);

        // Ready stages now reflects that S0 needs to recompute partition 2
        var ready = dag.GetReadyStages().ToList();
        Assert.Single(ready);
        Assert.Equal(0, ready[0].StageId);

        var schedulable = dag.GetSchedulableTasks().ToList();
        Assert.Single(schedulable);
        Assert.Equal(0, schedulable[0].StageId);
        Assert.Equal(2, schedulable[0].PartitionId);
    }

    [Fact]
    public void StageCompiler_Narrow_Pipeline_Fusion()
    {
        var compiler = new StageCompiler();
        var jobId = Guid.NewGuid();

        // Scan -> Filter -> Project (all narrow 1:1)
        var scan = new LogicalScanNode("s3://bucket/table.parquet", partitionCount: 16);
        var filter = new LogicalFilterNode(scan, "age > 30");
        var project = new LogicalProjectNode(filter, new[] { "name", "salary" });

        var dag = compiler.Compile(jobId, "NarrowQuery", project);

        // Should be fused into a single stage!
        Assert.Single(dag.Stages);
        var stage = dag.Stages.Values.Single();
        Assert.Equal(16, stage.PartitionsCount);
        Assert.False(stage.IsShuffleBoundary);
    }

    [Fact]
    public void StageCompiler_Wide_GroupBy_Shuffle_Boundary()
    {
        var compiler = new StageCompiler();
        var jobId = Guid.NewGuid();

        // Scan (8 partitions) -> GroupBy (4 target partitions)
        var scan = new LogicalScanNode("sales.csv", partitionCount: 8);
        var groupBy = new LogicalGroupByNode(
            scan,
            groupKeys: new[] { "dept_id" },
            aggregations: new[] { "SUM(amount)" },
            targetPartitions: 4);

        var dag = compiler.Compile(jobId, "GroupByQuery", groupBy);

        // Compiles into 2 stages: GroupByMap (8 partitions) -> GroupByReduce (4 partitions, shuffle boundary)
        Assert.Equal(2, dag.Stages.Count);

        var mapStage = dag.Stages.Values.First(s => s.PrimaryOperator == StageOperatorType.GroupByMap);
        var reduceStage = dag.Stages.Values.First(s => s.PrimaryOperator == StageOperatorType.GroupByReduce);

        Assert.Equal(8, mapStage.PartitionsCount);
        Assert.False(mapStage.IsShuffleBoundary);

        Assert.Equal(4, reduceStage.PartitionsCount);
        Assert.True(reduceStage.IsShuffleBoundary);
        Assert.Contains(mapStage.StageId, reduceStage.ParentStageIds);
    }

    [Fact]
    public void StageCompiler_Distributed_HashJoin()
    {
        var compiler = new StageCompiler();
        var jobId = Guid.NewGuid();

        // Left Scan (12 partitions) & Right Scan (6 partitions) -> HashJoin (8 target partitions)
        var leftScan = new LogicalScanNode("orders.parquet", partitionCount: 12);
        var rightScan = new LogicalScanNode("customers.parquet", partitionCount: 6);
        var join = new LogicalHashJoinNode(leftScan, rightScan, "customer_id", targetPartitions: 8);

        var dag = compiler.Compile(jobId, "JoinQuery", join);

        // 3 stages: LeftScan + RightScan -> HashJoin (wide shuffle)
        Assert.Equal(3, dag.Stages.Count);

        var joinStage = dag.Stages.Values.First(s => s.PrimaryOperator == StageOperatorType.HashJoinProbe);
        Assert.Equal(8, joinStage.PartitionsCount);
        Assert.True(joinStage.IsShuffleBoundary);
        Assert.Equal(2, joinStage.ParentStageIds.Count);

        Assert.Equal(2, dag.RootStages.Count);
        Assert.Single(dag.LeafStages);
        Assert.Equal(joinStage.StageId, dag.LeafStages[0].StageId);
    }
}
