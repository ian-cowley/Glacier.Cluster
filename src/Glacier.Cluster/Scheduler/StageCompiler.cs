namespace Glacier.Cluster.Scheduler;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler.LogicalPlan;

/// <summary>
/// Compiles a high-level query plan into a topologically sorted ExecutionDag.
/// Fuses narrow 1:1 operators and introduces shuffle boundaries for wide operations.
/// </summary>
public sealed class StageCompiler
{
    private int _stageIdCounter;

    public ExecutionDag Compile(Guid jobId, string jobName, LogicalPlanNode rootNode, int defaultPartitions = 8)
    {
        _stageIdCounter = 0;
        var stages = new List<ExecutionStage>();

        int _ = CompileNode(jobId, rootNode, stages, defaultPartitions);

        return new ExecutionDag(jobId, jobName, stages);
    }

    private int CompileNode(
        Guid jobId,
        LogicalPlanNode node,
        List<ExecutionStage> stages,
        int defaultPartitions)
    {
        switch (node)
        {
            case LogicalScanNode scan:
            {
                int stageId = _stageIdCounter++;
                byte[] payload = Encoding.UTF8.GetBytes(scan.DataSource);
                var stage = new ExecutionStage(
                    stageId,
                    jobId,
                    $"Scan_{stageId}",
                    isShuffleBoundary: false,
                    partitionsCount: scan.PartitionCount,
                    primaryOperator: StageOperatorType.Scan,
                    operatorPayload: payload);
                stages.Add(stage);
                return stageId;
            }

            case LogicalFilterNode filter:
            {
                int sourceStageId = CompileNode(jobId, filter.Source, stages, defaultPartitions);
                var sourceStage = stages.First(s => s.StageId == sourceStageId);

                if (!sourceStage.IsShuffleBoundary && sourceStage.PrimaryOperator == StageOperatorType.Scan)
                {
                    // Greedy 1:1 operator fusion: fuse filter into scan stage while preserving predicate
                    stages.Remove(sourceStage);
                    string existingPayload = Encoding.UTF8.GetString(sourceStage.OperatorPayload.Span);
                    string combined = existingPayload + "|" + filter.PredicateExpression;
                    byte[] fusedPayload = Encoding.UTF8.GetBytes(combined);

                    var fusedStage = new ExecutionStage(
                        sourceStage.StageId,
                        jobId,
                        $"{sourceStage.StageName}_Filter",
                        isShuffleBoundary: false,
                        partitionsCount: sourceStage.PartitionsCount,
                        primaryOperator: StageOperatorType.Scan,
                        operatorPayload: fusedPayload,
                        parentStageIds: sourceStage.ParentStageIds);
                    stages.Add(fusedStage);
                    return sourceStage.StageId;
                }

                int stageId = _stageIdCounter++;
                byte[] payload = Encoding.UTF8.GetBytes(filter.PredicateExpression);
                var stage = new ExecutionStage(
                    stageId,
                    jobId,
                    $"Filter_{stageId}",
                    isShuffleBoundary: false,
                    partitionsCount: sourceStage.PartitionsCount,
                    primaryOperator: StageOperatorType.Filter,
                    operatorPayload: payload,
                    parentStageIds: new[] { sourceStageId });
                stages.Add(stage);
                return stageId;
            }

            case LogicalProjectNode project:
            {
                int sourceStageId = CompileNode(jobId, project.Source, stages, defaultPartitions);
                var sourceStage = stages.First(s => s.StageId == sourceStageId);

                if (!sourceStage.IsShuffleBoundary)
                {
                    // Greedy 1:1 operator fusion: fuse projection into current narrow pipeline
                    return sourceStageId;
                }

                int stageId = _stageIdCounter++;
                byte[] payload = Encoding.UTF8.GetBytes(string.Join(",", project.Columns));
                var stage = new ExecutionStage(
                    stageId,
                    jobId,
                    $"Project_{stageId}",
                    isShuffleBoundary: false,
                    partitionsCount: sourceStage.PartitionsCount,
                    primaryOperator: StageOperatorType.Project,
                    operatorPayload: payload,
                    parentStageIds: new[] { sourceStageId });
                stages.Add(stage);
                return stageId;
            }

            case LogicalGroupByNode groupBy:
            {
                // 1. Compile upstream source stage
                int sourceStageId = CompileNode(jobId, groupBy.Source, stages, defaultPartitions);
                var sourceStage = stages.First(s => s.StageId == sourceStageId);

                int mapStageId;
                byte[] mapPayload = Encoding.UTF8.GetBytes(string.Join(",", groupBy.GroupKeys));

                if (!sourceStage.IsShuffleBoundary && sourceStage.PrimaryOperator == StageOperatorType.Scan)
                {
                    // Greedy 1:1 operator fusion: fuse GroupByMap into Scan stage
                    mapStageId = sourceStageId;
                    var fusedMapStage = new ExecutionStage(
                        sourceStageId,
                        jobId,
                        $"GroupByMap_{sourceStageId}",
                        isShuffleBoundary: false,
                        partitionsCount: sourceStage.PartitionsCount,
                        primaryOperator: StageOperatorType.GroupByMap,
                        operatorPayload: mapPayload,
                        parentStageIds: sourceStage.ParentStageIds);

                    int idx = stages.FindIndex(s => s.StageId == sourceStageId);
                    stages[idx] = fusedMapStage;
                }
                else
                {
                    mapStageId = _stageIdCounter++;
                    var mapStage = new ExecutionStage(
                        mapStageId,
                        jobId,
                        $"GroupByMap_{mapStageId}",
                        isShuffleBoundary: false,
                        partitionsCount: sourceStage.PartitionsCount,
                        primaryOperator: StageOperatorType.GroupByMap,
                        operatorPayload: mapPayload,
                        parentStageIds: new[] { sourceStageId });
                    stages.Add(mapStage);
                }

                // 3. Stage B: GroupByReduce (Final Merge) - Wide Shuffle Stage
                int reduceStageId = _stageIdCounter++;
                byte[] reducePayload = Encoding.UTF8.GetBytes(string.Join(",", groupBy.Aggregations));
                var reduceStage = new ExecutionStage(
                    reduceStageId,
                    jobId,
                    $"GroupByReduce_{reduceStageId}",
                    isShuffleBoundary: true,
                    partitionsCount: groupBy.TargetPartitions,
                    primaryOperator: StageOperatorType.GroupByReduce,
                    operatorPayload: reducePayload,
                    parentStageIds: new[] { mapStageId });
                stages.Add(reduceStage);

                return reduceStageId;
            }

            case LogicalHashJoinNode join:
            {
                // 1. Compile Left and Right upstream sources
                int leftStageId = CompileNode(jobId, join.LeftSource, stages, defaultPartitions);
                int rightStageId = CompileNode(jobId, join.RightSource, stages, defaultPartitions);

                // 2. Wide Stage: HashJoin (Consumes shuffled partitions from left & right)
                int joinStageId = _stageIdCounter++;
                byte[] joinPayload = Encoding.UTF8.GetBytes(join.JoinKey);
                var joinStage = new ExecutionStage(
                    joinStageId,
                    jobId,
                    $"HashJoin_{joinStageId}",
                    isShuffleBoundary: true,
                    partitionsCount: join.TargetPartitions,
                    primaryOperator: StageOperatorType.HashJoinProbe,
                    operatorPayload: joinPayload,
                    parentStageIds: new[] { leftStageId, rightStageId });
                stages.Add(joinStage);

                return joinStageId;
            }

            default:
                throw new NotSupportedException($"Unsupported logical plan node: {node.GetType().Name}");
        }
    }
}
