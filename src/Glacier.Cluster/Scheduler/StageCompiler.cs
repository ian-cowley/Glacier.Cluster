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
    private readonly bool _defaultAutoRepartitionJoins;
    private int _stageIdCounter;

    public StageCompiler(bool autoRepartitionJoins = false)
    {
        _defaultAutoRepartitionJoins = autoRepartitionJoins;
    }

    public ExecutionDag Compile(
        Guid jobId,
        string jobName,
        LogicalPlanNode rootNode,
        int defaultPartitions = 8,
        bool? autoRepartitionJoins = null)
    {
        _stageIdCounter = 0;
        var stages = new List<ExecutionStage>();
        var stagePartitionKeys = new Dictionary<int, string>();
        bool repartitionJoins = autoRepartitionJoins ?? _defaultAutoRepartitionJoins;

        int _ = CompileNode(jobId, rootNode, stages, defaultPartitions, repartitionJoins, stagePartitionKeys);

        return new ExecutionDag(jobId, jobName, stages);
    }

    private int CompileNode(
        Guid jobId,
        LogicalPlanNode node,
        List<ExecutionStage> stages,
        int defaultPartitions,
        bool autoRepartitionJoins,
        Dictionary<int, string> stagePartitionKeys)
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
                int sourceStageId = CompileNode(jobId, filter.Source, stages, defaultPartitions, autoRepartitionJoins, stagePartitionKeys);
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
                if (stagePartitionKeys.TryGetValue(sourceStageId, out string? srcKeys))
                {
                    stagePartitionKeys[stageId] = srcKeys;
                }
                return stageId;
            }

            case LogicalProjectNode project:
            {
                int sourceStageId = CompileNode(jobId, project.Source, stages, defaultPartitions, autoRepartitionJoins, stagePartitionKeys);
                var sourceStage = stages.First(s => s.StageId == sourceStageId);

                if (!sourceStage.IsShuffleBoundary)
                {
                    // Fuse projection into current narrow pipeline without dropping projection columns
                    stages.Remove(sourceStage);
                    string existingPayload = Encoding.UTF8.GetString(sourceStage.OperatorPayload.Span);
                    string projCols = string.Join(",", project.Columns);
                    string combined = string.IsNullOrEmpty(existingPayload)
                        ? $"PROJECT:{projCols}"
                        : $"{existingPayload}|PROJECT:{projCols}";
                    byte[] fusedPayload = Encoding.UTF8.GetBytes(combined);

                    var fusedStage = new ExecutionStage(
                        sourceStage.StageId,
                        jobId,
                        $"{sourceStage.StageName}_Project",
                        isShuffleBoundary: false,
                        partitionsCount: sourceStage.PartitionsCount,
                        primaryOperator: sourceStage.PrimaryOperator,
                        operatorPayload: fusedPayload,
                        parentStageIds: sourceStage.ParentStageIds);
                    stages.Add(fusedStage);
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
                if (stagePartitionKeys.TryGetValue(sourceStageId, out string? srcKeys))
                {
                    stagePartitionKeys[stageId] = srcKeys;
                }
                return stageId;
            }

            case LogicalGroupByNode groupBy:
            {
                // 1. Compile upstream source stage
                int sourceStageId = CompileNode(jobId, groupBy.Source, stages, defaultPartitions, autoRepartitionJoins, stagePartitionKeys);
                var sourceStage = stages.First(s => s.StageId == sourceStageId);

                int mapStageId;
                string groupKeysStr = string.Join(",", groupBy.GroupKeys);
                byte[] mapPayload = Encoding.UTF8.GetBytes(groupKeysStr);

                if (!sourceStage.IsShuffleBoundary && sourceStage.PrimaryOperator == StageOperatorType.Scan)
                {
                    // Greedy 1:1 operator fusion: fuse GroupByMap into Scan stage while preserving Scan/Filter/Project payload!
                    string existingPayload = Encoding.UTF8.GetString(sourceStage.OperatorPayload.Span);
                    string fusedStr = string.IsNullOrEmpty(existingPayload)
                        ? groupKeysStr
                        : $"{existingPayload}\u001fGROUPBY:{groupKeysStr}";
                    byte[] fusedMapPayload = Encoding.UTF8.GetBytes(fusedStr);

                    mapStageId = sourceStageId;
                    var fusedMapStage = new ExecutionStage(
                        sourceStageId,
                        jobId,
                        $"GroupByMap_{sourceStageId}",
                        isShuffleBoundary: false,
                        partitionsCount: sourceStage.PartitionsCount,
                        primaryOperator: StageOperatorType.GroupByMap,
                        operatorPayload: fusedMapPayload,
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
                stagePartitionKeys[reduceStageId] = groupKeysStr;

                return reduceStageId;
            }

            case LogicalHashJoinNode join:
            {
                // 1. Compile Left and Right upstream sources
                int leftStageId = CompileNode(jobId, join.LeftSource, stages, defaultPartitions, autoRepartitionJoins, stagePartitionKeys);
                int rightStageId = CompileNode(jobId, join.RightSource, stages, defaultPartitions, autoRepartitionJoins, stagePartitionKeys);

                if (autoRepartitionJoins)
                {
                    var leftStage = stages.First(s => s.StageId == leftStageId);
                    if (leftStage.PartitionsCount != join.TargetPartitions ||
                        !stagePartitionKeys.TryGetValue(leftStageId, out string? leftKeys) ||
                        !string.Equals(leftKeys, join.JoinKey, StringComparison.Ordinal))
                    {
                        int repLeftId = _stageIdCounter++;
                        stages.Add(new ExecutionStage(
                            repLeftId,
                            jobId,
                            $"Repartition_{repLeftId}",
                            isShuffleBoundary: true,
                            partitionsCount: join.TargetPartitions,
                            primaryOperator: StageOperatorType.Custom,
                            operatorPayload: Encoding.UTF8.GetBytes($"REPARTITION:{join.TargetPartitions}:{join.JoinKey}"),
                            parentStageIds: new[] { leftStageId }));
                        stagePartitionKeys[repLeftId] = join.JoinKey;
                        leftStageId = repLeftId;
                    }

                    var rightStage = stages.First(s => s.StageId == rightStageId);
                    if (rightStage.PartitionsCount != join.TargetPartitions ||
                        !stagePartitionKeys.TryGetValue(rightStageId, out string? rightKeys) ||
                        !string.Equals(rightKeys, join.JoinKey, StringComparison.Ordinal))
                    {
                        int repRightId = _stageIdCounter++;
                        stages.Add(new ExecutionStage(
                            repRightId,
                            jobId,
                            $"Repartition_{repRightId}",
                            isShuffleBoundary: true,
                            partitionsCount: join.TargetPartitions,
                            primaryOperator: StageOperatorType.Custom,
                            operatorPayload: Encoding.UTF8.GetBytes($"REPARTITION:{join.TargetPartitions}:{join.JoinKey}"),
                            parentStageIds: new[] { rightStageId }));
                        stagePartitionKeys[repRightId] = join.JoinKey;
                        rightStageId = repRightId;
                    }
                }

                // 2. Wide Stage: HashJoin (Consumes co-partitioned or hash-filtered partitions from left & right)
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
                stagePartitionKeys[joinStageId] = join.JoinKey;

                return joinStageId;
            }

            case LogicalRepartitionNode repartition:
            {
                int sourceStageId = CompileNode(jobId, repartition.Source, stages, defaultPartitions, autoRepartitionJoins, stagePartitionKeys);
                int repartitionStageId = _stageIdCounter++;
                string keysStr = string.Join(",", repartition.PartitionKeys);
                byte[] payload = Encoding.UTF8.GetBytes($"REPARTITION:{repartition.TargetPartitions}:{keysStr}");
                var repartitionStage = new ExecutionStage(
                    repartitionStageId,
                    jobId,
                    $"Repartition_{repartitionStageId}",
                    isShuffleBoundary: true,
                    partitionsCount: repartition.TargetPartitions,
                    primaryOperator: StageOperatorType.Custom,
                    operatorPayload: payload,
                    parentStageIds: new[] { sourceStageId });
                stages.Add(repartitionStage);
                if (!string.IsNullOrEmpty(keysStr))
                {
                    stagePartitionKeys[repartitionStageId] = keysStr;
                }
                return repartitionStageId;
            }

            default:
                throw new NotSupportedException($"Unsupported logical plan node: {node.GetType().Name}");
        }
    }
}
