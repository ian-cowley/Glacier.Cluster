namespace Glacier.Cluster.Scheduler.LogicalPlan;

using System;
using System.Collections.Generic;

/// <summary>
/// Abstract base class for nodes in a distributed logical query plan.
/// </summary>
public abstract class LogicalPlanNode
{
    public abstract string Name { get; }
    public abstract IReadOnlyList<LogicalPlanNode> Children { get; }
    public abstract bool IsWideBoundary { get; }
}

/// <summary>
/// Data source table/file scan node with fixed partition count.
/// </summary>
public sealed class LogicalScanNode : LogicalPlanNode
{
    public override string Name => "Scan";
    public string DataSource { get; }
    public int PartitionCount { get; }
    public override IReadOnlyList<LogicalPlanNode> Children => Array.Empty<LogicalPlanNode>();
    public override bool IsWideBoundary => false;

    public LogicalScanNode(string dataSource, int partitionCount)
    {
        DataSource = dataSource;
        PartitionCount = partitionCount;
    }
}

/// <summary>
/// Row predicate filtering node (narrow dependency).
/// </summary>
public sealed class LogicalFilterNode : LogicalPlanNode
{
    public override string Name => "Filter";
    public LogicalPlanNode Source { get; }
    public string PredicateExpression { get; }
    public override IReadOnlyList<LogicalPlanNode> Children => new[] { Source };
    public override bool IsWideBoundary => false;

    public LogicalFilterNode(LogicalPlanNode source, string predicateExpression)
    {
        Source = source;
        PredicateExpression = predicateExpression;
    }
}

/// <summary>
/// Column projection / selection node (narrow dependency).
/// </summary>
public sealed class LogicalProjectNode : LogicalPlanNode
{
    public override string Name => "Project";
    public LogicalPlanNode Source { get; }
    public string[] Columns { get; }
    public override IReadOnlyList<LogicalPlanNode> Children => new[] { Source };
    public override bool IsWideBoundary => false;

    public LogicalProjectNode(LogicalPlanNode source, string[] columns)
    {
        Source = source;
        Columns = columns;
    }
}

/// <summary>
/// GroupBy aggregation node (wide shuffle boundary).
/// </summary>
public sealed class LogicalGroupByNode : LogicalPlanNode
{
    public override string Name => "GroupBy";
    public LogicalPlanNode Source { get; }
    public string[] GroupKeys { get; }
    public string[] Aggregations { get; }
    public int TargetPartitions { get; }
    public override IReadOnlyList<LogicalPlanNode> Children => new[] { Source };
    public override bool IsWideBoundary => true;

    public LogicalGroupByNode(LogicalPlanNode source, string[] groupKeys, string[] aggregations, int targetPartitions = 8)
    {
        Source = source;
        GroupKeys = groupKeys;
        Aggregations = aggregations;
        TargetPartitions = targetPartitions;
    }
}

/// <summary>
/// Distributed hash join node across two input branches (wide shuffle boundary).
/// </summary>
public sealed class LogicalHashJoinNode : LogicalPlanNode
{
    public override string Name => "HashJoin";
    public LogicalPlanNode LeftSource { get; }
    public LogicalPlanNode RightSource { get; }
    public string JoinKey { get; }
    public int TargetPartitions { get; }
    public override IReadOnlyList<LogicalPlanNode> Children => new[] { LeftSource, RightSource };
    public override bool IsWideBoundary => true;

    public LogicalHashJoinNode(LogicalPlanNode leftSource, LogicalPlanNode rightSource, string joinKey, int targetPartitions = 8)
    {
        LeftSource = leftSource;
        RightSource = rightSource;
        JoinKey = joinKey;
        TargetPartitions = targetPartitions;
    }
}
