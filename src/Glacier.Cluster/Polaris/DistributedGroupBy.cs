namespace Glacier.Cluster.Polaris;

using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Glacier.Storage.Arrow;

/// <summary>
/// Fluent aggregation builder coordinating two-stage map-shuffle-reduce operations
/// across distributed cluster partitions.
/// </summary>
public sealed class DistributedGroupBy
{
    private readonly DistributedDataFrame _source;
    private readonly string[] _groupKeys;
    private int _targetPartitions;

    internal DistributedGroupBy(DistributedDataFrame source, string[] groupKeys, int targetPartitions = 8)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _groupKeys = groupKeys ?? throw new ArgumentNullException(nameof(groupKeys));
        _targetPartitions = targetPartitions > 0 ? targetPartitions : source.PartitionCount;
    }

    public DistributedGroupBy WithPartitions(int targetPartitions)
    {
        _targetPartitions = targetPartitions;
        return this;
    }

    public DistributedDataFrame Sum(params string[] columns) =>
        BuildAggregate(columns.Select(c => $"SUM({c})").ToArray());

    public DistributedDataFrame Count() =>
        BuildAggregate(new[] { "COUNT(*)" });

    public DistributedDataFrame Mean(params string[] columns) =>
        BuildAggregate(columns.Select(c => $"MEAN({c})").ToArray());

    public DistributedDataFrame Min(params string[] columns) =>
        BuildAggregate(columns.Select(c => $"MIN({c})").ToArray());

    public DistributedDataFrame Max(params string[] columns) =>
        BuildAggregate(columns.Select(c => $"MAX({c})").ToArray());

    public DistributedDataFrame Agg(params string[] aggregateExpressions) =>
        BuildAggregate(aggregateExpressions);

    private DistributedDataFrame BuildAggregate(string[] aggregations)
    {
        var plan = new LogicalGroupByNode(_source.LogicalPlan, _groupKeys, aggregations, _targetPartitions);

        var outputFields = new List<ArrowField>();
        foreach (var key in _groupKeys)
        {
            int idx = _source.Schema.GetFieldIndex(key);
            if (idx >= 0)
            {
                outputFields.Add(_source.Schema.GetField(idx));
            }
            else
            {
                outputFields.Add(new ArrowField(key, ArrowType.Int32));
            }
        }

        foreach (var agg in aggregations)
        {
            outputFields.Add(new ArrowField(agg, ArrowType.Double));
        }

        var outputSchema = new ArrowSchema(outputFields);

        return new DistributedDataFrame(
            Guid.NewGuid(),
            $"{_source.Name}_GroupBy",
            outputSchema,
            _targetPartitions,
            plan,
            _source.ExecutionContext);
    }
}
