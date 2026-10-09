namespace Glacier.Cluster.Polaris;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Polaris.Data;

/// <summary>
/// Execution engine contract coordinating task scheduling and partition data retrieval.
/// </summary>
public interface IDistributedExecutionContext
{
    /// <summary>
    /// Executes an immutable ExecutionDag and returns retained output partition buffers for the leaf stage.
    /// Caller takes ownership of the returned RefCountedPartitionBuffers and must release them.
    /// </summary>
    ValueTask<IReadOnlyList<RefCountedPartitionBuffer>> ExecuteDagAsync(
        ExecutionDag dag,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Evaluates a single partition task in-place.
    /// </summary>
    ValueTask<RefCountedPartitionBuffer> ExecuteTaskAsync(
        TaskDescriptor task,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// In-memory distributed execution context for tests, simulations, and single-node multi-partition execution.
/// Guarantees bit-for-bit parity with distributed multi-worker runs.
/// </summary>
public sealed class InMemoryDistributedExecutionContext : IDistributedExecutionContext, IDisposable
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, List<RefCountedPartitionBuffer>> s_registeredSources = new();
    private readonly List<RefCountedPartitionBuffer> _initialBuffers;
    private readonly List<string> _ownedUris = new();
    private int _isDisposed;

    public InMemoryDistributedExecutionContext(IReadOnlyList<DataFrame> partitions, string? dataSourceUri = null)
    {
        ArgumentNullException.ThrowIfNull(partitions);
        _initialBuffers = new List<RefCountedPartitionBuffer>(partitions.Count);
        foreach (var df in partitions)
        {
            _initialBuffers.Add(PolarisBridge.DataFrameToPartitionBuffer(df));
        }

        if (!string.IsNullOrEmpty(dataSourceUri))
        {
            RegisterSource(dataSourceUri);
        }
    }

    public InMemoryDistributedExecutionContext(IReadOnlyList<RefCountedPartitionBuffer> buffers, string? dataSourceUri = null)
    {
        ArgumentNullException.ThrowIfNull(buffers);
        _initialBuffers = new List<RefCountedPartitionBuffer>(buffers.Count);
        foreach (var b in buffers)
        {
            b.Retain();
            _initialBuffers.Add(b);
        }

        if (!string.IsNullOrEmpty(dataSourceUri))
        {
            RegisterSource(dataSourceUri);
        }
    }

    public void RegisterSource(string uri)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(uri);
        var copyList = new List<RefCountedPartitionBuffer>(_initialBuffers.Count);
        foreach (var b in _initialBuffers)
        {
            b.Retain();
            copyList.Add(b);
        }

        s_registeredSources[uri] = copyList;
        lock (_ownedUris)
        {
            _ownedUris.Add(uri);
        }
    }

    public static void RegisterDataSource(string uri, IReadOnlyList<RefCountedPartitionBuffer> buffers)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(uri);
        ArgumentNullException.ThrowIfNull(buffers);

        var copyList = new List<RefCountedPartitionBuffer>(buffers.Count);
        foreach (var b in buffers)
        {
            b.Retain();
            copyList.Add(b);
        }

        s_registeredSources[uri] = copyList;
    }

    public static bool TryGetSourcePartition(string uri, int partitionId, out RefCountedPartitionBuffer? buffer)
    {
        if (s_registeredSources.TryGetValue(uri, out var list) && partitionId >= 0 && partitionId < list.Count)
        {
            var b = list[partitionId];
            b.Retain();
            buffer = b;
            return true;
        }

        buffer = null;
        return false;
    }

    public ValueTask<RefCountedPartitionBuffer> ExecuteTaskAsync(
        TaskDescriptor task,
        CancellationToken cancellationToken = default)
    {
        int defaultBufferSize = 1024;
        var buffer = RefCountedPartitionBuffer.Allocate(defaultBufferSize);
        return ValueTask.FromResult(buffer);
    }

    public async ValueTask<IReadOnlyList<RefCountedPartitionBuffer>> ExecutePlanAsync(
        Glacier.Cluster.Scheduler.LogicalPlan.LogicalPlanNode node,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_isDisposed != 0, this);
        ArgumentNullException.ThrowIfNull(node);
        cancellationToken.ThrowIfCancellationRequested();

        switch (node)
        {
            case Glacier.Cluster.Scheduler.LogicalPlan.LogicalScanNode scan:
            {
                if (s_registeredSources.TryGetValue(scan.DataSource, out var registered) && registered.Count > 0)
                {
                    var result = new List<RefCountedPartitionBuffer>(registered.Count);
                    foreach (var b in registered)
                    {
                        b.Retain();
                        result.Add(b);
                    }
                    return result;
                }
                else
                {
                    var result = new List<RefCountedPartitionBuffer>(_initialBuffers.Count);
                    foreach (var b in _initialBuffers)
                    {
                        b.Retain();
                        result.Add(b);
                    }
                    return result;
                }
            }

            case Glacier.Cluster.Scheduler.LogicalPlan.LogicalFilterNode filter:
            {
                var sourceBuffers = await ExecutePlanAsync(filter.Source, cancellationToken).ConfigureAwait(false);
                var expr = PredicateCompiler.Compile(filter.PredicateExpression);
                var result = new List<RefCountedPartitionBuffer>(sourceBuffers.Count);

                try
                {
                    foreach (var inBuf in sourceBuffers)
                    {
                        var inDf = PolarisBridge.PartitionBufferToDataFrame(inBuf, zeroCopy: false);
                        var filteredDf = inDf.Filter(expr);
                        if (filteredDf.Columns.Count == 0 && inDf.Columns.Count > 0)
                        {
                            filteredDf = new DataFrame(inDf.Columns.Select(c => c.CloneEmpty(0)).ToList());
                        }
                        result.Add(PolarisBridge.DataFrameToPartitionBuffer(filteredDf));
                    }
                    return result;
                }
                finally
                {
                    foreach (var b in sourceBuffers)
                    {
                        b.Release();
                    }
                }
            }

            case Glacier.Cluster.Scheduler.LogicalPlan.LogicalProjectNode project:
            {
                var sourceBuffers = await ExecutePlanAsync(project.Source, cancellationToken).ConfigureAwait(false);
                var result = new List<RefCountedPartitionBuffer>(sourceBuffers.Count);

                try
                {
                    foreach (var inBuf in sourceBuffers)
                    {
                        var inDf = PolarisBridge.PartitionBufferToDataFrame(inBuf, zeroCopy: false);
                        var projDf = inDf.Select(project.Columns);
                        result.Add(PolarisBridge.DataFrameToPartitionBuffer(projDf));
                    }
                    return result;
                }
                finally
                {
                    foreach (var b in sourceBuffers)
                    {
                        b.Release();
                    }
                }
            }

            case Glacier.Cluster.Scheduler.LogicalPlan.LogicalGroupByNode groupBy:
            {
                var sourceBuffers = await ExecutePlanAsync(groupBy.Source, cancellationToken).ConfigureAwait(false);
                var dfs = new List<DataFrame>(sourceBuffers.Count);

                try
                {
                    foreach (var b in sourceBuffers)
                    {
                        dfs.Add(PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false));
                    }
                }
                finally
                {
                    foreach (var b in sourceBuffers)
                    {
                        b.Release();
                    }
                }

                var combinedDf = dfs.Count == 1 ? dfs[0] : (dfs.All(df => df.RowCount == 0) ? (dfs.FirstOrDefault(df => df.Columns.Count > 0) ?? new DataFrame()) : DataFrame.Concat(dfs));

                var parsedAggs = new List<(string col, string agg)>();
                var renameMap = new List<(string originalResultName, string targetAlias)>();

                foreach (var rawAgg in groupBy.Aggregations)
                {
                    string trimmed = rawAgg.Trim();
                    if (trimmed.StartsWith("SUM(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                    {
                        string col = trimmed.Substring(4, trimmed.Length - 5).Trim();
                        parsedAggs.Add((col, "sum"));
                        renameMap.Add(($"{col}_sum", trimmed));
                    }
                    else if (trimmed.StartsWith("COUNT(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                    {
                        string col = trimmed.Substring(6, trimmed.Length - 7).Trim();
                        if (col == "*" || string.IsNullOrWhiteSpace(col))
                        {
                            col = groupBy.GroupKeys.Length > 0 ? groupBy.GroupKeys[0] : (combinedDf.Columns.Count > 0 ? combinedDf.Columns[0].Name : "count");
                        }
                        parsedAggs.Add((col, "count"));
                        renameMap.Add(($"{col}_count", trimmed));
                    }
                    else if (trimmed.StartsWith("MEAN(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                    {
                        string col = trimmed.Substring(5, trimmed.Length - 6).Trim();
                        parsedAggs.Add((col, "mean"));
                        renameMap.Add(($"{col}_mean", trimmed));
                    }
                    else if (trimmed.StartsWith("MIN(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                    {
                        string col = trimmed.Substring(4, trimmed.Length - 5).Trim();
                        parsedAggs.Add((col, "min"));
                        renameMap.Add(($"{col}_min", trimmed));
                    }
                    else if (trimmed.StartsWith("MAX(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                    {
                        string col = trimmed.Substring(4, trimmed.Length - 5).Trim();
                        parsedAggs.Add((col, "max"));
                        renameMap.Add(($"{col}_max", trimmed));
                    }
                    else
                    {
                        parsedAggs.Add((trimmed, "sum"));
                        renameMap.Add(($"{trimmed}_sum", trimmed));
                    }
                }

                DataFrame reducedDf;
                if (combinedDf.RowCount == 0 || dfs.All(df => df.RowCount == 0))
                {
                    reducedDf = CreateEmptyGroupByDataFrame(dfs, groupBy.GroupKeys, parsedAggs, renameMap);
                }
                else
                {
                    reducedDf = combinedDf.GroupBy(groupBy.GroupKeys).Agg(parsedAggs.ToArray());

                    foreach (var (orig, target) in renameMap)
                    {
                        try
                        {
                            var c = reducedDf.GetColumn(orig);
                            c.Rename(target);
                        }
                        catch
                        {
                        }
                    }
                }

                return new List<RefCountedPartitionBuffer>(1)
                {
                    PolarisBridge.DataFrameToPartitionBuffer(reducedDf)
                };
            }

            case Glacier.Cluster.Scheduler.LogicalPlan.LogicalHashJoinNode join:
            {
                var leftBuffers = await ExecutePlanAsync(join.LeftSource, cancellationToken).ConfigureAwait(false);
                var rightBuffers = await ExecutePlanAsync(join.RightSource, cancellationToken).ConfigureAwait(false);

                var leftDfs = new List<DataFrame>(leftBuffers.Count);
                try
                {
                    foreach (var b in leftBuffers)
                    {
                        leftDfs.Add(PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false));
                    }
                }
                finally
                {
                    foreach (var b in leftBuffers) b.Release();
                }

                var rightDfs = new List<DataFrame>(rightBuffers.Count);
                try
                {
                    foreach (var b in rightBuffers)
                    {
                        rightDfs.Add(PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false));
                    }
                }
                finally
                {
                    foreach (var b in rightBuffers) b.Release();
                }

                var leftDf = leftDfs.Count == 1 ? leftDfs[0] : DataFrame.Concat(leftDfs);
                var rightDf = rightDfs.Count == 1 ? rightDfs[0] : DataFrame.Concat(rightDfs);

                var joinedDf = leftDf.Join(rightDf, join.JoinKey, JoinType.Inner);

                return new List<RefCountedPartitionBuffer>(1)
                {
                    PolarisBridge.DataFrameToPartitionBuffer(joinedDf)
                };
            }

            default:
                throw new NotSupportedException($"Unsupported logical plan node: {node.GetType().Name}");
        }
    }

    public ValueTask<IReadOnlyList<RefCountedPartitionBuffer>> ExecuteDagAsync(
        ExecutionDag dag,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_isDisposed != 0, this);
        ArgumentNullException.ThrowIfNull(dag);

        var stageOutputs = new Dictionary<int, List<RefCountedPartitionBuffer>>();

        try
        {
            foreach (var stage in dag.TopologicalSortOrder)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (stage.PrimaryOperator == StageOperatorType.Scan)
                {
                    string payloadStr = Encoding.UTF8.GetString(stage.OperatorPayload.Span);
                    string[] parts = payloadStr.Split('|');
                    string uri = parts[0];
                    var filterPredicates = parts.Skip(1).Where(p => !string.IsNullOrEmpty(p)).ToList();

                    List<RefCountedPartitionBuffer> buffers;
                    if (!string.IsNullOrEmpty(uri) && s_registeredSources.TryGetValue(uri, out var regList) && regList.Count > 0)
                    {
                        buffers = new List<RefCountedPartitionBuffer>(regList.Count);
                        foreach (var b in regList)
                        {
                            b.Retain();
                            buffers.Add(b);
                        }
                    }
                    else
                    {
                        buffers = new List<RefCountedPartitionBuffer>(_initialBuffers.Count);
                        foreach (var b in _initialBuffers)
                        {
                            b.Retain();
                            buffers.Add(b);
                        }
                    }

                    if (filterPredicates.Count > 0)
                    {
                        for (int i = 0; i < buffers.Count; i++)
                        {
                            var inBuf = buffers[i];
                            var inDf = PolarisBridge.PartitionBufferToDataFrame(inBuf, zeroCopy: false);
                            var originalCols = inDf.Columns;
                            foreach (var pred in filterPredicates)
                            {
                                var expr = PredicateCompiler.Compile(pred);
                                var filtered = inDf.Filter(expr);
                                if (filtered.Columns.Count == 0 && originalCols.Count > 0)
                                {
                                    filtered = new DataFrame(originalCols.Select(c => c.CloneEmpty(0)).ToList());
                                }
                                inDf = filtered;
                            }
                            var outBuf = PolarisBridge.DataFrameToPartitionBuffer(inDf);
                            inBuf.Release();
                            buffers[i] = outBuf;
                        }
                    }

                    stageOutputs[stage.StageId] = buffers;
                }
                else if (stage.PrimaryOperator == StageOperatorType.Filter)
                {
                    int parentId = stage.ParentStageIds.First();
                    var parentBuffers = stageOutputs[parentId];
                    string exprStr = Encoding.UTF8.GetString(stage.OperatorPayload.Span);
                    var expr = PredicateCompiler.Compile(exprStr);

                    var outBuffers = new List<RefCountedPartitionBuffer>(parentBuffers.Count);
                    for (int i = 0; i < parentBuffers.Count; i++)
                    {
                        var inBuf = parentBuffers[i];
                        var inDf = PolarisBridge.PartitionBufferToDataFrame(inBuf, zeroCopy: false);
                        var filteredDf = inDf.Filter(expr);
                        if (filteredDf.Columns.Count == 0 && inDf.Columns.Count > 0)
                        {
                            filteredDf = new DataFrame(inDf.Columns.Select(c => c.CloneEmpty(0)).ToList());
                        }
                        outBuffers.Add(PolarisBridge.DataFrameToPartitionBuffer(filteredDf));
                    }
                    stageOutputs[stage.StageId] = outBuffers;
                }
                else if (stage.PrimaryOperator == StageOperatorType.Project)
                {
                    int parentId = stage.ParentStageIds.First();
                    var parentBuffers = stageOutputs[parentId];
                    string colsStr = Encoding.UTF8.GetString(stage.OperatorPayload.Span);
                    string[] cols = colsStr.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

                    var outBuffers = new List<RefCountedPartitionBuffer>(parentBuffers.Count);
                    for (int i = 0; i < parentBuffers.Count; i++)
                    {
                        var inBuf = parentBuffers[i];
                        var inDf = PolarisBridge.PartitionBufferToDataFrame(inBuf, zeroCopy: false);
                        var projDf = inDf.Select(cols);
                        outBuffers.Add(PolarisBridge.DataFrameToPartitionBuffer(projDf));
                    }
                    stageOutputs[stage.StageId] = outBuffers;
                }
                else if (stage.PrimaryOperator == StageOperatorType.GroupByMap)
                {
                    IReadOnlyList<RefCountedPartitionBuffer> parentBuffers;
                    if (stage.ParentStageIds.Count > 0)
                    {
                        int parentId = stage.ParentStageIds.First();
                        parentBuffers = stageOutputs[parentId];
                    }
                    else
                    {
                        parentBuffers = _initialBuffers;
                    }

                    var outBuffers = new List<RefCountedPartitionBuffer>(parentBuffers.Count);
                    foreach (var b in parentBuffers)
                    {
                        b.Retain();
                        outBuffers.Add(b);
                    }
                    stageOutputs[stage.StageId] = outBuffers;
                }
                else if (stage.PrimaryOperator == StageOperatorType.GroupByReduce)
                {
                    int mapStageId = stage.ParentStageIds.First();
                    var mapBuffers = stageOutputs[mapStageId];
                    var mapStage = dag.Stages[mapStageId];

                    string groupKeysStr = Encoding.UTF8.GetString(mapStage.OperatorPayload.Span);
                    string[] groupKeys = groupKeysStr.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

                    string aggsStr = Encoding.UTF8.GetString(stage.OperatorPayload.Span);
                    string[] aggs = aggsStr.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

                    var dfs = new List<DataFrame>(mapBuffers.Count);
                    foreach (var b in mapBuffers)
                    {
                        dfs.Add(PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false));
                    }
                    var combinedDf = dfs.Count == 1 ? dfs[0] : (dfs.All(df => df.RowCount == 0) ? (dfs.FirstOrDefault(df => df.Columns.Count > 0) ?? new DataFrame()) : DataFrame.Concat(dfs));

                    var parsedAggs = new List<(string col, string agg)>();
                    var renameMap = new List<(string originalResultName, string targetAlias)>();

                    foreach (var rawAgg in aggs)
                    {
                        string trimmed = rawAgg.Trim();
                        if (trimmed.StartsWith("SUM(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                        {
                            string col = trimmed.Substring(4, trimmed.Length - 5).Trim();
                            parsedAggs.Add((col, "sum"));
                            renameMap.Add(($"{col}_sum", trimmed));
                        }
                        else if (trimmed.StartsWith("COUNT(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                        {
                            string col = trimmed.Substring(6, trimmed.Length - 7).Trim();
                            if (col == "*" || string.IsNullOrWhiteSpace(col))
                            {
                                col = groupKeys.Length > 0 ? groupKeys[0] : (combinedDf.Columns.Count > 0 ? combinedDf.Columns[0].Name : "count");
                            }
                            parsedAggs.Add((col, "count"));
                            renameMap.Add(($"{col}_count", trimmed));
                        }
                        else if (trimmed.StartsWith("MEAN(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                        {
                            string col = trimmed.Substring(5, trimmed.Length - 6).Trim();
                            parsedAggs.Add((col, "mean"));
                            renameMap.Add(($"{col}_mean", trimmed));
                        }
                        else if (trimmed.StartsWith("MIN(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                        {
                            string col = trimmed.Substring(4, trimmed.Length - 5).Trim();
                            parsedAggs.Add((col, "min"));
                            renameMap.Add(($"{col}_min", trimmed));
                        }
                        else if (trimmed.StartsWith("MAX(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(')'))
                        {
                            string col = trimmed.Substring(4, trimmed.Length - 5).Trim();
                            parsedAggs.Add((col, "max"));
                            renameMap.Add(($"{col}_max", trimmed));
                        }
                        else
                        {
                            parsedAggs.Add((trimmed, "sum"));
                            renameMap.Add(($"{trimmed}_sum", trimmed));
                        }
                    }

                    DataFrame reducedDf;
                    if (combinedDf.RowCount == 0 || dfs.All(df => df.RowCount == 0))
                    {
                        reducedDf = CreateEmptyGroupByDataFrame(dfs, groupKeys, parsedAggs, renameMap);
                    }
                    else
                    {
                        reducedDf = combinedDf.GroupBy(groupKeys).Agg(parsedAggs.ToArray());

                        foreach (var (orig, target) in renameMap)
                        {
                            try
                            {
                                var c = reducedDf.GetColumn(orig);
                                c.Rename(target);
                            }
                            catch
                            {
                                // ignore if column name already matches
                            }
                        }
                    }

                    var outBuffers = new List<RefCountedPartitionBuffer>(1)
                    {
                        PolarisBridge.DataFrameToPartitionBuffer(reducedDf)
                    };
                    stageOutputs[stage.StageId] = outBuffers;
                }
                else if (stage.PrimaryOperator == StageOperatorType.HashJoinProbe)
                {
                    int leftStageId = stage.ParentStageIds.First();
                    int rightStageId = stage.ParentStageIds.Skip(1).First();
                    string joinKey = Encoding.UTF8.GetString(stage.OperatorPayload.Span);

                    var leftBuffers = stageOutputs[leftStageId];
                    var rightBuffers = stageOutputs[rightStageId];

                    var leftDfs = leftBuffers.Select(b => PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false)).ToList();
                    var rightDfs = rightBuffers.Select(b => PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false)).ToList();

                    var leftDf = leftDfs.Count == 1 ? leftDfs[0] : DataFrame.Concat(leftDfs);
                    var rightDf = rightDfs.Count == 1 ? rightDfs[0] : DataFrame.Concat(rightDfs);

                    var joinedDf = leftDf.Join(rightDf, joinKey, JoinType.Inner);

                    var outBuffers = new List<RefCountedPartitionBuffer>(1)
                    {
                        PolarisBridge.DataFrameToPartitionBuffer(joinedDf)
                    };
                    stageOutputs[stage.StageId] = outBuffers;
                }
            }

            var leafStage = dag.LeafStages[0];
            var result = stageOutputs[leafStage.StageId];

            foreach (var r in result)
            {
                r.Retain();
            }

            return ValueTask.FromResult<IReadOnlyList<RefCountedPartitionBuffer>>(result);
        }
        finally
        {
            foreach (var kvp in stageOutputs)
            {
                foreach (var buf in kvp.Value)
                {
                    buf.Release();
                }
            }
        }
    }

    private static DataFrame CreateEmptyGroupByDataFrame(
        IEnumerable<DataFrame> dfs,
        string[] groupKeys,
        List<(string col, string agg)> parsedAggs,
        List<(string originalResultName, string targetAlias)> renameMap)
    {
        var sourceDf = dfs.FirstOrDefault(d => d.Columns.Count > 0);
        var resultColumns = new List<ISeries>();

        foreach (var keyName in groupKeys)
        {
            if (sourceDf != null && sourceDf.Columns.Any(c => c.Name == keyName))
            {
                resultColumns.Add(sourceDf.GetColumn(keyName).CloneEmpty(0));
            }
            else
            {
                resultColumns.Add(new Utf8StringSeries(keyName, 0));
            }
        }

        for (int i = 0; i < parsedAggs.Count; i++)
        {
            var (col, agg) = parsedAggs[i];
            string targetName = i < renameMap.Count ? renameMap[i].targetAlias : $"{col}_{agg}";

            ISeries? srcCol = null;
            if (sourceDf != null && sourceDf.Columns.Any(c => c.Name == col))
            {
                srcCol = sourceDf.GetColumn(col);
            }

            ISeries aggCol;
            if (agg == "count" || agg == "nunique" || agg == "null_count")
            {
                aggCol = new Int32Series(targetName, 0);
            }
            else if (agg == "sum")
            {
                if (srcCol != null && (srcCol is Float64Series || srcCol is Float32Series))
                {
                    aggCol = new Float64Series(targetName, 0);
                }
                else if (srcCol != null && srcCol is Int64Series)
                {
                    aggCol = new Float64Series(targetName, 0);
                }
                else
                {
                    aggCol = new Int32Series(targetName, 0);
                }
            }
            else if (agg == "mean")
            {
                aggCol = new Float64Series(targetName, 0);
            }
            else if (agg == "min" || agg == "max" || agg == "first" || agg == "last")
            {
                if (srcCol != null)
                {
                    var emptyCol = srcCol.CloneEmpty(0);
                    emptyCol.Rename(targetName);
                    aggCol = emptyCol;
                }
                else
                {
                    aggCol = new Float64Series(targetName, 0);
                }
            }
            else if (agg == "all" || agg == "any")
            {
                aggCol = new BooleanSeries(targetName, 0);
            }
            else
            {
                aggCol = new Float64Series(targetName, 0);
            }

            resultColumns.Add(aggCol);
        }

        return new DataFrame(resultColumns);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;

        lock (_ownedUris)
        {
            foreach (var uri in _ownedUris)
            {
                if (s_registeredSources.TryRemove(uri, out var list))
                {
                    foreach (var b in list)
                    {
                        b.Release();
                    }
                }
            }
            _ownedUris.Clear();
        }

        foreach (var b in _initialBuffers)
        {
            b.Release();
        }
        _initialBuffers.Clear();
    }
}
