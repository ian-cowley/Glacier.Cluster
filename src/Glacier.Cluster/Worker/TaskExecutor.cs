namespace Glacier.Cluster.Worker;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Polaris.Data;

/// <summary>
/// Concurrency-gated task execution engine supporting Scan, Filter, Project, PartialGroupBy,
/// HashJoinProbe, and custom operators across cluster workers.
/// </summary>
public sealed class TaskExecutor : IDisposable
{
    private readonly Guid _localNodeId;
    private readonly LocalPartitionStore _partitionStore;
    private readonly IArrowShuffleClient _shuffleClient;
    private readonly SemaphoreSlim _concurrencySemaphore;
    private int _activeTaskCount;
    private int _isDisposed;

    public int ActiveTaskCount => Volatile.Read(ref _activeTaskCount);
    public Guid LocalNodeId => _localNodeId;

    public TaskExecutor(
        Guid localNodeId,
        LocalPartitionStore partitionStore,
        IArrowShuffleClient shuffleClient,
        int maxConcurrency = 0)
    {
        _localNodeId = localNodeId;
        _partitionStore = partitionStore ?? throw new ArgumentNullException(nameof(partitionStore));
        _shuffleClient = shuffleClient ?? throw new ArgumentNullException(nameof(shuffleClient));
        _concurrencySemaphore = new SemaphoreSlim(maxConcurrency > 0 ? maxConcurrency : Environment.ProcessorCount);
    }

    public async Task<TaskStatusUpdateMessage> ExecuteTaskAsync(
        StageDispatchMessage dispatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);
        ArgumentNullException.ThrowIfNull(dispatch);

        await _concurrencySemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        Interlocked.Increment(ref _activeTaskCount);
        long startTimestamp = Stopwatch.GetTimestamp();

        try
        {
            var outputKey = new PartitionKey(dispatch.JobId, dispatch.StageId, dispatch.PartitionId);

            // Special handling for Scan operator or fused Scan stage when partition is already pre-registered in store or in-memory
            if (dispatch.OperatorType == StageOperatorType.Scan || dispatch.UpstreamPartitions.Length == 0)
            {
                if (_partitionStore.TryGetPartition(outputKey, out var existingEntry) &&
                    existingEntry != null &&
                    existingEntry.TryRetainBuffer(out var existingBuffer) &&
                    existingBuffer != null)
                {
                    ulong existingRowCount = 0;
                    try
                    {
                        var df = PolarisBridge.PartitionBufferToDataFrame(existingBuffer, zeroCopy: true);
                        existingRowCount = (ulong)df.RowCount;
                    }
                    catch
                    {
                        existingRowCount = 1;
                    }
                    finally
                    {
                        existingBuffer.Release();
                    }

                    return new TaskStatusUpdateMessage(
                        dispatch.JobId,
                        dispatch.StageId,
                        dispatch.PartitionId,
                        TaskExecutionStatus.Completed,
                        elapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds,
                        outputBytes: (ulong)existingEntry.ByteLength,
                        outputRowCount: existingRowCount);
                }

                if (!dispatch.OperatorPayload.IsEmpty)
                {
                    string payloadStr = Encoding.UTF8.GetString(dispatch.OperatorPayload.Span);
                    string[] parts = payloadStr.Split('|');
                    string uri = parts[0];
                    var filterPredicates = parts.Skip(1).Where(p => !string.IsNullOrEmpty(p)).ToList();

                    if (InMemoryDistributedExecutionContext.TryGetSourcePartition(uri, dispatch.PartitionId, out var regBuffer) && regBuffer != null)
                    {
                        if (filterPredicates.Count > 0)
                        {
                            try
                            {
                                var inDf = PolarisBridge.PartitionBufferToDataFrame(regBuffer, zeroCopy: false);
                                var originalCols = inDf.Columns;
                                foreach (var filterPredicate in filterPredicates)
                                {
                                    var expr = PredicateCompiler.Compile(filterPredicate);
                                    var filteredDf = inDf.Filter(expr);
                                    if (filteredDf.Columns.Count == 0 && originalCols.Count > 0)
                                    {
                                        filteredDf = new DataFrame(originalCols.Select(c => c.CloneEmpty(0)).ToList());
                                    }
                                    inDf = filteredDf;
                                }
                                var filteredBuf = PolarisBridge.DataFrameToPartitionBuffer(inDf);
                                regBuffer.Release();
                                regBuffer = filteredBuf;
                            }
                            catch
                            {
                                regBuffer.Release();
                                throw;
                            }
                        }

                        _partitionStore.RegisterPartition(outputKey, regBuffer);
                        ulong regRowCount = 0;
                        try
                        {
                            var df = PolarisBridge.PartitionBufferToDataFrame(regBuffer, zeroCopy: true);
                            regRowCount = (ulong)df.RowCount;
                        }
                        catch
                        {
                            regRowCount = 1;
                        }
                        finally
                        {
                            regBuffer.Release();
                        }

                        return new TaskStatusUpdateMessage(
                            dispatch.JobId,
                            dispatch.StageId,
                            dispatch.PartitionId,
                            TaskExecutionStatus.Completed,
                            elapsedMs: Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds,
                            outputBytes: (ulong)regBuffer.ByteLength,
                            outputRowCount: regRowCount);
                    }
                }
            }

            // 1. Resolve Upstream Shuffle Partitions (if any)
            RefCountedPartitionBuffer[] upstreamBuffers = new RefCountedPartitionBuffer[dispatch.UpstreamPartitions.Length];
            try
            {
                for (int i = 0; i < dispatch.UpstreamPartitions.Length; i++)
                {
                    var loc = dispatch.UpstreamPartitions[i];
                    try
                    {
                        if (loc.ProducerNodeId == _localNodeId)
                        {
                            // Local Zero-Copy Fetch
                            var key = new PartitionKey(dispatch.JobId, loc.StageId, loc.PartitionId);
                            upstreamBuffers[i] = await _partitionStore.FetchPartitionAsync(key, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            // Remote P2P Arrow Shuffle Fetch
                            upstreamBuffers[i] = await _shuffleClient.FetchPartitionAsync(
                                loc.Hostname,
                                loc.ShufflePort,
                                dispatch.JobId,
                                loc.StageId,
                                loc.PartitionId,
                                cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"FetchFailed: Stage={loc.StageId}, Partition={loc.PartitionId}, Producer={loc.ProducerNodeId}: {ex.Message}", ex);
                    }
                }

                // 2. Execute Operator Kernel
                (RefCountedPartitionBuffer outputBuffer, ulong outputRowCount) = await ExecuteOperatorKernelAsync(
                    dispatch.PartitionId,
                    dispatch.OperatorType,
                    dispatch.OperatorPayload,
                    upstreamBuffers,
                    dispatch.UpstreamPartitions,
                    cancellationToken).ConfigureAwait(false);

                // 3. Register Output Partition in Local Partition Store
                _partitionStore.RegisterPartition(outputKey, outputBuffer);
                outputBuffer.Release();

                double elapsedMs = Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds;

                return new TaskStatusUpdateMessage(
                    dispatch.JobId,
                    dispatch.StageId,
                    dispatch.PartitionId,
                    TaskExecutionStatus.Completed,
                    elapsedMs: elapsedMs,
                    outputBytes: (ulong)outputBuffer.ByteLength,
                    outputRowCount: outputRowCount);
            }
            finally
            {
                // Release upstream buffers
                for (int i = 0; i < upstreamBuffers.Length; i++)
                {
                    if (upstreamBuffers[i] != null)
                    {
                        upstreamBuffers[i].Dispose();
                    }
                }
            }
        }
        catch (Exception ex)
        {
            double elapsedMs = Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds;
            return new TaskStatusUpdateMessage(
                dispatch.JobId,
                dispatch.StageId,
                dispatch.PartitionId,
                TaskExecutionStatus.Failed,
                elapsedMs: elapsedMs,
                outputBytes: 0,
                outputRowCount: 0,
                errorMessage: ex.Message);
        }
        finally
        {
            Interlocked.Decrement(ref _activeTaskCount);
            _concurrencySemaphore.Release();
        }
    }

    private ValueTask<(RefCountedPartitionBuffer Buffer, ulong RowCount)> ExecuteOperatorKernelAsync(
        int partitionId,
        StageOperatorType operatorType,
        ReadOnlyMemory<byte> payload,
        RefCountedPartitionBuffer[] upstreams,
        UpstreamPartitionLocation[] upstreamLocations,
        CancellationToken cancellationToken)
    {
        // 1. If upstream partitions exist, deserialize and process with Polaris
        if (upstreams.Length > 0)
        {
            DataFrame resultDf;

            switch (operatorType)
            {
                case StageOperatorType.Filter:
                {
                    if (upstreams[0].ByteLength == 0) throw new InvalidOperationException("Upstream partition buffer is empty.");
                    var inputDf = PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: false);
                    resultDf = ExecuteFilter(inputDf, payload);
                    break;
                }

                case StageOperatorType.Project:
                {
                    if (upstreams[0].ByteLength == 0) throw new InvalidOperationException("Upstream partition buffer is empty.");
                    var inputDf = PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: false);
                    resultDf = ExecuteProject(inputDf, payload);
                    break;
                }

                case StageOperatorType.GroupByMap:
                {
                    if (upstreams[0].ByteLength == 0) throw new InvalidOperationException("Upstream partition buffer is empty.");
                    var inputDf = PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: false);
                    resultDf = inputDf;
                    break;
                }

                case StageOperatorType.GroupByReduce:
                {
                    if (partitionId > 0)
                    {
                        var firstBuffer = upstreams.FirstOrDefault(b => b != null && b.ByteLength > 0);
                        if (firstBuffer != null)
                        {
                            var sampleDf = PolarisBridge.PartitionBufferToDataFrame(firstBuffer, zeroCopy: false);
                            resultDf = new DataFrame(sampleDf.Columns.Select(c => c.CloneEmpty(0)).ToList());
                        }
                        else
                        {
                            resultDf = new DataFrame();
                        }
                    }
                    else
                    {
                        var dfs = new List<DataFrame>(upstreams.Length);
                        foreach (var b in upstreams)
                        {
                            if (b != null && b.ByteLength > 0)
                            {
                                dfs.Add(PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false));
                            }
                        }
                        var combinedDf = dfs.Count == 1 ? dfs[0] : (dfs.Count > 1 ? DataFrame.Concat(dfs) : new DataFrame());
                        resultDf = ExecuteGroupByReduce(combinedDf, payload, dfs);
                    }
                    break;
                }

                case StageOperatorType.HashJoinProbe:
                {
                    resultDf = ExecuteHashJoin(partitionId, upstreams, upstreamLocations, payload);
                    break;
                }

                case StageOperatorType.Custom:
                {
                    string customPayload = Encoding.UTF8.GetString(payload.Span);
                    if (customPayload.StartsWith("REPARTITION:", StringComparison.Ordinal))
                    {
                        string[] parts = customPayload.Split(':', 3);
                        int targetPartitions = int.TryParse(parts[1], out int tp) && tp > 0 ? tp : 1;
                        string[] keys = parts.Length > 2 && !string.IsNullOrWhiteSpace(parts[2])
                            ? parts[2].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                            : Array.Empty<string>();

                        var dfs = new List<DataFrame>(upstreams.Length);
                        foreach (var b in upstreams)
                        {
                            if (b != null && b.ByteLength > 0)
                            {
                                dfs.Add(PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false));
                            }
                        }

                        var combinedDf = dfs.Count == 1
                            ? dfs[0]
                            : (dfs.Count > 1
                                ? (dfs.All(d => d.RowCount == 0) ? (dfs.FirstOrDefault(d => d.Columns.Count > 0) ?? new DataFrame()) : DataFrame.Concat(dfs))
                                : new DataFrame());

                        var buckets = InMemoryDistributedExecutionContext.RepartitionDataFrame(combinedDf, targetPartitions, keys);
                        resultDf = partitionId >= 0 && partitionId < buckets.Count
                            ? buckets[partitionId]
                            : (combinedDf.Columns.Count > 0 ? new DataFrame(combinedDf.Columns.Select(c => c.CloneEmpty(0)).ToList()) : new DataFrame());
                    }
                    else
                    {
                        resultDf = upstreams[0].ByteLength > 0
                            ? PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: false)
                            : new DataFrame();
                    }
                    break;
                }

                default:
                {
                    resultDf = upstreams[0].ByteLength > 0
                        ? PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: false)
                        : new DataFrame();
                    break;
                }
            }

            var outBuffer = PolarisBridge.DataFrameToPartitionBuffer(resultDf);
            return ValueTask.FromResult((outBuffer, (ulong)resultDf.RowCount));
        }

        if (operatorType != StageOperatorType.Scan)
        {
            throw new InvalidOperationException($"Stage operator '{operatorType}' requires upstream input partitions to execute.");
        }

        return ExecuteFileScanAsync(partitionId, payload, cancellationToken);
    }

    private static async ValueTask<(RefCountedPartitionBuffer Buffer, ulong RowCount)> ExecuteFileScanAsync(
        int partitionId,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        if (payload.IsEmpty)
        {
            throw new InvalidOperationException("Scan operator requires a non-empty data source URI or file path.");
        }

        string payloadStr = Encoding.UTF8.GetString(payload.Span);
        string[] parts = payloadStr.Split('|');
        string rawUri = parts[0].Trim();
        var filterPredicates = parts.Skip(1).Where(p => !string.IsNullOrEmpty(p)).ToList();

        string localPath = rawUri;
        if (rawUri.StartsWith("file://", StringComparison.OrdinalIgnoreCase) &&
            Uri.TryCreate(rawUri, UriKind.Absolute, out var parsedUri) &&
            parsedUri.IsFile)
        {
            localPath = parsedUri.LocalPath;
        }

        if (!System.IO.File.Exists(localPath))
        {
            throw new System.IO.FileNotFoundException(
                $"Scan source '{rawUri}' was not found in registered cluster sources, local partition store, or filesystem.",
                localPath);
        }

        DataFrame scannedDf;
        string ext = System.IO.Path.GetExtension(localPath);
        if (ext.Equals(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            var reader = new Glacier.Polaris.IO.ParquetReader(localPath);
            var rowGroups = new List<DataFrame>();
            await foreach (var rg in reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rowGroups.Add(rg);
            }
            scannedDf = rowGroups.Count == 0
                ? new DataFrame()
                : (rowGroups.Count == 1 ? rowGroups[0] : DataFrame.Concat(rowGroups));
        }
        else if (ext.Equals(".csv", StringComparison.OrdinalIgnoreCase))
        {
            scannedDf = await DataFrame.ScanCsv(localPath).Collect().ConfigureAwait(false);
        }
        else if (ext.Equals(".json", StringComparison.OrdinalIgnoreCase) || ext.Equals(".ndjson", StringComparison.OrdinalIgnoreCase))
        {
            scannedDf = DataFrame.ReadJson(localPath);
        }
        else
        {
            byte[] rawBytes = await System.IO.File.ReadAllBytesAsync(localPath, cancellationToken).ConfigureAwait(false);
            using var tempBuf = RefCountedPartitionBuffer.Allocate(rawBytes.Length);
            rawBytes.CopyTo(tempBuf.Span);
            scannedDf = PolarisBridge.PartitionBufferToDataFrame(tempBuf, zeroCopy: false);
        }

        if (partitionId > 0 && scannedDf.Columns.Count > 0)
        {
            scannedDf = new DataFrame(scannedDf.Columns.Select(c => c.CloneEmpty(0)).ToList());
        }

        var originalCols = scannedDf.Columns;
        foreach (var filterPredicate in filterPredicates)
        {
            var expr = PredicateCompiler.Compile(filterPredicate);
            var filteredDf = scannedDf.Filter(expr);
            if (filteredDf.Columns.Count == 0 && originalCols.Count > 0)
            {
                filteredDf = new DataFrame(originalCols.Select(c => c.CloneEmpty(0)).ToList());
            }
            scannedDf = filteredDf;
        }

        var outBuffer = PolarisBridge.DataFrameToPartitionBuffer(scannedDf);
        return (outBuffer, (ulong)scannedDf.RowCount);
    }

    private static DataFrame ExecuteFilter(DataFrame df, ReadOnlyMemory<byte> payload)
    {
        if (payload.IsEmpty) return df;
        string exprStr = Encoding.UTF8.GetString(payload.Span);
        var expr = PredicateCompiler.Compile(exprStr);
        var filteredDf = df.Filter(expr);
        if (filteredDf.Columns.Count == 0 && df.Columns.Count > 0)
        {
            filteredDf = new DataFrame(df.Columns.Select(c => c.CloneEmpty(0)).ToList());
        }
        return filteredDf;
    }

    private static DataFrame ExecuteProject(DataFrame df, ReadOnlyMemory<byte> payload)
    {
        if (payload.IsEmpty) return df;
        string colsStr = Encoding.UTF8.GetString(payload.Span);
        string[] columns = colsStr.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        return df.Select(columns);
    }

    private static DataFrame ExecuteGroupByReduce(DataFrame combinedDf, ReadOnlyMemory<byte> payload, List<DataFrame> dfs)
    {
        string payloadStr = Encoding.UTF8.GetString(payload.Span).Trim();
        string[] groupKeys;
        string[] aggs;

        if (payloadStr.Contains(';'))
        {
            var parts = payloadStr.Split(';', 2, StringSplitOptions.TrimEntries);
            groupKeys = parts[0].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            aggs = parts[1].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        }
        else if (payloadStr.Contains('|'))
        {
            var parts = payloadStr.Split('|', 2, StringSplitOptions.TrimEntries);
            groupKeys = parts[0].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            aggs = parts[1].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        }
        else
        {
            aggs = payloadStr.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

            // Infer groupKeys: all columns in combinedDf that are not targeted by aggregations
            var targetCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var rawAgg in aggs)
            {
                int open = rawAgg.IndexOf('(');
                int close = rawAgg.LastIndexOf(')');
                if (open >= 0 && close > open)
                {
                    string col = rawAgg.Substring(open + 1, close - open - 1).Trim();
                    if (col != "*") targetCols.Add(col);
                }
            }

            var nonAggCols = combinedDf.Columns
                .Where(c => !targetCols.Contains(c.Name))
                .Select(c => c.Name)
                .ToArray();

            groupKeys = nonAggCols.Length > 0
                ? nonAggCols
                : (combinedDf.Columns.Count > 0 ? new[] { combinedDf.Columns[0].Name } : Array.Empty<string>());
        }

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
                    // Ignore if name matches or not found
                }
            }
        }

        return reducedDf;
    }

    private static DataFrame ExecuteHashJoin(
        int partitionId,
        RefCountedPartitionBuffer[] upstreams,
        UpstreamPartitionLocation[] upstreamLocations,
        ReadOnlyMemory<byte> payload)
    {
        string joinKey = Encoding.UTF8.GetString(payload.Span).Trim();

        var leftBuffers = new List<RefCountedPartitionBuffer>();
        var rightBuffers = new List<RefCountedPartitionBuffer>();

        if (upstreamLocations.Length > 0)
        {
            int firstStageId = upstreamLocations[0].StageId;
            bool matchedSpecificLeft = false;
            for (int i = 0; i < upstreams.Length; i++)
            {
                if (i < upstreamLocations.Length && upstreamLocations[i].StageId == firstStageId)
                {
                    if (upstreamLocations[i].PartitionId == partitionId)
                    {
                        leftBuffers.Add(upstreams[i]);
                        matchedSpecificLeft = true;
                    }
                }
                else
                {
                    rightBuffers.Add(upstreams[i]);
                }
            }

            if (!matchedSpecificLeft)
            {
                for (int i = 0; i < upstreams.Length; i++)
                {
                    if (i < upstreamLocations.Length && upstreamLocations[i].StageId == firstStageId)
                        leftBuffers.Add(upstreams[i]);
                }
            }
        }
        else
        {
            int mid = upstreams.Length / 2;
            for (int i = 0; i < upstreams.Length; i++)
            {
                if (i < mid) leftBuffers.Add(upstreams[i]);
                else rightBuffers.Add(upstreams[i]);
            }
        }

        var leftDfs = leftBuffers.Where(b => b != null && b.ByteLength > 0).Select(b => PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false)).ToList();
        var rightDfs = rightBuffers.Where(b => b != null && b.ByteLength > 0).Select(b => PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: false)).ToList();

        var leftDf = leftDfs.Count == 1 ? leftDfs[0] : (leftDfs.Count > 1 ? DataFrame.Concat(leftDfs) : new DataFrame());
        var rightDf = rightDfs.Count == 1 ? rightDfs[0] : (rightDfs.Count > 1 ? DataFrame.Concat(rightDfs) : new DataFrame());

        return leftDf.Join(rightDf, joinKey, JoinType.Inner);
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
                    aggCol = new Float64Series(targetName, 0);
                else if (srcCol != null && srcCol is Int64Series)
                    aggCol = new Float64Series(targetName, 0);
                else
                    aggCol = new Int32Series(targetName, 0);
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
        _concurrencySemaphore.Dispose();
    }
}
