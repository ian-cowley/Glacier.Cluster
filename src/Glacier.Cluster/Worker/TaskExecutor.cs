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
                        using var df = PolarisBridge.PartitionBufferToDataFrame(existingBuffer, zeroCopy: true);
                        existingRowCount = (ulong)df.RowCount;
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        Trace.TraceInformation($"[TaskExecutor] Non-Arrow partition buffer size fallback: {ex.Message}");
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
                    ExtractStagePayloadParts(payloadStr, out string scanAndOps, out _, out _);
                    var parts = PredicateCompiler.SplitPipelinePayload(scanAndOps, '|');
                    string uri = parts.Count > 0 ? parts[0] : string.Empty;
                    var filterPredicates = parts.Skip(1).Where(p => !string.IsNullOrEmpty(p)).ToList();

                    if (!string.IsNullOrEmpty(uri) &&
                        InMemoryDistributedExecutionContext.TryGetSourcePartition(uri, dispatch.PartitionId, out var regBuffer) &&
                        regBuffer != null)
                    {
                        if (filterPredicates.Count > 0)
                        {
                            DataFrame? inDf = null;
                            DataFrame? outDf = null;
                            var fusedTransientDfs = new List<DataFrame>();
                            try
                            {
                                inDf = PolarisBridge.PartitionBufferToDataFrame(regBuffer, zeroCopy: true);
                                outDf = ApplyFusedOperators(inDf, filterPredicates, fusedTransientDfs);
                                var filteredBuf = PolarisBridge.DataFrameToPartitionBuffer(outDf);
                                regBuffer.Release();
                                regBuffer = filteredBuf;
                            }
                            catch
                            {
                                regBuffer.Release();
                                throw;
                            }
                            finally
                            {
                                foreach (var d in fusedTransientDfs)
                                {
                                    if (!ReferenceEquals(d, inDf) && !ReferenceEquals(d, outDf))
                                    {
                                        d.Dispose();
                                    }
                                }
                                if (outDf != null && !ReferenceEquals(outDf, inDf))
                                {
                                    outDf.Dispose();
                                }
                                inDf?.Dispose();
                            }
                        }

                        _partitionStore.RegisterPartition(outputKey, regBuffer);
                        ulong regRowCount = 0;
                        try
                        {
                            using var df = PolarisBridge.PartitionBufferToDataFrame(regBuffer, zeroCopy: true);
                            regRowCount = (ulong)df.RowCount;
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            Trace.TraceInformation($"[TaskExecutor] Non-Arrow registered partition size fallback: {ex.Message}");
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
            var transientDfs = new List<DataFrame>();
            DataFrame? resultDf = null;

            try
            {
                switch (operatorType)
                {
                    case StageOperatorType.Filter:
                    {
                        if (upstreams[0].ByteLength == 0) throw new InvalidOperationException("Upstream partition buffer is empty.");
                        var inputDf = PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: true);
                        transientDfs.Add(inputDf);
                        resultDf = ExecuteFilter(inputDf, payload, transientDfs);
                        break;
                    }

                    case StageOperatorType.Project:
                    {
                        if (upstreams[0].ByteLength == 0) throw new InvalidOperationException("Upstream partition buffer is empty.");
                        var inputDf = PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: true);
                        transientDfs.Add(inputDf);
                        resultDf = ExecuteProject(inputDf, payload);
                        break;
                    }

                    case StageOperatorType.GroupByMap:
                    {
                        if (upstreams[0].ByteLength == 0) throw new InvalidOperationException("Upstream partition buffer is empty.");
                        var inputDf = PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: true);
                        transientDfs.Add(inputDf);
                        resultDf = inputDf;
                        break;
                    }

                    case StageOperatorType.GroupByReduce:
                    {
                        resultDf = ExecuteGroupByReducePartitioned(partitionId, upstreams, payload, transientDfs);
                        break;
                    }

                    case StageOperatorType.HashJoinProbe:
                    {
                        resultDf = ExecuteHashJoin(partitionId, upstreams, upstreamLocations, payload, transientDfs);
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

                            var bucketDfs = new List<DataFrame>(upstreams.Length);
                            DataFrame? schemaTemplate = null;
                            foreach (var b in upstreams)
                            {
                                if (b != null && b.ByteLength > 0)
                                {
                                    var partDf = PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: true);
                                    transientDfs.Add(partDf);
                                    schemaTemplate ??= partDf;
                                    var sliced = SliceByHashBucket(partDf, partitionId, targetPartitions, keys);
                                    if (sliced.RowCount > 0)
                                    {
                                        bucketDfs.Add(sliced);
                                        if (!ReferenceEquals(sliced, partDf)) transientDfs.Add(sliced);
                                    }
                                    else if (!ReferenceEquals(sliced, partDf))
                                    {
                                        sliced.Dispose();
                                    }
                                }
                            }

                            resultDf = bucketDfs.Count == 1
                                ? bucketDfs[0]
                                : (bucketDfs.Count > 1
                                    ? DataFrame.Concat(bucketDfs)
                                    : (schemaTemplate != null && schemaTemplate.Columns.Count > 0
                                        ? new DataFrame(schemaTemplate.Columns.Select(c => c.CloneEmpty(0)).ToList())
                                        : new DataFrame()));
                        }
                        else
                        {
                            resultDf = upstreams[0].ByteLength > 0
                                ? PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: true)
                                : new DataFrame();
                        }
                        break;
                    }

                    default:
                    {
                        resultDf = upstreams[0].ByteLength > 0
                            ? PolarisBridge.PartitionBufferToDataFrame(upstreams[0], zeroCopy: true)
                            : new DataFrame();
                        break;
                    }
                }

                var outBuffer = PolarisBridge.DataFrameToPartitionBuffer(resultDf);
                return ValueTask.FromResult((outBuffer, (ulong)resultDf.RowCount));
            }
            finally
            {
                foreach (var d in transientDfs)
                {
                    if (!ReferenceEquals(d, resultDf))
                    {
                        d.Dispose();
                    }
                }
                resultDf?.Dispose();
            }
        }

        if (operatorType != StageOperatorType.Scan &&
            !(operatorType == StageOperatorType.GroupByMap && !payload.IsEmpty &&
              Encoding.UTF8.GetString(payload.Span).Contains("\u001fGROUPBY:", StringComparison.Ordinal)))
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
        ExtractStagePayloadParts(payloadStr, out string scanAndOps, out _, out int totalPartitions);
        var parts = PredicateCompiler.SplitPipelinePayload(scanAndOps, '|');
        string rawUri = parts.Count > 0 ? parts[0].Trim() : string.Empty;
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

        var transientDfs = new List<DataFrame>();
        DataFrame? scannedDf = null;
        try
        {
            string ext = System.IO.Path.GetExtension(localPath);
            bool rowGroupsAlreadyDistributed = false;
            if (ext.Equals(".parquet", StringComparison.OrdinalIgnoreCase))
            {
                var reader = new Glacier.Polaris.IO.ParquetReader(localPath);
                var ownedRowGroups = new List<DataFrame>();
                DataFrame? schemaDf = null;
                int rgIdx = 0;
                await foreach (var rg in reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    transientDfs.Add(rg);
                    schemaDf ??= rg;
                    if (totalPartitions > 1)
                    {
                        if ((rgIdx % totalPartitions) == partitionId)
                        {
                            ownedRowGroups.Add(rg);
                        }
                    }
                    else
                    {
                        ownedRowGroups.Add(rg);
                    }
                    rgIdx++;
                }

                if (totalPartitions > 1 && rgIdx == 1 && schemaDf != null)
                {
                    // Single-row-group Parquet fallback: slice rows evenly across workers
                    scannedDf = SliceRowsByRange(schemaDf, partitionId, totalPartitions);
                    rowGroupsAlreadyDistributed = true;
                }
                else if (totalPartitions > 1)
                {
                    rowGroupsAlreadyDistributed = true;
                    scannedDf = ownedRowGroups.Count == 0
                        ? (schemaDf != null && schemaDf.Columns.Count > 0
                            ? new DataFrame(schemaDf.Columns.Select(c => c.CloneEmpty(0)).ToList())
                            : new DataFrame())
                        : (ownedRowGroups.Count == 1 ? ownedRowGroups[0] : DataFrame.Concat(ownedRowGroups));
                }
                else
                {
                    scannedDf = ownedRowGroups.Count == 0
                        ? new DataFrame()
                        : (ownedRowGroups.Count == 1 ? ownedRowGroups[0] : DataFrame.Concat(ownedRowGroups));
                }
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

            if (!rowGroupsAlreadyDistributed)
            {
                if (totalPartitions > 1)
                {
                    var slicedDf = SliceRowsByRange(scannedDf, partitionId, totalPartitions);
                    if (!ReferenceEquals(slicedDf, scannedDf)) transientDfs.Add(scannedDf);
                    scannedDf = slicedDf;
                }
                else if (partitionId > 0 && scannedDf.Columns.Count > 0)
                {
                    var emptyDf = new DataFrame(scannedDf.Columns.Select(c => c.CloneEmpty(0)).ToList());
                    transientDfs.Add(scannedDf);
                    scannedDf = emptyDf;
                }
            }

            scannedDf = ApplyFusedOperators(scannedDf, filterPredicates, transientDfs);

            var outBuffer = PolarisBridge.DataFrameToPartitionBuffer(scannedDf);
            return (outBuffer, (ulong)scannedDf.RowCount);
        }
        finally
        {
            foreach (var d in transientDfs)
            {
                if (!ReferenceEquals(d, scannedDf))
                {
                    d.Dispose();
                }
            }
            scannedDf?.Dispose();
        }
    }

    internal static DataFrame ApplyFusedOperators(DataFrame df, IEnumerable<string> operations, List<DataFrame>? transientDfs = null)
    {
        DataFrame initialDf = df;
        try
        {
            foreach (var op in operations)
            {
                if (string.IsNullOrWhiteSpace(op)) continue;

                DataFrame nextDf;
                if (op.StartsWith("PROJECT:", StringComparison.Ordinal))
                {
                    string[] cols = op["PROJECT:".Length..].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                    nextDf = df.Select(cols);
                }
                else
                {
                    var preCols = df.Columns;
                    var expr = PredicateCompiler.Compile(op);
                    var filtered = df.Filter(expr);
                    if (filtered.Columns.Count == 0 && preCols.Count > 0)
                    {
                        var emptyFiltered = new DataFrame(preCols.Select(c => c.CloneEmpty(0)).ToList());
                        filtered.Dispose();
                        filtered = emptyFiltered;
                    }
                    nextDf = filtered;
                }

                if (!ReferenceEquals(nextDf, df))
                {
                    if (transientDfs != null)
                    {
                        if (!transientDfs.Contains(df)) transientDfs.Add(df);
                        transientDfs.Add(nextDf);
                    }
                    else if (!ReferenceEquals(df, initialDf))
                    {
                        df.Dispose();
                    }
                    df = nextDf;
                }
            }
            return df;
        }
        catch
        {
            if (transientDfs == null && !ReferenceEquals(df, initialDf))
            {
                df.Dispose();
            }
            throw;
        }
    }

    private static DataFrame ExecuteFilter(DataFrame df, ReadOnlyMemory<byte> payload, List<DataFrame>? transientDfs = null)
    {
        if (payload.IsEmpty) return df;
        string exprStr = Encoding.UTF8.GetString(payload.Span);
        var ops = PredicateCompiler.SplitPipelinePayload(exprStr, '|').Where(p => !string.IsNullOrEmpty(p));
        return ApplyFusedOperators(df, ops, transientDfs);
    }

    private static DataFrame ExecuteProject(DataFrame df, ReadOnlyMemory<byte> payload)
    {
        if (payload.IsEmpty) return df;
        string colsStr = Encoding.UTF8.GetString(payload.Span);
        string[] columns = colsStr.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        return df.Select(columns);
    }

    private static DataFrame ExecuteGroupByReducePartitioned(
        int partitionId,
        RefCountedPartitionBuffer[] upstreams,
        ReadOnlyMemory<byte> payload,
        List<DataFrame> transientDfs)
    {
        string rawPayload = Encoding.UTF8.GetString(payload.Span).Trim();
        ExtractStagePayloadParts(rawPayload, out string payloadStr, out _, out int totalPartitions);
        if (totalPartitions <= 1)
        {
            if (partitionId > 0)
            {
                var firstBuffer = upstreams.FirstOrDefault(b => b != null && b.ByteLength > 0);
                if (firstBuffer != null)
                {
                    using var sampleDf = PolarisBridge.PartitionBufferToDataFrame(firstBuffer, zeroCopy: true);
                    return new DataFrame(sampleDf.Columns.Select(c => c.CloneEmpty(0)).ToList());
                }
                return new DataFrame();
            }

            var singleDfs = new List<DataFrame>(upstreams.Length);
            foreach (var b in upstreams)
            {
                if (b != null && b.ByteLength > 0)
                {
                    var df = PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: true);
                    transientDfs.Add(df);
                    singleDfs.Add(df);
                }
            }
            var singleCombined = singleDfs.Count == 1 ? singleDfs[0] : (singleDfs.Count > 1 ? DataFrame.Concat(singleDfs) : new DataFrame());
            if (singleDfs.Count > 1) transientDfs.Add(singleCombined);
            return ExecuteGroupByReduceCore(singleCombined, payloadStr, singleDfs);
        }

        ParseGroupBySpec(payloadStr, upstreams, transientDfs, out string[] groupKeys, out _, out _);
        var bucketDfs = new List<DataFrame>(upstreams.Length);
        DataFrame? schemaTemplate = null;
        foreach (var b in upstreams)
        {
            if (b != null && b.ByteLength > 0)
            {
                var df = PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: true);
                transientDfs.Add(df);
                schemaTemplate ??= df;
                if (df.RowCount > 0)
                {
                    var sliced = SliceByHashBucket(df, partitionId, totalPartitions, groupKeys);
                    if (sliced.RowCount > 0)
                    {
                        bucketDfs.Add(sliced);
                        if (!ReferenceEquals(sliced, df)) transientDfs.Add(sliced);
                    }
                    else if (!ReferenceEquals(sliced, df))
                    {
                        sliced.Dispose();
                    }
                }
            }
        }

        var combinedDf = bucketDfs.Count == 1
            ? bucketDfs[0]
            : (bucketDfs.Count > 1
                ? DataFrame.Concat(bucketDfs)
                : (schemaTemplate != null && schemaTemplate.Columns.Count > 0
                    ? new DataFrame(schemaTemplate.Columns.Select(c => c.CloneEmpty(0)).ToList())
                    : new DataFrame()));
        if (bucketDfs.Count > 1 || bucketDfs.Count == 0) transientDfs.Add(combinedDf);

        var templateList = bucketDfs.Count > 0
            ? bucketDfs
            : (schemaTemplate != null ? new List<DataFrame> { schemaTemplate } : new List<DataFrame>());
        return ExecuteGroupByReduceCore(combinedDf, payloadStr, templateList);
    }

    private static void ParseGroupBySpec(
        string payloadStr,
        RefCountedPartitionBuffer[]? upstreams,
        List<DataFrame> transientDfs,
        out string[] groupKeys,
        out string[] aggs,
        out DataFrame? firstDf)
    {
        firstDf = null;
        if (payloadStr.Contains(';'))
        {
            var parts = payloadStr.Split(';', 2, StringSplitOptions.TrimEntries);
            groupKeys = parts[0].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            aggs = parts[1].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            return;
        }
        if (payloadStr.Contains('|'))
        {
            var parts = payloadStr.Split('|', 2, StringSplitOptions.TrimEntries);
            groupKeys = parts[0].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            aggs = parts[1].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            return;
        }

        aggs = payloadStr.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (upstreams != null)
        {
            var firstBuf = upstreams.FirstOrDefault(b => b != null && b.ByteLength > 0);
            if (firstBuf != null)
            {
                firstDf = PolarisBridge.PartitionBufferToDataFrame(firstBuf, zeroCopy: true);
                transientDfs.Add(firstDf);
            }
        }

        if (firstDf != null && firstDf.Columns.Count > 0)
        {
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
            var nonAggCols = firstDf.Columns.Where(c => !targetCols.Contains(c.Name)).Select(c => c.Name).ToArray();
            groupKeys = nonAggCols.Length > 0 ? nonAggCols : new[] { firstDf.Columns[0].Name };
        }
        else
        {
            groupKeys = Array.Empty<string>();
        }
    }

    private static DataFrame ExecuteGroupByReduceCore(DataFrame combinedDf, string payloadStr, List<DataFrame> dfs)
    {
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
                var col = reducedDf.Columns.FirstOrDefault(c => string.Equals(c.Name, orig, StringComparison.Ordinal));
                col?.Rename(target);
            }
        }

        return reducedDf;
    }

    private static DataFrame ExecuteHashJoin(
        int partitionId,
        RefCountedPartitionBuffer[] upstreams,
        UpstreamPartitionLocation[] upstreamLocations,
        ReadOnlyMemory<byte> payload,
        List<DataFrame> transientDfs)
    {
        string rawPayload = Encoding.UTF8.GetString(payload.Span).Trim();
        bool isCoPartitioned = false;
        int totalPartitions = 0;
        string joinKey = rawPayload;
        int coPartIdx = rawPayload.IndexOf("\u001fCO_PART:", StringComparison.Ordinal);
        if (coPartIdx >= 0)
        {
            joinKey = rawPayload[..coPartIdx].Trim();
            isCoPartitioned = true;
            _ = int.TryParse(rawPayload[(coPartIdx + "\u001fCO_PART:".Length)..], out totalPartitions);
        }
        else
        {
            ExtractStagePayloadParts(rawPayload, out string basePart, out _, out totalPartitions);
            joinKey = basePart.Trim();
        }

        var leftBuffers = new List<RefCountedPartitionBuffer>();
        var rightBuffers = new List<RefCountedPartitionBuffer>();

        if (upstreamLocations.Length > 0)
        {
            int firstStageId = upstreamLocations[0].StageId;
            if (isCoPartitioned)
            {
                for (int i = 0; i < upstreams.Length; i++)
                {
                    if (i < upstreamLocations.Length && upstreamLocations[i].StageId == firstStageId)
                        leftBuffers.Add(upstreams[i]);
                    else
                        rightBuffers.Add(upstreams[i]);
                }
            }
            else if (totalPartitions > 1)
            {
                for (int i = 0; i < upstreams.Length; i++)
                {
                    if (i < upstreamLocations.Length && upstreamLocations[i].StageId == firstStageId)
                        leftBuffers.Add(upstreams[i]);
                    else
                        rightBuffers.Add(upstreams[i]);
                }
            }
            else
            {
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

        string[] joinKeys = string.IsNullOrEmpty(joinKey) ? Array.Empty<string>() : new[] { joinKey };
        bool filterBuckets = !isCoPartitioned && totalPartitions > 1 && joinKeys.Length > 0;

        var leftDfs = new List<DataFrame>(leftBuffers.Count);
        DataFrame? leftTemplate = null;
        foreach (var b in leftBuffers)
        {
            if (b != null && b.ByteLength > 0)
            {
                var df = PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: true);
                transientDfs.Add(df);
                leftTemplate ??= df;
                if (filterBuckets)
                {
                    var sliced = SliceByHashBucket(df, partitionId, totalPartitions, joinKeys);
                    if (sliced.RowCount > 0)
                    {
                        leftDfs.Add(sliced);
                        if (!ReferenceEquals(sliced, df)) transientDfs.Add(sliced);
                    }
                    else if (!ReferenceEquals(sliced, df))
                    {
                        sliced.Dispose();
                    }
                }
                else
                {
                    leftDfs.Add(df);
                }
            }
        }

        var rightDfs = new List<DataFrame>(rightBuffers.Count);
        DataFrame? rightTemplate = null;
        foreach (var b in rightBuffers)
        {
            if (b != null && b.ByteLength > 0)
            {
                var df = PolarisBridge.PartitionBufferToDataFrame(b, zeroCopy: true);
                transientDfs.Add(df);
                rightTemplate ??= df;
                if (filterBuckets)
                {
                    var sliced = SliceByHashBucket(df, partitionId, totalPartitions, joinKeys);
                    if (sliced.RowCount > 0)
                    {
                        rightDfs.Add(sliced);
                        if (!ReferenceEquals(sliced, df)) transientDfs.Add(sliced);
                    }
                    else if (!ReferenceEquals(sliced, df))
                    {
                        sliced.Dispose();
                    }
                }
                else
                {
                    rightDfs.Add(df);
                }
            }
        }

        var leftDf = leftDfs.Count == 1
            ? leftDfs[0]
            : (leftDfs.Count > 1
                ? DataFrame.Concat(leftDfs)
                : (leftTemplate != null && leftTemplate.Columns.Count > 0
                    ? new DataFrame(leftTemplate.Columns.Select(c => c.CloneEmpty(0)).ToList())
                    : new DataFrame()));
        if (leftDfs.Count != 1) transientDfs.Add(leftDf);

        var rightDf = rightDfs.Count == 1
            ? rightDfs[0]
            : (rightDfs.Count > 1
                ? DataFrame.Concat(rightDfs)
                : (rightTemplate != null && rightTemplate.Columns.Count > 0
                    ? new DataFrame(rightTemplate.Columns.Select(c => c.CloneEmpty(0)).ToList())
                    : new DataFrame()));
        if (rightDfs.Count != 1) transientDfs.Add(rightDf);

        return leftDf.Join(rightDf, joinKey, JoinType.Inner);
    }

    private static void ExtractStagePayloadParts(
        string payload,
        out string scanAndOps,
        out string groupKeys,
        out int totalPartitions)
    {
        totalPartitions = 0;
        string working = payload;
        int pIdx = working.IndexOf("\u001fP:", StringComparison.Ordinal);
        if (pIdx >= 0)
        {
            _ = int.TryParse(working[(pIdx + "\u001fP:".Length)..], out totalPartitions);
            working = working[..pIdx];
        }

        int gbIdx = working.IndexOf("\u001fGROUPBY:", StringComparison.Ordinal);
        if (gbIdx >= 0)
        {
            scanAndOps = working[..gbIdx];
            groupKeys = working[(gbIdx + "\u001fGROUPBY:".Length)..];
        }
        else
        {
            scanAndOps = working;
            groupKeys = string.Empty;
        }
    }

    private static DataFrame SliceRowsByRange(DataFrame df, int partitionId, int totalPartitions)
    {
        if (df.Columns.Count == 0 || df.RowCount == 0)
            return df;

        int totalRows = df.RowCount;
        int start = (int)((long)partitionId * totalRows / totalPartitions);
        int end = (int)((long)(partitionId + 1) * totalRows / totalPartitions);
        int length = Math.Max(0, end - start);
        if (length == 0)
        {
            return new DataFrame(df.Columns.Select(c => c.CloneEmpty(0)).ToList());
        }

        return df.Slice(start, length);
    }

    private static DataFrame SliceByHashBucket(
        DataFrame df,
        int targetBucket,
        int totalPartitions,
        string[] keys)
    {
        if (df.Columns.Count == 0 || df.RowCount == 0 || totalPartitions <= 1)
            return df;

        var indices = new List<int>((df.RowCount / totalPartitions) + 1);
        if (keys.Length > 0 && keys.All(k => df.Columns.Any(c => string.Equals(c.Name, k, StringComparison.Ordinal))))
        {
            var keyCols = keys.Select(df.GetColumn).ToArray();
            for (int row = 0; row < df.RowCount; row++)
            {
                var hc = new HashCode();
                for (int k = 0; k < keyCols.Length; k++)
                {
                    hc.Add(keyCols[k].Get(row));
                }
                int bucket = (int)((uint)hc.ToHashCode() % (uint)totalPartitions);
                if (bucket == targetBucket)
                {
                    indices.Add(row);
                }
            }
        }
        else
        {
            for (int row = 0; row < df.RowCount; row++)
            {
                if ((row % totalPartitions) == targetBucket)
                {
                    indices.Add(row);
                }
            }
        }

        if (indices.Count == 0)
        {
            return new DataFrame(df.Columns.Select(c => c.CloneEmpty(0)).ToList());
        }

        var span = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(indices);
        var subCols = new List<ISeries>(df.Columns.Count);
        foreach (var col in df.Columns)
        {
            var subCol = Glacier.Polaris.Compute.UniqueKernels.CreateSeriesForTake(col, span);
            col.Take(subCol, span);
            subCols.Add(subCol);
        }
        return new DataFrame(subCols);
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
