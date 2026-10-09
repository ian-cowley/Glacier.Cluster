namespace Glacier.Cluster.Polaris;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Storage.Arrow;

/// <summary>
/// Columnar distributed dataframe abstraction partitioned across cluster workers.
/// Represents an immutable logical query plan executed via pure zero-copy Arrow IPC exchanges.
/// </summary>
public sealed class DistributedDataFrame : IAsyncDisposable, IDisposable
{
    public Guid JobId { get; }
    public string Name { get; }
    public ArrowSchema Schema { get; }
    public int PartitionCount { get; }
    public LogicalPlanNode LogicalPlan { get; }
    public IDistributedExecutionContext ExecutionContext { get; }

    internal DistributedDataFrame(
        Guid jobId,
        string name,
        ArrowSchema schema,
        int partitionCount,
        LogicalPlanNode logicalPlan,
        IDistributedExecutionContext context)
    {
        JobId = jobId;
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        PartitionCount = partitionCount > 0 ? partitionCount : 1;
        LogicalPlan = logicalPlan ?? throw new ArgumentNullException(nameof(logicalPlan));
        ExecutionContext = context ?? throw new ArgumentNullException(nameof(context));
    }

    /// <summary>
    /// Narrow pipelined row filter evaluated across worker partitions.
    /// </summary>
    public DistributedDataFrame Filter(string predicateExpression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(predicateExpression);
        var plan = new LogicalFilterNode(LogicalPlan, predicateExpression);
        return new DistributedDataFrame(
            Guid.NewGuid(),
            $"{Name}_Filter",
            Schema,
            PartitionCount,
            plan,
            ExecutionContext);
    }

    /// <summary>
    /// Narrow pipelined row filter evaluated with a strongly-typed Polaris Expr.
    /// </summary>
    public DistributedDataFrame Filter(Expr predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return Filter(predicate.ToString() ?? "true");
    }

    /// <summary>
    /// Narrow columnar projection selecting a subset of columns.
    /// </summary>
    public DistributedDataFrame Select(params string[] columnNames)
    {
        ArgumentNullException.ThrowIfNull(columnNames);
        if (columnNames.Length == 0)
            throw new ArgumentException("At least one column name must be specified.", nameof(columnNames));

        var projectedFields = new List<ArrowField>(columnNames.Length);
        foreach (var col in columnNames)
        {
            int idx = Schema.GetFieldIndex(col);
            if (idx >= 0)
            {
                projectedFields.Add(Schema.GetField(idx));
            }
            else
            {
                projectedFields.Add(new ArrowField(col, ArrowType.Int32));
            }
        }

        var projectedSchema = new ArrowSchema(projectedFields);
        var plan = new LogicalProjectNode(LogicalPlan, columnNames);

        return new DistributedDataFrame(
            Guid.NewGuid(),
            $"{Name}_Select",
            projectedSchema,
            PartitionCount,
            plan,
            ExecutionContext);
    }

    /// <summary>
    /// Distributed GroupBy builder coordinating two-stage map-shuffle-reduce aggregation.
    /// </summary>
    public DistributedGroupBy GroupBy(params string[] keys)
    {
        return new DistributedGroupBy(this, keys, PartitionCount);
    }

    /// <summary>
    /// Distributed shuffle-hash join with another distributed dataframe.
    /// </summary>
    public DistributedDataFrame Join(
        DistributedDataFrame right,
        string onKey,
        JoinType joinType = JoinType.Inner)
    {
        ArgumentNullException.ThrowIfNull(right);
        ArgumentException.ThrowIfNullOrWhiteSpace(onKey);

        int targetPartitions = Math.Max(PartitionCount, right.PartitionCount);
        var plan = new LogicalHashJoinNode(LogicalPlan, right.LogicalPlan, onKey, targetPartitions);

        // Derive joined schema: left columns + right non-key columns
        var joinedFields = new List<ArrowField>();
        for (int i = 0; i < Schema.FieldCount; i++)
        {
            joinedFields.Add(Schema.GetField(i));
        }

        for (int i = 0; i < right.Schema.FieldCount; i++)
        {
            var rf = right.Schema.GetField(i);
            if (!string.Equals(rf.Name, onKey, StringComparison.OrdinalIgnoreCase))
            {
                joinedFields.Add(rf);
            }
        }

        var joinedSchema = new ArrowSchema(joinedFields);

        return new DistributedDataFrame(
            Guid.NewGuid(),
            $"{Name}_Join_{right.Name}",
            joinedSchema,
            targetPartitions,
            plan,
            ExecutionContext);
    }

    /// <summary>
    /// Repartitions the distributed dataframe into a different partition count.
    /// </summary>
    public DistributedDataFrame Repartition(int targetPartitions, params string[]? partitionKeys)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(targetPartitions);
        return new DistributedDataFrame(
            Guid.NewGuid(),
            $"{Name}_Repartition",
            Schema,
            targetPartitions,
            LogicalPlan,
            ExecutionContext);
    }

    /// <summary>
    /// Terminal action: Compiles the logical plan to an ExecutionDag, executes across the cluster,
    /// and gathers all partition buffers into a single consolidated in-process DataFrame.
    /// </summary>
    public async ValueTask<DataFrame> CollectAsync(CancellationToken cancellationToken = default)
    {
        IReadOnlyList<RefCountedPartitionBuffer> leafBuffers;

        if (ExecutionContext is InMemoryDistributedExecutionContext inMemory)
        {
            leafBuffers = await inMemory.ExecutePlanAsync(LogicalPlan, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var compiler = new StageCompiler();
            var dag = compiler.Compile(JobId, Name, LogicalPlan, PartitionCount);
            leafBuffers = await ExecutionContext.ExecuteDagAsync(dag, cancellationToken).ConfigureAwait(false);
        }

        var dataFrames = new List<DataFrame>(leafBuffers.Count);
        try
        {
            for (int i = 0; i < leafBuffers.Count; i++)
            {
                var buffer = leafBuffers[i];
                var df = PolarisBridge.PartitionBufferToDataFrame(buffer, zeroCopy: true);
                dataFrames.Add(df);
            }

            if (dataFrames.Count == 0)
            {
                return new DataFrame();
            }

            if (dataFrames.Count == 1)
            {
                return dataFrames[0];
            }

            if (dataFrames.All(df => df.RowCount == 0))
            {
                var firstWithCols = dataFrames.FirstOrDefault(df => df.Columns.Count > 0);
                if (firstWithCols != null)
                {
                    return new DataFrame(firstWithCols.Columns.Select(c => c.CloneEmpty(0)).ToList());
                }
                return new DataFrame();
            }

            return DataFrame.Concat(dataFrames);
        }
        finally
        {
            for (int i = 0; i < leafBuffers.Count; i++)
            {
                leafBuffers[i].Release();
            }
        }
    }

    /// <summary>
    /// Terminal action: counts total rows across all distributed partitions.
    /// </summary>
    public async ValueTask<long> CountAsync(CancellationToken cancellationToken = default)
    {
        var df = await CollectAsync(cancellationToken).ConfigureAwait(false);
        return df.RowCount;
    }

    /// <summary>
    /// Terminal action: materializes the first N rows into an in-process DataFrame.
    /// </summary>
    public async ValueTask<DataFrame> TakeAsync(int rowCount, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rowCount);
        var df = await CollectAsync(cancellationToken).ConfigureAwait(false);
        return df.Slice(0, Math.Min(rowCount, df.RowCount));
    }

    /// <summary>
    /// Writes distributed partitions to Parquet or Arrow IPC stream.
    /// </summary>
    public async ValueTask WriteParquetAsync(string clusterPath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterPath);
        var df = await CollectAsync(cancellationToken).ConfigureAwait(false);
        using var fs = File.Create(clusterPath);
        df.ToArrowIpc(fs, leaveOpen: false);
    }

    public void Dispose()
    {
        if (ExecutionContext is IDisposable disp)
        {
            disp.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Factory helpers for instantiating <see cref="DistributedDataFrame"/> instances.
/// </summary>
public static class DistributedDataFrameFactory
{
    public static DistributedDataFrame FromDataSource(
        string dataSourcePath,
        int partitionCount,
        ArrowSchema schema,
        IDistributedExecutionContext executionContext,
        string? datasetName = null)
    {
        ArgumentNullException.ThrowIfNull(dataSourcePath);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(executionContext);

        var plan = new LogicalScanNode(dataSourcePath, partitionCount);
        return new DistributedDataFrame(
            Guid.NewGuid(),
            datasetName ?? $"Dataset_{Guid.NewGuid():N}",
            schema,
            partitionCount,
            plan,
            executionContext);
    }

    public static DistributedDataFrame FromPartitions(
        IReadOnlyList<DataFrame> partitions,
        IDistributedExecutionContext? executionContext = null,
        string? datasetName = null)
    {
        ArgumentNullException.ThrowIfNull(partitions);
        if (partitions.Count == 0)
            throw new ArgumentException("Partitions collection cannot be empty.", nameof(partitions));

        var schema = partitions[0].ToGlacierRecordBatch().Schema;
        string uri = $"memory://{Guid.NewGuid():N}";
        var context = executionContext ?? new InMemoryDistributedExecutionContext(partitions, uri);
        if (context is InMemoryDistributedExecutionContext inMem)
        {
            inMem.RegisterSource(uri);
        }

        return FromDataSource(
            uri,
            partitions.Count,
            schema,
            context,
            datasetName);
    }
}
