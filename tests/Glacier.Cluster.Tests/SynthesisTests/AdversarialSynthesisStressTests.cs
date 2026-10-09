[assembly: Xunit.CollectionBehavior(DisableTestParallelization = true)]

namespace Glacier.Cluster.Tests.SynthesisTests;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;
using Glacier.Cluster.ML;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Storage.Arrow;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.Worker;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Xunit;

public sealed class AdversarialSynthesisStressTests
{
    #region Pipe Test Infrastructure

    private sealed class DuplexPipePair : IDuplexPipe
    {
        public PipeReader Input { get; }
        public PipeWriter Output { get; }

        public DuplexPipePair(PipeReader input, PipeWriter output)
        {
            Input = input;
            Output = output;
        }
    }

    private sealed class CountingPipeWriter : PipeWriter
    {
        private readonly PipeWriter _inner;
        private long _bytesWritten;

        public long BytesWritten => Volatile.Read(ref _bytesWritten);

        public CountingPipeWriter(PipeWriter inner)
        {
            _inner = inner;
        }

        public override void Advance(int bytes)
        {
            Interlocked.Add(ref _bytesWritten, bytes);
            _inner.Advance(bytes);
        }

        public override Memory<byte> GetMemory(int sizeHint = 0) => _inner.GetMemory(sizeHint);
        public override Span<byte> GetSpan(int sizeHint = 0) => _inner.GetSpan(sizeHint);
        public override void CancelPendingFlush() => _inner.CancelPendingFlush();
        public override void Complete(Exception? exception = null) => _inner.Complete(exception);
        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default) => _inner.FlushAsync(cancellationToken);
    }

    private static (RingAllReduceWorker[] Workers, CountingPipeWriter[] Writers, Func<Task> Cleanup) CreateRingWorkersWithTracking(int worldSize, int totalElements)
    {
        var nodeIds = Enumerable.Range(0, worldSize).Select(_ => Guid.NewGuid()).ToArray();
        var pipes = Enumerable.Range(0, worldSize).Select(_ => new Pipe()).ToArray();
        var countingWriters = new CountingPipeWriter[worldSize];
        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int prevIdx = (i - 1 + worldSize) % worldSize;
            countingWriters[i] = new CountingPipeWriter(pipes[i].Writer);

            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), countingWriters[i]);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodeIds[i],
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        return (workers, countingWriters, async () =>
        {
            for (int i = 0; i < worldSize; i++)
            {
                await workers[i].DisposeAsync();
            }
        });
    }

    #endregion

    #region Single-Node Reference KMeans Oracle

    private static float[] ReferenceSingleNodeKMeans(
        ReadOnlySpan<float> allData,
        int rows,
        int cols,
        int k,
        int maxIterations,
        float tolerance,
        float[] initialCentroids)
    {
        float[] centroids = (float[])initialCentroids.Clone();
        int[] assignments = new int[rows];
        float[] newCentroids = new float[k * cols];
        int[] counts = new int[k];

        for (int iter = 0; iter < maxIterations; iter++)
        {
            // Assignment step
            for (int r = 0; r < rows; r++)
            {
                var sample = allData.Slice(r * cols, cols);
                float minD = float.MaxValue;
                int bestC = 0;
                for (int c = 0; c < k; c++)
                {
                    float d = 0f;
                    int offset = c * cols;
                    for (int dim = 0; dim < cols; dim++)
                    {
                        float diff = sample[dim] - centroids[offset + dim];
                        d += diff * diff;
                    }
                    if (d < minD)
                    {
                        minD = d;
                        bestC = c;
                    }
                }
                assignments[r] = bestC;
            }

            // Update step
            Array.Clear(newCentroids);
            Array.Clear(counts);
            for (int r = 0; r < rows; r++)
            {
                int c = assignments[r];
                counts[c]++;
                int offset = c * cols;
                var sample = allData.Slice(r * cols, cols);
                for (int dim = 0; dim < cols; dim++)
                {
                    newCentroids[offset + dim] += sample[dim];
                }
            }

            float maxShift = 0f;
            for (int c = 0; c < k; c++)
            {
                float count = Math.Max(1.0f, counts[c]);
                int offset = c * cols;
                float shift = 0f;
                for (int dim = 0; dim < cols; dim++)
                {
                    float updated = newCentroids[offset + dim] / count;
                    float diff = updated - centroids[offset + dim];
                    shift += diff * diff;
                    centroids[offset + dim] = updated;
                }
                if (shift > maxShift) maxShift = shift;
            }

            if (maxShift < tolerance) break;
        }

        return centroids;
    }

    #endregion

    #region Area 1: PredicateCompiler Adversarial Stress Tests

    [Theory]
    // Unbalanced quotes
    [InlineData("age == 'unclosed")]
    [InlineData("age == \"unclosed")]
    [InlineData("age == unclosed'")]
    [InlineData("age == unclosed\"")]
    [InlineData("age == '")]
    [InlineData("age == \"")]
    // Missing operators / unrecognized tokens
    [InlineData("foo")]
    [InlineData("foo 123")]
    [InlineData("foo === 123")]
    [InlineData("foo <=> 123")]
    [InlineData("foo ?? 123")]
    [InlineData("foo ==")]
    [InlineData("foo >")]
    [InlineData("foo <=")]
    [InlineData("foo == @#$%")]
    // Null, empty, whitespace
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void PredicateCompiler_MalformedPredicates_ThrowsGracefullyWithoutCrash(string? predicate)
    {
        Assert.ThrowsAny<Exception>(() =>
        {
            _ = PredicateCompiler.Compile(predicate!);
        });
    }

    [Theory]
    // Empty column name: PredicateCompiler must reject missing LHS with FormatException
    [InlineData("== 123")]
    [InlineData("> 123")]
    [InlineData("< 456")]
    // Reject non-SQL operators
    [InlineData("foo ~= 123")]
    [InlineData("foo := 123")]
    [InlineData("~=")]
    [InlineData(":=")]
    public void PredicateCompiler_SubtleMalformedPredicates_ThrowsFormatException(string predicate)
    {
        Assert.Throws<FormatException>(() =>
        {
            _ = PredicateCompiler.Compile(predicate);
        });
    }

    [Theory]
    // Unsupported operators on string literals
    [InlineData("name > 'abc'")]
    [InlineData("name < 'abc'")]
    [InlineData("name >= 'abc'")]
    [InlineData("name <= 'abc'")]
    // Unsupported operators on boolean literals
    [InlineData("flag > true")]
    [InlineData("flag < true")]
    [InlineData("flag >= false")]
    [InlineData("flag <= false")]
    public void PredicateCompiler_TypeMismatches_ThrowsNotSupportedException(string predicate)
    {
        Assert.Throws<NotSupportedException>(() =>
        {
            _ = PredicateCompiler.Compile(predicate);
        });
    }

    [Theory]
    [InlineData("val == 0", "val")]
    [InlineData("val == -42", "val")]
    [InlineData("score >= 3.14159", "score")]
    [InlineData("score < -0.005", "score")]
    [InlineData("active == true", "active")]
    [InlineData("active != false", "active")]
    [InlineData("tag == ''", "tag")]
    [InlineData("tag != 'test_label'", "tag")]
    [InlineData("tag == \"double_quote\"", "tag")]
    public void PredicateCompiler_ValidDiverseLiterals_CompilesSuccessfully(string predicate, string col)
    {
        var expr = PredicateCompiler.Compile(predicate);
        Assert.NotNull(expr);
        string exprStr = expr.Expression.ToString();
        Assert.Contains(col, exprStr);
    }

    #endregion

    #region Area 2: Deeply Chained Query ASTs

    [Fact]
    public async Task DeeplyChainedAST_FilterSelectGroupBySumFilter_ParityWithPolaris()
    {
        int partitionCount = 4;
        int rowsPerPartition = 25;
        var partitions = new List<DataFrame>(partitionCount);

        var allIds = new List<int?>();
        var allDepts = new List<string?>();
        var allSalaries = new List<double?>();
        var allAges = new List<int?>();

        int currentId = 1;
        for (int p = 0; p < partitionCount; p++)
        {
            var pIds = new int?[rowsPerPartition];
            var pDepts = new string?[rowsPerPartition];
            var pSalaries = new double?[rowsPerPartition];
            var pAges = new int?[rowsPerPartition];

            for (int r = 0; r < rowsPerPartition; r++)
            {
                int id = currentId++;
                string dept = (id % 3) switch
                {
                    0 => "Engineering",
                    1 => "Finance",
                    _ => "Sales"
                };
                double salary = 30000.0 + (id * 400.0);
                int age = 22 + (id % 20);

                pIds[r] = id;
                pDepts[r] = dept;
                pSalaries[r] = salary;
                pAges[r] = age;

                allIds.Add(id);
                allDepts.Add(dept);
                allSalaries.Add(salary);
                allAges.Add(age);
            }

            partitions.Add(new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("id", pIds),
                Utf8StringSeries.FromStrings("dept", pDepts),
                Float64Series.FromValues("salary", pSalaries),
                Int32Series.FromValues("age", pAges)
            }));
        }

        var consolidatedDf = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", allIds.ToArray()),
            Utf8StringSeries.FromStrings("dept", allDepts.ToArray()),
            Float64Series.FromValues("salary", allSalaries.ToArray()),
            Int32Series.FromValues("age", allAges.ToArray())
        });

        using var ddf = DistributedDataFrameFactory.FromPartitions(partitions);

        // Deep chain: Filter -> Select -> GroupBy -> Sum -> Filter
        var query = ddf
            .Filter("age > 25")
            .Select("dept", "salary")
            .GroupBy("dept")
            .Sum("salary")
            .Filter("dept == 'Finance'");

        var distributedResult = await query.CollectAsync();

        var expectedDf = consolidatedDf
            .Filter(Expr.Col("age") > 25)
            .Select("dept", "salary")
            .GroupBy("dept")
            .Agg(("salary", "sum"))
            .Filter(Expr.Col("dept") == Expr.Lit("Finance"));

        Assert.Equal(expectedDf.RowCount, distributedResult.RowCount);
        Assert.Equal(1, distributedResult.RowCount);

        var deptCol = (Utf8StringSeries)distributedResult.GetColumn("dept");
        Assert.Equal("Finance", System.Text.Encoding.UTF8.GetString(deptCol.GetStringSpan(0)));

        var sumCol = (Float64Series)distributedResult.GetColumn("SUM(salary)");
        var expectedSumCol = (Float64Series)expectedDf.GetColumn("salary_sum");
        Assert.Equal(expectedSumCol.Memory.Span[0], sumCol.Memory.Span[0], precision: 4);
    }

    [Fact]
    public async Task DeeplyChainedAST_MultipleConsecutiveFiltersAndProjections_MatchesParity()
    {
        int rowCount = 80;
        var ids = new int?[rowCount];
        var values = new double?[rowCount];
        var statuses = new string?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            ids[i] = i + 1;
            values[i] = i * 10.0;
            statuses[i] = (i % 2 == 0) ? "Active" : "Inactive";
        }

        var partition0 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", ids.Take(40).ToArray()),
            Float64Series.FromValues("value", values.Take(40).ToArray()),
            Utf8StringSeries.FromStrings("status", statuses.Take(40).ToArray())
        });

        var partition1 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", ids.Skip(40).ToArray()),
            Float64Series.FromValues("value", values.Skip(40).ToArray()),
            Utf8StringSeries.FromStrings("status", statuses.Skip(40).ToArray())
        });

        var singleNodeDf = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", ids),
            Float64Series.FromValues("value", values),
            Utf8StringSeries.FromStrings("status", statuses)
        });

        using var ddf = DistributedDataFrameFactory.FromPartitions(new[] { partition0, partition1 });

        // Triple filter chain with projection
        var query = ddf
            .Filter("id > 10")
            .Filter("id < 70")
            .Filter("value >= 200.0")
            .Select("id", "value");

        var resultDf = await query.CollectAsync();

        var expectedDf = singleNodeDf
            .Filter(Expr.Col("id") > 10)
            .Filter(Expr.Col("id") < 70)
            .Filter(Expr.Col("value") >= 200.0)
            .Select("id", "value");

        Assert.Equal(expectedDf.RowCount, resultDf.RowCount);
        Assert.Equal(2, resultDf.Columns.Count);

        var idCol = (Int32Series)resultDf.GetColumn("id");
        var expIdCol = (Int32Series)expectedDf.GetColumn("id");
        for (int i = 0; i < resultDf.RowCount; i++)
        {
            Assert.Equal(expIdCol.Memory.Span[i], idCol.Memory.Span[i]);
        }
    }

    #endregion

    #region Area 3: Dataset Boundary Edge Cases & Parity

    [Fact]
    public async Task EdgeCase_EmptyDataFrames_HandledGracefullyAcrossOperations()
    {
        var emptyPart0 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", Array.Empty<int?>()),
            Float64Series.FromValues("score", Array.Empty<double?>()),
            Utf8StringSeries.FromStrings("dept", Array.Empty<string?>())
        });

        var emptyPart1 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", Array.Empty<int?>()),
            Float64Series.FromValues("score", Array.Empty<double?>()),
            Utf8StringSeries.FromStrings("dept", Array.Empty<string?>())
        });

        using var ddf = DistributedDataFrameFactory.FromPartitions(new[] { emptyPart0, emptyPart1 });

        // CountAsync on empty
        long count = await ddf.CountAsync();
        Assert.Equal(0, count);

        // TakeAsync on empty
        var takeDf = await ddf.TakeAsync(10);
        Assert.Equal(0, takeDf.RowCount);

        // Filter on empty
        var filteredDdf = ddf.Filter("score > 50.0");
        var filteredDf = await filteredDdf.CollectAsync();
        Assert.Equal(0, filteredDf.RowCount);

        // Select on empty returns 0 rows, preserving schema (Columns.Count is 2)
        var selectDdf = ddf.Select("id", "score");
        var selectDf = await selectDdf.CollectAsync();
        Assert.Equal(0, selectDf.RowCount);
        Assert.Equal(2, selectDf.Columns.Count);
        Assert.NotNull(selectDf.GetColumn("id"));
        Assert.NotNull(selectDf.GetColumn("score"));

        // GroupBy on empty DataFrame handles gracefully without exception
        var groupDdf = ddf.GroupBy("dept").Sum("score");
        var groupDf = await groupDdf.CollectAsync();
        Assert.Equal(0, groupDf.RowCount);
        Assert.Equal(2, groupDf.Columns.Count);
        Assert.NotNull(groupDf.GetColumn("dept"));
        Assert.NotNull(groupDf.GetColumn("SUM(score)"));
    }

    [Fact]
    public async Task Defect_FilterZeroMatchPartition_CausesDownstreamSelectToCrash()
    {
        // Partition 0 has id 1..3
        // Partition 1 has id 101..103
        var part0 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3 }),
            Float64Series.FromValues("val", new double?[] { 10.0, 20.0, 30.0 })
        });

        var part1 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 101, 102, 103 }),
            Float64Series.FromValues("val", new double?[] { 110.0, 120.0, 130.0 })
        });

        using var ddf = DistributedDataFrameFactory.FromPartitions(new[] { part0, part1 });

        // Filter id > 50 produces 0 rows in Partition 0.
        // Partition 0 preserves its column schema, allowing downstream Select("val") to succeed cleanly!
        var query = ddf.Filter("id > 50").Select("val");
        var resultDf = await query.CollectAsync();
        Assert.Equal(3, resultDf.RowCount);
        Assert.Single(resultDf.Columns);
        Assert.Equal("val", resultDf.Columns[0].Name);
    }

    [Fact]
    public async Task EdgeCase_SingleRowDataFrame_ParityWithSingleNode()
    {
        var part0 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 42 }),
            Float64Series.FromValues("amount", new double?[] { 999.5 }),
            Utf8StringSeries.FromStrings("category", new string?[] { "Hardware" })
        });

        var part1 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", Array.Empty<int?>()),
            Float64Series.FromValues("amount", Array.Empty<double?>()),
            Utf8StringSeries.FromStrings("category", Array.Empty<string?>())
        });

        using var ddf = DistributedDataFrameFactory.FromPartitions(new[] { part0, part1 });

        Assert.Equal(1, await ddf.CountAsync());

        // Match filter
        var matchDf = await ddf.Filter("id == 42").CollectAsync();
        Assert.Equal(1, matchDf.RowCount);

        // Non-match filter
        var noMatchDf = await ddf.Filter("id == 999").CollectAsync();
        Assert.Equal(0, noMatchDf.RowCount);

        // GroupBy single row
        var groupDf = await ddf.GroupBy("category").Sum("amount").CollectAsync();
        Assert.Equal(1, groupDf.RowCount);
        var sumCol = (Float64Series)groupDf.GetColumn("SUM(amount)");
        Assert.Equal(999.5, sumCol.Memory.Span[0], precision: 2);
    }

    [Fact]
    public async Task EdgeCase_SingleColumnDataFrame_FilterAndAggregation()
    {
        var part0 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("val", new int?[] { 10, 20, 30 })
        });
        var part1 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("val", new int?[] { 40, 50, 60 })
        });

        using var ddf = DistributedDataFrameFactory.FromPartitions(new[] { part0, part1 });

        var filtered = await ddf.Filter("val >= 30").CollectAsync();
        Assert.Equal(4, filtered.RowCount);
        Assert.Single(filtered.Columns);

        var grouped = await ddf.GroupBy("val").Count().CollectAsync();
        Assert.Equal(6, grouped.RowCount);
    }

    [Fact]
    public async Task EdgeCase_AllNullSeries_PreservesNullsWithoutFailure()
    {
        int rows = 15;
        var ids = new int?[rows];
        var allNullFloats = new double?[rows];
        var allNullStrings = new string?[rows];

        for (int i = 0; i < rows; i++)
        {
            ids[i] = i + 1;
            allNullFloats[i] = null;
            allNullStrings[i] = null;
        }

        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", ids),
            Float64Series.FromValues("null_score", allNullFloats),
            Utf8StringSeries.FromStrings("null_name", allNullStrings)
        });

        // Roundtrip through PolarisBridge
        using var buffer = PolarisBridge.DataFrameToPartitionBuffer(df);
        var deserialized = PolarisBridge.PartitionBufferToDataFrame(buffer, zeroCopy: true);

        Assert.Equal(rows, deserialized.RowCount);
        var scoreCol = deserialized.GetColumn("null_score");
        Assert.Equal(rows, scoreCol.ValidityMask.NullCount);

        for (int i = 0; i < rows; i++)
        {
            Assert.Null(scoreCol.Get(i));
        }

        // Distributed query over all-null series
        using var ddf = DistributedDataFrameFactory.FromPartitions(new[] { df });
        var filtered = await ddf.Filter("id > 5").Select("id", "null_score").CollectAsync();
        Assert.Equal(10, filtered.RowCount);
        var filteredScore = filtered.GetColumn("null_score");
        Assert.Equal(10, filteredScore.ValidityMask.NullCount);
    }

    [Fact]
    public async Task EdgeCase_LargeMultiColumnDataset_50Columns_ParityOracle()
    {
        int colCount = 50;
        int rowsPerPartition = 250;
        int partitionCount = 4;
        int totalRows = rowsPerPartition * partitionCount;

        var consolidatedSeries = new List<ISeries>(colCount);

        // Pre-generate column data
        for (int c = 0; c < colCount; c++)
        {
            string colName = $"col_{c}";
            if (c % 4 == 0) // Int32
            {
                var vals = Enumerable.Range(0, totalRows).Select(r => (int?)(r + (c * 100))).ToArray();
                consolidatedSeries.Add(Int32Series.FromValues(colName, vals));
            }
            else if (c % 4 == 1) // Float64
            {
                var vals = Enumerable.Range(0, totalRows).Select(r => (double?)(r * 1.25 + c)).ToArray();
                consolidatedSeries.Add(Float64Series.FromValues(colName, vals));
            }
            else if (c % 4 == 2) // Utf8
            {
                var vals = Enumerable.Range(0, totalRows).Select(r => $"val_{r % 5}_{c}").ToArray();
                consolidatedSeries.Add(Utf8StringSeries.FromStrings(colName, vals));
            }
            else // Boolean
            {
                var vals = Enumerable.Range(0, totalRows).Select(r => (bool?)(r % 2 == 0)).ToArray();
                consolidatedSeries.Add(BooleanSeries.FromValues(colName, vals));
            }
        }

        var consolidatedDf = new DataFrame(consolidatedSeries);

        // Slice DataFrame into 4 partitions using DataFrame.Slice
        var partitions = new List<DataFrame>(partitionCount);
        for (int p = 0; p < partitionCount; p++)
        {
            int start = p * rowsPerPartition;
            partitions.Add(consolidatedDf.Slice(start, rowsPerPartition));
        }

        using var ddf = DistributedDataFrameFactory.FromPartitions(partitions);

        // Execute complex projection and filter across 50 columns where all partitions have rows
        var query = ddf
            .Filter("col_0 >= 0")
            .Select("col_0", "col_1", "col_2", "col_48", "col_49");

        var distributedResult = await query.CollectAsync();

        var expectedDf = consolidatedDf
            .Filter(Expr.Col("col_0") >= 0)
            .Select("col_0", "col_1", "col_2", "col_48", "col_49");

        Assert.Equal(expectedDf.RowCount, distributedResult.RowCount);
        Assert.Equal(5, distributedResult.Columns.Count);

        var dCol0 = (Int32Series)distributedResult.GetColumn("col_0");
        var expCol0 = (Int32Series)expectedDf.GetColumn("col_0");

        for (int i = 0; i < distributedResult.RowCount; i++)
        {
            Assert.Equal(expCol0.Memory.Span[i], dCol0.Memory.Span[i]);
        }
    }

    #endregion

    #region Area 4: Distributed KMeans on Challenging Datasets & Zero Data Row Movement

    [Fact]
    public async Task DistributedKMeans_TightlyClusteredBlobs_MatchesSingleNodeEpsilon()
    {
        int k = 4;
        int cols = 3;
        int rowsPerNode = 60;
        int totalRows = rowsPerNode * 2;
        int totalReductionFloats = (k * cols) + k;

        // 4 tightly clustered blobs in 3D: (50, 50, 50), (-50, 50, -50), (50, -50, -50), (-50, -50, 50)
        float[][] clusterCenters = new float[][]
        {
            new float[] { 50.0f, 50.0f, 50.0f },
            new float[] { -50.0f, 50.0f, -50.0f },
            new float[] { 50.0f, -50.0f, -50.0f },
            new float[] { -50.0f, -50.0f, 50.0f }
        };

        float[] allData = new float[totalRows * cols];
        var rand = new Random(42);

        for (int r = 0; r < totalRows; r++)
        {
            int cluster = r % k;
            for (int d = 0; d < cols; d++)
            {
                // tight std dev = 0.05
                float jitter = ((float)rand.NextDouble() - 0.5f) * 0.1f;
                allData[(r * cols) + d] = clusterCenters[cluster][d] + jitter;
            }
        }

        // Split data between Node 0 and Node 1
        float[] data0 = new float[rowsPerNode * cols];
        float[] data1 = new float[rowsPerNode * cols];
        Array.Copy(allData, 0, data0, 0, rowsPerNode * cols);
        Array.Copy(allData, rowsPerNode * cols, data1, 0, rowsPerNode * cols);

        float[] initialCentroids = new float[]
        {
            45.0f, 45.0f, 45.0f,
            -45.0f, 45.0f, -45.0f,
            45.0f, -45.0f, -45.0f,
            -45.0f, -45.0f, 45.0f
        };

        var (workers, _, cleanup) = CreateRingWorkersWithTracking(worldSize: 2, totalElements: totalReductionFloats);

        try
        {
            var kmeans0 = new DistributedKMeans(k, maxIterations: 50, tolerance: 1e-4f);
            var kmeans1 = new DistributedKMeans(k, maxIterations: 50, tolerance: 1e-4f);

            Task t0 = kmeans0.FitAsync(data0.AsMemory(), rowsPerNode, cols, workers[0], (float[])initialCentroids.Clone());
            Task t1 = kmeans1.FitAsync(data1.AsMemory(), rowsPerNode, cols, workers[1], (float[])initialCentroids.Clone());

            await Task.WhenAll(t0, t1);

            float[] referenceCentroids = ReferenceSingleNodeKMeans(
                allData.AsSpan(),
                totalRows,
                cols,
                k,
                maxIterations: 50,
                tolerance: 1e-4f,
                initialCentroids);

            Assert.Equal(referenceCentroids.Length, kmeans0.Centroids.Length);
            for (int i = 0; i < referenceCentroids.Length; i++)
            {
                Assert.Equal(referenceCentroids[i], kmeans0.Centroids[i], precision: 3);
                Assert.Equal(kmeans0.Centroids[i], kmeans1.Centroids[i], precision: 4);
            }
        }
        finally
        {
            await cleanup();
        }
    }

    [Fact]
    public async Task DistributedKMeans_OverlappingClusters_ConvergesStably()
    {
        int k = 2;
        int cols = 2;
        int rowsPerNode = 50;
        int totalRows = rowsPerNode * 2;
        int totalReductionFloats = (k * cols) + k;

        // Overlapping blobs centered at (0, 0) and (1.5, 1.5) with large variance
        float[] allData = new float[totalRows * cols];
        var rand = new Random(1337);

        for (int r = 0; r < totalRows; r++)
        {
            float center = (r % 2 == 0) ? 0.0f : 1.5f;
            for (int d = 0; d < cols; d++)
            {
                float noise = ((float)rand.NextDouble() - 0.5f) * 3.0f; // std dev ~ 1.5
                allData[(r * cols) + d] = center + noise;
            }
        }

        float[] data0 = new float[rowsPerNode * cols];
        float[] data1 = new float[rowsPerNode * cols];
        Array.Copy(allData, 0, data0, 0, rowsPerNode * cols);
        Array.Copy(allData, rowsPerNode * cols, data1, 0, rowsPerNode * cols);

        float[] initialCentroids = new float[] { -0.5f, -0.5f, 2.0f, 2.0f };

        var (workers, _, cleanup) = CreateRingWorkersWithTracking(worldSize: 2, totalElements: totalReductionFloats);

        try
        {
            var kmeans0 = new DistributedKMeans(k, maxIterations: 40, tolerance: 1e-4f);
            var kmeans1 = new DistributedKMeans(k, maxIterations: 40, tolerance: 1e-4f);

            Task t0 = kmeans0.FitAsync(data0.AsMemory(), rowsPerNode, cols, workers[0], (float[])initialCentroids.Clone());
            Task t1 = kmeans1.FitAsync(data1.AsMemory(), rowsPerNode, cols, workers[1], (float[])initialCentroids.Clone());

            await Task.WhenAll(t0, t1);

            float[] refCentroids = ReferenceSingleNodeKMeans(
                allData.AsSpan(),
                totalRows,
                cols,
                k,
                maxIterations: 40,
                tolerance: 1e-4f,
                initialCentroids);

            for (int i = 0; i < refCentroids.Length; i++)
            {
                Assert.Equal(refCentroids[i], kmeans0.Centroids[i], precision: 3);
            }
        }
        finally
        {
            await cleanup();
        }
    }

    [Fact]
    public async Task DistributedKMeans_CollinearData_ConvergesWithoutNaN()
    {
        int k = 3;
        int cols = 3;
        int rowsPerNode = 40;
        int totalRows = rowsPerNode * 2;
        int totalReductionFloats = (k * cols) + k;

        // Points along 1D subspace in 3D: (t, 2t, 3t) for t in [-50, 50]
        float[] allData = new float[totalRows * cols];
        for (int r = 0; r < totalRows; r++)
        {
            float t = (r - (totalRows / 2.0f));
            allData[(r * cols)] = t;
            allData[(r * cols) + 1] = 2.0f * t;
            allData[(r * cols) + 2] = 3.0f * t;
        }

        float[] data0 = new float[rowsPerNode * cols];
        float[] data1 = new float[rowsPerNode * cols];
        Array.Copy(allData, 0, data0, 0, rowsPerNode * cols);
        Array.Copy(allData, rowsPerNode * cols, data1, 0, rowsPerNode * cols);

        float[] initialCentroids = new float[]
        {
            -20.0f, -40.0f, -60.0f,
            0.0f, 0.0f, 0.0f,
            20.0f, 40.0f, 60.0f
        };

        var (workers, _, cleanup) = CreateRingWorkersWithTracking(worldSize: 2, totalElements: totalReductionFloats);

        try
        {
            var kmeans0 = new DistributedKMeans(k, maxIterations: 30, tolerance: 1e-4f);
            var kmeans1 = new DistributedKMeans(k, maxIterations: 30, tolerance: 1e-4f);

            Task t0 = kmeans0.FitAsync(data0.AsMemory(), rowsPerNode, cols, workers[0], (float[])initialCentroids.Clone());
            Task t1 = kmeans1.FitAsync(data1.AsMemory(), rowsPerNode, cols, workers[1], (float[])initialCentroids.Clone());

            await Task.WhenAll(t0, t1);

            // Assert no NaN or Inf
            foreach (var c in kmeans0.Centroids)
            {
                Assert.False(float.IsNaN(c));
                Assert.False(float.IsInfinity(c));
            }

            float[] refCentroids = ReferenceSingleNodeKMeans(
                allData.AsSpan(),
                totalRows,
                cols,
                k,
                maxIterations: 30,
                tolerance: 1e-4f,
                initialCentroids);

            for (int i = 0; i < refCentroids.Length; i++)
            {
                Assert.Equal(refCentroids[i], kmeans0.Centroids[i], precision: 2);
            }
        }
        finally
        {
            await cleanup();
        }
    }

    [Fact]
    public async Task DistributedKMeans_EmptyClusterEdgeCase_DoesNotProduceNaNOrCrash()
    {
        int k = 4;
        int cols = 2;
        int rowsPerNode = 20;
        int totalReductionFloats = (k * cols) + k;

        // All points clustered tightly around (0, 0)
        float[] data0 = new float[rowsPerNode * cols];
        float[] data1 = new float[rowsPerNode * cols];
        for (int i = 0; i < data0.Length; i++)
        {
            data0[i] = 0.01f * (i % 5);
            data1[i] = 0.01f * (i % 5);
        }

        // 3 of the 4 initial centroids are far away: clusters 1, 2, 3 receive 0 samples
        float[] initialCentroids = new float[]
        {
            0.0f, 0.0f,       // Cluster 0 takes all samples
            1000.0f, 1000.0f, // Empty
            -1000.0f, 1000.0f,// Empty
            1000.0f, -1000.0f // Empty
        };

        var (workers, _, cleanup) = CreateRingWorkersWithTracking(worldSize: 2, totalElements: totalReductionFloats);

        try
        {
            var kmeans0 = new DistributedKMeans(k, maxIterations: 10, tolerance: 1e-4f);
            var kmeans1 = new DistributedKMeans(k, maxIterations: 10, tolerance: 1e-4f);

            Task t0 = kmeans0.FitAsync(data0.AsMemory(), rowsPerNode, cols, workers[0], initialCentroids);
            Task t1 = kmeans1.FitAsync(data1.AsMemory(), rowsPerNode, cols, workers[1], initialCentroids);

            await Task.WhenAll(t0, t1);

            foreach (var val in kmeans0.Centroids)
            {
                Assert.False(float.IsNaN(val), "Centroid coordinate must not be NaN for empty clusters.");
                Assert.False(float.IsInfinity(val), "Centroid coordinate must not be Infinity.");
            }
        }
        finally
        {
            await cleanup();
        }
    }

    [Fact]
    public async Task DistributedKMeans_ZeroDataRowMovement_AssertsOnlyCentroidStatsExchanged()
    {
        int k = 2;
        int cols = 4;
        int rowsPerNode = 5000; // Large dataset: 5,000 rows * 4 cols * 4 bytes = 80,000 bytes per node
        int totalReductionFloats = (k * cols) + k; // Only 10 floats = 40 bytes!

        float[] data0 = new float[rowsPerNode * cols];
        float[] data1 = new float[rowsPerNode * cols];

        for (int r = 0; r < rowsPerNode; r++)
        {
            float center = (r % 2 == 0) ? 50.0f : -50.0f;
            for (int d = 0; d < cols; d++)
            {
                data0[(r * cols) + d] = center + (d * 0.1f);
                data1[(r * cols) + d] = center - (d * 0.1f);
            }
        }

        float[] initialCentroids = new float[]
        {
            40.0f, 40.0f, 40.0f, 40.0f,
            -40.0f, -40.0f, -40.0f, -40.0f
        };

        var (workers, countingWriters, cleanup) = CreateRingWorkersWithTracking(worldSize: 2, totalElements: totalReductionFloats);

        try
        {
            var kmeans0 = new DistributedKMeans(k, maxIterations: 15, tolerance: 1e-4f);
            var kmeans1 = new DistributedKMeans(k, maxIterations: 15, tolerance: 1e-4f);

            Task t0 = kmeans0.FitAsync(data0.AsMemory(), rowsPerNode, cols, workers[0], (float[])initialCentroids.Clone());
            Task t1 = kmeans1.FitAsync(data1.AsMemory(), rowsPerNode, cols, workers[1], (float[])initialCentroids.Clone());

            await Task.WhenAll(t0, t1);

            long rawDataBytesPerNode = (long)rowsPerNode * cols * sizeof(float); // 80,000 bytes

            long node0BytesSent = countingWriters[0].BytesWritten;
            long node1BytesSent = countingWriters[1].BytesWritten;

            // Assert that transferred bytes are orders of magnitude smaller than raw row data
            Assert.True(node0BytesSent < rawDataBytesPerNode,
                $"Zero row movement violation: Node 0 sent {node0BytesSent} bytes which exceeds raw dataset size of {rawDataBytesPerNode} bytes!");
            Assert.True(node1BytesSent < rawDataBytesPerNode,
                $"Zero row movement violation: Node 1 sent {node1BytesSent} bytes which exceeds raw dataset size of {rawDataBytesPerNode} bytes!");

            Assert.True(node0BytesSent > 0);
            Assert.True(node1BytesSent > 0);
        }
        finally
        {
            await cleanup();
        }
    }

    #endregion

    #region Area 5: Fused Scan+Filter Execution Adversarial Tests

    [Fact]
    public void FusedScanFilter_StageCompiler_FusesPredicateAndPreservesPayload()
    {
        var compiler = new StageCompiler();
        var jobId = Guid.NewGuid();
        string uri = "memory://table_fused_test";

        var scan = new LogicalScanNode(uri, partitionCount: 2);
        var filter = new LogicalFilterNode(scan, "val >= 50");

        var dag = compiler.Compile(jobId, "FusedScanFilterJob", filter);

        Assert.Single(dag.Stages);
        var stage = dag.Stages.Values.Single();
        Assert.Equal(StageOperatorType.Scan, stage.PrimaryOperator);
        Assert.False(stage.IsShuffleBoundary);
        Assert.Equal(2, stage.PartitionsCount);

        string payloadStr = System.Text.Encoding.UTF8.GetString(stage.OperatorPayload.Span);
        Assert.Equal($"{uri}|val >= 50", payloadStr);
    }

    [Fact]
    public async Task FusedScanFilter_ExecuteDagAsync_EvaluatesFilterPredicateAgainstScannedData()
    {
        string uri = $"memory://fused_scan_{Guid.NewGuid():N}";
        var part0 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4 }),
            Float64Series.FromValues("score", new double?[] { 10.0, 20.0, 60.0, 80.0 })
        });
        var part1 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 5, 6, 7, 8 }),
            Float64Series.FromValues("score", new double?[] { 30.0, 40.0, 70.0, 90.0 })
        });

        using var buf0 = PolarisBridge.DataFrameToPartitionBuffer(part0);
        using var buf1 = PolarisBridge.DataFrameToPartitionBuffer(part1);

        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf0, buf1 });

        using var executionContext = new InMemoryDistributedExecutionContext(new[] { buf0, buf1 });

        var scan = new LogicalScanNode(uri, partitionCount: 2);
        var filter = new LogicalFilterNode(scan, "score >= 60.0");

        var compiler = new StageCompiler();
        var dag = compiler.Compile(Guid.NewGuid(), "FusedDag", filter);

        Assert.Single(dag.Stages);
        Assert.Equal(StageOperatorType.Scan, dag.Stages.Values.Single().PrimaryOperator);

        var outputBuffers = await executionContext.ExecuteDagAsync(dag);
        Assert.Equal(2, outputBuffers.Count);

        try
        {
            var df0 = PolarisBridge.PartitionBufferToDataFrame(outputBuffers[0], zeroCopy: true);
            var df1 = PolarisBridge.PartitionBufferToDataFrame(outputBuffers[1], zeroCopy: true);

            Assert.Equal(2, df0.RowCount); // 60, 80
            Assert.Equal(2, df1.RowCount); // 70, 90

            var score0 = (Float64Series)df0.GetColumn("score");
            for (int i = 0; i < df0.RowCount; i++)
            {
                Assert.True(score0.Memory.Span[i] >= 60.0);
            }

            var score1 = (Float64Series)df1.GetColumn("score");
            for (int i = 0; i < df1.RowCount; i++)
            {
                Assert.True(score1.Memory.Span[i] >= 60.0);
            }
        }
        finally
        {
            foreach (var b in outputBuffers)
            {
                b.Release();
            }
        }
    }

    [Fact]
    public async Task FusedScanFilter_ClusterWorkerDaemon_EvaluatesFilterPredicateAgainstScannedData()
    {
        string uri = $"memory://daemon_fused_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5 }),
            Int32Series.FromValues("age", new int?[] { 20, 35, 42, 18, 50 })
        });

        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        var coordOptions = new ClusterCoordinatorOptions { Port = 0 };
        using var coordinator = new ClusterCoordinator(coordOptions);
        await coordinator.StartAsync();

        var workerOptions = new ClusterWorkerOptions
        {
            NodeId = Guid.NewGuid(),
            Hostname = "test-fused-worker",
            ShufflePort = 0
        };
        using var worker = new ClusterWorker(workerOptions);

        var toCoordinator = new Pipe();
        var toWorker = new Pipe();
        var workerSidePipe = new DuplexPipePair(toWorker.Reader, toCoordinator.Writer);
        var coordSidePipe = new DuplexPipePair(toCoordinator.Reader, toWorker.Writer);

        await coordinator.RegisterInProcessWorkerAsync(worker.NodeId, coordSidePipe);
        await worker.StartAsync(workerSidePipe);

        Guid jobId = Guid.NewGuid();
        var scan = new LogicalScanNode(uri, partitionCount: 1);
        var filter = new LogicalFilterNode(scan, "age > 30");

        var compiler = new StageCompiler();
        var dag = compiler.Compile(jobId, "DaemonFusedJob", filter);

        Assert.Single(dag.Stages);
        var stage = dag.Stages.Values.Single();
        Assert.Equal(StageOperatorType.Scan, stage.PrimaryOperator);

        var jobCompletion = await coordinator.SubmitJobAsync(dag);
        Assert.NotNull(jobCompletion);
        Assert.Equal(JobCompletionStatus.Succeeded, jobCompletion.Status);

        var resultKey = new PartitionKey(jobId, stage.StageId, 0);
        using var resultBuf = await worker.PartitionStore.FetchPartitionAsync(resultKey);
        var resultDf = PolarisBridge.PartitionBufferToDataFrame(resultBuf, zeroCopy: true);

        Assert.Equal(3, resultDf.RowCount); // 35, 42, 50
        var ageCol = (Int32Series)resultDf.GetColumn("age");
        for (int i = 0; i < resultDf.RowCount; i++)
        {
            Assert.True(ageCol.Memory.Span[i] > 30);
        }

        await worker.StopAsync();
    }

    [Fact]
    public async Task FusedScanFilter_ZeroMatchPartition_PreservesSchema()
    {
        string uri = $"memory://zero_match_fused_{Guid.NewGuid():N}";
        var part0 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3 }),
            Float64Series.FromValues("val", new double?[] { 10.0, 20.0, 30.0 })
        });
        var part1 = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 101, 102, 103 }),
            Float64Series.FromValues("val", new double?[] { 110.0, 120.0, 130.0 })
        });

        using var buf0 = PolarisBridge.DataFrameToPartitionBuffer(part0);
        using var buf1 = PolarisBridge.DataFrameToPartitionBuffer(part1);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf0, buf1 });

        using var executionContext = new InMemoryDistributedExecutionContext(new[] { buf0, buf1 });

        var scan = new LogicalScanNode(uri, partitionCount: 2);
        var filter = new LogicalFilterNode(scan, "id > 50"); // partition 0 has 0 matches

        var compiler = new StageCompiler();
        var dag = compiler.Compile(Guid.NewGuid(), "ZeroMatchDag", filter);

        var outputBuffers = await executionContext.ExecuteDagAsync(dag);
        Assert.Equal(2, outputBuffers.Count);

        try
        {
            var df0 = PolarisBridge.PartitionBufferToDataFrame(outputBuffers[0], zeroCopy: true);
            var df1 = PolarisBridge.PartitionBufferToDataFrame(outputBuffers[1], zeroCopy: true);

            Assert.Equal(0, df0.RowCount);
            Assert.Equal(2, df0.Columns.Count); // Schema preserved!
            Assert.NotNull(df0.GetColumn("id"));
            Assert.NotNull(df0.GetColumn("val"));

            Assert.Equal(3, df1.RowCount);
            Assert.Equal(2, df1.Columns.Count);
        }
        finally
        {
            foreach (var b in outputBuffers)
            {
                b.Release();
            }
        }
    }

    [Fact]
    public async Task FusedScanFilter_MultipleChainedFilters_EvaluatesInDag()
    {
        string uri = $"memory://chained_fused_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5, 6 }),
            Int32Series.FromValues("age", new int?[] { 15, 25, 35, 45, 55, 65 })
        });

        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        using var executionContext = new InMemoryDistributedExecutionContext(new[] { buf });

        var scan = new LogicalScanNode(uri, partitionCount: 1);
        var filter1 = new LogicalFilterNode(scan, "age > 20");
        var filter2 = new LogicalFilterNode(filter1, "age < 50");

        var compiler = new StageCompiler();
        var dag = compiler.Compile(Guid.NewGuid(), "ChainedFusedJob", filter2);

        // StageCompiler fuses both filters into scan:
        Assert.Single(dag.Stages);
        var stage = dag.Stages.Values.Single();
        Assert.Equal(StageOperatorType.Scan, stage.PrimaryOperator);

        var outputBuffers = await executionContext.ExecuteDagAsync(dag);
        Assert.Single(outputBuffers);

        try
        {
            var resultDf = PolarisBridge.PartitionBufferToDataFrame(outputBuffers[0], zeroCopy: true);
            // Should contain 25, 35, 45 (3 rows)
            Assert.Equal(3, resultDf.RowCount);
            var ageCol = (Int32Series)resultDf.GetColumn("age");
            for (int i = 0; i < resultDf.RowCount; i++)
            {
                int val = ageCol.Memory.Span[i];
                Assert.True(val > 20 && val < 50);
            }
        }
        finally
        {
            outputBuffers[0].Release();
        }
    }

    [Fact]
    public async Task FusedScanFilter_MultipleChainedFilters_ClusterWorkerExecution()
    {
        string uri = $"memory://chained_daemon_fused_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5, 6 }),
            Int32Series.FromValues("age", new int?[] { 15, 25, 35, 45, 55, 65 })
        });

        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        var coordOptions = new ClusterCoordinatorOptions { Port = 0 };
        using var coordinator = new ClusterCoordinator(coordOptions);
        await coordinator.StartAsync();

        var workerOptions = new ClusterWorkerOptions
        {
            NodeId = Guid.NewGuid(),
            Hostname = "test-chained-worker",
            ShufflePort = 0
        };
        using var worker = new ClusterWorker(workerOptions);

        var toCoordinator = new Pipe();
        var toWorker = new Pipe();
        var workerSidePipe = new DuplexPipePair(toWorker.Reader, toCoordinator.Writer);
        var coordSidePipe = new DuplexPipePair(toCoordinator.Reader, toWorker.Writer);

        await coordinator.RegisterInProcessWorkerAsync(worker.NodeId, coordSidePipe);
        await worker.StartAsync(workerSidePipe);

        Guid jobId = Guid.NewGuid();
        var scan = new LogicalScanNode(uri, partitionCount: 1);
        var filter1 = new LogicalFilterNode(scan, "age > 20");
        var filter2 = new LogicalFilterNode(filter1, "age < 50");

        var compiler = new StageCompiler();
        var dag = compiler.Compile(jobId, "ChainedDaemonFusedJob", filter2);

        var stage = dag.Stages.Values.Single();
        var jobCompletion = await coordinator.SubmitJobAsync(dag);
        Assert.NotNull(jobCompletion);
        Assert.Equal(JobCompletionStatus.Succeeded, jobCompletion.Status);

        var resultKey = new PartitionKey(jobId, stage.StageId, 0);
        using var resultBuf = await worker.PartitionStore.FetchPartitionAsync(resultKey);
        var resultDf = PolarisBridge.PartitionBufferToDataFrame(resultBuf, zeroCopy: true);

        Assert.Equal(3, resultDf.RowCount); // 25, 35, 45

        await worker.StopAsync();
    }

    #endregion
}
