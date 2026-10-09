namespace Glacier.Cluster.Tests.SynthesisTests;

using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;
using Glacier.Cluster.ML;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

public sealed class DistributedKMeansTests
{
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

    private static (RingAllReduceWorker[] Workers, Func<Task> Cleanup) CreateRingWorkers(int worldSize, int totalElements)
    {
        var nodeIds = Enumerable.Range(0, worldSize).Select(_ => Guid.NewGuid()).ToArray();
        var pipes = Enumerable.Range(0, worldSize).Select(_ => new Pipe()).ToArray();
        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int nextIdx = (i + 1) % worldSize;
            int prevIdx = (i - 1 + worldSize) % worldSize;

            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), pipes[i].Writer);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodeIds[i],
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        return (workers, async () =>
        {
            for (int i = 0; i < worldSize; i++)
            {
                await workers[i].DisposeAsync();
            }
        });
    }

    [Fact]
    public async Task DistributedKMeans_TwoNodes_ConvergesOnDistinctClusters()
    {
        int k = 2;
        int cols = 2;
        int rowsPerNode = 40;
        int totalReductionFloats = (k * cols) + k; // 6 floats

        // Worker 0 data: Points around (10, 10) and (-10, -10)
        // Worker 1 data: Points around (10, 10) and (-10, -10)
        float[] data0 = new float[rowsPerNode * cols];
        float[] data1 = new float[rowsPerNode * cols];

        for (int r = 0; r < rowsPerNode; r++)
        {
            float center = (r % 2 == 0) ? 10.0f : -10.0f;
            float offset = (r % 5) * 0.1f;

            data0[(r * 2)] = center + offset;
            data0[(r * 2) + 1] = center - offset;

            data1[(r * 2)] = center - offset;
            data1[(r * 2) + 1] = center + offset;
        }

        var (workers, cleanup) = CreateRingWorkers(worldSize: 2, totalElements: totalReductionFloats);

        try
        {
            var kmeans0 = new DistributedKMeans(k: 2, maxIterations: 30, tolerance: 1e-4f);
            var kmeans1 = new DistributedKMeans(k: 2, maxIterations: 30, tolerance: 1e-4f);

            float[] initialCentroids = new float[] { 8.0f, 8.0f, -8.0f, -8.0f };

            Task t0 = kmeans0.FitAsync(data0.AsMemory(), rowsPerNode, cols, workers[0], initialCentroids);
            Task t1 = kmeans1.FitAsync(data1.AsMemory(), rowsPerNode, cols, workers[1], initialCentroids);

            await Task.WhenAll(t0, t1);

            // Centroids on both nodes must be identical due to Ring AllReduce
            Assert.Equal(kmeans0.Centroids.Length, kmeans1.Centroids.Length);
            for (int i = 0; i < kmeans0.Centroids.Length; i++)
            {
                Assert.Equal(kmeans0.Centroids[i], kmeans1.Centroids[i], precision: 2);
            }

            // Test Predictions
            int predPositive = kmeans0.Predict(new float[] { 10.2f, 9.8f });
            int predNegative = kmeans0.Predict(new float[] { -9.9f, -10.1f });

            Assert.NotEqual(predPositive, predNegative);
        }
        finally
        {
            await cleanup();
        }
    }

    [Fact]
    public async Task DistributedKMeans_DataFrameInterface_TrainsAndPredicts()
    {
        int k = 2;
        int cols = 2;
        int rows = 30;
        int totalReductionFloats = (k * cols) + k;

        var f1Vals = new double?[rows];
        var f2Vals = new double?[rows];

        for (int i = 0; i < rows; i++)
        {
            double center = (i % 2 == 0) ? 20.0 : -20.0;
            f1Vals[i] = center + (i * 0.05);
            f2Vals[i] = center - (i * 0.05);
        }

        var df0 = new DataFrame(new ISeries[]
        {
            Float64Series.FromValues("x", f1Vals),
            Float64Series.FromValues("y", f2Vals)
        });

        var df1 = new DataFrame(new ISeries[]
        {
            Float64Series.FromValues("x", f1Vals),
            Float64Series.FromValues("y", f2Vals)
        });

        var (workers, cleanup) = CreateRingWorkers(worldSize: 2, totalElements: totalReductionFloats);

        try
        {
            var kmeans0 = new DistributedKMeans(k: 2, maxIterations: 20, tolerance: 1e-4f);
            var kmeans1 = new DistributedKMeans(k: 2, maxIterations: 20, tolerance: 1e-4f);

            float[] initialCentroids = new float[] { 15.0f, 15.0f, -15.0f, -15.0f };

            Task t0 = kmeans0.FitAsync(df0, workers[0], initialCentroids);
            Task t1 = kmeans1.FitAsync(df1, workers[1], initialCentroids);

            await Task.WhenAll(t0, t1);

            var predSeries = kmeans0.Predict(df0);
            Assert.Equal(rows, predSeries.Length);

            // Row 0 and Row 1 must belong to different clusters
            Assert.NotEqual(predSeries.Memory.Span[0], predSeries.Memory.Span[1]);
        }
        finally
        {
            await cleanup();
        }
    }
}
