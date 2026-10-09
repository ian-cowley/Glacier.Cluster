namespace Glacier.Cluster.ML;

using System;
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;
using Glacier.Polaris;
using Glacier.Polaris.Data;

/// <summary>
/// Distributed, data-parallel KMeans clustering coordinator and worker engine.
/// Trains high-dimensional clusters over partitioned cluster data with zero raw data shuffling.
/// Aggregates intermediate centroid coordinates and counts via Ring AllReduce in 2(N-1) communication steps.
/// </summary>
public sealed class DistributedKMeans
{
    private readonly int _k;
    private readonly int _maxIterations;
    private readonly float _tolerance;
    private float[] _centroids = Array.Empty<float>();
    private float[] _centroidNormsSq = Array.Empty<float>();
    private int _dimensions;

    public int K => _k;
    public int MaxIterations => _maxIterations;
    public float Tolerance => _tolerance;
    public float[] Centroids => _centroids;
    public int Dimensions => _dimensions;

    public DistributedKMeans(int k = 8, int maxIterations = 100, float tolerance = 1e-4f)
    {
        if (k <= 0) throw new ArgumentOutOfRangeException(nameof(k), "K must be positive.");
        if (maxIterations <= 0) throw new ArgumentOutOfRangeException(nameof(maxIterations), "Max iterations must be positive.");
        _k = k;
        _maxIterations = maxIterations;
        _tolerance = tolerance;
    }

    /// <summary>
    /// Fits KMeans model across cluster workers holding stationary feature partitions in row-major memory.
    /// Aggregates partial centroid sums and sample counts via Ring AllReduce without transferring raw rows.
    /// </summary>
    public async Task FitAsync(
        ReadOnlyMemory<float> localFeatures,
        int rows,
        int cols,
        RingAllReduceWorker ringWorker,
        float[]? initialCentroids = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(ringWorker);
        if (rows <= 0) throw new ArgumentOutOfRangeException(nameof(rows), "Rows must be positive.");
        if (cols <= 0) throw new ArgumentOutOfRangeException(nameof(cols), "Columns must be positive.");
        if (localFeatures.Length < rows * cols) throw new ArgumentException("Feature memory too small for specified dimensions.", nameof(localFeatures));

        _dimensions = cols;
        int k = _k;

        // 1. Initialize Centroids (Seed from provided initial centroids or deterministic slice of local partition)
        _centroids = new float[k * cols];
        if (initialCentroids != null && initialCentroids.Length == k * cols)
        {
            Array.Copy(initialCentroids, _centroids, k * cols);
        }
        else
        {
            var featSpan = localFeatures.Span;
            for (int c = 0; c < k; c++)
            {
                int rowIdx = (c * (rows / k)) % rows;
                featSpan.Slice(rowIdx * cols, cols).CopyTo(_centroids.AsSpan(c * cols, cols));
            }
        }

        UpdateCentroidNorms();

        // 2. Prepare Reduction Vector Buffer: [ K * D centroid coords ][ K cluster counts ]
        int totalReductionFloats = (k * cols) + k;
        float[] reductionBuffer = new float[totalReductionFloats];

        int numThreads = Math.Min(Environment.ProcessorCount, Math.Max(1, rows / 512));
        float[][] threadCentroids = new float[numThreads][];
        int[][] threadCounts = new int[numThreads][];
        for (int t = 0; t < numThreads; t++)
        {
            threadCentroids[t] = new float[k * cols];
            threadCounts[t] = new int[k];
        }

        int[] assignments = new int[rows];

        // 3. Iterative Optimization Loop
        for (int iter = 0; iter < _maxIterations; iter++)
        {
            ct.ThrowIfCancellationRequested();

            // E-step: Multi-threaded local assignment using SIMD Euclidean distance
            int blockSize = rows < 2048 ? 512 : 1024;
            int numBlocks = (rows + blockSize - 1) / blockSize;

            Parallel.For(0, numBlocks, b =>
            {
                int startRow = b * blockSize;
                int endRow = Math.Min(startRow + blockSize, rows);
                var span = localFeatures.Span;

                for (int r = startRow; r < endRow; r++)
                {
                    ReadOnlySpan<float> sample = span.Slice(r * cols, cols);
                    assignments[r] = FindNearestCentroidSimd(sample, _centroids, k, cols);
                }
            });

            // Local M-step: Accumulate thread-local partial centroid sums and counts
            Parallel.For(0, numThreads, t =>
            {
                Array.Clear(threadCentroids[t]);
                Array.Clear(threadCounts[t]);

                int startRow = t * rows / numThreads;
                int endRow = (t == numThreads - 1) ? rows : (t + 1) * rows / numThreads;
                var span = localFeatures.Span;

                float[] lCentroids = threadCentroids[t];
                int[] lCounts = threadCounts[t];

                for (int r = startRow; r < endRow; r++)
                {
                    int cluster = assignments[r];
                    lCounts[cluster]++;
                    int offset = cluster * cols;
                    ReadOnlySpan<float> sample = span.Slice(r * cols, cols);

                    for (int d = 0; d < cols; d++)
                    {
                        lCentroids[offset + d] += sample[d];
                    }
                }
            });

            // Flatten thread buffers into reductionBuffer
            Array.Clear(reductionBuffer);
            for (int t = 0; t < numThreads; t++)
            {
                float[] tCent = threadCentroids[t];
                int[] tCnt = threadCounts[t];

                for (int i = 0; i < k * cols; i++)
                {
                    reductionBuffer[i] += tCent[i];
                }
                for (int c = 0; c < k; c++)
                {
                    reductionBuffer[(k * cols) + c] += tCnt[c];
                }
            }

            // 4. Collective AllReduce across cluster workers (aggregates sums and counts across nodes)
            Guid stepSessionId = Guid.NewGuid();
            await ringWorker.AllReduceAsync(reductionBuffer.AsMemory(), stepSessionId, ct).ConfigureAwait(false);

            // 5. Global Centroid Recomputation & Convergence Check
            float maxShift = 0f;
            for (int c = 0; c < k; c++)
            {
                float globalCount = Math.Max(1.0f, reductionBuffer[(k * cols) + c]);
                int offset = c * cols;
                float shift = 0f;

                for (int d = 0; d < cols; d++)
                {
                    float updated = reductionBuffer[offset + d] / globalCount;
                    float diff = updated - _centroids[offset + d];
                    shift += diff * diff;
                    _centroids[offset + d] = updated;
                }

                if (shift > maxShift) maxShift = shift;
            }

            UpdateCentroidNorms();

            if (maxShift < _tolerance)
            {
                break; // Converged across cluster
            }
        }
    }

    /// <summary>
    /// Fits KMeans model using a local Polaris DataFrame partition.
    /// Extracts all numeric columns into row-major contiguous memory for SIMD processing.
    /// </summary>
    public async Task FitAsync(
        DataFrame localDf,
        RingAllReduceWorker ringWorker,
        float[]? initialCentroids = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(localDf);
        int rows = localDf.RowCount;
        int cols = localDf.Columns.Count;

        float[] flatData = new float[rows * cols];
        for (int c = 0; c < cols; c++)
        {
            var col = localDf.Columns[c];
            if (col is Float32Series f32)
            {
                var span = f32.Memory.Span;
                for (int r = 0; r < rows; r++) flatData[(r * cols) + c] = span[r];
            }
            else if (col is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int r = 0; r < rows; r++) flatData[(r * cols) + c] = (float)span[r];
            }
            else if (col is Int32Series i32)
            {
                var span = i32.Memory.Span;
                for (int r = 0; r < rows; r++) flatData[(r * cols) + c] = (float)span[r];
            }
            else
            {
                for (int r = 0; r < rows; r++)
                {
                    object? val = col.Get(r);
                    flatData[(r * cols) + c] = val != null ? Convert.ToSingle(val) : 0f;
                }
            }
        }

        await FitAsync(flatData.AsMemory(), rows, cols, ringWorker, initialCentroids, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Predicts nearest cluster index for a given sample vector.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int PredictRow(ReadOnlySpan<float> sample)
    {
        return FindNearestCentroidSimd(sample, _centroids, _k, _dimensions);
    }

    /// <summary>
    /// Predicts nearest cluster index for a given sample vector.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Predict(ReadOnlySpan<float> sample) => PredictRow(sample);

    /// <summary>
    /// Predicts cluster assignments for a batch of samples in row-major memory.
    /// </summary>
    public void Predict(ReadOnlySpan<float> features, int rows, Span<int> assignments)
    {
        int cols = _dimensions;
        int k = _k;

        for (int r = 0; r < rows; r++)
        {
            ReadOnlySpan<float> sample = features.Slice(r * cols, cols);
            assignments[r] = FindNearestCentroidSimd(sample, _centroids, k, cols);
        }
    }

    /// <summary>
    /// Predicts cluster assignments for a batch of samples in row-major memory.
    /// </summary>
    public int[] Predict(ReadOnlySpan<float> features, int rows)
    {
        var result = new int[rows];
        Predict(features, rows, result);
        return result;
    }

    /// <summary>
    /// Predicts cluster assignments for a Polaris DataFrame partition.
    /// </summary>
    public Int32Series Predict(DataFrame df)
    {
        ArgumentNullException.ThrowIfNull(df);
        int rows = df.RowCount;
        int cols = _dimensions;
        float[] flatData = new float[rows * cols];

        for (int c = 0; c < Math.Min(cols, df.Columns.Count); c++)
        {
            var col = df.Columns[c];
            if (col is Float32Series f32)
            {
                var span = f32.Memory.Span;
                for (int r = 0; r < rows; r++) flatData[(r * cols) + c] = span[r];
            }
            else if (col is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int r = 0; r < rows; r++) flatData[(r * cols) + c] = (float)span[r];
            }
            else if (col is Int32Series i32)
            {
                var span = i32.Memory.Span;
                for (int r = 0; r < rows; r++) flatData[(r * cols) + c] = (float)span[r];
            }
            else
            {
                for (int r = 0; r < rows; r++)
                {
                    object? val = col.Get(r);
                    flatData[(r * cols) + c] = val != null ? Convert.ToSingle(val) : 0f;
                }
            }
        }

        var assignments = new int[rows];
        Predict(flatData, rows, assignments);
        return new Int32Series("cluster", assignments);
    }

    private void UpdateCentroidNorms()
    {
        int k = _k;
        int d = _dimensions;
        if (_centroidNormsSq.Length != k)
        {
            _centroidNormsSq = new float[k];
        }

        for (int c = 0; c < k; c++)
        {
            float sum = 0f;
            int offset = c * d;
            for (int dim = 0; dim < d; dim++)
            {
                float v = _centroids[offset + dim];
                sum += v * v;
            }
            _centroidNormsSq[c] = sum;
        }
    }

    /// <summary>
    /// Finds nearest centroid index using SIMD-accelerated squared Euclidean distance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe int FindNearestCentroidSimd(
        ReadOnlySpan<float> sample,
        ReadOnlySpan<float> centroids,
        int numCentroids,
        int dimensions)
    {
        float minDistance = float.MaxValue;
        int bestCluster = 0;

        fixed (float* pSample = sample)
        fixed (float* pCentroids = centroids)
        {
            for (int c = 0; c < numCentroids; c++)
            {
                float* pC = pCentroids + (c * dimensions);
                float distSq = 0f;
                int d = 0;

                // AVX-512: 16 floats per instruction
                if (Vector512.IsHardwareAccelerated && dimensions >= 16)
                {
                    var acc512 = Vector512<float>.Zero;
                    for (; d <= dimensions - 16; d += 16)
                    {
                        var vs = Vector512.Load(pSample + d);
                        var vc = Vector512.Load(pC + d);
                        var diff = Vector512.Subtract(vs, vc);
                        acc512 = Vector512.MultiplyAddEstimate(diff, diff, acc512);
                    }
                    distSq += Vector512.Sum(acc512);
                }

                // AVX2: 8 floats per instruction
                if (Vector256.IsHardwareAccelerated && d <= dimensions - 8)
                {
                    var acc256 = Vector256<float>.Zero;
                    for (; d <= dimensions - 8; d += 8)
                    {
                        var vs = Vector256.Load(pSample + d);
                        var vc = Vector256.Load(pC + d);
                        var diff = Vector256.Subtract(vs, vc);
                        acc256 = Vector256.MultiplyAddEstimate(diff, diff, acc256);
                    }
                    distSq += Vector256.Sum(acc256);
                }

                // SSE / Neon: 4 floats per instruction
                if (Vector128.IsHardwareAccelerated && d <= dimensions - 4)
                {
                    var acc128 = Vector128<float>.Zero;
                    for (; d <= dimensions - 4; d += 4)
                    {
                        var vs = Vector128.Load(pSample + d);
                        var vc = Vector128.Load(pC + d);
                        var diff = Vector128.Subtract(vs, vc);
                        acc128 = Vector128.MultiplyAddEstimate(diff, diff, acc128);
                    }
                    distSq += Vector128.Sum(acc128);
                }

                // Scalar remainder
                for (; d < dimensions; d++)
                {
                    float diff = pSample[d] - pC[d];
                    distSq += diff * diff;
                }

                if (distSq < minDistance)
                {
                    minDistance = distSq;
                    bestCluster = c;
                }
            }
        }

        return bestCluster;
    }
}
