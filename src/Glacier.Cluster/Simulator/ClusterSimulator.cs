namespace Glacier.Cluster.Simulator;

using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.ML;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;

/// <summary>
/// High-performance in-memory cluster simulator managing 4 to 64 virtual worker nodes
/// and an authoritative coordinator within a single .NET 10 process.
/// Eliminates physical OS socket binding collisions and Windows loopback exhaustion.
/// </summary>
public sealed class ClusterSimulator : IAsyncDisposable, IDisposable
{
    private readonly ClusterSimulatorOptions _options;
    private readonly InMemoryShuffleBus _shuffleBus;
    private readonly List<SimulatedWorkerNode> _workers = new();
    private readonly List<IDuplexPipe> _workerPipes = new();
    private readonly List<IDuplexPipe> _coordPipes = new();

    private ClusterCoordinator? _coordinator;
    private bool _isRunning;
    private int _isDisposed;

    public int NodeCount => _options.NodeCount;
    public ClusterCoordinator Coordinator => _coordinator ?? throw new InvalidOperationException("Simulator is not booted.");
    public IReadOnlyList<SimulatedWorkerNode> Workers => _workers;
    public InMemoryShuffleBus ShuffleBus => _shuffleBus;
    public ClusterSimulatorOptions Options => _options;
    public bool IsRunning => _isRunning && Volatile.Read(ref _isDisposed) == 0;

    public ClusterSimulator(ClusterSimulatorOptions? options = null)
    {
        _options = options ?? new ClusterSimulatorOptions();
        _shuffleBus = new InMemoryShuffleBus();
    }

    /// <summary>
    /// Factory creating and booting an in-memory cluster with N virtual nodes.
    /// </summary>
    public static async Task<ClusterSimulator> CreateAsync(
        int nodeCount = 8,
        Action<ClusterSimulatorOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        var options = new ClusterSimulatorOptions { NodeCount = nodeCount };
        configure?.Invoke(options);

        var simulator = new ClusterSimulator(options);
        await simulator.BootAsync(cancellationToken).ConfigureAwait(false);
        return simulator;
    }

    /// <summary>
    /// Gracefully boots the coordinator and all virtual worker nodes.
    /// </summary>
    public async Task BootAsync(CancellationToken cancellationToken = default)
    {
        if (_isRunning) return;

        // 1. Initialize and start ClusterCoordinator
        _coordinator = new ClusterCoordinator(_options.CoordinatorOptions);
        await _coordinator.StartAsync(cancellationToken).ConfigureAwait(false);

        // 2. Initialize and boot virtual worker nodes
        for (int i = 0; i < _options.NodeCount; i++)
        {
            Guid workerId = Guid.NewGuid();
            var profile = SimulatorTopologyBuilder.CreateProfile(
                _options.TopologyStrategy,
                i,
                workerId,
                _options.CustomHardwareProfileFactory);

            var workerNode = new SimulatedWorkerNode(
                i,
                profile,
                _shuffleBus,
                _options.ShuffleMode,
                _options.WorkerHeartbeatInterval);

            _workers.Add(workerNode);

            var (workerSide, coordSide) = DuplexPipeFactory.CreateInProcessPair();
            _workerPipes.Add(workerSide);
            _coordPipes.Add(coordSide);

            await _coordinator.RegisterInProcessWorkerAsync(workerNode.NodeId, coordSide, cancellationToken).ConfigureAwait(false);
            await workerNode.StartAsync(workerSide, cancellationToken).ConfigureAwait(false);
        }

        _isRunning = true;
    }

    /// <summary>
    /// Gracefully shuts down all virtual workers and coordinator, verifying zero unmanaged leaks.
    /// </summary>
    public async Task ShutdownAsync(CancellationToken cancellationToken = default)
    {
        if (!_isRunning) return;
        _isRunning = false;

        foreach (var worker in _workers)
        {
            try
            {
                await worker.StopAsync(cancellationToken).ConfigureAwait(false);
            }
            catch { /* Ignore disconnect errors during teardown */ }
        }

        if (_coordinator != null)
        {
            await _coordinator.DisposeAsync().ConfigureAwait(false);
        }

        await _shuffleBus.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Submits an ExecutionDag directly to the cluster coordinator.
    /// </summary>
    public Task<JobCompletionMessage> SubmitJobAsync(
        ExecutionDag dag,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dag);
        if (_coordinator == null || !_isRunning)
            throw new InvalidOperationException("Simulator is not running.");

        return _coordinator.SubmitJobAsync(dag, cancellationToken);
    }

    /// <summary>
    /// Creates a DistributedDataFrame partitioned evenly across the simulated workers.
    /// </summary>
    public DistributedDataFrame CreateDataFrame(
        DataFrame sourceDataFrame,
        int? partitionCount = null,
        string? datasetName = null)
    {
        ArgumentNullException.ThrowIfNull(sourceDataFrame);

        int count = partitionCount ?? _options.NodeCount;
        if (count <= 0) count = 1;

        int totalRows = sourceDataFrame.RowCount;
        int rowsPerPartition = (totalRows + count - 1) / count;

        var partitions = new List<DataFrame>(count);
        for (int i = 0; i < count; i++)
        {
            int start = i * rowsPerPartition;
            int len = Math.Min(rowsPerPartition, Math.Max(0, totalRows - start));
            partitions.Add(sourceDataFrame.Slice(start, len));
        }

        return CreateDataFrame(partitions, datasetName);
    }

    /// <summary>
    /// Creates a DistributedDataFrame from pre-sliced partition DataFrames.
    /// </summary>
    public DistributedDataFrame CreateDataFrame(
        IReadOnlyList<DataFrame> partitions,
        string? datasetName = null)
    {
        ArgumentNullException.ThrowIfNull(partitions);
        if (partitions.Count == 0)
            throw new ArgumentException("Partitions collection cannot be empty.", nameof(partitions));

        string uri = $"memory://sim_dataset_{Guid.NewGuid():N}";
        var buffers = new List<RefCountedPartitionBuffer>(partitions.Count);
        for (int i = 0; i < partitions.Count; i++)
        {
            var buf = PolarisBridge.DataFrameToPartitionBuffer(partitions[i]);
            buffers.Add(buf);

            // Also register in worker's partition store for local zero-copy access if worker exists and is alive
            if (i < _workers.Count && !_workers[i].IsAbruptlyKilled)
            {
                try
                {
                    var workerBuf = PolarisBridge.DataFrameToPartitionBuffer(partitions[i]);
                    _workers[i].PartitionStore.RegisterPartition(new PartitionKey(Guid.Empty, 0, i), workerBuf);
                    workerBuf.Release();
                }
                catch (ObjectDisposedException)
                {
                    // Ignore if worker was concurrently terminated
                }
            }
        }

        InMemoryDistributedExecutionContext.RegisterDataSource(uri, buffers);
        foreach (var col in partitions[0].Columns)
        {
            InMemoryDistributedExecutionContext.RegisterDataSource(col.Name, buffers);
        }

        var execContext = new ClusterSimulatorExecutionContext(this, datasetName);
        foreach (var b in buffers)
        {
            b.Release();
        }

        var schema = partitions[0].ToGlacierRecordBatch().Schema;
        return DistributedDataFrameFactory.FromDataSource(
            uri,
            partitions.Count,
            schema,
            execContext,
            datasetName);
    }

    /// <summary>
    /// Executes distributed Ring AllReduce across the virtual workers in-process.
    /// </summary>
    public async Task RunRingAllReduceAsync(
        float[][] nodeBuffers,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(nodeBuffers);
        if (nodeBuffers.Length < 2)
            throw new ArgumentException("Ring AllReduce requires at least 2 node buffers.", nameof(nodeBuffers));

        int worldSize = nodeBuffers.Length;
        int totalElements = nodeBuffers[0].Length;

        var targetNodes = _workers.Take(worldSize).ToList();
        var (workers, cleanupAsync) = SimulatorRingWiring.WireInProcessRing(targetNodes, totalElements);

        try
        {
            Guid sessionId = Guid.NewGuid();
            var tasks = new Task[worldSize];

            for (int i = 0; i < worldSize; i++)
            {
                int rank = i;
                tasks[i] = Task.Run(async () =>
                {
                    await workers[rank].AllReduceAsync(nodeBuffers[rank], sessionId, cancellationToken).ConfigureAwait(false);
                }, cancellationToken);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        finally
        {
            await cleanupAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes distributed KMeans clustering across stationary data partitions held by virtual workers.
    /// </summary>
    public async Task<float[]> RunDistributedKMeansAsync(
        float[][] nodeFeatures,
        int rowsPerNode,
        int dimensions,
        int k,
        int maxIterations = 50,
        float tolerance = 1e-4f,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(nodeFeatures);
        int worldSize = nodeFeatures.Length;

        var targetNodes = _workers.Take(worldSize).ToList();
        int reductionSize = k * dimensions + k;
        var (ringWorkers, cleanupAsync) = SimulatorRingWiring.WireInProcessRing(targetNodes, reductionSize);

        try
        {
            var kmeansInstances = new DistributedKMeans[worldSize];
            for (int i = 0; i < worldSize; i++)
            {
                kmeansInstances[i] = new DistributedKMeans(k, maxIterations, tolerance);
            }

            // Shared initial centroids
            float[] initialCentroids = new float[k * dimensions];
            for (int c = 0; c < k; c++)
            {
                int srcIdx = (c % rowsPerNode) * dimensions;
                Array.Copy(nodeFeatures[0], srcIdx, initialCentroids, c * dimensions, dimensions);
            }

            var tasks = new Task[worldSize];
            for (int i = 0; i < worldSize; i++)
            {
                int rank = i;
                tasks[i] = Task.Run(async () =>
                {
                    await kmeansInstances[rank].FitAsync(
                        nodeFeatures[rank],
                        rowsPerNode,
                        dimensions,
                        ringWorkers[rank],
                        initialCentroids,
                        cancellationToken).ConfigureAwait(false);
                }, cancellationToken);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            return kmeansInstances[0].Centroids;
        }
        finally
        {
            await cleanupAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Fault injection: abruptly disconnects a virtual worker mid-job without sending NodeLeave.
    /// </summary>
    public void SimulateWorkerCrash(Guid workerNodeId)
    {
        var worker = _workers.FirstOrDefault(w => w.NodeId == workerNodeId);
        if (worker != null)
        {
            worker.KillAbruptly();
        }
    }

    /// <summary>
    /// Restores a previously crashed worker node and re-registers it with the coordinator.
    /// </summary>
    public async Task SimulateWorkerRecoverAsync(Guid workerNodeId, CancellationToken cancellationToken = default)
    {
        var worker = _workers.FirstOrDefault(w => w.NodeId == workerNodeId);
        if (worker == null || !worker.IsAbruptlyKilled) return;

        int index = worker.NodeIndex;
        var profile = worker.HardwareProfile;

        var recoveredNode = new SimulatedWorkerNode(
            index,
            profile,
            _shuffleBus,
            _options.ShuffleMode,
            _options.WorkerHeartbeatInterval);

        _workers[index] = recoveredNode;

        var (workerSide, coordSide) = DuplexPipeFactory.CreateInProcessPair();
        _workerPipes[index] = workerSide;
        _coordPipes[index] = coordSide;

        await Coordinator.RegisterInProcessWorkerAsync(recoveredNode.NodeId, coordSide, cancellationToken).ConfigureAwait(false);
        await recoveredNode.StartAsync(workerSide, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        await ShutdownAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
