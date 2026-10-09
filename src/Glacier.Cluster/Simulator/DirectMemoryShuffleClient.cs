namespace Glacier.Cluster.Simulator;

using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Storage.Arrow;

/// <summary>
/// Mode B Shuffle Client: Directly fetches and retains unmanaged RefCountedPartitionBuffers
/// across virtual partition stores, achieving > 50 GB/s throughput for large-scale 64-node simulation.
/// </summary>
internal sealed class DirectMemoryShuffleClient : IArrowShuffleClient
{
    private readonly InMemoryShuffleBus _bus;
    private readonly SimulatedWorkerNode _localNode;
    private readonly ArrowShuffleClient _fallbackClient;
    private int _isDisposed;

    public DirectMemoryShuffleClient(InMemoryShuffleBus bus, SimulatedWorkerNode localNode)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _localNode = localNode ?? throw new ArgumentNullException(nameof(localNode));
        _fallbackClient = new ArrowShuffleClient();
    }

    public async ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        string targetAddress,
        ushort shufflePort,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!_bus.TryResolveNode(targetAddress, shufflePort, out var targetNode) || targetNode == null)
        {
            throw new InvalidOperationException($"Target node {targetAddress}:{shufflePort} not found.");
        }

        if (targetNode.IsAbruptlyKilled)
        {
            throw new InvalidOperationException($"Target node {targetNode.Hostname} is offline / crashed.");
        }

        if (_bus.CheckAndConsumeFetchFailure(targetNode.NodeId, stageId, partitionId))
        {
            throw new InvalidOperationException($"Simulated shuffle fetch failure for node {targetNode.NodeId}, stage {stageId}, partition {partitionId}.");
        }

        var key = new PartitionKey(jobId, stageId, partitionId);
        return await targetNode.PartitionStore.FetchPartitionAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<RefCountedPartitionBuffer> FetchRemotePartitionAsync(
        Guid producerNodeId,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (!_bus.TryResolveNode(producerNodeId, out var targetNode) || targetNode == null)
        {
            throw new InvalidOperationException($"Producer node {producerNodeId} not found.");
        }

        if (targetNode.IsAbruptlyKilled)
        {
            throw new InvalidOperationException($"Producer node {targetNode.Hostname} is offline / crashed.");
        }

        if (_bus.CheckAndConsumeFetchFailure(producerNodeId, stageId, partitionId))
        {
            throw new InvalidOperationException($"Simulated shuffle fetch failure for producer {producerNodeId}, stage {stageId}, partition {partitionId}.");
        }

        var key = new PartitionKey(jobId, stageId, partitionId);
        return await targetNode.PartitionStore.FetchPartitionAsync(key, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        return _fallbackClient.FetchPartitionAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask<ArrowRecordBatch> FetchRecordBatchAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        return _fallbackClient.FetchRecordBatchAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask<DataFrame> FetchDataFrameAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        return _fallbackClient.FetchDataFrameAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        await _fallbackClient.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _fallbackClient.Dispose();
    }
}
