namespace Glacier.Cluster.Simulator;

using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Storage.Arrow;

/// <summary>
/// Mode A Shuffle Client: Executes complete wire protocol over in-process DuplexPipes without sockets.
/// Exercises 32-byte framing, CRC32 verification, chunking, credit flow control, and reassembly.
/// </summary>
internal sealed class PipeShuffleClient : IArrowShuffleClient
{
    private readonly InMemoryShuffleBus _bus;
    private readonly SimulatedWorkerNode _localNode;
    private readonly ArrowShuffleClient _underlyingClient;
    private int _isDisposed;

    public PipeShuffleClient(InMemoryShuffleBus bus, SimulatedWorkerNode localNode)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _localNode = localNode ?? throw new ArgumentNullException(nameof(localNode));
        _underlyingClient = new ArrowShuffleClient();
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
            throw new InvalidOperationException($"Unable to resolve virtual target node {targetAddress}:{shufflePort}");
        }

        if (targetNode.IsAbruptlyKilled)
        {
            throw new InvalidOperationException($"Target node {targetNode.Hostname} is offline / crashed.");
        }

        if (_bus.CheckAndConsumeFetchFailure(targetNode.NodeId, stageId, partitionId))
        {
            throw new InvalidOperationException($"Simulated shuffle fetch failure for node {targetNode.NodeId}, stage {stageId}, partition {partitionId}.");
        }

        // 1. Create in-process pipe pair: client <-> server
        var (clientSidePipe, serverSidePipe) = DuplexPipeFactory.CreateInProcessPair();

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var ct = linkedCts.Token;

        // 2. Start server connection processing on target node's shuffle server
        var serverTask = Task.Run(async () =>
        {
            try
            {
                await targetNode.ShuffleServer.ProcessConnectionAsync(serverSidePipe, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch (Exception) { }
        }, ct);

        // 3. Client executes standard ArrowShuffleClient request over clientSidePipe
        try
        {
            return await _underlyingClient.FetchPartitionAsync(
                clientSidePipe,
                jobId,
                stageId,
                partitionId,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            linkedCts.Cancel();
            try { await serverTask.ConfigureAwait(false); } catch { }
        }
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
            throw new InvalidOperationException($"Unable to resolve producer node {producerNodeId}");
        }

        return await FetchPartitionAsync(
            targetNode.Hostname,
            targetNode.VirtualShufflePort,
            jobId,
            stageId,
            partitionId,
            cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        return _underlyingClient.FetchPartitionAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask<ArrowRecordBatch> FetchRecordBatchAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        return _underlyingClient.FetchRecordBatchAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask<DataFrame> FetchDataFrameAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        return _underlyingClient.FetchDataFrameAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        await _underlyingClient.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _underlyingClient.Dispose();
    }
}
