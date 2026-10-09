namespace Glacier.Cluster.Shuffle;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Unified implementation of <see cref="IShuffleDataExchange"/> routing partition requests
/// either to the local in-memory store or remote cluster workers via P2P Arrow IPC.
/// </summary>
public sealed class ShuffleDataExchange : IShuffleDataExchange
{
    private readonly LocalPartitionStore _localStore;
    private readonly IShuffleRemoteClient? _remoteClient;
    private int _disposed;

    public Guid LocalNodeId => _localStore.LocalNodeId;
    public LocalPartitionStore LocalStore => _localStore;

    public ShuffleDataExchange(LocalPartitionStore localStore, IShuffleRemoteClient? remoteClient = null)
    {
        _localStore = localStore ?? throw new ArgumentNullException(nameof(localStore));
        _remoteClient = remoteClient;
    }

    public ValueTask RegisterPartitionAsync(
        Guid jobId,
        int stageId,
        int partitionId,
        RefCountedPartitionBuffer buffer,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var key = new PartitionKey(jobId, stageId, partitionId);
        _localStore.RegisterPartition(key, buffer);
        return ValueTask.CompletedTask;
    }

    public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        Guid producerNodeId,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var key = new PartitionKey(jobId, stageId, partitionId);

        // Local Fetch Optimization: zero network roundtrip
        if (producerNodeId == Guid.Empty || producerNodeId == _localStore.LocalNodeId)
        {
            return _localStore.FetchPartitionAsync(key, cancellationToken);
        }

        // Remote Fetch: delegate to P2P ArrowShuffleClient
        if (_remoteClient == null)
        {
            throw new InvalidOperationException($"Cannot fetch remote partition from node {producerNodeId}: No remote shuffle client configured.");
        }

        return _remoteClient.FetchRemotePartitionAsync(producerNodeId, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask AcknowledgeStageCompleteAsync(
        Guid jobId,
        int stageId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        _localStore.AcknowledgeStageComplete(jobId, stageId);
        return ValueTask.CompletedTask;
    }

    public ValueTask AcknowledgeJobCompleteAsync(
        Guid jobId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        _localStore.AcknowledgeJobComplete(jobId);
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _localStore.Dispose();
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
    }
}
