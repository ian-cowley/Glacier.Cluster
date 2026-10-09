namespace Glacier.Cluster.FaultInjection;

using System;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Storage.Arrow;

/// <summary>
/// Shuffle client proxy injecting deterministic partition fetch failures and simulated data loss.
/// </summary>
public sealed class FaultInjectingShuffleClient : IArrowShuffleClient
{
    private readonly IArrowShuffleClient _inner;
    private readonly ConcurrentDictionary<(Guid JobId, int StageId, int PartitionId), Exception> _failureTriggers = new();
    private readonly ConcurrentDictionary<(int StageId, int PartitionId), Exception> _stagePartitionTriggers = new();
    private readonly ConcurrentDictionary<(Guid ProducerNodeId, int StageId, int PartitionId), Exception> _producerTriggers = new();

    public FaultInjectingShuffleClient(IArrowShuffleClient inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public void ArmFetchFailure(Guid jobId, int stageId, int partitionId, Exception ex)
    {
        _failureTriggers[(jobId, stageId, partitionId)] = ex;
    }

    public void ArmFetchFailure(int stageId, int partitionId, Exception ex)
    {
        _stagePartitionTriggers[(stageId, partitionId)] = ex;
    }

    public void ArmProducerFetchFailure(Guid producerNodeId, int stageId, int partitionId, Exception ex)
    {
        _producerTriggers[(producerNodeId, stageId, partitionId)] = ex;
    }

    public void Reset()
    {
        _failureTriggers.Clear();
        _stagePartitionTriggers.Clear();
        _producerTriggers.Clear();
    }

    public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        string targetAddress,
        ushort shufflePort,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (_failureTriggers.TryRemove((jobId, stageId, partitionId), out var ex) ||
            _stagePartitionTriggers.TryRemove((stageId, partitionId), out ex))
        {
            return ValueTask.FromException<RefCountedPartitionBuffer>(ex);
        }

        return _inner.FetchPartitionAsync(targetAddress, shufflePort, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (_failureTriggers.TryRemove((jobId, stageId, partitionId), out var ex) ||
            _stagePartitionTriggers.TryRemove((stageId, partitionId), out ex))
        {
            return ValueTask.FromException<RefCountedPartitionBuffer>(ex);
        }

        return _inner.FetchPartitionAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask<RefCountedPartitionBuffer> FetchRemotePartitionAsync(
        Guid producerNodeId,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (_producerTriggers.TryRemove((producerNodeId, stageId, partitionId), out var ex) ||
            _failureTriggers.TryRemove((jobId, stageId, partitionId), out ex) ||
            _stagePartitionTriggers.TryRemove((stageId, partitionId), out ex))
        {
            return ValueTask.FromException<RefCountedPartitionBuffer>(ex);
        }

        return _inner.FetchRemotePartitionAsync(producerNodeId, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask<ArrowRecordBatch> FetchRecordBatchAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        return _inner.FetchRecordBatchAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask<DataFrame> FetchDataFrameAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        return _inner.FetchDataFrameAsync(pipe, jobId, stageId, partitionId, cancellationToken);
    }

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
    public void Dispose() => _inner.Dispose();
}
