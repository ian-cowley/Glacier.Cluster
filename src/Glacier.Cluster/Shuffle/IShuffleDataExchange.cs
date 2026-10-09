namespace Glacier.Cluster.Shuffle;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Core contract for distributed shuffle data exchange. Facilitates local partition
/// registration, inter-node partition fetching, and stage/job resource acknowledgment.
/// </summary>
public interface IShuffleDataExchange : IDisposable
{
    /// <summary>
    /// Registers a locally computed partition buffer into the shuffle exchange.
    /// The exchange retains the buffer; caller may release its own handle.
    /// </summary>
    ValueTask RegisterPartitionAsync(
        Guid jobId,
        int stageId,
        int partitionId,
        RefCountedPartitionBuffer buffer,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Fetches a partition from the exchange. If producerNodeId is local (or Guid.Empty),
    /// retrieves directly from local unmanaged storage without network overhead;
    /// if remote, streams partition via peer-to-peer Arrow IPC over System.IO.Pipelines.
    /// Caller acquires ownership of the returned buffer and must call buffer.Release() / Dispose() when done.
    /// </summary>
    ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        Guid producerNodeId,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges completion of a stage, releasing all partition buffers registered
    /// for that stage and deleting any spilled disk files.
    /// </summary>
    ValueTask AcknowledgeStageCompleteAsync(
        Guid jobId,
        int stageId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges completion of an entire job, purging all stages and cleaning up all buffers.
    /// </summary>
    ValueTask AcknowledgeJobCompleteAsync(
        Guid jobId,
        CancellationToken cancellationToken = default);
}
