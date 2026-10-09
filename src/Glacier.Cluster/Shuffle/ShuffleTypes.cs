namespace Glacier.Cluster.Shuffle;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Uniquely identifies a single partition within a distributed stage.
/// </summary>
public readonly record struct PartitionKey(Guid JobId, int StageId, int PartitionId) : IEquatable<PartitionKey>
{
    public override string ToString() => $"[Job: {JobId}, Stage: {StageId}, Partition: {PartitionId}]";
}

/// <summary>
/// Residency state of a partition registered in the local shuffle store.
/// </summary>
public enum PartitionStorageState : byte
{
    /// <summary>Partition is resident in 64-byte aligned unmanaged memory.</summary>
    InMemory = 0,

    /// <summary>Partition write to NVMe scratch file is currently in progress.</summary>
    Spilling = 1,

    /// <summary>Partition has been spilled to local NVMe scratch disk and memory has been reclaimed.</summary>
    SpilledToDisk = 2,

    /// <summary>Partition has been acknowledged and evicted.</summary>
    Evicted = 3,

    /// <summary>Partition was not found in the local store.</summary>
    NotFound = 4
}

/// <summary>
/// Metadata describing a partition spilled to NVMe disk storage.
/// </summary>
public sealed record SpilledPartitionMetadata(
    Guid JobId,
    int StageId,
    int PartitionId,
    string FilePath,
    long ByteLength,
    DateTime SpilledAtUtc);

/// <summary>
/// Bridge interface allowing shuffle data exchange to invoke remote partition fetches
/// without direct circular coupling.
/// </summary>
public interface IShuffleRemoteClient
{
    /// <summary>
    /// Fetches a remote partition from the specified producer node over P2P Arrow IPC.
    /// </summary>
    ValueTask<RefCountedPartitionBuffer> FetchRemotePartitionAsync(
        Guid producerNodeId,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default);
}
