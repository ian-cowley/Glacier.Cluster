namespace Glacier.Cluster.Shuffle;

using System;
using Glacier.Cluster.Protocol;

/// <summary>
/// Exception thrown when a requested partition cannot be found in the local or remote shuffle store.
/// </summary>
public sealed class PartitionNotFoundException : Exception
{
    public Guid JobId { get; }
    public int StageId { get; }
    public int PartitionId { get; }

    public PartitionNotFoundException(Guid jobId, int stageId, int partitionId)
        : base($"Partition not found: Job={jobId}, Stage={stageId}, Partition={partitionId}")
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
    }

    public PartitionNotFoundException(Guid jobId, int stageId, int partitionId, string message)
        : base(message)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
    }
}

/// <summary>
/// Exception thrown when a P2P partition fetch fails on the remote server.
/// </summary>
public sealed class PartitionFetchException : Exception
{
    public Guid JobId { get; }
    public int StageId { get; }
    public int PartitionId { get; }
    public PartitionFetchStatus Status { get; }

    public PartitionFetchException(Guid jobId, int stageId, int partitionId, PartitionFetchStatus status, string? message = null)
        : base(message ?? $"Partition fetch failed with status {status}: Job={jobId}, Stage={stageId}, Partition={partitionId}")
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
        Status = status;
    }
}
