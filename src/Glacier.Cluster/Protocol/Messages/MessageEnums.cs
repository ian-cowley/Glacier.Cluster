namespace Glacier.Cluster.Protocol;

using System;

/// <summary>Status code returned in NodeJoinResponseMessage.</summary>
public enum NodeJoinStatus : byte
{
    Accepted = 0,
    Rejected = 1,
    VersionMismatch = 2,
    CapacityExceeded = 3,
    Unauthorized = 4
}

/// <summary>Type of distributed execution operator in StageDispatchMessage.</summary>
public enum StageOperatorType : int
{
    Scan = 1,
    Filter = 2,
    Project = 3,
    GroupByMap = 4,
    GroupByReduce = 5,
    HashJoinBuild = 6,
    HashJoinProbe = 7,
    Sort = 8,
    KMeansLocal = 9,
    AllReduceRing = 10,
    Custom = 99
}

/// <summary>Execution status of a distributed task partition in TaskStatusUpdateMessage.</summary>
public enum TaskExecutionStatus : byte
{
    Pending = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
    Cancelled = 4
}

/// <summary>Status of a P2P partition fetch in PartitionFetchResponseMessage.</summary>
public enum PartitionFetchStatus : byte
{
    Success = 0,
    NotFound = 1,
    SpilledToDisk = 2,
    NotReadyYet = 3,
    Error = 4
}

/// <summary>Flags decorating partition data in PartitionFetchResponseMessage.</summary>
[Flags]
public enum PartitionDataFlags : byte
{
    None = 0,
    ArrowIpcBatch = 1 << 0,
    Compressed = 1 << 1,
    FinalChunk = 1 << 2
}

/// <summary>Outcome of a distributed job in JobCompletionMessage.</summary>
public enum JobCompletionStatus : byte
{
    Succeeded = 0,
    Failed = 1,
    Cancelled = 2,
    TimedOut = 3
}
