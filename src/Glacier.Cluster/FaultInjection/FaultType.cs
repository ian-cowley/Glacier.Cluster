namespace Glacier.Cluster.FaultInjection;

/// <summary>
/// Taxonomy of deterministic fault injection types for distributed cluster testing.
/// </summary>
public enum FaultType : byte
{
    /// <summary>Abruptly terminates worker process/daemon, severing coordinator connection without leave message.</summary>
    WorkerCrash = 1,

    /// <summary>Worker remains running but suspends heartbeat transmissions; tests Phi-accrual eviction.</summary>
    HeartbeatSilence = 2,

    /// <summary>Injects artificial latency into heartbeat frames to test suspect state without eviction.</summary>
    HeartbeatDelay = 3,

    /// <summary>Abruptly severs the underlying DuplexPipe/Socket mid-exchange with an IOException.</summary>
    TransportDisconnect = 4,

    /// <summary>Causes IArrowShuffleClient.FetchPartitionAsync to fail, simulating lost intermediate data.</summary>
    PartitionFetchFailure = 5,

    /// <summary>Corrupts frame CRC32 or partition bytes to test framing pipeline error handling.</summary>
    PartitionDataCorruption = 6,

    /// <summary>Injects deterministic delay into TaskExecutor operator kernel execution.</summary>
    TaskExecutionDelay = 7,

    /// <summary>Injects an unhandled exception into TaskExecutor to test application-level retries.</summary>
    TaskExecutionFailure = 8
}
