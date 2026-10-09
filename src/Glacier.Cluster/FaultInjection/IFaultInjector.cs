namespace Glacier.Cluster.FaultInjection;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Authoritative fault injection contract for deterministic chaos testing of cluster resilience,
/// adaptive failure detection, dynamic task rescheduling, and functional DAG lineage recomputation.
/// </summary>
public interface IFaultInjector : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Abruptly halts the specified worker daemon without sending a graceful NodeLeaveMessage.
    /// Simulates power failure or SIGKILL.
    /// </summary>
    ValueTask CrashWorkerAsync(Guid workerNodeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Silences heartbeat transmissions from the specified worker for the specified duration.
    /// </summary>
    void SilenceWorkerHeartbeats(Guid workerNodeId, TimeSpan duration);

    /// <summary>
    /// Injects latency into worker heartbeat transmissions to test jitter resilience without false evictions.
    /// </summary>
    void DelayWorkerHeartbeats(Guid workerNodeId, TimeSpan latency);

    /// <summary>
    /// Simulates loss of an intermediate partition in a worker's LocalPartitionStore.
    /// </summary>
    bool InvalidateWorkerPartition(Guid workerNodeId, Guid jobId, int stageId, int partitionId);

    /// <summary>
    /// Arms a trigger to crash a worker the moment a specific stage or partition begins executing.
    /// </summary>
    void ArmMidTaskWorkerCrash(Guid workerNodeId, int targetStageId, int targetPartitionId);

    /// <summary>
    /// Registers an interception rule causing shuffle fetches matching the predicate to fail.
    /// </summary>
    void InjectShuffleFetchFailure(Guid producerNodeId, int stageId, int partitionId);

    /// <summary>
    /// Clears all active fault injection rules, restoring normal cluster operation.
    /// </summary>
    void Reset();
}
