namespace Glacier.Cluster.FaultInjection;

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Simulator;

/// <summary>
/// Deterministic chaos injection harness simulating node crashes mid-task, heartbeat silence/jitter,
/// transport disconnections, and intermediate partition fetch failures.
/// </summary>
public sealed class FaultInjector : IFaultInjector
{
    private readonly ClusterSimulator? _simulator;
    private readonly ConcurrentDictionary<Guid, FaultInjectingDuplexPipe> _workerPipes = new();
    private int _isDisposed;

    public FaultInjector(ClusterSimulator? simulator = null)
    {
        _simulator = simulator;
    }

    public void RegisterWorkerPipe(Guid workerNodeId, FaultInjectingDuplexPipe pipe)
    {
        ArgumentNullException.ThrowIfNull(pipe);
        _workerPipes[workerNodeId] = pipe;
    }

    public ValueTask CrashWorkerAsync(Guid workerNodeId, CancellationToken cancellationToken = default)
    {
        if (_simulator != null)
        {
            _simulator.SimulateWorkerCrash(workerNodeId);
        }
        else if (_workerPipes.TryGetValue(workerNodeId, out var pipe))
        {
            pipe.Sever();
        }

        return ValueTask.CompletedTask;
    }

    public void SilenceWorkerHeartbeats(Guid workerNodeId, TimeSpan duration)
    {
        if (_workerPipes.TryGetValue(workerNodeId, out var pipe))
        {
            pipe.SetSilenceHeartbeats(true);
            if (duration > TimeSpan.Zero)
            {
                Task.Delay(duration).ContinueWith(_ => pipe.SetSilenceHeartbeats(false));
            }
        }
        else if (_simulator != null)
        {
            var worker = _simulator.Workers.FirstOrDefault(w => w.NodeId == workerNodeId);
            if (worker != null)
            {
                worker.KillAbruptly();
            }
        }
    }

    public void DelayWorkerHeartbeats(Guid workerNodeId, TimeSpan latency)
    {
        if (_workerPipes.TryGetValue(workerNodeId, out var pipe))
        {
            pipe.SetHeartbeatDelay(latency);
        }
    }

    public bool InvalidateWorkerPartition(Guid workerNodeId, Guid jobId, int stageId, int partitionId)
    {
        if (_simulator != null)
        {
            var worker = _simulator.Workers.FirstOrDefault(w => w.NodeId == workerNodeId);
            if (worker != null)
            {
                worker.PartitionStore.AcknowledgeStageComplete(jobId, stageId);
                _simulator.ShuffleBus.InjectFetchFailure(workerNodeId, stageId, partitionId);
                return true;
            }
        }

        return false;
    }

    public void ArmMidTaskWorkerCrash(Guid workerNodeId, int targetStageId, int targetPartitionId)
    {
        InjectShuffleFetchFailure(workerNodeId, targetStageId, targetPartitionId);
    }

    public void InjectShuffleFetchFailure(Guid producerNodeId, int stageId, int partitionId)
    {
        if (_simulator != null)
        {
            _simulator.ShuffleBus.InjectFetchFailure(producerNodeId, stageId, partitionId);
        }
    }

    public void Reset()
    {
        foreach (var pipe in _workerPipes.Values)
        {
            pipe.SetSilenceHeartbeats(false);
            pipe.SetHeartbeatDelay(TimeSpan.Zero);
        }
        _simulator?.ShuffleBus.ClearFetchFailures();
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return ValueTask.CompletedTask;
        Reset();
        _workerPipes.Clear();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        Reset();
        _workerPipes.Clear();
    }
}
