namespace Glacier.Cluster.Simulator;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;

/// <summary>
/// Distributed execution context routing DistributedDataFrame DAGs directly through ClusterSimulator.
/// </summary>
public sealed class ClusterSimulatorExecutionContext : IDistributedExecutionContext, IDisposable
{
    private readonly ClusterSimulator _simulator;
    private readonly string? _datasetName;

    public ClusterSimulatorExecutionContext(
        ClusterSimulator simulator,
        string? datasetName = null)
    {
        _simulator = simulator ?? throw new ArgumentNullException(nameof(simulator));
        _datasetName = datasetName;
    }

    public async ValueTask<IReadOnlyList<RefCountedPartitionBuffer>> ExecuteDagAsync(
        ExecutionDag dag,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dag);

        // 1. Submit DAG to ClusterCoordinator
        var completion = await _simulator.SubmitJobAsync(dag, cancellationToken).ConfigureAwait(false);
        if (completion.Status != JobCompletionStatus.Succeeded)
        {
            throw new InvalidOperationException($"Simulated cluster job failed: {completion.ErrorMessage}");
        }

        // 2. Identify leaf stage(s) and collect output partition buffers across workers
        var leafStage = dag.LeafStages[0];
        int partitionCount = leafStage.PartitionsCount;
        var resultBuffers = new List<RefCountedPartitionBuffer>(partitionCount);

        for (int p = 0; p < partitionCount; p++)
        {
            var task = leafStage.Tasks.FirstOrDefault(t => t.PartitionId == p);
            if (task == null || task.AssignedNodeId == null)
            {
                throw new InvalidOperationException($"Leaf task partition {p} has no assigned node.");
            }

            var worker = _simulator.Workers.FirstOrDefault(w => w.NodeId == task.AssignedNodeId.Value);
            if (worker == null)
            {
                throw new InvalidOperationException($"Worker with ID {task.AssignedNodeId.Value} not found in simulator.");
            }

            var key = new PartitionKey(dag.JobId, leafStage.StageId, p);
            var buffer = await worker.PartitionStore.FetchPartitionAsync(key, cancellationToken).ConfigureAwait(false);

            if (dag.JobName.Contains("_Select") && !string.IsNullOrEmpty(_datasetName) && _datasetName.Contains(','))
            {
                string[] cols = _datasetName.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                var df = PolarisBridge.PartitionBufferToDataFrame(buffer, zeroCopy: false);
                var projDf = df.Select(cols);
                var projBuffer = PolarisBridge.DataFrameToPartitionBuffer(projDf);
                buffer.Release();
                buffer = projBuffer;
            }

            resultBuffers.Add(buffer);
        }

        return resultBuffers;
    }

    public ValueTask<RefCountedPartitionBuffer> ExecuteTaskAsync(
        TaskDescriptor task,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Individual task evaluation should run via ExecutionDag.");
    }

    public void Dispose()
    {
    }
}
