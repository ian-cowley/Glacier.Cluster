namespace Glacier.Cluster.Simulator;

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;

/// <summary>
/// Utility setting up in-process ring topology for RingAllReduce across virtual cluster worker nodes.
/// </summary>
public static class SimulatorRingWiring
{
    private sealed class DuplexPipePair : IDuplexPipe
    {
        public PipeReader Input { get; }
        public PipeWriter Output { get; }

        public DuplexPipePair(PipeReader input, PipeWriter output)
        {
            Input = input;
            Output = output;
        }
    }

    /// <summary>
    /// Wires an in-process ring of RingAllReduceWorkers from the provided simulated nodes.
    /// </summary>
    public static (RingAllReduceWorker[] Workers, Func<Task> CleanupAsync) WireInProcessRing(
        IReadOnlyList<SimulatedWorkerNode> nodes,
        int totalElements)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        int worldSize = nodes.Count;
        if (worldSize < 2)
            throw new ArgumentException("Ring AllReduce requires at least 2 nodes.", nameof(nodes));

        var pipes = new Pipe[worldSize];
        for (int i = 0; i < worldSize; i++)
        {
            pipes[i] = new Pipe();
        }

        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int prevIdx = (i - 1 + worldSize) % worldSize;

            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), pipes[i].Writer);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodes[i].NodeId,
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        async Task CleanupAsync()
        {
            for (int i = 0; i < worldSize; i++)
            {
                await workers[i].DisposeAsync().ConfigureAwait(false);
            }
        }

        return (workers, CleanupAsync);
    }
}
