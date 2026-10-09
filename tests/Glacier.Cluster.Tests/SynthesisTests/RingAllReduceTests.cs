namespace Glacier.Cluster.Tests.SynthesisTests;

using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;
using Glacier.Cluster.Protocol;
using Xunit;

public sealed class RingAllReduceTests
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

    [Fact]
    public void VectorReductionKernels_AccumulateInPlace_VerifiesCorrectness()
    {
        int[] testLengths = { 1, 7, 8, 15, 16, 31, 32, 63, 64, 100, 1024, 4097 };

        foreach (int len in testLengths)
        {
            float[] target = new float[len];
            float[] source = new float[len];
            float[] expected = new float[len];

            for (int i = 0; i < len; i++)
            {
                target[i] = i * 1.5f;
                source[i] = (i + 1) * 2.25f;
                expected[i] = target[i] + source[i];
            }

            VectorReductionKernels.AddInPlace(target, source);

            for (int i = 0; i < len; i++)
            {
                Assert.Equal(expected[i], target[i], precision: 4);
            }
        }
    }

    [Fact]
    public void RingAllReduceCoordinator_OrderNodesTopologyAware_OrdersCorrectly()
    {
        var node1 = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());
        var node2 = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());
        var node3 = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());

        var candidateNodes = new[] { node1, node2, node3 };
        var ordered = RingAllReduceCoordinator.OrderNodesTopologyAware(candidateNodes);

        Assert.Equal(3, ordered.Count);
        Assert.Contains(node1, ordered);
        Assert.Contains(node2, ordered);
        Assert.Contains(node3, ordered);
    }

    [Theory]
    [InlineData(2, 64)]
    [InlineData(4, 128)]
    [InlineData(8, 256)]
    public async Task RingAllReduce_ScatterReduceAndAllGather_ReachesExactSum(int worldSize, int totalElements)
    {
        var nodeIds = Enumerable.Range(0, worldSize).Select(_ => Guid.NewGuid()).ToArray();
        var pipes = Enumerable.Range(0, worldSize).Select(_ => new Pipe()).ToArray();
        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int nextIdx = (i + 1) % worldSize;
            int prevIdx = (i - 1 + worldSize) % worldSize;

            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), pipes[i].Writer);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodeIds[i],
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        // Initialize buffers: Node i has buffer initialized to (i + 1) * 0.5f per element
        float[][] buffers = new float[worldSize][];
        float[] expectedSum = new float[totalElements];

        for (int i = 0; i < worldSize; i++)
        {
            buffers[i] = new float[totalElements];
            float val = (i + 1) * 0.5f;
            for (int e = 0; e < totalElements; e++)
            {
                buffers[i][e] = val;
                expectedSum[e] += val;
            }
        }

        Guid sessionId = Guid.NewGuid();
        Task[] allReduceTasks = new Task[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int rank = i;
            allReduceTasks[i] = Task.Run(async () =>
            {
                await workers[rank].AllReduceAsync(buffers[rank], sessionId);
            });
        }

        await Task.WhenAll(allReduceTasks);

        // Assert all nodes have identical, exact sums
        for (int i = 0; i < worldSize; i++)
        {
            for (int e = 0; e < totalElements; e++)
            {
                Assert.Equal(expectedSum[e], buffers[i][e], precision: 4);
            }
        }

        for (int i = 0; i < worldSize; i++)
        {
            await workers[i].DisposeAsync();
        }
    }
}
