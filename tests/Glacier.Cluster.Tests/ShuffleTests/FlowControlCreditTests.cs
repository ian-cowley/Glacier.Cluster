namespace Glacier.Cluster.Tests.ShuffleTests;

using System;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Shuffle;
using Xunit;

public sealed class FlowControlCreditTests
{
    [Fact]
    public void WindowSizing_DefaultOptions()
    {
        using var manager = new FlowControlCreditManager();

        Assert.Equal(4u, manager.Options.DefaultCreditTokensPerFetch);
        Assert.Equal(65536u, manager.Options.MaxChunkSizeBytes);
        Assert.Equal(32, manager.Options.MaxGlobalConcurrentStreams);
        Assert.Equal(4, manager.Options.MaxStreamsPerPeer);
        Assert.Equal(32, manager.AvailableGlobalCredits);
    }

    [Fact]
    public async Task ConsumerLeaseAcquisition_AndRelease()
    {
        var options = new FlowControlOptions { MaxGlobalConcurrentStreams = 10, MaxStreamsPerPeer = 3 };
        using var manager = new FlowControlCreditManager(options);
        var peerId = Guid.NewGuid();

        Assert.Equal(10, manager.AvailableGlobalCredits);

        using (var lease = await manager.AcquireConsumerFetchLeaseAsync(peerId))
        {
            Assert.Equal(9, manager.AvailableGlobalCredits);
        }

        Assert.Equal(10, manager.AvailableGlobalCredits);
    }

    [Fact]
    public async Task ConsumerPeerLimit_EnforcesBackpressure()
    {
        var options = new FlowControlOptions { MaxGlobalConcurrentStreams = 10, MaxStreamsPerPeer = 2 };
        using var manager = new FlowControlCreditManager(options);
        var peerId = Guid.NewGuid();

        var lease1 = await manager.AcquireConsumerFetchLeaseAsync(peerId);
        var lease2 = await manager.AcquireConsumerFetchLeaseAsync(peerId);

        // Third acquisition must block
        using var cts = new CancellationTokenSource(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await manager.AcquireConsumerFetchLeaseAsync(peerId, cts.Token);
        });

        // Release one lease; now acquire should succeed immediately
        lease1.Dispose();

        var lease3 = await manager.AcquireConsumerFetchLeaseAsync(peerId);

        lease2.Dispose();
        lease3.Dispose();
    }

    [Fact]
    public async Task GlobalLimit_EnforcesBackpressureAcrossMultiplePeers()
    {
        var options = new FlowControlOptions { MaxGlobalConcurrentStreams = 2, MaxStreamsPerPeer = 2 };
        using var manager = new FlowControlCreditManager(options);

        var peer1 = Guid.NewGuid();
        var peer2 = Guid.NewGuid();
        var peer3 = Guid.NewGuid();

        var lease1 = await manager.AcquireConsumerFetchLeaseAsync(peer1);
        var lease2 = await manager.AcquireConsumerFetchLeaseAsync(peer2);

        Assert.Equal(0, manager.AvailableGlobalCredits);

        using var cts = new CancellationTokenSource(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await manager.AcquireConsumerFetchLeaseAsync(peer3, cts.Token);
        });

        lease1.Dispose();
        var lease3 = await manager.AcquireConsumerFetchLeaseAsync(peer3);

        lease2.Dispose();
        lease3.Dispose();
        Assert.Equal(2, manager.AvailableGlobalCredits);
    }

    [Fact]
    public async Task ProducerCredits_GrantAndConsume()
    {
        using var manager = new FlowControlCreditManager();
        var peerId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        int stageId = 1;
        int partitionId = 0;

        manager.GrantProducerCredits(peerId, jobId, stageId, partitionId, 2);

        Assert.Equal(2, manager.GetAvailableCredits(peerId, jobId, stageId, partitionId));

        // Consume first credit
        await manager.WaitForProducerCreditAsync(peerId, jobId, stageId, partitionId);
        Assert.Equal(1, manager.GetAvailableCredits(peerId, jobId, stageId, partitionId));

        // Consume second credit
        await manager.WaitForProducerCreditAsync(peerId, jobId, stageId, partitionId);
        Assert.Equal(0, manager.GetAvailableCredits(peerId, jobId, stageId, partitionId));

        // Third consume should block
        using var cts = new CancellationTokenSource(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await manager.WaitForProducerCreditAsync(peerId, jobId, stageId, partitionId, cts.Token);
        });
    }

    [Fact]
    public async Task ProducerCredits_ReplenishOnAcknowledgment_UnblocksWaitingProducer()
    {
        using var manager = new FlowControlCreditManager();
        var peerId = Guid.NewGuid();
        var jobId = Guid.NewGuid();
        int stageId = 1;
        int partitionId = 0;

        // Producer starts with 0 credits
        Assert.Equal(0, manager.GetAvailableCredits(peerId, jobId, stageId, partitionId));

        // Background task replenishes credits after 30ms
        _ = Task.Run(async () =>
        {
            await Task.Delay(30);
            manager.ReplenishProducerCredits(peerId, jobId, stageId, partitionId, 1);
        });

        using var cts = new CancellationTokenSource(5000);
        await manager.WaitForProducerCreditAsync(peerId, jobId, stageId, partitionId, cts.Token);

        Assert.Equal(0, manager.GetAvailableCredits(peerId, jobId, stageId, partitionId));
    }
}
