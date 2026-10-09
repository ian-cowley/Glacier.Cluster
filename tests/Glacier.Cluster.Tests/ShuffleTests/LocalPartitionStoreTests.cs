namespace Glacier.Cluster.Tests.ShuffleTests;

using System;
using System.IO;
using System.Threading.Tasks;
using Glacier.Cluster.Shuffle;
using Xunit;

public sealed class LocalPartitionStoreTests : IDisposable
{
    private readonly Guid _nodeId = Guid.NewGuid();
    private readonly LocalPartitionStore _store;

    public LocalPartitionStoreTests()
    {
        _store = new LocalPartitionStore(_nodeId);
    }

    public void Dispose()
    {
        _store.Dispose();
    }

    [Fact]
    public async Task RegisterAndFetch_LocalPartition()
    {
        var jobId = Guid.NewGuid();
        var key = new PartitionKey(jobId, 1, 0);

        var originalBuffer = RefCountedPartitionBuffer.Allocate(256);
        for (int i = 0; i < originalBuffer.ByteLength; i++)
        {
            originalBuffer.Span[i] = (byte)(i + 1);
        }

        _store.RegisterPartition(key, originalBuffer);

        // Store retained it; caller can release its registration handle
        Assert.Equal(2, originalBuffer.ReferenceCount);
        originalBuffer.Release();
        Assert.Equal(1, originalBuffer.ReferenceCount);

        // Fetch from store
        var fetched = await _store.FetchPartitionAsync(key);
        Assert.NotNull(fetched);
        Assert.Equal(2, fetched.ReferenceCount); // 1 in store + 1 for caller
        Assert.Equal(256, fetched.ByteLength);
        Assert.Equal(1, fetched.Span[0]);
        Assert.Equal(256 % 256, fetched.Span[255]);

        fetched.Release();
        Assert.Equal(1, fetched.ReferenceCount);
    }

    [Fact]
    public async Task FetchNonExistent_ThrowsPartitionNotFoundException()
    {
        var key = new PartitionKey(Guid.NewGuid(), 1, 99);
        await Assert.ThrowsAsync<PartitionNotFoundException>(async () =>
        {
            await _store.FetchPartitionAsync(key);
        });
    }

    [Fact]
    public async Task AcknowledgeStageComplete_ReleasesStageBuffersOnly()
    {
        var jobId = Guid.NewGuid();
        var stage1Key = new PartitionKey(jobId, 1, 0);
        var stage2Key = new PartitionKey(jobId, 2, 0);

        var buf1 = RefCountedPartitionBuffer.Allocate(64);
        var buf2 = RefCountedPartitionBuffer.Allocate(64);

        _store.RegisterPartition(stage1Key, buf1);
        _store.RegisterPartition(stage2Key, buf2);

        buf1.Release();
        buf2.Release();

        // Acknowledge Stage 1 complete
        _store.AcknowledgeStageComplete(jobId, 1);

        // Stage 1 partition must be gone and disposed
        await Assert.ThrowsAsync<PartitionNotFoundException>(async () =>
        {
            await _store.FetchPartitionAsync(stage1Key);
        });
        Assert.True(buf1.IsDisposed);

        // Stage 2 partition must still be present and valid
        var fetched2 = await _store.FetchPartitionAsync(stage2Key);
        Assert.NotNull(fetched2);
        Assert.False(buf2.IsDisposed);
        fetched2.Release();
    }

    [Fact]
    public async Task AcknowledgeJobComplete_ReleasesAllJobBuffers()
    {
        var job1 = Guid.NewGuid();
        var job2 = Guid.NewGuid();

        var key1 = new PartitionKey(job1, 1, 0);
        var key2 = new PartitionKey(job2, 1, 0);

        var buf1 = RefCountedPartitionBuffer.Allocate(64);
        var buf2 = RefCountedPartitionBuffer.Allocate(64);

        _store.RegisterPartition(key1, buf1);
        _store.RegisterPartition(key2, buf2);

        buf1.Release();
        buf2.Release();

        _store.AcknowledgeJobComplete(job1);

        await Assert.ThrowsAsync<PartitionNotFoundException>(async () =>
        {
            await _store.FetchPartitionAsync(key1);
        });
        Assert.True(buf1.IsDisposed);

        var fetched2 = await _store.FetchPartitionAsync(key2);
        Assert.NotNull(fetched2);
        Assert.False(buf2.IsDisposed);
        fetched2.Release();
    }

    [Fact]
    public void ConcurrentFetch_MultiTenantIsolation()
    {
        var key = new PartitionKey(Guid.NewGuid(), 1, 0);
        var buffer = RefCountedPartitionBuffer.Allocate(512);
        buffer.Span.Fill(0xAB);

        _store.RegisterPartition(key, buffer);
        buffer.Release();

        const int readers = 20;

        Parallel.For(0, readers, _ =>
        {
            var fetched = _store.FetchPartitionAsync(key).AsTask().GetAwaiter().GetResult();
            Assert.Equal(512, fetched.ByteLength);
            Assert.Equal(0xAB, fetched.Span[0]);
            Assert.Equal(0xAB, fetched.Span[511]);
            fetched.Release();
        });

        // After all readers release, store should still hold exactly 1 reference
        Assert.Equal(1, buffer.ReferenceCount);
        Assert.False(buffer.IsDisposed);
    }

    [Fact]
    public void SpillCandidateSelection_ProtectsPinnedBuffers()
    {
        var jobId = Guid.NewGuid();
        var keyPinned = new PartitionKey(jobId, 1, 0);
        var keyUnpinned = new PartitionKey(jobId, 1, 1);

        var buf1 = RefCountedPartitionBuffer.Allocate(1000);
        var buf2 = RefCountedPartitionBuffer.Allocate(2000);

        _store.RegisterPartition(keyPinned, buf1);
        _store.RegisterPartition(keyUnpinned, buf2);

        buf2.Release(); // Unpinned: ref count == 1 (store only)

        // buf1 has ref count == 2 (pinned because of active simulated stream)
        var candidates = _store.GetSpillableCandidates(5000);

        Assert.Single(candidates);
        Assert.Equal(keyUnpinned, candidates[0].Key);

        buf1.Release();
    }

    [Fact]
    public async Task SpillStateTransition_AndReload()
    {
        var key = new PartitionKey(Guid.NewGuid(), 1, 0);
        var buffer = RefCountedPartitionBuffer.Allocate(128);
        for (int i = 0; i < buffer.ByteLength; i++)
        {
            buffer.Span[i] = (byte)(i * 2);
        }

        _store.RegisterPartition(key, buffer);
        buffer.Release();

        string tempPath = Path.Combine(Path.GetTempPath(), $"glacier_test_spill_{Guid.NewGuid():N}.arrow");
        try
        {
            // Write buffer to temp file
            File.WriteAllBytes(tempPath, buffer.Span.ToArray());

            // Transition to spilled in store
            bool spilled = _store.TransitionToSpilled(key, tempPath);
            Assert.True(spilled);
            Assert.True(buffer.IsDisposed); // In-memory buffer released
            Assert.Equal(128, _store.TotalSpilledBytes);
            Assert.Equal(0, _store.TotalInMemoryBytes);

            // Fetch partition: automatically reloads from disk
            var reloaded = await _store.FetchPartitionAsync(key);
            Assert.NotNull(reloaded);
            Assert.Equal(128, reloaded.ByteLength);
            Assert.False(reloaded.IsDisposed);

            for (int i = 0; i < reloaded.ByteLength; i++)
            {
                Assert.Equal((byte)(i * 2), reloaded.Span[i]);
            }

            reloaded.Release();
        }
        finally
        {
            if (File.Exists(tempPath)) File.Delete(tempPath);
        }
    }
}
