namespace Glacier.Cluster.Tests.ShuffleTests;

using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Glacier.Cluster.Shuffle;
using Xunit;

public sealed class DiskSpillManagerTests : IAsyncDisposable
{
    private readonly string _testScratchDir;
    private readonly DiskSpillManager _spillManager;

    public DiskSpillManagerTests()
    {
        _testScratchDir = Path.Combine(Path.GetTempPath(), $"glacier_spill_tests_{Guid.NewGuid():N}");
        _spillManager = new DiskSpillManager(
            scratchDirectory: _testScratchDir,
            maxMemoryBytes: 10_000,
            highWatermark: 0.80,
            lowWatermark: 0.60);
    }

    public async ValueTask DisposeAsync()
    {
        await _spillManager.DisposeAsync();
        try
        {
            if (Directory.Exists(_testScratchDir))
            {
                Directory.Delete(_testScratchDir, recursive: true);
            }
        }
        catch { }
    }

    [Fact]
    public void MemoryThresholdTrigger_CalculatesHighPressure()
    {
        Assert.False(_spillManager.IsMemoryPressureHigh(7_999));
        Assert.True(_spillManager.IsMemoryPressureHigh(8_000));
        Assert.True(_spillManager.IsMemoryPressureHigh(9_500));
    }

    [Fact]
    public async Task SpillPartitionAsync_WritesAtomicScratchFile_AndReleasesMemory()
    {
        var jobId = Guid.NewGuid();
        int stageId = 1;
        int partitionId = 0;

        var buffer = RefCountedPartitionBuffer.Allocate(1024);
        buffer.Span.Fill(0x5A);

        var meta = await _spillManager.SpillPartitionAsync(jobId, stageId, partitionId, buffer);

        Assert.NotNull(meta);
        Assert.Equal(1024, meta.ByteLength);
        Assert.True(File.Exists(meta.FilePath));
        Assert.EndsWith(".arrow", meta.FilePath);

        // Memory buffer was atomically released upon successful spill
        Assert.True(buffer.IsDisposed);
        Assert.Equal(1, _spillManager.SpilledPartitionCount);
        Assert.Equal(PartitionStorageState.SpilledToDisk, _spillManager.GetPartitionState(jobId, stageId, partitionId));
    }

    [Fact]
    public async Task ReloadPartitionAsync_MatchesOriginalData()
    {
        var jobId = Guid.NewGuid();
        int stageId = 2;
        int partitionId = 1;

        var original = RefCountedPartitionBuffer.Allocate(512);
        for (int i = 0; i < original.ByteLength; i++)
        {
            original.Span[i] = (byte)(i % 179);
        }

        await _spillManager.SpillPartitionAsync(jobId, stageId, partitionId, original);

        using var reloaded = await _spillManager.ReloadPartitionAsync(jobId, stageId, partitionId);

        Assert.NotNull(reloaded);
        Assert.Equal(512, reloaded.ByteLength);
        Assert.True((ulong)reloaded.Pointer % 64 == 0, "Reloaded buffer must be 64-byte aligned.");

        for (int i = 0; i < reloaded.ByteLength; i++)
        {
            Assert.Equal((byte)(i % 179), reloaded.Span[i]);
        }
    }

    [Fact]
    public async Task StreamSpilledPartitionAsync_DirectlyIntoPipeWriter()
    {
        var jobId = Guid.NewGuid();
        int stageId = 3;
        int partitionId = 0;

        var original = RefCountedPartitionBuffer.Allocate(2048);
        for (int i = 0; i < original.ByteLength; i++)
        {
            original.Span[i] = (byte)(i & 0xFF);
        }

        await _spillManager.SpillPartitionAsync(jobId, stageId, partitionId, original);

        var pipe = new Pipe();

        // Stream spilled file into pipe writer
        await _spillManager.StreamSpilledPartitionAsync(jobId, stageId, partitionId, pipe.Writer);
        await pipe.Writer.CompleteAsync();

        // Read back from pipe reader
        var readResult = await pipe.Reader.ReadAsync();
        var sequence = readResult.Buffer;

        Assert.Equal(2048, sequence.Length);
        byte[] readBytes = sequence.ToArray();

        for (int i = 0; i < 2048; i++)
        {
            Assert.Equal((byte)(i & 0xFF), readBytes[i]);
        }

        pipe.Reader.AdvanceTo(sequence.End);
    }

    [Fact]
    public async Task CleanupStage_RemovesStageFilesOnly()
    {
        var jobId = Guid.NewGuid();

        var buf1 = RefCountedPartitionBuffer.Allocate(256);
        var buf2 = RefCountedPartitionBuffer.Allocate(256);

        var meta1 = await _spillManager.SpillPartitionAsync(jobId, 1, 0, buf1);
        var meta2 = await _spillManager.SpillPartitionAsync(jobId, 2, 0, buf2);

        Assert.True(File.Exists(meta1.FilePath));
        Assert.True(File.Exists(meta2.FilePath));

        _spillManager.CleanupStage(jobId, 1);

        Assert.False(File.Exists(meta1.FilePath));
        Assert.True(File.Exists(meta2.FilePath));
    }

    [Fact]
    public async Task CleanupJob_PurgesJobDirectory()
    {
        var jobId = Guid.NewGuid();
        var buf = RefCountedPartitionBuffer.Allocate(256);
        var meta = await _spillManager.SpillPartitionAsync(jobId, 1, 0, buf);

        Assert.True(File.Exists(meta.FilePath));

        _spillManager.CleanupJob(jobId);

        string jobDir = Path.Combine(_testScratchDir, jobId.ToString("N"));
        Assert.False(Directory.Exists(jobDir));
        Assert.Equal(0, _spillManager.SpilledPartitionCount);
    }
}
