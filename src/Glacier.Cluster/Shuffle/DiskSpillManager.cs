namespace Glacier.Cluster.Shuffle;

using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Interface for managing tiered out-of-core spilling of shuffle partitions to NVMe disk storage.
/// </summary>
public interface IDiskSpillManager : IAsyncDisposable, IDisposable
{
    string ScratchDirectory { get; }
    int SpilledPartitionCount { get; }
    PartitionStorageState GetPartitionState(Guid jobId, int stageId, int partitionId);
    bool IsMemoryPressureHigh(ulong currentMemoryBytes);

    ValueTask<SpilledPartitionMetadata> SpillPartitionAsync(
        Guid jobId,
        int stageId,
        int partitionId,
        RefCountedPartitionBuffer buffer,
        CancellationToken ct = default);

    ValueTask StreamSpilledPartitionAsync(
        Guid jobId,
        int stageId,
        int partitionId,
        PipeWriter pipeWriter,
        CancellationToken ct = default);

    ValueTask<RefCountedPartitionBuffer> ReloadPartitionAsync(
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken ct = default);

    void CleanupStage(Guid jobId, int stageId);
    void CleanupJob(Guid jobId);
    bool TryGetSpilledMetadata(Guid jobId, int stageId, int partitionId, out SpilledPartitionMetadata? metadata);
}

/// <summary>
/// Manages tiered spilling of unpinned shuffle partition buffers to local NVMe scratch storage.
/// Provides zero-copy direct socket streaming, unmanaged memory reloading, and automated stage/job cleanup.
/// </summary>
public sealed class DiskSpillManager : IDiskSpillManager
{
    private readonly string _scratchDirectory;
    private readonly ulong _maxMemoryBytes;
    private readonly double _highWatermark;
    private readonly double _lowWatermark;
    private readonly LocalPartitionStore? _partitionStore;
    private readonly ConcurrentDictionary<(Guid JobId, int StageId, int PartitionId), SpilledPartitionMetadata> _spillIndex = new();
    private readonly ConcurrentDictionary<(Guid JobId, int StageId, int PartitionId), PartitionStorageState> _partitionStates = new();
    private readonly SemaphoreSlim _spillLock = new(1, 1);
    private int _isDisposed;

    public string ScratchDirectory => _scratchDirectory;
    public int SpilledPartitionCount => _spillIndex.Count;
    public ulong MaxMemoryBytes => _maxMemoryBytes;
    public double HighWatermark => _highWatermark;
    public double LowWatermark => _lowWatermark;

    public DiskSpillManager(
        string? scratchDirectory = null,
        ulong maxMemoryBytes = 0,
        double highWatermark = 0.80,
        double lowWatermark = 0.60,
        LocalPartitionStore? partitionStore = null)
    {
        _scratchDirectory = scratchDirectory ?? Path.Combine(Path.GetTempPath(), ".glacier_shuffle");
        _maxMemoryBytes = maxMemoryBytes > 0 ? maxMemoryBytes : GetDefaultMaxMemory();
        _highWatermark = highWatermark;
        _lowWatermark = lowWatermark;
        _partitionStore = partitionStore;

        // Ensure root scratch directory exists and purge stale remnants from prior crashed runs
        Directory.CreateDirectory(_scratchDirectory);
        PurgeStaleScratchFiles();
    }

    /// <summary>Looks up spilled partition metadata if present.</summary>
    public bool TryGetSpilledMetadata(Guid jobId, int stageId, int partitionId, out SpilledPartitionMetadata? metadata)
    {
        return _spillIndex.TryGetValue((jobId, stageId, partitionId), out metadata);
    }

    /// <summary>Queries the current storage state of a partition.</summary>
    public PartitionStorageState GetPartitionState(Guid jobId, int stageId, int partitionId)
    {
        return _partitionStates.TryGetValue((jobId, stageId, partitionId), out var state)
            ? state
            : PartitionStorageState.NotFound;
    }

    /// <summary>
    /// Checks whether memory pressure exceeds high watermark and initiates spilling if needed.
    /// </summary>
    public bool IsMemoryPressureHigh(ulong currentMemoryBytes)
    {
        return (double)currentMemoryBytes / _maxMemoryBytes >= _highWatermark;
    }

    /// <summary>
    /// Spills an unpinned partition buffer to local NVMe scratch disk.
    /// Atomically transitions state and frees the unmanaged memory.
    /// </summary>
    public async ValueTask<SpilledPartitionMetadata> SpillPartitionAsync(
        Guid jobId,
        int stageId,
        int partitionId,
        RefCountedPartitionBuffer buffer,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);
        ArgumentNullException.ThrowIfNull(buffer);

        var key = (jobId, stageId, partitionId);
        _partitionStates[key] = PartitionStorageState.Spilling;

        string jobDir = Path.Combine(_scratchDirectory, jobId.ToString("N"));
        Directory.CreateDirectory(jobDir);

        string finalPath = Path.Combine(jobDir, $"{stageId}_{partitionId}.arrow");
        string tmpPath = finalPath + ".tmp";

        try
        {
            // Direct unmanaged binary write to disk
            await using (var fs = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536, FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                await fs.WriteAsync(buffer.Memory, ct).ConfigureAwait(false);
                await fs.FlushAsync(ct).ConfigureAwait(false);
            }

            // Atomic rename
            File.Move(tmpPath, finalPath, overwrite: true);

            var metadata = new SpilledPartitionMetadata(
                jobId, stageId, partitionId, finalPath, buffer.ByteLength, DateTime.UtcNow);

            _spillIndex[key] = metadata;
            _partitionStates[key] = PartitionStorageState.SpilledToDisk;

            // Coordinate with LocalPartitionStore if configured
            if (_partitionStore != null)
            {
                var pKey = new PartitionKey(jobId, stageId, partitionId);
                if (!_partitionStore.TransitionToSpilled(pKey, finalPath))
                {
                    buffer.Release();
                }
            }
            else
            {
                // Free the unmanaged memory buffer held by the store
                buffer.Release();
            }

            return metadata;
        }
        catch
        {
            _partitionStates[key] = PartitionStorageState.InMemory;
            if (File.Exists(tmpPath))
            {
                try { File.Delete(tmpPath); } catch { }
            }
            throw;
        }
    }

    /// <summary>
    /// Zero-Copy Socket Streaming: Streams a spilled partition file directly into a PipeWriter
    /// without loading intermediate managed byte[] arrays into memory.
    /// </summary>
    public async ValueTask StreamSpilledPartitionAsync(
        Guid jobId,
        int stageId,
        int partitionId,
        PipeWriter pipeWriter,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);
        ArgumentNullException.ThrowIfNull(pipeWriter);

        var key = (jobId, stageId, partitionId);
        if (!_spillIndex.TryGetValue(key, out var metadata))
        {
            throw new FileNotFoundException($"Spilled partition ({jobId}, {stageId}, {partitionId}) was not found in SpillIndex.");
        }

        await using var fs = new FileStream(metadata.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, FileOptions.Asynchronous | FileOptions.SequentialScan);

        while (!ct.IsCancellationRequested)
        {
            Memory<byte> memory = pipeWriter.GetMemory(65536);
            int bytesRead = await fs.ReadAsync(memory, ct).ConfigureAwait(false);
            if (bytesRead == 0) break;

            pipeWriter.Advance(bytesRead);
            FlushResult result = await pipeWriter.FlushAsync(ct).ConfigureAwait(false);
            if (result.IsCompleted || result.IsCanceled) break;
        }
    }

    /// <summary>
    /// Reloads a spilled partition from NVMe disk back into a 64-byte aligned unmanaged buffer.
    /// </summary>
    public async ValueTask<RefCountedPartitionBuffer> ReloadPartitionAsync(
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);

        var key = (jobId, stageId, partitionId);
        if (!_spillIndex.TryGetValue(key, out var metadata))
        {
            throw new FileNotFoundException($"Spilled partition ({jobId}, {stageId}, {partitionId}) was not found in SpillIndex.");
        }

        var buffer = RefCountedPartitionBuffer.Allocate((int)metadata.ByteLength);
        try
        {
            await using var fs = new FileStream(metadata.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, FileOptions.Asynchronous | FileOptions.SequentialScan);

            int totalRead = 0;
            while (totalRead < (int)metadata.ByteLength)
            {
                int read = await fs.ReadAsync(buffer.Memory.Slice(totalRead), ct).ConfigureAwait(false);
                if (read == 0) break;
                totalRead += read;
            }

            _partitionStates[key] = PartitionStorageState.InMemory;
            return buffer;
        }
        catch
        {
            buffer.Release();
            throw;
        }
    }

    /// <summary>
    /// Cleans up scratch files associated with a completed stage.
    /// </summary>
    public void CleanupStage(Guid jobId, int stageId)
    {
        foreach (var kvp in _spillIndex)
        {
            if (kvp.Key.JobId == jobId && kvp.Key.StageId == stageId)
            {
                if (_spillIndex.TryRemove(kvp.Key, out var meta))
                {
                    _partitionStates.TryRemove(kvp.Key, out _);
                    try { if (File.Exists(meta.FilePath)) File.Delete(meta.FilePath); } catch { }
                }
            }
        }
    }

    /// <summary>
    /// Cleans up all scratch files and directories associated with a completed job.
    /// </summary>
    public void CleanupJob(Guid jobId)
    {
        foreach (var kvp in _spillIndex)
        {
            if (kvp.Key.JobId == jobId)
            {
                _spillIndex.TryRemove(kvp.Key, out _);
                _partitionStates.TryRemove(kvp.Key, out _);
            }
        }

        string jobDir = Path.Combine(_scratchDirectory, jobId.ToString("N"));
        try
        {
            if (Directory.Exists(jobDir))
            {
                Directory.Delete(jobDir, recursive: true);
            }
        }
        catch { }
    }

    private void PurgeStaleScratchFiles()
    {
        try
        {
            if (Directory.Exists(_scratchDirectory))
            {
                foreach (string dir in Directory.GetDirectories(_scratchDirectory))
                {
                    try { Directory.Delete(dir, recursive: true); } catch { }
                }
            }
        }
        catch { }
    }

    private static ulong GetDefaultMaxMemory()
    {
        var memInfo = GC.GetGCMemoryInfo();
        return memInfo.TotalAvailableMemoryBytes > 0
            ? (ulong)(memInfo.TotalAvailableMemoryBytes * 0.50) // Default 50% of available RAM
            : 4UL * 1024 * 1024 * 1024; // 4 GB fallback
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _spillLock.Dispose();
        _spillIndex.Clear();
        _partitionStates.Clear();
        await Task.CompletedTask;
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
