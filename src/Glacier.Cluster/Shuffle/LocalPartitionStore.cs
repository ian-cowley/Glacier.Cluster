namespace Glacier.Cluster.Shuffle;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Thread-safe metadata and storage container for a registered partition in the local store.
/// </summary>
public sealed class PartitionEntry
{
    private readonly object _syncLock = new();
    private RefCountedPartitionBuffer? _buffer;
    private string? _spillFilePath;

    public PartitionKey Key { get; }
    public PartitionStorageState State { get; private set; }
    public int ByteLength { get; private set; }
    public DateTime CreatedAtUtc { get; }
    public DateTime LastAccessedAtUtc { get; private set; }

    public RefCountedPartitionBuffer? Buffer
    {
        get { lock (_syncLock) return _buffer; }
    }

    public string? SpillFilePath
    {
        get { lock (_syncLock) return _spillFilePath; }
    }

    public PartitionEntry(PartitionKey key, RefCountedPartitionBuffer buffer)
    {
        Key = key;
        _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
        State = PartitionStorageState.InMemory;
        ByteLength = buffer.ByteLength;
        CreatedAtUtc = DateTime.UtcNow;
        LastAccessedAtUtc = CreatedAtUtc;
    }

    /// <summary>
    /// Atomically retains and returns the in-memory buffer if available.
    /// </summary>
    public bool TryRetainBuffer([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out RefCountedPartitionBuffer? retainedBuffer)
    {
        lock (_syncLock)
        {
            LastAccessedAtUtc = DateTime.UtcNow;
            if (State == PartitionStorageState.InMemory && _buffer != null)
            {
                if (_buffer.TryRetain())
                {
                    retainedBuffer = _buffer;
                    return true;
                }
            }

            retainedBuffer = null;
            return false;
        }
    }

    /// <summary>
    /// Transitions entry from InMemory to SpilledToDisk, releasing the in-memory buffer.
    /// </summary>
    public bool TryTransitionToSpilled(string filePath, out RefCountedPartitionBuffer? bufferToRelease)
    {
        lock (_syncLock)
        {
            if (State != PartitionStorageState.InMemory || _buffer == null)
            {
                bufferToRelease = null;
                return false;
            }

            _spillFilePath = filePath;
            State = PartitionStorageState.SpilledToDisk;
            bufferToRelease = _buffer;
            _buffer = null;
            return true;
        }
    }

    /// <summary>
    /// Transitions entry from SpilledToDisk to InMemory, storing the newly loaded buffer.
    /// </summary>
    public bool TryTransitionToInMemory(RefCountedPartitionBuffer newBuffer)
    {
        lock (_syncLock)
        {
            if (State != PartitionStorageState.SpilledToDisk)
                return false;

            _buffer = newBuffer;
            State = PartitionStorageState.InMemory;
            _spillFilePath = null;
            LastAccessedAtUtc = DateTime.UtcNow;
            return true;
        }
    }

    /// <summary>
    /// Atomically claims either the active in-memory buffer or the spilled file path for eviction.
    /// Guarantees single cleanup even under concurrent eviction calls.
    /// </summary>
    public bool TryConsumeForEviction(
        out RefCountedPartitionBuffer? buffer,
        out string? spillFilePath,
        out int byteLength)
    {
        lock (_syncLock)
        {
            byteLength = ByteLength;
            if (_buffer != null)
            {
                buffer = _buffer;
                spillFilePath = null;
                _buffer = null;
                _spillFilePath = null;
                return true;
            }

            if (State == PartitionStorageState.SpilledToDisk && _spillFilePath != null)
            {
                buffer = null;
                spillFilePath = _spillFilePath;
                _spillFilePath = null;
                return true;
            }

            buffer = null;
            spillFilePath = null;
            return false;
        }
    }

    public int GetCurrentReferenceCount()
    {
        lock (_syncLock)
        {
            return _buffer?.ReferenceCount ?? 0;
        }
    }
}

/// <summary>
/// Node-level thread-safe in-memory partition registry and out-of-core spill coordinator.
/// </summary>
public sealed class LocalPartitionStore : IDisposable
{
    private readonly ConcurrentDictionary<PartitionKey, PartitionEntry> _partitions = new();
    private long _totalInMemoryBytes;
    private long _totalSpilledBytes;
    private int _disposed;

    public Guid LocalNodeId { get; }
    public long TotalInMemoryBytes => Volatile.Read(ref _totalInMemoryBytes);
    public long TotalSpilledBytes => Volatile.Read(ref _totalSpilledBytes);
    public int PartitionCount => _partitions.Count;

    public LocalPartitionStore(Guid localNodeId)
    {
        LocalNodeId = localNodeId;
    }

    /// <summary>
    /// Registers a computed partition buffer into the local store.
    /// Retains the buffer upon registration.
    /// </summary>
    public void RegisterPartition(PartitionKey key, RefCountedPartitionBuffer buffer)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(buffer);

        // Atomically retain the buffer on behalf of the store
        buffer.Retain();

        var entry = new PartitionEntry(key, buffer);
        if (_partitions.TryAdd(key, entry))
        {
            Interlocked.Add(ref _totalInMemoryBytes, buffer.ByteLength);
        }
        else
        {
            // Replace existing entry (e.g. speculative re-execution)
            if (_partitions.TryRemove(key, out var oldEntry))
            {
                EvictEntry(oldEntry);
            }

            _partitions[key] = entry;
            Interlocked.Add(ref _totalInMemoryBytes, buffer.ByteLength);
        }
    }

    /// <summary>
    /// Attempts to retrieve a partition entry for the specified partition key.
    /// </summary>
    public bool TryGetPartition(Guid jobId, int stageId, int partitionId, out PartitionEntry? entry) =>
        TryGetPartition(new PartitionKey(jobId, stageId, partitionId), out entry);

    /// <summary>
    /// Attempts to retrieve a partition entry for the specified partition key.
    /// </summary>
    public bool TryGetPartition(PartitionKey key, out PartitionEntry? entry)
    {
        ThrowIfDisposed();
        return _partitions.TryGetValue(key, out entry);
    }

    /// <summary>
    /// Fetches a registered partition. If in memory, increments reference count and returns.
    /// If spilled to disk, reloads from disk into a fresh 64-byte aligned buffer.
    /// </summary>
    public async ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        PartitionKey key,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (!_partitions.TryGetValue(key, out var entry))
        {
            throw new PartitionNotFoundException(key.JobId, key.StageId, key.PartitionId);
        }

        // Fast Path: In Memory
        if (entry.TryRetainBuffer(out var retainedBuffer) && retainedBuffer != null)
        {
            return retainedBuffer;
        }

        // Slow Path: Spilled to disk
        if (entry.State == PartitionStorageState.SpilledToDisk)
        {
            string? spillPath = entry.SpillFilePath;
            if (spillPath == null || !File.Exists(spillPath))
            {
                throw new FileNotFoundException($"Spilled partition file for {key} was not found on disk.", spillPath);
            }

            // Reload partition into unmanaged 64-byte aligned memory
            var reloaded = await ReloadFromDiskAsync(spillPath, cancellationToken).ConfigureAwait(false);
            if (entry.TryTransitionToInMemory(reloaded))
            {
                Interlocked.Add(ref _totalInMemoryBytes, reloaded.ByteLength);
                Interlocked.Add(ref _totalSpilledBytes, -entry.ByteLength);
                try
                {
                    if (File.Exists(spillPath))
                    {
                        File.Delete(spillPath);
                    }
                }
                catch (IOException ex)
                {
                    Trace.TraceWarning($"[LocalPartitionStore] Best-effort spill cleanup failed for '{spillPath}': {ex.Message}");
                }
                catch (UnauthorizedAccessException ex)
                {
                    Trace.TraceWarning($"[LocalPartitionStore] Best-effort spill cleanup unauthorized for '{spillPath}': {ex.Message}");
                }

                reloaded.Retain();
                return reloaded;
            }
            else
            {
                reloaded.Release();
                if (entry.TryRetainBuffer(out var winningBuffer) && winningBuffer != null)
                    return winningBuffer;
                throw new InvalidOperationException($"Partition {key} in unexpected state: {entry.State}.");
            }
        }

        throw new InvalidOperationException($"Partition {key} is in an invalid state: {entry.State}.");
    }

    /// <summary>
    /// Acknowledges stage completion, releasing all partition buffers for this stage
    /// and cleaning up spilled disk scratch files.
    /// </summary>
    public void AcknowledgeStageComplete(Guid jobId, int stageId)
    {
        ThrowIfDisposed();

        var keysToEvict = _partitions.Keys
            .Where(k => k.JobId == jobId && k.StageId == stageId)
            .ToList();

        foreach (var key in keysToEvict)
        {
            if (_partitions.TryRemove(key, out var entry))
            {
                EvictEntry(entry);
            }
        }
    }

    /// <summary>
    /// Acknowledges job completion, releasing all partition buffers for this job.
    /// </summary>
    public void AcknowledgeJobComplete(Guid jobId)
    {
        ThrowIfDisposed();

        var keysToEvict = _partitions.Keys
            .Where(k => k.JobId == jobId)
            .ToList();

        foreach (var key in keysToEvict)
        {
            if (_partitions.TryRemove(key, out var entry))
            {
                EvictEntry(entry);
            }
        }
    }

    /// <summary>
    /// Returns unpinned in-memory partition entries eligible for spilling (ref count == 1).
    /// </summary>
    public IReadOnlyList<PartitionEntry> GetSpillableCandidates(long targetBytesToReclaim)
    {
        ThrowIfDisposed();

        var candidates = new List<PartitionEntry>();
        long accumulated = 0;

        foreach (var entry in _partitions.Values)
        {
            if (entry.State == PartitionStorageState.InMemory && entry.GetCurrentReferenceCount() == 1)
            {
                candidates.Add(entry);
                accumulated += entry.ByteLength;
                if (accumulated >= targetBytesToReclaim)
                    break;
            }
        }

        return candidates;
    }

    /// <summary>
    /// Atomically transitions an in-memory entry to spilled status, releasing its buffer.
    /// </summary>
    public bool TransitionToSpilled(PartitionKey key, string spillPath)
    {
        ThrowIfDisposed();

        if (_partitions.TryGetValue(key, out var entry))
        {
            if (entry.TryTransitionToSpilled(spillPath, out var bufferToRelease))
            {
                if (bufferToRelease != null)
                {
                    Interlocked.Add(ref _totalInMemoryBytes, -bufferToRelease.ByteLength);
                    Interlocked.Add(ref _totalSpilledBytes, bufferToRelease.ByteLength);
                    bufferToRelease.Release();
                }
                return true;
            }
        }

        return false;
    }

    private void EvictEntry(PartitionEntry entry)
    {
        if (!entry.TryConsumeForEviction(out var buf, out var spillPath, out int byteLength))
        {
            return;
        }

        if (buf != null)
        {
            // Release the store's hold on the buffer
            Interlocked.Add(ref _totalInMemoryBytes, -byteLength);
            buf.Release();
        }
        else if (spillPath != null)
        {
            try
            {
                if (File.Exists(spillPath))
                {
                    File.Delete(spillPath);
                }
            }
            catch (IOException ex)
            {
                Trace.TraceWarning($"[LocalPartitionStore] Best-effort spill file deletion failed for '{spillPath}': {ex.Message}");
            }
            catch (UnauthorizedAccessException ex)
            {
                Trace.TraceWarning($"[LocalPartitionStore] Best-effort spill file deletion unauthorized for '{spillPath}': {ex.Message}");
            }

            Interlocked.Add(ref _totalSpilledBytes, -byteLength);
        }
    }

    private static async ValueTask<RefCountedPartitionBuffer> ReloadFromDiskAsync(string filePath, CancellationToken ct)
    {
        await using var stream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 65536,
            useAsync: true);

        int length = checked((int)stream.Length);
        var buffer = RefCountedPartitionBuffer.Allocate(length);
        int readTotal = 0;

        while (readTotal < length)
        {
            int read = await stream.ReadAsync(buffer.Memory.Slice(readTotal), ct).ConfigureAwait(false);
            if (read == 0) break;
            readTotal += read;
        }

        return buffer;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            foreach (var kvp in _partitions)
            {
                EvictEntry(kvp.Value);
            }
            _partitions.Clear();
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
    }
}
