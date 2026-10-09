namespace Glacier.Cluster.Shuffle;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Configuration parameters governing credit-based flow control and incast prevention.
/// </summary>
public sealed record FlowControlOptions
{
    /// <summary>Default credit tokens granted per partition fetch request (e.g. 4 chunks).</summary>
    public uint DefaultCreditTokensPerFetch { get; init; } = 4;

    /// <summary>Maximum chunk payload size in bytes (e.g. 64 KB).</summary>
    public uint MaxChunkSizeBytes { get; init; } = 65536;

    /// <summary>Maximum concurrent active partition streams across all peers per worker node.</summary>
    public int MaxGlobalConcurrentStreams { get; init; } = 32;

    /// <summary>Maximum concurrent active partition streams per individual peer node.</summary>
    public int MaxStreamsPerPeer { get; init; } = 4;
}

/// <summary>
/// Coordinates credit tokens between consumer and producer nodes.
/// Prevents TCP incast by throttling transmission to granted credit windows.
/// </summary>
public sealed class FlowControlCreditManager : IDisposable
{
    private readonly FlowControlOptions _options;
    private readonly SemaphoreSlim _globalStreamSemaphore;
    private readonly ConcurrentDictionary<Guid, SemaphoreSlim> _peerStreamSemaphores = new();

    // Producer-side credit tracking: (RemoteNodeId, JobId, StageId, PartitionId) -> ProducerCreditState
    private readonly ConcurrentDictionary<(Guid RemoteNodeId, Guid JobId, int StageId, int PartitionId), ProducerCreditState> _producerStates = new();

    private int _isDisposed;

    public FlowControlOptions Options => _options;
    public int AvailableGlobalCredits => _globalStreamSemaphore.CurrentCount;

    public FlowControlCreditManager(FlowControlOptions? options = null)
    {
        _options = options ?? new FlowControlOptions();
        _globalStreamSemaphore = new SemaphoreSlim(_options.MaxGlobalConcurrentStreams, _options.MaxGlobalConcurrentStreams);
    }

    #region Consumer Operations

    /// <summary>
    /// Consumer acquires a transmission slot before issuing a PartitionFetchRequest to a peer.
    /// Limits global and per-peer concurrency to avoid receiver incast collapse.
    /// </summary>
    public async ValueTask<ConsumerFetchLease> AcquireConsumerFetchLeaseAsync(
        Guid peerNodeId,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);

        // 1. Acquire global concurrency slot
        await _globalStreamSemaphore.WaitAsync(ct).ConfigureAwait(false);

        // 2. Acquire per-peer concurrency slot
        SemaphoreSlim peerSemaphore = _peerStreamSemaphores.GetOrAdd(
            peerNodeId,
            _ => new SemaphoreSlim(_options.MaxStreamsPerPeer, _options.MaxStreamsPerPeer));

        try
        {
            await peerSemaphore.WaitAsync(ct).ConfigureAwait(false);
            return new ConsumerFetchLease(this, peerNodeId);
        }
        catch
        {
            _globalStreamSemaphore.Release();
            throw;
        }
    }

    internal void ReleaseConsumerFetchLease(Guid peerNodeId)
    {
        if (_peerStreamSemaphores.TryGetValue(peerNodeId, out var peerSemaphore))
        {
            peerSemaphore.Release();
        }
        _globalStreamSemaphore.Release();
    }

    #endregion

    #region Producer Operations

    /// <summary>
    /// Producer registers credit tokens granted by consumer via PartitionFetchRequestMessage.
    /// </summary>
    public void GrantProducerCredits(Guid remoteNodeId, Guid jobId, int stageId, int partitionId, uint tokens)
    {
        var key = (remoteNodeId, jobId, stageId, partitionId);
        var state = _producerStates.GetOrAdd(key, _ => new ProducerCreditState());
        state.Grant(tokens);
    }

    /// <summary>
    /// Producer awaits an available credit token before transmitting a chunk/batch to the consumer.
    /// </summary>
    public async ValueTask WaitForProducerCreditAsync(
        Guid remoteNodeId,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken ct = default)
    {
        var key = (remoteNodeId, jobId, stageId, partitionId);
        var state = _producerStates.GetOrAdd(key, _ => new ProducerCreditState());
        await state.WaitForCreditAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets current available credits for a producer transfer.
    /// </summary>
    public int GetAvailableCredits(Guid remoteNodeId, Guid jobId, int stageId, int partitionId)
    {
        var key = (remoteNodeId, jobId, stageId, partitionId);
        return _producerStates.TryGetValue(key, out var state) ? state.Credits : 0;
    }

    /// <summary>
    /// Producer replenishes credits upon receiving PartitionAcknowledgeMessage from consumer.
    /// </summary>
    public void ReplenishProducerCredits(Guid remoteNodeId, Guid jobId, int stageId, int partitionId, uint tokensReleased)
    {
        GrantProducerCredits(remoteNodeId, jobId, stageId, partitionId, tokensReleased);
    }

    /// <summary>
    /// Cleans up state when a partition transfer is completed or aborted.
    /// </summary>
    public void CleanupPartitionState(Guid remoteNodeId, Guid jobId, int stageId, int partitionId)
    {
        var key = (remoteNodeId, jobId, stageId, partitionId);
        if (_producerStates.TryRemove(key, out var state))
        {
            state.Dispose();
        }
    }

    #endregion

    private sealed class ProducerCreditState : IDisposable
    {
        private readonly object _lock = new();
        private int _credits;
        private readonly AsyncAutoResetEvent _signal = new();

        public int Credits
        {
            get
            {
                lock (_lock)
                {
                    return _credits;
                }
            }
        }

        public void Grant(uint tokens)
        {
            if (tokens == 0) return;
            lock (_lock)
            {
                _credits += checked((int)tokens);
            }
            for (uint i = 0; i < tokens; i++)
            {
                _signal.Set();
            }
        }

        public bool TryConsume()
        {
            lock (_lock)
            {
                if (_credits > 0)
                {
                    _credits--;
                    return true;
                }
                return false;
            }
        }

        public async ValueTask WaitForCreditAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                if (TryConsume())
                {
                    return;
                }

                await _signal.WaitAsync(ct).ConfigureAwait(false);
            }

            ct.ThrowIfCancellationRequested();
        }

        public void Dispose()
        {
            _signal.Dispose();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _globalStreamSemaphore.Dispose();
        foreach (var sem in _peerStreamSemaphores.Values)
        {
            sem.Dispose();
        }
        _peerStreamSemaphores.Clear();

        foreach (var state in _producerStates.Values)
        {
            state.Dispose();
        }
        _producerStates.Clear();
    }
}

/// <summary>
/// RAII lease releasing global and per-peer consumer slots upon partition transfer completion.
/// </summary>
public readonly struct ConsumerFetchLease : IDisposable
{
    private readonly FlowControlCreditManager _manager;
    private readonly Guid _peerNodeId;

    internal ConsumerFetchLease(FlowControlCreditManager manager, Guid peerNodeId)
    {
        _manager = manager;
        _peerNodeId = peerNodeId;
    }

    public void Dispose()
    {
        _manager?.ReleaseConsumerFetchLease(_peerNodeId);
    }
}

/// <summary>
/// Asynchronous AutoResetEvent for non-blocking credit wait signaling.
/// </summary>
internal sealed class AsyncAutoResetEvent : IDisposable
{
    private static readonly Task s_completed = Task.CompletedTask;
    private readonly object _lock = new();
    private readonly Queue<TaskCompletionSource<bool>> _waiters = new();
    private bool _isSet;

    public void Set()
    {
        TaskCompletionSource<bool>? toRelease = null;
        lock (_lock)
        {
            while (_waiters.Count > 0)
            {
                var waiter = _waiters.Dequeue();
                if (!waiter.Task.IsCompleted)
                {
                    toRelease = waiter;
                    break;
                }
            }

            if (toRelease == null)
            {
                _isSet = true;
            }
        }

        toRelease?.TrySetResult(true);
    }

    public Task WaitAsync(CancellationToken ct = default)
    {
        if (ct.IsCancellationRequested)
        {
            return Task.FromCanceled(ct);
        }

        lock (_lock)
        {
            if (_isSet)
            {
                _isSet = false;
                return s_completed;
            }

            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (ct.CanBeCanceled)
            {
                var reg = ct.Register(() => tcs.TrySetCanceled(ct));
                tcs.Task.ContinueWith(_ => reg.Dispose(), TaskScheduler.Default);
            }

            _waiters.Enqueue(tcs);
            return tcs.Task;
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            while (_waiters.Count > 0)
            {
                _waiters.Dequeue().TrySetCanceled();
            }
        }
    }
}
