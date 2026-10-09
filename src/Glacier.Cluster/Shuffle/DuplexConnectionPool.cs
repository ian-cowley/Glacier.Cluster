namespace Glacier.Cluster.Shuffle;

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;

/// <summary>
/// Encapsulates a complete parsed Glacier.Cluster wire frame.
/// </summary>
public readonly struct ClusterFrame
{
    public readonly ClusterFrameHeader Header;
    public readonly ReadOnlyMemory<byte> Payload;

    public ClusterFrame(ClusterFrameHeader header, ReadOnlyMemory<byte> payload)
    {
        Header = header;
        Payload = payload;
    }

    public ClusterFrame(ClusterFrameHeader header, ReadOnlySequence<byte> payloadSequence)
    {
        Header = header;
        if (payloadSequence.IsEmpty)
        {
            Payload = ReadOnlyMemory<byte>.Empty;
        }
        else if (payloadSequence.IsSingleSegment)
        {
            Payload = payloadSequence.First;
        }
        else
        {
            Payload = payloadSequence.ToArray();
        }
    }
}

/// <summary>
/// Represents a persistent bi-directional duplex connection between the local node and a remote peer node.
/// Multiplexes concurrent request/response flows over a single pipe pair with serialized frame writes.
/// </summary>
public sealed class DuplexConnection : IAsyncDisposable, IDisposable
{
    private readonly Guid _remoteNodeId;
    private readonly IDuplexPipe _duplexPipe;
    private readonly Socket? _underlyingSocket;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<ClusterFrame>> _pendingRequests = new();
    private readonly Func<ClusterFrame, ValueTask>? _unsolicitedFrameHandler;
    private readonly CancellationTokenSource _cts = new();
    private Task? _readLoopTask;
    private int _isDisposed;

    public Guid RemoteNodeId => _remoteNodeId;
    public bool IsConnected => Volatile.Read(ref _isDisposed) == 0;
    public IDuplexPipe Pipe => _duplexPipe;

    public DuplexConnection(
        Guid remoteNodeId,
        IDuplexPipe duplexPipe,
        Socket? underlyingSocket = null,
        Func<ClusterFrame, ValueTask>? unsolicitedFrameHandler = null)
    {
        _remoteNodeId = remoteNodeId;
        _duplexPipe = duplexPipe ?? throw new ArgumentNullException(nameof(duplexPipe));
        _underlyingSocket = underlyingSocket;
        _unsolicitedFrameHandler = unsolicitedFrameHandler;
    }

    /// <summary>Starts the continuous background read loop.</summary>
    public void Start()
    {
        _readLoopTask = Task.Run(() => ReadLoopAsync(_cts.Token));
    }

    /// <summary>
    /// Writes a single frame atomically into the duplex pipe writer and flushes it.
    /// </summary>
    public async ValueTask SendFrameAsync(
        IClusterMessage message,
        ulong correlationId,
        byte flags = 0,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        await _writeLock.WaitAsync(linkedCts.Token).ConfigureAwait(false);
        try
        {
            PipeWriter writer = _duplexPipe.Output;
            int payloadSize = message.GetByteCount();
            int totalFrameSize = ClusterFrameHeader.HeaderSize + payloadSize;

            Span<byte> span = writer.GetSpan(totalFrameSize);
            ClusterMessageSerializer.PackFrame(message, correlationId, flags, span);
            writer.Advance(totalFrameSize);

            FlushResult result = await writer.FlushAsync(linkedCts.Token).ConfigureAwait(false);
            if (result.IsCanceled || result.IsCompleted)
            {
                throw new InvalidOperationException($"PipeWriter completed or cancelled during frame send to {_remoteNodeId}.");
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Writes pre-serialized frame bytes atomically into the duplex pipe writer and flushes it.
    /// </summary>
    public async ValueTask SendRawFrameAsync(
        ReadOnlyMemory<byte> frameBytes,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        await _writeLock.WaitAsync(linkedCts.Token).ConfigureAwait(false);
        try
        {
            PipeWriter writer = _duplexPipe.Output;
            Memory<byte> span = writer.GetMemory(frameBytes.Length);
            frameBytes.CopyTo(span);
            writer.Advance(frameBytes.Length);

            FlushResult result = await writer.FlushAsync(linkedCts.Token).ConfigureAwait(false);
            if (result.IsCanceled || result.IsCompleted)
            {
                throw new InvalidOperationException($"PipeWriter completed or cancelled during frame send to {_remoteNodeId}.");
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Sends a request frame and awaits the matching response frame correlated by CorrelationId.
    /// </summary>
    public async ValueTask<ClusterFrame> SendRequestAsync(
        IClusterMessage request,
        ulong correlationId,
        TimeSpan timeout,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);

        var tcs = new TaskCompletionSource<ClusterFrame>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_pendingRequests.TryAdd(correlationId, tcs))
        {
            throw new InvalidOperationException($"CorrelationId {correlationId} is already in flight for node {_remoteNodeId}.");
        }

        try
        {
            await SendFrameAsync(request, correlationId, flags: 0, ct).ConfigureAwait(false);

            using var timeoutCts = new CancellationTokenSource(timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token, _cts.Token);

            return await tcs.Task.WaitAsync(linkedCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException($"Request with CorrelationId {correlationId} to node {_remoteNodeId} timed out after {timeout.TotalMilliseconds}ms.");
        }
        finally
        {
            _pendingRequests.TryRemove(correlationId, out _);
        }
    }

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        PipeReader reader = _duplexPipe.Input;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(ct).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (ClusterFrameReader.TryReadFrame(ref buffer, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
                {
                    // Copy payload to ensure thread-safe longevity past AdvanceTo
                    var frame = new ClusterFrame(header, payload.ToArray());

                    if (_pendingRequests.TryRemove(header.CorrelationId, out var tcs))
                    {
                        tcs.TrySetResult(frame);
                    }
                    else if (_unsolicitedFrameHandler != null)
                    {
                        await _unsolicitedFrameHandler(frame).ConfigureAwait(false);
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Fault all pending requests with IOException
            foreach (var kvp in _pendingRequests)
            {
                kvp.Value.TrySetException(new IOException($"Connection to peer {_remoteNodeId} closed unexpectedly.", ex));
            }
            _pendingRequests.Clear();
        }
        finally
        {
            await DisposeAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _cts.Cancel();

        try
        {
            await _duplexPipe.Output.CompleteAsync().ConfigureAwait(false);
            await _duplexPipe.Input.CompleteAsync().ConfigureAwait(false);
        }
        catch { /* Ignore pipe shutdown errors */ }

        _underlyingSocket?.Dispose();
        _writeLock.Dispose();

        // Fault any remaining pending requests
        foreach (var kvp in _pendingRequests)
        {
            kvp.Value.TrySetException(new ObjectDisposedException(nameof(DuplexConnection)));
        }
        _pendingRequests.Clear();

        if (_readLoopTask != null)
        {
            try { await _readLoopTask.ConfigureAwait(false); } catch { }
        }

        _cts.Dispose();
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}

/// <summary>
/// Connection factory delegate allowing transparent swapping of physical TCP sockets or in-process IDuplexPipes.
/// </summary>
public delegate ValueTask<IDuplexPipe> DuplexTransportFactory(
    Guid remoteNodeId,
    EndPoint remoteEndPoint,
    CancellationToken cancellationToken);

/// <summary>
/// Manages exactly ONE bi-directional duplex connection per cluster node pair.
/// Prevents O(N^2) socket exhaustion and arbitrates symmetric connection establishment races via deterministic GUID comparison.
/// </summary>
public sealed class DuplexConnectionPool : IAsyncDisposable, IDisposable
{
    private readonly Guid _localNodeId;
    private readonly ConcurrentDictionary<Guid, DuplexConnection> _activeConnections = new();
    private readonly ConcurrentDictionary<Guid, SemaphoreSlim> _connectionLocks = new();
    private readonly DuplexTransportFactory _transportFactory;
    private readonly Func<ClusterFrame, ValueTask>? _inboundFrameHandler;
    private ulong _nextCorrelationId;
    private int _isDisposed;

    public Guid LocalNodeId => _localNodeId;
    public int ActiveConnectionCount => _activeConnections.Count;

    public DuplexConnectionPool(
        Guid localNodeId,
        DuplexTransportFactory? transportFactory = null,
        Func<ClusterFrame, ValueTask>? inboundFrameHandler = null)
    {
        _localNodeId = localNodeId;
        _transportFactory = transportFactory ?? DefaultSocketTransportFactory;
        _inboundFrameHandler = inboundFrameHandler;
    }

    /// <summary>Generates a cluster-unique correlation ID for request/response multiplexing.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong NextCorrelationId() => Interlocked.Increment(ref _nextCorrelationId);

    /// <summary>
    /// Gets an existing active connection to the specified peer or atomically creates a new one.
    /// </summary>
    public async ValueTask<DuplexConnection> GetOrCreateConnectionAsync(
        Guid remoteNodeId,
        EndPoint remoteEndPoint,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);

        if (_activeConnections.TryGetValue(remoteNodeId, out var existing) && existing.IsConnected)
        {
            return existing;
        }

        SemaphoreSlim gate = _connectionLocks.GetOrAdd(remoteNodeId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_activeConnections.TryGetValue(remoteNodeId, out existing) && existing.IsConnected)
            {
                return existing;
            }

            IDuplexPipe pipe = await _transportFactory(remoteNodeId, remoteEndPoint, ct).ConfigureAwait(false);
            var connection = new DuplexConnection(remoteNodeId, pipe, unsolicitedFrameHandler: _inboundFrameHandler);
            connection.Start();

            _activeConnections[remoteNodeId] = connection;
            return connection;
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Registers an accepted inbound connection from a remote peer, resolving simultaneous connect races.
    /// Uses deterministic GUID comparison: the node with the smaller GUID is the winning initiator.
    /// </summary>
    public async ValueTask<DuplexConnection> RegisterInboundConnectionAsync(
        Guid remoteNodeId,
        IDuplexPipe pipe,
        Socket? underlyingSocket = null)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);

        SemaphoreSlim gate = _connectionLocks.GetOrAdd(remoteNodeId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_activeConnections.TryGetValue(remoteNodeId, out var existing) && existing.IsConnected)
            {
                // Tie-breaking rule: The connection initiated by the node with smaller GUID wins
                Span<byte> localBytes = stackalloc byte[16];
                Span<byte> remoteBytes = stackalloc byte[16];
                _localNodeId.TryWriteBytes(localBytes);
                remoteNodeId.TryWriteBytes(remoteBytes);

                int cmp = localBytes.SequenceCompareTo(remoteBytes);
                if (cmp < 0)
                {
                    // Local initiated connection wins; reject/close incoming
                    var incomingConn = new DuplexConnection(remoteNodeId, pipe, underlyingSocket);
                    await incomingConn.DisposeAsync().ConfigureAwait(false);
                    return existing;
                }
                else
                {
                    // Remote initiated connection wins; dispose old local one and accept new
                    await existing.DisposeAsync().ConfigureAwait(false);
                }
            }

            var newConn = new DuplexConnection(remoteNodeId, pipe, underlyingSocket, _inboundFrameHandler);
            newConn.Start();
            _activeConnections[remoteNodeId] = newConn;
            return newConn;
        }
        finally
        {
            gate.Release();
        }
    }

    private static async ValueTask<IDuplexPipe> DefaultSocketTransportFactory(
        Guid remoteNodeId,
        EndPoint endPoint,
        CancellationToken ct)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)
        {
            NoDelay = true
        };
        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

        await socket.ConnectAsync(endPoint, ct).ConfigureAwait(false);
        return new SocketDuplexPipe(socket);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        foreach (var kvp in _activeConnections)
        {
            await kvp.Value.DisposeAsync().ConfigureAwait(false);
        }
        _activeConnections.Clear();

        foreach (var kvp in _connectionLocks)
        {
            kvp.Value.Dispose();
        }
        _connectionLocks.Clear();
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
