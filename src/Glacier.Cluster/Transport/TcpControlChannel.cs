namespace Glacier.Cluster.Transport;

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;

/// <summary>
/// Reliable, ordered TCP NoDelay control channel backed by <see cref="System.IO.Pipelines"/>,
/// <see cref="ClusterFrameReader"/> (<see cref="ClusterFrameReader.DefaultMaxFrameBytes"/>), and <see cref="ClusterCrc32"/>.
/// </summary>
public sealed class TcpControlChannel : IAsyncDisposable, IDisposable
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private readonly PipeReader _reader;
    private readonly PipeWriter _writer;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private ulong _correlationCounter;
    private int _disposed;

    public Socket Socket => _socket;

    public TcpControlChannel(Socket connectedSocket, bool ownsSocket = true)
    {
        _socket = connectedSocket ?? throw new ArgumentNullException(nameof(connectedSocket));
        _socket.NoDelay = true;
        _stream = new NetworkStream(_socket, ownsSocket: ownsSocket);
        _reader = PipeReader.Create(_stream, new StreamPipeReaderOptions(leaveOpen: true));
        _writer = PipeWriter.Create(_stream, new StreamPipeWriterOptions(leaveOpen: true));
    }

    public async ValueTask SendMessageAsync(
        IClusterMessage message,
        ulong? correlationId = null,
        ClusterFrameFlags flags = ClusterFrameFlags.None,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentNullException.ThrowIfNull(message);

        ulong corrId = correlationId ?? Interlocked.Increment(ref _correlationCounter);
        int payloadSize = message.GetByteCount();
        int totalSize = ClusterFrameHeader.HeaderSize + payloadSize;

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Span<byte> span = _writer.GetSpan(totalSize);
            ClusterMessageSerializer.PackFrame(message, corrId, flags, span);
            _writer.Advance(totalSize);
            await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask SendRawPayloadAsync(
        ClusterMessageType messageType,
        ReadOnlyMemory<byte> payload,
        ulong? correlationId = null,
        ClusterFrameFlags flags = ClusterFrameFlags.None,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if ((ulong)payload.Length > ClusterFrameReader.DefaultMaxFrameBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(payload), $"Payload ({payload.Length} bytes) exceeds MaxFrameBytes.");
        }

        ulong corrId = correlationId ?? Interlocked.Increment(ref _correlationCounter);
        int totalSize = ClusterFrameHeader.HeaderSize + payload.Length;
        uint crc32 = ClusterCrc32.Compute(payload.Span);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Span<byte> span = _writer.GetSpan(totalSize);
            var header = new ClusterFrameHeader(messageType, corrId, (ulong)payload.Length, crc32, flags);
            header.WriteTo(span.Slice(0, ClusterFrameHeader.HeaderSize));
            if (!payload.IsEmpty)
            {
                payload.Span.CopyTo(span.Slice(ClusterFrameHeader.HeaderSize, payload.Length));
            }
            _writer.Advance(totalSize);
            await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    /// <summary>
    /// Continuously reads validated frames from the channel, invoking <paramref name="onFrame"/>
    /// with a contiguous payload span (using <see cref="ArrayPool{T}"/> only when multi-segment).
    /// </summary>
    public async Task ReadLoopAsync(
        Action<ClusterFrameHeader, ReadOnlySpan<byte>> onFrame,
        ulong maxFrameBytes = ClusterFrameReader.DefaultMaxFrameBytes,
        bool verifyCrc32 = true,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentNullException.ThrowIfNull(onFrame);

        byte[]? scratch = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult result = await _reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (ClusterFrameReader.TryReadFrame(ref buffer, out var header, out var payload, maxFrameBytes))
                {
                    if (verifyCrc32)
                    {
                        uint computedCrc = ClusterCrc32.Compute(in payload);
                        if (computedCrc != header.PayloadCrc32)
                        {
                            throw new InvalidDataException(
                                $"TcpControlChannel CRC32 mismatch on {header.MessageType}: expected 0x{header.PayloadCrc32:X8}, computed 0x{computedCrc:X8}.");
                        }
                    }

                    if (payload.IsEmpty)
                    {
                        onFrame(header, ReadOnlySpan<byte>.Empty);
                    }
                    else if (payload.IsSingleSegment)
                    {
                        onFrame(header, payload.FirstSpan);
                    }
                    else
                    {
                        int len = checked((int)payload.Length);
                        if (scratch == null || scratch.Length < len)
                        {
                            if (scratch != null) ArrayPool<byte>.Shared.Return(scratch);
                            scratch = ArrayPool<byte>.Shared.Rent(len);
                        }
                        payload.CopyTo(scratch);
                        onFrame(header, scratch.AsSpan(0, len));
                    }
                }

                _reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        finally
        {
            if (scratch != null)
            {
                ArrayPool<byte>.Shared.Return(scratch);
            }
        }
    }

    /// <summary>
    /// Reads the next complete frame matching <paramref name="expectedType"/> and copies its payload into <paramref name="destinationPayload"/>.
    /// </summary>
    public async ValueTask<(ClusterFrameHeader Header, int PayloadLength)> ReceiveExactFrameAsync(
        ClusterMessageType expectedType,
        Memory<byte> destinationPayload,
        ulong maxFrameBytes = ClusterFrameReader.DefaultMaxFrameBytes,
        bool verifyCrc32 = true,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        while (true)
        {
            ReadResult result = await _reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            ReadOnlySequence<byte> buffer = result.Buffer;

            if (ClusterFrameReader.TryReadFrame(ref buffer, out var header, out var payload, maxFrameBytes))
            {
                if (verifyCrc32)
                {
                    uint computedCrc = ClusterCrc32.Compute(in payload);
                    if (computedCrc != header.PayloadCrc32)
                    {
                        _reader.AdvanceTo(buffer.Start, buffer.End);
                        throw new InvalidDataException(
                            $"TcpControlChannel CRC32 mismatch: expected 0x{header.PayloadCrc32:X8}, computed 0x{computedCrc:X8}.");
                    }
                }

                if (header.MessageType != expectedType)
                {
                    _reader.AdvanceTo(buffer.Start, buffer.End);
                    throw new InvalidDataException($"Expected frame type {expectedType}, got {header.MessageType}.");
                }

                int len = checked((int)payload.Length);
                if (len > destinationPayload.Length)
                {
                    _reader.AdvanceTo(buffer.Start, buffer.End);
                    throw new InvalidDataException($"Frame payload ({len} bytes) exceeds destination buffer ({destinationPayload.Length} bytes).");
                }

                if (len > 0)
                {
                    payload.CopyTo(destinationPayload.Span);
                }

                _reader.AdvanceTo(buffer.Start, buffer.Start);
                return (header, len);
            }

            _reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted)
            {
                throw new EndOfStreamException($"TcpControlChannel closed before {expectedType} arrived.");
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        try { await _reader.CompleteAsync().ConfigureAwait(false); }
        catch (Exception ex) { Trace.TraceWarning($"[TcpControlChannel] Reader complete warning: {ex.Message}"); }

        try { await _writer.CompleteAsync().ConfigureAwait(false); }
        catch (Exception ex) { Trace.TraceWarning($"[TcpControlChannel] Writer complete warning: {ex.Message}"); }

        try { _stream.Dispose(); }
        catch (Exception ex) { Trace.TraceWarning($"[TcpControlChannel] Stream dispose warning: {ex.Message}"); }

        _writeLock.Dispose();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        try { _reader.Complete(); }
        catch (Exception ex) { Trace.TraceWarning($"[TcpControlChannel] Reader complete warning: {ex.Message}"); }

        try { _writer.Complete(); }
        catch (Exception ex) { Trace.TraceWarning($"[TcpControlChannel] Writer complete warning: {ex.Message}"); }

        try { _stream.Dispose(); }
        catch (Exception ex) { Trace.TraceWarning($"[TcpControlChannel] Stream dispose warning: {ex.Message}"); }

        _writeLock.Dispose();
    }
}
