namespace Glacier.Cluster.Shuffle;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Transport;
using Glacier.Polaris;
using Glacier.Polaris.IO;
using Glacier.Formats.Arrow;

/// <summary>
/// Interface for pulling partitions, Arrow IPC record batches, and DataFrames from remote shuffle servers.
/// </summary>
public interface IArrowShuffleClient : IShuffleRemoteClient, IAsyncDisposable, IDisposable
{
    ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default);

    ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        string targetAddress,
        ushort shufflePort,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default);

    ValueTask<ArrowRecordBatch> FetchRecordBatchAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default);

    ValueTask<DataFrame> FetchDataFrameAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default);
}

internal sealed class PartitionFetchSession
{
    public readonly Guid JobId;
    public readonly int StageId;
    public readonly int PartitionId;
    public readonly ulong CorrelationId;
    public readonly uint CreditWindow;
    public readonly PipeWriter Writer;
    public readonly SemaphoreSlim WriteLock;
    public readonly TaskCompletionSource<RefCountedPartitionBuffer> CompletionSource;
    public RefCountedPartitionBuffer? Buffer;
    public ulong TotalLength;
    public ulong BytesReceived;
    public uint UnackedChunks;

    public PartitionFetchSession(
        Guid jobId,
        int stageId,
        int partitionId,
        ulong correlationId,
        uint creditWindow,
        PipeWriter writer,
        SemaphoreSlim writeLock)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
        CorrelationId = correlationId;
        CreditWindow = creditWindow;
        Writer = writer;
        WriteLock = writeLock;
        CompletionSource = new TaskCompletionSource<RefCountedPartitionBuffer>(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

/// <summary>
/// Multi-channel shuffle client pulling partitions over System.IO.Pipelines,
/// reassembling chunks directly into 64-byte aligned <see cref="RefCountedPartitionBuffer"/>s,
/// and deserializing pure C# Arrow IPC record batches with zero GC heap allocations.
/// </summary>
public sealed class ArrowShuffleClient : IArrowShuffleClient
{
    private const uint DefaultCreditWindow = 4;

    private readonly DuplexConnectionPool? _connectionPool;
    private readonly Func<Guid, EndPoint>? _nodeAddressResolver;
    private readonly string? _clusterSecret;
    private readonly ConcurrentDictionary<ulong, PartitionFetchSession> _activeSessions = new();
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> _pendingAuths = new();
    private readonly ConcurrentDictionary<PipeReader, Task> _receiverLoops = new();
    private readonly ConditionalWeakTable<PipeWriter, SemaphoreSlim> _writeLocks = new();
    private readonly ConditionalWeakTable<IDuplexPipe, SemaphoreSlim> _authLocks = new();
    private readonly ConditionalWeakTable<IDuplexPipe, StrongBox<bool>> _authenticatedPipes = new();
    private readonly CancellationTokenSource _cts = new();
    private ulong _correlationCounter;
    private int _isDisposed;

    public ArrowShuffleClient(
        DuplexConnectionPool? connectionPool = null,
        Func<Guid, EndPoint>? nodeAddressResolver = null,
        string? clusterSecret = null)
    {
        _connectionPool = connectionPool;
        _nodeAddressResolver = nodeAddressResolver;
        _clusterSecret = clusterSecret;
    }

    private SemaphoreSlim GetWriteLock(PipeWriter writer) =>
        _writeLocks.GetValue(writer, static _ => new SemaphoreSlim(1, 1));

    private async ValueTask EnsureAuthenticatedAsync(IDuplexPipe pipe, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(_clusterSecret))
            return;

        var state = _authenticatedPipes.GetValue(pipe, static _ => new StrongBox<bool>(false));
        if (Volatile.Read(ref state.Value))
            return;

        var authLock = _authLocks.GetValue(pipe, static _ => new SemaphoreSlim(1, 1));
        await authLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref state.Value))
                return;

            ulong authCorrId = Interlocked.Increment(ref _correlationCounter);
            var authTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingAuths[authCorrId] = authTcs;

            try
            {
                var authMsg = new ShuffleAuthMessage(Guid.Empty, _clusterSecret);
                var writeLock = GetWriteLock(pipe.Output);
                await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    int frameSize = ClusterFrameHeader.HeaderSize + authMsg.GetByteCount();
                    Span<byte> span = pipe.Output.GetSpan(frameSize);
                    ClusterMessageSerializer.PackFrame(authMsg, authCorrId, ClusterFrameFlags.None, span);
                    pipe.Output.Advance(frameSize);
                    await pipe.Output.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    writeLock.Release();
                }

                bool accepted = await authTcs.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                if (!accepted)
                {
                    throw new UnauthorizedAccessException("Shuffle server rejected cluster secret authentication.");
                }

                Volatile.Write(ref state.Value, true);
            }
            finally
            {
                _pendingAuths.TryRemove(authCorrId, out _);
            }
        }
        finally
        {
            authLock.Release();
        }
    }

    /// <summary>
    /// Fetches a raw unmanaged partition buffer over an existing duplex pipe.
    /// </summary>
    public async ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipe);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) != 0, this);

        EnsureReceiverLoopRunning(pipe);
        await EnsureAuthenticatedAsync(pipe, cancellationToken).ConfigureAwait(false);

        ulong correlationId = Interlocked.Increment(ref _correlationCounter);
        SemaphoreSlim writeLock = GetWriteLock(pipe.Output);
        var session = new PartitionFetchSession(jobId, stageId, partitionId, correlationId, DefaultCreditWindow, pipe.Output, writeLock);

        if (!_activeSessions.TryAdd(correlationId, session))
        {
            throw new InvalidOperationException($"Duplicate correlation ID {correlationId}.");
        }

        try
        {
            // 2. Transmit PartitionFetchRequestMessage
            var request = new PartitionFetchRequestMessage(
                jobId: jobId,
                stageId: stageId,
                partitionId: partitionId,
                creditTokens: DefaultCreditWindow,
                byteOffset: 0,
                maxChunkSizeBytes: 65536);

            await writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                int frameSize = ClusterFrameHeader.HeaderSize + PartitionFetchRequestMessage.PayloadSize;
                Span<byte> frameSpan = pipe.Output.GetSpan(frameSize);
                ClusterMessageSerializer.PackFrame(request, correlationId, ClusterFrameFlags.None, frameSpan);
                pipe.Output.Advance(frameSize);
                await pipe.Output.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                writeLock.Release();
            }

            // 3. Await completion from receiver loop
            using var reg = cancellationToken.Register(() => session.CompletionSource.TrySetCanceled(cancellationToken));
            var resultBuffer = await session.CompletionSource.Task.ConfigureAwait(false);

            // 4. Send final acknowledgment releasing remaining credits
            await SendAckAsync(
                pipe.Output,
                writeLock,
                correlationId,
                jobId,
                stageId,
                partitionId,
                tokensReleased: Math.Max(1u, session.UnackedChunks),
                bytesReceived: session.BytesReceived,
                isComplete: true).ConfigureAwait(false);

            return resultBuffer;
        }
        catch
        {
            session.Buffer?.Release();
            throw;
        }
        finally
        {
            _activeSessions.TryRemove(correlationId, out _);
        }
    }

    private static async Task SendAckAsync(
        PipeWriter writer,
        SemaphoreSlim writeLock,
        ulong correlationId,
        Guid jobId,
        int stageId,
        int partitionId,
        uint tokensReleased,
        ulong bytesReceived,
        bool isComplete)
    {
        try
        {
            await writeLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                var ack = new PartitionAcknowledgeMessage(
                    jobId: jobId,
                    stageId: stageId,
                    partitionId: partitionId,
                    tokensReleased: tokensReleased,
                    bytesReceived: bytesReceived,
                    isComplete: isComplete);

                int ackFrameSize = ClusterFrameHeader.HeaderSize + PartitionAcknowledgeMessage.PayloadSize;
                Span<byte> ackSpan = writer.GetSpan(ackFrameSize);
                ClusterMessageSerializer.PackFrame(ack, correlationId, ClusterFrameFlags.None, ackSpan);
                writer.Advance(ackFrameSize);
                await writer.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                writeLock.Release();
            }
        }
        catch (Exception ex)
        {
            Trace.TraceWarning($"[ArrowShuffleClient] Ack send warning (Job={jobId}, Stage={stageId}, Part={partitionId}): {ex.Message}");
        }
    }

    /// <summary>
    /// Fetches a raw unmanaged partition buffer by connecting to a remote worker address and port.
    /// </summary>
    public async ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
        string targetAddress,
        ushort shufflePort,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 5);
        }
        socket.ReceiveTimeout = 120_000;
        socket.SendTimeout = 120_000;

        await socket.ConnectAsync(targetAddress, shufflePort, cancellationToken).ConfigureAwait(false);
        await using var pipe = new SocketDuplexPipe(socket);

        return await FetchPartitionAsync(pipe, jobId, stageId, partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Implements <see cref="IShuffleRemoteClient.FetchRemotePartitionAsync"/> via DuplexConnectionPool.
    /// </summary>
    public async ValueTask<RefCountedPartitionBuffer> FetchRemotePartitionAsync(
        Guid producerNodeId,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        if (_connectionPool == null || _nodeAddressResolver == null)
        {
            throw new InvalidOperationException("Remote partition fetching requires a configured DuplexConnectionPool and node address resolver.");
        }

        EndPoint endPoint = _nodeAddressResolver(producerNodeId);
        DuplexConnection connection = await _connectionPool.GetOrCreateConnectionAsync(producerNodeId, endPoint, cancellationToken).ConfigureAwait(false);

        return await FetchPartitionAsync(connection.Pipe, jobId, stageId, partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Fetches and deserializes an ArrowRecordBatch with zero-copy stream deserialization.
    /// </summary>
    public async ValueTask<ArrowRecordBatch> FetchRecordBatchAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        var buffer = await FetchPartitionAsync(pipe, jobId, stageId, partitionId, cancellationToken).ConfigureAwait(false);
        try
        {
            unsafe
            {
                using var stream = new UnmanagedMemoryStream((byte*)buffer.UnmanagedPointer, buffer.ByteLength);
                using var reader = new ArrowStreamReader(stream);
                var batch = reader.ReadNextRecordBatch();
                if (batch == null)
                {
                    throw new InvalidOperationException("No ArrowRecordBatch found in partition stream.");
                }
                return batch;
            }
        }
        finally
        {
            buffer.Release();
        }
    }

    /// <summary>
    /// Fetches and converts a partition directly into a Glacier.Polaris DataFrame.
    /// </summary>
    public async ValueTask<DataFrame> FetchDataFrameAsync(
        IDuplexPipe pipe,
        Guid jobId,
        int stageId,
        int partitionId,
        CancellationToken cancellationToken = default)
    {
        var buffer = await FetchPartitionAsync(pipe, jobId, stageId, partitionId, cancellationToken).ConfigureAwait(false);
        try
        {
            var batch = ArrowIpcZeroCopyReader.ReadRecordBatch(buffer);
            return GlacierStorageBridge.FromGlacierRecordBatch(batch);
        }
        finally
        {
            buffer.Release();
        }
    }

    private void EnsureReceiverLoopRunning(IDuplexPipe pipe)
    {
        _receiverLoops.GetOrAdd(pipe.Input, reader => Task.Run(() => ReceiverLoopAsync(reader, _cts.Token)));
    }

    private async Task ReceiverLoopAsync(PipeReader reader, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(ct).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (ClusterFrameReader.TryReadFrame(ref buffer, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
                {
                    uint computedCrc = ClusterCrc32.Compute(in payload);
                    if (computedCrc != header.PayloadCrc32)
                    {
                        var crcEx = new InvalidDataException($"Frame CRC32 mismatch: expected 0x{header.PayloadCrc32:X8}, computed 0x{computedCrc:X8}.");
                        if (_activeSessions.TryGetValue(header.CorrelationId, out var faultedSession))
                        {
                            faultedSession.CompletionSource.TrySetException(crcEx);
                        }
                        throw crcEx;
                    }

                    if (header.MessageType == ClusterMessageType.PartitionFetchResponse)
                    {
                        OnChunkReceived(header, payload);
                    }
                    else if (header.MessageType == ClusterMessageType.ShuffleAuth)
                    {
                        ProcessShuffleAuthResponse(header.CorrelationId, in payload);
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (result.IsCompleted || result.IsCanceled)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            // Fault active sessions and pending auths on this pipe
            foreach (var kvp in _activeSessions)
            {
                kvp.Value.CompletionSource.TrySetException(ex);
            }
            foreach (var kvp in _pendingAuths)
            {
                kvp.Value.TrySetException(ex);
            }
        }
        finally
        {
            _receiverLoops.TryRemove(reader, out _);
        }
    }

    private void ProcessShuffleAuthResponse(ulong correlationId, in ReadOnlySequence<byte> payload)
    {
        int len = checked((int)payload.Length);
        byte[]? rented = null;
        Span<byte> authSpan = len <= 512
            ? stackalloc byte[len]
            : (rented = ArrayPool<byte>.Shared.Rent(len)).AsSpan(0, len);
        try
        {
            payload.CopyTo(authSpan);
            var authMsg = ShuffleAuthMessage.Deserialize(authSpan);
            if (_pendingAuths.TryGetValue(correlationId, out var authTcs))
            {
                authTcs.TrySetResult(authMsg.Accepted);
            }
        }
        finally
        {
            if (rented != null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private void OnChunkReceived(ClusterFrameHeader header, ReadOnlySequence<byte> payload)
    {
        if (!_activeSessions.TryGetValue(header.CorrelationId, out var session))
        {
            return;
        }

        if (payload.Length < PartitionFetchResponseMessage.HeaderSize)
        {
            session.CompletionSource.TrySetException(new InvalidOperationException("Truncated PartitionFetchResponse frame."));
            return;
        }

        // 1. Parse response header (48 bytes)
        Span<byte> respHeaderSpan = stackalloc byte[PartitionFetchResponseMessage.HeaderSize];
        payload.Slice(0, PartitionFetchResponseMessage.HeaderSize).CopyTo(respHeaderSpan);

        ulong totalDataLen = BinaryPrimitives.ReadUInt64LittleEndian(respHeaderSpan.Slice(24, 8));
        var status = (PartitionFetchStatus)respHeaderSpan[32];
        var flags = (PartitionDataFlags)respHeaderSpan[33];
        ulong chunkOffset = BinaryPrimitives.ReadUInt64LittleEndian(respHeaderSpan.Slice(36, 8));
        int chunkLen = BinaryPrimitives.ReadInt32LittleEndian(respHeaderSpan.Slice(44, 4));

        if (status == PartitionFetchStatus.NotFound)
        {
            session.CompletionSource.TrySetException(new PartitionNotFoundException(session.JobId, session.StageId, session.PartitionId));
            return;
        }

        if (status != PartitionFetchStatus.Success)
        {
            session.CompletionSource.TrySetException(new PartitionFetchException(session.JobId, session.StageId, session.PartitionId, status));
            return;
        }

        if (totalDataLen > int.MaxValue)
        {
            session.CompletionSource.TrySetException(new InvalidDataException($"Partition total length ({totalDataLen}) exceeds maximum addressable buffer size."));
            return;
        }

        if (chunkLen < 0 || PartitionFetchResponseMessage.HeaderSize + (long)chunkLen > payload.Length)
        {
            session.CompletionSource.TrySetException(new InvalidDataException($"Invalid chunk length {chunkLen} for payload size {payload.Length}."));
            return;
        }

        if (chunkOffset > totalDataLen || chunkOffset + (ulong)chunkLen > totalDataLen)
        {
            session.CompletionSource.TrySetException(new InvalidDataException($"Chunk bounds [{chunkOffset}, {chunkOffset + (ulong)chunkLen}) exceed total partition length {totalDataLen}."));
            return;
        }

        // 2. Lazy allocation of 64-byte aligned unmanaged buffer on first chunk
        if (session.Buffer == null)
        {
            session.TotalLength = totalDataLen;
            session.Buffer = RefCountedPartitionBuffer.Allocate((int)totalDataLen);
        }
        else if (totalDataLen != session.TotalLength)
        {
            session.CompletionSource.TrySetException(
                new InvalidDataException($"Partition total length changed mid-stream from {session.TotalLength} to {totalDataLen}."));
            return;
        }

        // 3. Direct unmanaged copy (or compressed block decompression) from sequence to unmanaged memory
        if (chunkLen > 0)
        {
            var chunkSlice = payload.Slice(PartitionFetchResponseMessage.HeaderSize, chunkLen);
            if (flags.HasFlag(PartitionDataFlags.Compressed))
            {
                if (chunkLen < 4)
                {
                    session.CompletionSource.TrySetException(new InvalidDataException("Truncated compressed shuffle chunk."));
                    return;
                }

                byte[] compRented = ArrayPool<byte>.Shared.Rent(chunkLen);
                try
                {
                    chunkSlice.CopyTo(compRented);
                    int uncompressedLen = BinaryPrimitives.ReadInt32LittleEndian(compRented.AsSpan(0, 4));
                    if (uncompressedLen < 0 || chunkOffset + (ulong)uncompressedLen > totalDataLen)
                    {
                        session.CompletionSource.TrySetException(new InvalidDataException($"Invalid compressed chunk uncompressed length {uncompressedLen}."));
                        return;
                    }

                    int decoded = ClusterBlockCodec.Decompress(
                        compRented.AsSpan(4, chunkLen - 4),
                        session.Buffer.Span.Slice((int)chunkOffset, uncompressedLen));
                    if (decoded != uncompressedLen)
                    {
                        session.CompletionSource.TrySetException(new InvalidDataException($"Compressed shuffle chunk decoded {decoded} bytes, expected {uncompressedLen}."));
                        return;
                    }
                    session.BytesReceived += (ulong)uncompressedLen;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(compRented);
                }
            }
            else
            {
                chunkSlice.CopyTo(session.Buffer.Span.Slice((int)chunkOffset, chunkLen));
                session.BytesReceived += (ulong)chunkLen;
            }
        }

        session.UnackedChunks++;

        // 4. Final chunk check or incremental credit replenishment
        bool isFinal = flags.HasFlag(PartitionDataFlags.FinalChunk) || (session.BytesReceived >= session.TotalLength);
        if (isFinal)
        {
            session.CompletionSource.TrySetResult(session.Buffer);
        }
        else
        {
            uint ackThreshold = Math.Max(1u, session.CreditWindow / 2);
            if (session.UnackedChunks >= ackThreshold)
            {
                uint tokensToRelease = session.UnackedChunks;
                session.UnackedChunks = 0;
                _ = SendAckAsync(
                    session.Writer,
                    session.WriteLock,
                    session.CorrelationId,
                    session.JobId,
                    session.StageId,
                    session.PartitionId,
                    tokensToRelease,
                    session.BytesReceived,
                    isComplete: false);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _cts.Cancel();

        foreach (var kvp in _activeSessions)
        {
            kvp.Value.CompletionSource.TrySetCanceled();
            kvp.Value.Buffer?.Release();
        }
        _activeSessions.Clear();

        foreach (var kvp in _pendingAuths)
        {
            kvp.Value.TrySetCanceled();
        }
        _pendingAuths.Clear();

        _cts.Dispose();
    }
}
