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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Transport;

/// <summary>
/// Embedded high-throughput shuffle server running on cluster worker nodes.
/// Reads P2P partition fetch requests and streams chunked Arrow IPC responses
/// over System.IO.Pipelines with zero GC heap allocations on hot paths.
/// </summary>
public sealed class ArrowShuffleServer : IAsyncDisposable, IDisposable
{
    private readonly IShuffleDataExchange? _dataExchange;
    private readonly LocalPartitionStore _partitionStore;
    private readonly IDiskSpillManager? _spillManager;
    private readonly FlowControlCreditManager? _creditManager;
    private readonly byte[]? _expectedSecretHash;
    private readonly bool _enableCompression;
    private readonly ushort _requestedPort;
    private ushort _actualPort;
    private TcpListener? _listener;
    private Task? _acceptLoopTask;
    private readonly CancellationTokenSource _cts = new();
    private int _isDisposed;

    public ushort Port => _actualPort;
    public bool IsRunning => _listener != null && Volatile.Read(ref _isDisposed) == 0;

    public ArrowShuffleServer(
        IShuffleDataExchange? dataExchange,
        LocalPartitionStore partitionStore,
        IDiskSpillManager? spillManager = null,
        ushort port = 0,
        FlowControlCreditManager? creditManager = null,
        string? clusterSecret = null,
        bool enableCompression = false)
    {
        _dataExchange = dataExchange;
        _partitionStore = partitionStore ?? throw new ArgumentNullException(nameof(partitionStore));
        _spillManager = spillManager;
        _requestedPort = port;
        _actualPort = port;
        _creditManager = creditManager;
        _enableCompression = enableCompression;
        _expectedSecretHash = string.IsNullOrEmpty(clusterSecret)
            ? null
            : SHA256.HashData(Encoding.UTF8.GetBytes(clusterSecret));
    }

    /// <summary>
    /// Starts the shuffle server listening on the configured TCP port.
    /// </summary>
    public void Start()
    {
        if (_listener != null) return;

        _listener = new TcpListener(IPAddress.Any, _requestedPort);
        _listener.Server.NoDelay = true;
        _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            _listener.Server.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);
            _listener.Server.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
            _listener.Server.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 5);
        }
        _listener.Start();

        _actualPort = (ushort)((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoopTask = Task.Run(() => AcceptLoopAsync(_cts.Token));
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                Socket socket = await _listener!.AcceptSocketAsync(ct).ConfigureAwait(false);
                socket.NoDelay = true;
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
                {
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
                    socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 5);
                }
                socket.ReceiveTimeout = 120_000;
                socket.SendTimeout = 120_000;

                _ = Task.Run(async () =>
                {
                    await using var pipe = new SocketDuplexPipe(socket);
                    await ProcessConnectionAsync(pipe, ct).ConfigureAwait(false);
                }, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                if (ct.IsCancellationRequested) break;
                Trace.TraceWarning($"[ArrowShuffleServer] AcceptLoop warning: {ex.Message}");
            }
        }
    }

    private bool ProcessShuffleAuth(in ReadOnlySequence<byte> payload)
    {
        int payloadLen = checked((int)payload.Length);
        byte[]? rented = null;
        Span<byte> authSpan = payloadLen <= 512
            ? stackalloc byte[payloadLen]
            : (rented = ArrayPool<byte>.Shared.Rent(payloadLen)).AsSpan(0, payloadLen);
        try
        {
            payload.CopyTo(authSpan);
            var authMsg = ShuffleAuthMessage.Deserialize(authSpan);
            return VerifyClusterSecret(authMsg.ClusterSecret);
        }
        finally
        {
            if (rented != null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private bool VerifyClusterSecret(string? providedSecret)
    {
        if (_expectedSecretHash == null) return true;
        string secret = providedSecret ?? string.Empty;
        int maxBytes = Encoding.UTF8.GetMaxByteCount(secret.Length);
        byte[]? rented = null;
        Span<byte> utf8Buf = maxBytes <= 512
            ? stackalloc byte[maxBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        try
        {
            int written = Encoding.UTF8.GetBytes(secret, utf8Buf);
            Span<byte> hash = stackalloc byte[32];
            SHA256.HashData(utf8Buf.Slice(0, written), hash);
            return CryptographicOperations.FixedTimeEquals(_expectedSecretHash, hash);
        }
        finally
        {
            if (rented != null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Processes an arbitrary duplex pipe connection (in-memory or socket-backed).
    /// Used directly by unit tests, simulators, and TCP connection handlers.
    /// </summary>
    public async Task ProcessConnectionAsync(IDuplexPipe pipe, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pipe);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        var ct = linkedCts.Token;

        var reader = pipe.Input;
        var writer = pipe.Output;
        using var writeLock = new SemaphoreSlim(1, 1);
        Guid connectionId = Guid.NewGuid();
        bool isAuthenticated = _expectedSecretHash == null;
        var activeFetches = new ConcurrentDictionary<ulong, Task>();

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(ct).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;
                bool closeConnection = false;

                while (ClusterFrameReader.TryReadFrame(ref buffer, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
                {
                    uint computedCrc = ClusterCrc32.Compute(in payload);
                    if (computedCrc != header.PayloadCrc32)
                    {
                        throw new InvalidDataException($"Frame CRC32 mismatch: expected 0x{header.PayloadCrc32:X8}, computed 0x{computedCrc:X8}.");
                    }

                    if (header.MessageType == ClusterMessageType.ShuffleAuth)
                    {
                        bool accepted = ProcessShuffleAuth(in payload);
                        isAuthenticated = accepted;

                        var resp = new ShuffleAuthMessage(Guid.Empty, string.Empty, isResponse: true, accepted: accepted);
                        await SendMessageAsync(resp, header.CorrelationId, writer, writeLock, ct).ConfigureAwait(false);
                        if (!accepted)
                        {
                            closeConnection = true;
                            break;
                        }
                        continue;
                    }

                    if (!isAuthenticated)
                    {
                        if (header.MessageType == ClusterMessageType.PartitionFetchRequest &&
                            payload.Length >= PartitionFetchRequestMessage.PayloadSize)
                        {
                            Span<byte> reqSpan = stackalloc byte[PartitionFetchRequestMessage.PayloadSize];
                            payload.Slice(0, PartitionFetchRequestMessage.PayloadSize).CopyTo(reqSpan);
                            var unauthReq = PartitionFetchRequestMessage.Deserialize(reqSpan);
                            await SendStatusResponseAsync(
                                header.CorrelationId,
                                unauthReq,
                                PartitionFetchStatus.Unauthorized,
                                writer,
                                writeLock,
                                ct).ConfigureAwait(false);
                        }
                        closeConnection = true;
                        break;
                    }

                    switch (header.MessageType)
                    {
                        case ClusterMessageType.PartitionFetchRequest:
                        {
                            if (payload.Length < PartitionFetchRequestMessage.PayloadSize)
                                throw new InvalidDataException("Truncated PartitionFetchRequest payload.");

                            Span<byte> requestSpan = stackalloc byte[PartitionFetchRequestMessage.PayloadSize];
                            payload.Slice(0, PartitionFetchRequestMessage.PayloadSize).CopyTo(requestSpan);
                            var request = PartitionFetchRequestMessage.Deserialize(requestSpan);

                            if (_creditManager != null)
                            {
                                _creditManager.GrantProducerCredits(connectionId, request.JobId, request.StageId, request.PartitionId, request.CreditTokens);
                            }

                            // Decouple streaming from the read loop so PartitionAcknowledge frames are processed concurrently without deadlock
                            ulong corrId = header.CorrelationId;
                            var fetchTask = Task.Run(async () =>
                            {
                                try
                                {
                                    await HandleFetchRequestAsync(connectionId, corrId, request, writer, writeLock, ct).ConfigureAwait(false);
                                }
                                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                                {
                                    // Connection closed
                                }
                                catch (Exception ex)
                                {
                                    Trace.TraceWarning($"[ArrowShuffleServer] Fetch stream error (Job={request.JobId}, Stage={request.StageId}, Part={request.PartitionId}): {ex.Message}");
                                }
                                finally
                                {
                                    _creditManager?.CleanupPartitionState(connectionId, request.JobId, request.StageId, request.PartitionId);
                                    activeFetches.TryRemove(corrId, out _);
                                }
                            }, ct);
                            activeFetches[corrId] = fetchTask;
                            break;
                        }

                        case ClusterMessageType.PartitionAcknowledge:
                        {
                            if (payload.Length < PartitionAcknowledgeMessage.PayloadSize)
                                throw new InvalidDataException("Truncated PartitionAcknowledge payload.");

                            Span<byte> ackSpan = stackalloc byte[PartitionAcknowledgeMessage.PayloadSize];
                            payload.Slice(0, PartitionAcknowledgeMessage.PayloadSize).CopyTo(ackSpan);
                            var ack = PartitionAcknowledgeMessage.Deserialize(ackSpan);

                            if (_creditManager != null)
                            {
                                _creditManager.ReplenishProducerCredits(connectionId, ack.JobId, ack.StageId, ack.PartitionId, ack.TokensReleased);
                                if (ack.IsComplete != 0)
                                {
                                    _creditManager.CleanupPartitionState(connectionId, ack.JobId, ack.StageId, ack.PartitionId);
                                }
                            }
                            break;
                        }
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);

                if (closeConnection || result.IsCompleted || result.IsCanceled)
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
            Trace.TraceWarning($"[ArrowShuffleServer] Connection closed or faulted: {ex.Message}");
        }
        finally
        {
            linkedCts.Cancel();
            if (!activeFetches.IsEmpty)
            {
                try { await Task.WhenAll(activeFetches.Values).ConfigureAwait(false); }
                catch (OperationCanceledException) { }
                catch (Exception ex) { Trace.TraceWarning($"[ArrowShuffleServer] Fetch teardown warning: {ex.Message}"); }
            }
        }
    }

    private static async ValueTask SendMessageAsync(
        IClusterMessage message,
        ulong correlationId,
        PipeWriter writer,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        await writeLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            int payloadSize = message.GetByteCount();
            int totalSize = ClusterFrameHeader.HeaderSize + payloadSize;
            Span<byte> span = writer.GetSpan(totalSize);
            ClusterMessageSerializer.PackFrame(message, correlationId, ClusterFrameFlags.None, span);
            writer.Advance(totalSize);
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            writeLock.Release();
        }
    }

    private async ValueTask HandleFetchRequestAsync(
        Guid connectionId,
        ulong correlationId,
        PartitionFetchRequestMessage request,
        PipeWriter writer,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        Guid jobId = request.JobId;
        int stageId = request.StageId;
        int partitionId = request.PartitionId;
        uint maxChunk = request.MaxChunkSizeBytes > 0 ? request.MaxChunkSizeBytes : 65536;

        // 1. Try local memory store
        if (_partitionStore.TryGetPartition(jobId, stageId, partitionId, out PartitionEntry? entry) && entry != null)
        {
            if (entry.State == PartitionStorageState.InMemory && entry.TryRetainBuffer(out var buffer) && buffer != null)
            {
                try
                {
                    await StreamBufferChunksAsync(connectionId, correlationId, request, buffer, maxChunk, writer, writeLock, ct).ConfigureAwait(false);
                    return;
                }
                finally
                {
                    buffer.Release();
                }
            }
            else if (entry.State == PartitionStorageState.SpilledToDisk && entry.SpillFilePath != null && File.Exists(entry.SpillFilePath))
            {
                // 2. Stream directly from disk scratch file in chunked frames
                await StreamFileChunksAsync(connectionId, correlationId, request, entry.SpillFilePath, maxChunk, writer, writeLock, ct).ConfigureAwait(false);
                return;
            }
        }

        // 2. Check spill manager if available
        if (_spillManager != null && _spillManager.TryGetSpilledMetadata(jobId, stageId, partitionId, out var spilledMeta) && spilledMeta != null && File.Exists(spilledMeta.FilePath))
        {
            await StreamFileChunksAsync(connectionId, correlationId, request, spilledMeta.FilePath, maxChunk, writer, writeLock, ct).ConfigureAwait(false);
            return;
        }

        // 3. Partition not found
        await SendStatusResponseAsync(correlationId, request, PartitionFetchStatus.NotFound, writer, writeLock, ct).ConfigureAwait(false);
    }

    private async ValueTask StreamBufferChunksAsync(
        Guid connectionId,
        ulong correlationId,
        PartitionFetchRequestMessage request,
        RefCountedPartitionBuffer buffer,
        uint maxChunk,
        PipeWriter writer,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        int totalLen = buffer.ByteLength;
        ulong offset = request.ByteOffset;

        if (offset >= (ulong)totalLen)
        {
            if (_creditManager != null)
            {
                await _creditManager.WaitForProducerCreditAsync(connectionId, request.JobId, request.StageId, request.PartitionId, ct).ConfigureAwait(false);
            }
            await WriteChunkFrameAsync(
                correlationId, request.JobId, request.StageId, request.PartitionId,
                (ulong)totalLen, offset, ReadOnlyMemory<byte>.Empty, isFinal: true, _enableCompression, writer, writeLock, ct).ConfigureAwait(false);
            return;
        }

        while (offset < (ulong)totalLen && !ct.IsCancellationRequested)
        {
            if (_creditManager != null)
            {
                await _creditManager.WaitForProducerCreditAsync(connectionId, request.JobId, request.StageId, request.PartitionId, ct).ConfigureAwait(false);
            }

            int remaining = totalLen - (int)offset;
            int chunkLen = (int)Math.Min((ulong)remaining, (ulong)maxChunk);
            bool isFinal = (offset + (ulong)chunkLen) >= (ulong)totalLen;

            ReadOnlyMemory<byte> chunkMemory = buffer.Memory.Slice((int)offset, chunkLen);

            await WriteChunkFrameAsync(
                correlationId, request.JobId, request.StageId, request.PartitionId,
                (ulong)totalLen, offset, chunkMemory, isFinal, _enableCompression, writer, writeLock, ct).ConfigureAwait(false);

            offset += (ulong)chunkLen;
        }
    }

    private async ValueTask StreamFileChunksAsync(
        Guid connectionId,
        ulong correlationId,
        PartitionFetchRequestMessage request,
        string filePath,
        uint maxChunk,
        PipeWriter writer,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, useAsync: true);
        ulong totalLen = (ulong)fs.Length;
        ulong offset = request.ByteOffset;

        if (offset >= totalLen)
        {
            if (_creditManager != null)
            {
                await _creditManager.WaitForProducerCreditAsync(connectionId, request.JobId, request.StageId, request.PartitionId, ct).ConfigureAwait(false);
            }
            await WriteChunkFrameAsync(
                correlationId, request.JobId, request.StageId, request.PartitionId,
                totalLen, offset, ReadOnlyMemory<byte>.Empty, isFinal: true, _enableCompression, writer, writeLock, ct).ConfigureAwait(false);
            return;
        }

        fs.Seek((long)offset, SeekOrigin.Begin);

        byte[] chunkBuffer = ArrayPool<byte>.Shared.Rent((int)maxChunk);
        try
        {
            while (offset < totalLen && !ct.IsCancellationRequested)
            {
                int remaining = (int)(totalLen - offset);
                int toRead = (int)Math.Min((ulong)remaining, (ulong)maxChunk);
                int read = await fs.ReadAsync(chunkBuffer.AsMemory(0, toRead), ct).ConfigureAwait(false);
                if (read == 0) break;

                if (_creditManager != null)
                {
                    await _creditManager.WaitForProducerCreditAsync(connectionId, request.JobId, request.StageId, request.PartitionId, ct).ConfigureAwait(false);
                }

                bool isFinal = (offset + (ulong)read) >= totalLen;

                await WriteChunkFrameAsync(
                    correlationId, request.JobId, request.StageId, request.PartitionId,
                    totalLen, offset, chunkBuffer.AsMemory(0, read), isFinal, _enableCompression, writer, writeLock, ct).ConfigureAwait(false);

                offset += (ulong)read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
    }

    private static async ValueTask SendStatusResponseAsync(
        ulong correlationId,
        PartitionFetchRequestMessage request,
        PartitionFetchStatus status,
        PipeWriter writer,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        await writeLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            int payloadSize = PartitionFetchResponseMessage.HeaderSize;
            int totalFrameSize = ClusterFrameHeader.HeaderSize + payloadSize;

            Span<byte> frameSpan = writer.GetSpan(totalFrameSize);

            Span<byte> msgHeaderSpan = frameSpan.Slice(ClusterFrameHeader.HeaderSize, PartitionFetchResponseMessage.HeaderSize);
            request.JobId.TryWriteBytes(msgHeaderSpan.Slice(0, 16));
            BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(16, 4), request.StageId);
            BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(20, 4), request.PartitionId);
            BinaryPrimitives.WriteUInt64LittleEndian(msgHeaderSpan.Slice(24, 8), 0);
            msgHeaderSpan[32] = (byte)status;
            msgHeaderSpan[33] = 0;
            BinaryPrimitives.WriteUInt16LittleEndian(msgHeaderSpan.Slice(34, 2), 0);
            BinaryPrimitives.WriteUInt64LittleEndian(msgHeaderSpan.Slice(36, 8), 0);
            BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(44, 4), 0);

            uint crc32 = ClusterCrc32.Compute(msgHeaderSpan);

            var header = new ClusterFrameHeader(
                ClusterMessageType.PartitionFetchResponse,
                correlationId,
                (ulong)payloadSize,
                crc32,
                ClusterFrameFlags.None);
            header.WriteTo(frameSpan.Slice(0, ClusterFrameHeader.HeaderSize));

            writer.Advance(totalFrameSize);
            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            writeLock.Release();
        }
    }

    private static async ValueTask WriteChunkFrameAsync(
        ulong correlationId,
        Guid jobId,
        int stageId,
        int partitionId,
        ulong totalLength,
        ulong chunkOffset,
        ReadOnlyMemory<byte> data,
        bool isFinal,
        bool enableCompression,
        PipeWriter writer,
        SemaphoreSlim writeLock,
        CancellationToken ct)
    {
        int rawLen = data.Length;
        int wireChunkLen = rawLen;
        bool compressed = false;
        byte[]? compBuffer = null;

        if (enableCompression && rawLen >= 64)
        {
            int maxComp = ClusterBlockCodec.GetMaxCompressedLength(rawLen);
            compBuffer = ArrayPool<byte>.Shared.Rent(maxComp);
            int compLen = ClusterBlockCodec.Compress(data.Span, compBuffer.AsSpan(0, maxComp));
            if (compLen > 0 && 4 + compLen < rawLen)
            {
                wireChunkLen = 4 + compLen;
                compressed = true;
            }
            else
            {
                ArrayPool<byte>.Shared.Return(compBuffer);
                compBuffer = null;
            }
        }

        try
        {
            await writeLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                int payloadSize = PartitionFetchResponseMessage.HeaderSize + wireChunkLen;
                int totalFrameSize = ClusterFrameHeader.HeaderSize + payloadSize;

                Span<byte> frameSpan = writer.GetSpan(totalFrameSize);

                // 1. Write PartitionFetchResponseMessage header into offset 32..79
                Span<byte> msgHeaderSpan = frameSpan.Slice(ClusterFrameHeader.HeaderSize, PartitionFetchResponseMessage.HeaderSize);
                jobId.TryWriteBytes(msgHeaderSpan.Slice(0, 16));
                BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(16, 4), stageId);
                BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(20, 4), partitionId);
                BinaryPrimitives.WriteUInt64LittleEndian(msgHeaderSpan.Slice(24, 8), totalLength);
                msgHeaderSpan[32] = (byte)PartitionFetchStatus.Success;

                PartitionDataFlags flags = PartitionDataFlags.ArrowIpcBatch;
                if (compressed) flags |= PartitionDataFlags.Compressed;
                if (isFinal) flags |= PartitionDataFlags.FinalChunk;
                msgHeaderSpan[33] = (byte)flags;

                BinaryPrimitives.WriteUInt16LittleEndian(msgHeaderSpan.Slice(34, 2), 0); // Reserved
                BinaryPrimitives.WriteUInt64LittleEndian(msgHeaderSpan.Slice(36, 8), chunkOffset);
                BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(44, 4), wireChunkLen);

                // 2. Copy chunk memory (or compressed block) into offset 80..80+wireChunkLen
                if (wireChunkLen > 0)
                {
                    Span<byte> dstData = frameSpan.Slice(ClusterFrameHeader.HeaderSize + PartitionFetchResponseMessage.HeaderSize, wireChunkLen);
                    if (compressed && compBuffer != null)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(dstData.Slice(0, 4), rawLen);
                        compBuffer.AsSpan(0, wireChunkLen - 4).CopyTo(dstData.Slice(4));
                    }
                    else
                    {
                        data.Span.CopyTo(dstData);
                    }
                }

                // 3. Compute CRC32 over entire payload (header + data)
                uint crc32 = ClusterCrc32.Compute(frameSpan.Slice(ClusterFrameHeader.HeaderSize, payloadSize));

                // 4. Write 32-byte ClusterFrameHeader into offset 0..31
                var header = new ClusterFrameHeader(
                    ClusterMessageType.PartitionFetchResponse,
                    correlationId,
                    (ulong)payloadSize,
                    crc32,
                    compressed ? ClusterFrameFlags.Compressed : ClusterFrameFlags.None);
                header.WriteTo(frameSpan.Slice(0, ClusterFrameHeader.HeaderSize));

                writer.Advance(totalFrameSize);
                await writer.FlushAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                writeLock.Release();
            }
        }
        finally
        {
            if (compBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(compBuffer);
            }
        }
    }

    public async Task StopAsync()
    {
        _cts.Cancel();
        _listener?.Stop();

        if (_acceptLoopTask != null)
        {
            try { await _acceptLoopTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Trace.TraceWarning($"[ArrowShuffleServer] Stop warning: {ex.Message}"); }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        await StopAsync().ConfigureAwait(false);
        _cts.Dispose();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _cts.Cancel();
        _listener?.Stop();
        _cts.Dispose();
    }
}
