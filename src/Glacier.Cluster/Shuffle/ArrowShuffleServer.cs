namespace Glacier.Cluster.Shuffle;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;

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
        FlowControlCreditManager? creditManager = null)
    {
        _dataExchange = dataExchange;
        _partitionStore = partitionStore ?? throw new ArgumentNullException(nameof(partitionStore));
        _spillManager = spillManager;
        _requestedPort = port;
        _actualPort = port;
        _creditManager = creditManager;
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
            catch (Exception)
            {
                if (ct.IsCancellationRequested) break;
            }
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

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(ct).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (ClusterFrameReader.TryReadFrame(ref buffer, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
                {
                    uint computedCrc = ComputePayloadCrc32(payload);
                    if (computedCrc != header.PayloadCrc32)
                    {
                        throw new InvalidDataException($"Frame CRC32 mismatch: expected 0x{header.PayloadCrc32:X8}, computed 0x{computedCrc:X8}.");
                    }

                    await HandleFrameAsync(header, payload, writer, ct).ConfigureAwait(false);
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
        catch (Exception)
        {
            // Connection closed or faulted
        }
    }

    private async ValueTask HandleFrameAsync(
        ClusterFrameHeader header,
        ReadOnlySequence<byte> payload,
        PipeWriter writer,
        CancellationToken ct)
    {
        switch (header.MessageType)
        {
            case ClusterMessageType.PartitionFetchRequest:
            {
                Span<byte> requestSpan = stackalloc byte[PartitionFetchRequestMessage.PayloadSize];
                payload.Slice(0, PartitionFetchRequestMessage.PayloadSize).CopyTo(requestSpan);

                var request = PartitionFetchRequestMessage.Deserialize(requestSpan);
                await HandleFetchRequestAsync(header.CorrelationId, request, writer, ct).ConfigureAwait(false);
                break;
            }

            case ClusterMessageType.PartitionAcknowledge:
            {
                Span<byte> ackSpan = stackalloc byte[PartitionAcknowledgeMessage.PayloadSize];
                payload.Slice(0, PartitionAcknowledgeMessage.PayloadSize).CopyTo(ackSpan);

                var ack = PartitionAcknowledgeMessage.Deserialize(ackSpan);
                if (_creditManager != null)
                {
                    _creditManager.ReplenishProducerCredits(ack.JobId, ack.JobId, ack.StageId, ack.PartitionId, ack.TokensReleased);
                    if (ack.IsComplete != 0)
                    {
                        _creditManager.CleanupPartitionState(ack.JobId, ack.JobId, ack.StageId, ack.PartitionId);
                    }
                }
                break;
            }

            default:
                break;
        }
    }

    private async ValueTask HandleFetchRequestAsync(
        ulong correlationId,
        PartitionFetchRequestMessage request,
        PipeWriter writer,
        CancellationToken ct)
    {
        Guid jobId = request.JobId;
        int stageId = request.StageId;
        int partitionId = request.PartitionId;
        uint maxChunk = request.MaxChunkSizeBytes > 0 ? request.MaxChunkSizeBytes : 65536;

        if (_creditManager != null)
        {
            _creditManager.GrantProducerCredits(jobId, jobId, stageId, partitionId, request.CreditTokens);
        }

        // 1. Try local memory store
        if (_partitionStore.TryGetPartition(jobId, stageId, partitionId, out PartitionEntry? entry) && entry != null)
        {
            if (entry.State == PartitionStorageState.InMemory && entry.TryRetainBuffer(out var buffer) && buffer != null)
            {
                try
                {
                    await StreamBufferChunksAsync(correlationId, request, buffer, maxChunk, writer, ct).ConfigureAwait(false);
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
                await StreamFileChunksAsync(correlationId, request, entry.SpillFilePath, maxChunk, writer, ct).ConfigureAwait(false);
                return;
            }
        }

        // 2. Check spill manager if available
        if (_spillManager != null && _spillManager.TryGetSpilledMetadata(jobId, stageId, partitionId, out var spilledMeta) && spilledMeta != null && File.Exists(spilledMeta.FilePath))
        {
            await StreamFileChunksAsync(correlationId, request, spilledMeta.FilePath, maxChunk, writer, ct).ConfigureAwait(false);
            return;
        }

        // 3. Partition not found
        await SendNotFoundResponseAsync(correlationId, request, writer, ct).ConfigureAwait(false);
    }

    private static async ValueTask StreamBufferChunksAsync(
        ulong correlationId,
        PartitionFetchRequestMessage request,
        RefCountedPartitionBuffer buffer,
        uint maxChunk,
        PipeWriter writer,
        CancellationToken ct)
    {
        int totalLen = buffer.ByteLength;
        ulong offset = request.ByteOffset;

        if (offset >= (ulong)totalLen)
        {
            await WriteChunkFrameAsync(
                correlationId, request.JobId, request.StageId, request.PartitionId,
                (ulong)totalLen, offset, ReadOnlyMemory<byte>.Empty, isFinal: true, writer, ct).ConfigureAwait(false);
            return;
        }

        while (offset < (ulong)totalLen && !ct.IsCancellationRequested)
        {
            int remaining = totalLen - (int)offset;
            int chunkLen = (int)Math.Min((ulong)remaining, (ulong)maxChunk);
            bool isFinal = (offset + (ulong)chunkLen) >= (ulong)totalLen;

            ReadOnlyMemory<byte> chunkMemory = buffer.Memory.Slice((int)offset, chunkLen);

            await WriteChunkFrameAsync(
                correlationId, request.JobId, request.StageId, request.PartitionId,
                (ulong)totalLen, offset, chunkMemory, isFinal, writer, ct).ConfigureAwait(false);

            offset += (ulong)chunkLen;
        }
    }

    private static async ValueTask StreamFileChunksAsync(
        ulong correlationId,
        PartitionFetchRequestMessage request,
        string filePath,
        uint maxChunk,
        PipeWriter writer,
        CancellationToken ct)
    {
        await using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536, useAsync: true);
        ulong totalLen = (ulong)fs.Length;
        ulong offset = request.ByteOffset;

        if (offset >= totalLen)
        {
            await WriteChunkFrameAsync(
                correlationId, request.JobId, request.StageId, request.PartitionId,
                totalLen, offset, ReadOnlyMemory<byte>.Empty, isFinal: true, writer, ct).ConfigureAwait(false);
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

                bool isFinal = (offset + (ulong)read) >= totalLen;

                await WriteChunkFrameAsync(
                    correlationId, request.JobId, request.StageId, request.PartitionId,
                    totalLen, offset, chunkBuffer.AsMemory(0, read), isFinal, writer, ct).ConfigureAwait(false);

                offset += (ulong)read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
    }

    private static async ValueTask SendNotFoundResponseAsync(
        ulong correlationId,
        PartitionFetchRequestMessage request,
        PipeWriter writer,
        CancellationToken ct)
    {
        int payloadSize = PartitionFetchResponseMessage.HeaderSize;
        int totalFrameSize = ClusterFrameHeader.HeaderSize + payloadSize;

        Span<byte> frameSpan = writer.GetSpan(totalFrameSize);

        Span<byte> msgHeaderSpan = frameSpan.Slice(ClusterFrameHeader.HeaderSize, PartitionFetchResponseMessage.HeaderSize);
        request.JobId.TryWriteBytes(msgHeaderSpan.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(16, 4), request.StageId);
        BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(20, 4), request.PartitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(msgHeaderSpan.Slice(24, 8), 0);
        msgHeaderSpan[32] = (byte)PartitionFetchStatus.NotFound;
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

    private static async ValueTask WriteChunkFrameAsync(
        ulong correlationId,
        Guid jobId,
        int stageId,
        int partitionId,
        ulong totalLength,
        ulong chunkOffset,
        ReadOnlyMemory<byte> data,
        bool isFinal,
        PipeWriter writer,
        CancellationToken ct)
    {
        int chunkLen = data.Length;
        int payloadSize = PartitionFetchResponseMessage.HeaderSize + chunkLen;
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
        if (isFinal) flags |= PartitionDataFlags.FinalChunk;
        msgHeaderSpan[33] = (byte)flags;

        BinaryPrimitives.WriteUInt16LittleEndian(msgHeaderSpan.Slice(34, 2), 0); // Reserved
        BinaryPrimitives.WriteUInt64LittleEndian(msgHeaderSpan.Slice(36, 8), chunkOffset);
        BinaryPrimitives.WriteInt32LittleEndian(msgHeaderSpan.Slice(44, 4), chunkLen);

        // 2. Copy unmanaged chunk memory into offset 80..80+chunkLen
        if (chunkLen > 0)
        {
            data.Span.CopyTo(frameSpan.Slice(ClusterFrameHeader.HeaderSize + PartitionFetchResponseMessage.HeaderSize, chunkLen));
        }

        // 3. Compute CRC32 over entire payload (header + data)
        uint crc32 = ClusterCrc32.Compute(frameSpan.Slice(ClusterFrameHeader.HeaderSize, payloadSize));

        // 4. Write 32-byte ClusterFrameHeader into offset 0..31
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

    private static uint ComputePayloadCrc32(ReadOnlySequence<byte> sequence)
    {
        if (sequence.IsEmpty)
            return 0;

        if (sequence.IsSingleSegment)
            return ClusterCrc32.Compute(sequence.FirstSpan);

        int length = checked((int)sequence.Length);
        byte[]? rented = null;
        try
        {
            Span<byte> span = length <= 4096
                ? stackalloc byte[length]
                : (rented = ArrayPool<byte>.Shared.Rent(length)).AsSpan(0, length);
            sequence.CopyTo(span);
            return ClusterCrc32.Compute(span);
        }
        finally
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    public async Task StopAsync()
    {
        _cts.Cancel();
        _listener?.Stop();

        if (_acceptLoopTask != null)
        {
            try { await _acceptLoopTask.ConfigureAwait(false); } catch { }
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
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
