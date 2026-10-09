namespace Glacier.Cluster.Shuffle;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Polaris;
using Glacier.Polaris.IO;
using Glacier.Storage.Arrow;

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
    public readonly TaskCompletionSource<RefCountedPartitionBuffer> CompletionSource;
    public RefCountedPartitionBuffer? Buffer;
    public ulong TotalLength;
    public ulong BytesReceived;

    public PartitionFetchSession(Guid jobId, int stageId, int partitionId)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
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
    private readonly DuplexConnectionPool? _connectionPool;
    private readonly Func<Guid, EndPoint>? _nodeAddressResolver;
    private readonly ConcurrentDictionary<ulong, PartitionFetchSession> _activeSessions = new();
    private readonly ConcurrentDictionary<PipeReader, Task> _receiverLoops = new();
    private readonly CancellationTokenSource _cts = new();
    private ulong _correlationCounter;
    private int _isDisposed;

    public ArrowShuffleClient(
        DuplexConnectionPool? connectionPool = null,
        Func<Guid, EndPoint>? nodeAddressResolver = null)
    {
        _connectionPool = connectionPool;
        _nodeAddressResolver = nodeAddressResolver;
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

        ulong correlationId = Interlocked.Increment(ref _correlationCounter);
        var session = new PartitionFetchSession(jobId, stageId, partitionId);

        if (!_activeSessions.TryAdd(correlationId, session))
        {
            throw new InvalidOperationException($"Duplicate correlation ID {correlationId}.");
        }

        try
        {
            // 1. Ensure receiver loop is active on this pipe
            EnsureReceiverLoopRunning(pipe);

            // 2. Transmit PartitionFetchRequestMessage
            var request = new PartitionFetchRequestMessage(
                jobId: jobId,
                stageId: stageId,
                partitionId: partitionId,
                creditTokens: 4,
                byteOffset: 0,
                maxChunkSizeBytes: 65536);

            int frameSize = ClusterFrameHeader.HeaderSize + PartitionFetchRequestMessage.PayloadSize;
            Span<byte> frameSpan = pipe.Output.GetSpan(frameSize);
            ClusterMessageSerializer.PackFrame(request, correlationId, ClusterFrameFlags.None, frameSpan);
            pipe.Output.Advance(frameSize);
            await pipe.Output.FlushAsync(cancellationToken).ConfigureAwait(false);

            // 3. Await completion from receiver loop
            using var reg = cancellationToken.Register(() => session.CompletionSource.TrySetCanceled(cancellationToken));
            var resultBuffer = await session.CompletionSource.Task.ConfigureAwait(false);

            // 4. Send acknowledgment releasing credits
            try
            {
                var ack = new PartitionAcknowledgeMessage(
                    jobId: jobId,
                    stageId: stageId,
                    partitionId: partitionId,
                    tokensReleased: 4,
                    bytesReceived: session.BytesReceived,
                    isComplete: true);

                int ackFrameSize = ClusterFrameHeader.HeaderSize + PartitionAcknowledgeMessage.PayloadSize;
                Span<byte> ackSpan = pipe.Output.GetSpan(ackFrameSize);
                ClusterMessageSerializer.PackFrame(ack, correlationId, ClusterFrameFlags.None, ackSpan);
                pipe.Output.Advance(ackFrameSize);
                await pipe.Output.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Non-critical ack flush failure
            }

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
                    uint computedCrc = ComputePayloadCrc32(payload);
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
            // Fault active sessions on this pipe
            foreach (var kvp in _activeSessions)
            {
                kvp.Value.CompletionSource.TrySetException(ex);
            }
        }
        finally
        {
            _receiverLoops.TryRemove(reader, out _);
        }
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

    private void OnChunkReceived(ClusterFrameHeader header, ReadOnlySequence<byte> payload)
    {
        if (!_activeSessions.TryGetValue(header.CorrelationId, out var session))
        {
            return;
        }

        uint computedCrc = ComputePayloadCrc32(payload);
        if (computedCrc != header.PayloadCrc32)
        {
            session.CompletionSource.TrySetException(new InvalidDataException(
                $"Frame CRC32 mismatch: expected 0x{header.PayloadCrc32:X8}, computed 0x{computedCrc:X8}."));
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

        // 2. Lazy allocation of 64-byte aligned unmanaged buffer on first chunk
        if (session.Buffer == null)
        {
            session.TotalLength = totalDataLen;
            session.Buffer = RefCountedPartitionBuffer.Allocate((int)totalDataLen);
        }

        // 3. Direct unmanaged copy from sequence to unmanaged memory
        if (chunkLen > 0)
        {
            var chunkSlice = payload.Slice(PartitionFetchResponseMessage.HeaderSize, chunkLen);
            chunkSlice.CopyTo(session.Buffer.Span.Slice((int)chunkOffset, chunkLen));
            session.BytesReceived += (ulong)chunkLen;
        }

        // 4. Final chunk check
        bool isFinal = flags.HasFlag(PartitionDataFlags.FinalChunk) || (session.BytesReceived >= session.TotalLength);
        if (isFinal)
        {
            session.CompletionSource.TrySetResult(session.Buffer);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            return;

        _cts.Cancel();

        // Cancel all pending sessions
        foreach (var kvp in _activeSessions)
        {
            kvp.Value.CompletionSource.TrySetCanceled();
            kvp.Value.Buffer?.Release();
        }
        _activeSessions.Clear();

        await Task.CompletedTask;
        _cts.Dispose();
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
