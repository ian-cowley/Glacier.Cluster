namespace Glacier.Cluster.Compute;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Shuffle;

/// <summary>
/// Executes the 2(N-1) communication step Ring AllReduce algorithm on a local node.
/// Operates directly over System.IO.Pipelines with zero GC allocation on the hot path.
/// </summary>
public sealed class RingAllReduceWorker : IAsyncDisposable
{
    private readonly Guid _localNodeId;
    private readonly int _rank;
    private readonly int _worldSize;
    private readonly IDuplexPipe _successorPipe;
    private readonly IDuplexPipe _predecessorPipe;
    private readonly int[] _chunkOffsets;
    private readonly int[] _chunkLengths;

    public Guid LocalNodeId => _localNodeId;
    public int Rank => _rank;
    public int WorldSize => _worldSize;

    public RingAllReduceWorker(
        Guid localNodeId,
        int rank,
        int worldSize,
        IDuplexPipe successorPipe,
        IDuplexPipe predecessorPipe,
        int totalElements)
    {
        _localNodeId = localNodeId;
        _rank = rank;
        _worldSize = worldSize;
        _successorPipe = successorPipe ?? throw new ArgumentNullException(nameof(successorPipe));
        _predecessorPipe = predecessorPipe ?? throw new ArgumentNullException(nameof(predecessorPipe));

        _chunkOffsets = new int[worldSize];
        _chunkLengths = new int[worldSize];
        ComputeChunkPartitions(totalElements, worldSize, _chunkOffsets, _chunkLengths);
    }

    /// <summary>
    /// Computes deterministic chunk offsets and lengths across ring nodes.
    /// </summary>
    public static void ComputeChunkPartitions(
        int totalElements,
        int worldSize,
        Span<int> offsets,
        Span<int> lengths)
    {
        if (worldSize <= 0) throw new ArgumentOutOfRangeException(nameof(worldSize));
        int baseChunkSize = totalElements / worldSize;
        int remainder = totalElements % worldSize;

        for (int k = 0; k < worldSize; k++)
        {
            lengths[k] = (k < remainder) ? baseChunkSize + 1 : baseChunkSize;
            offsets[k] = k * baseChunkSize + Math.Min(k, remainder);
        }
    }

    /// <summary>
    /// Executes the full 2(N-1) Ring AllReduce steps on the provided vector buffer in-place.
    /// </summary>
    public async ValueTask AllReduceAsync(
        Memory<float> vectorBuffer,
        Guid sessionId,
        CancellationToken cancellationToken = default)
    {
        if (_worldSize <= 1)
        {
            // Trivial reduction: 1 node has all data already
            return;
        }

        // =========================================================================
        // Phase 1: Scatter-Reduce (N - 1 communication steps)
        // =========================================================================
        for (int step = 0; step < _worldSize - 1; step++)
        {
            int sendChunkIdx = (_rank - step + _worldSize) % _worldSize;
            int recvChunkIdx = (_rank - step - 1 + _worldSize) % _worldSize;

            Memory<float> sendSlice = vectorBuffer.Slice(_chunkOffsets[sendChunkIdx], _chunkLengths[sendChunkIdx]);
            Memory<float> targetRecvSlice = vectorBuffer.Slice(_chunkOffsets[recvChunkIdx], _chunkLengths[recvChunkIdx]);

            Task sendTask = SendChunkAsync(
                _successorPipe.Output,
                sessionId,
                step,
                phase: 0, // Scatter-Reduce
                sendChunkIdx,
                sendSlice,
                cancellationToken);

            Task recvTask = ReceiveAndReduceChunkAsync(
                _predecessorPipe.Input,
                sessionId,
                step,
                expectedPhase: 0,
                expectedChunkIdx: recvChunkIdx,
                targetChunk: targetRecvSlice,
                ct: cancellationToken);

            await Task.WhenAll(sendTask, recvTask).ConfigureAwait(false);
        }

        // =========================================================================
        // Phase 2: AllGather (N - 1 communication steps)
        // =========================================================================
        for (int step = 0; step < _worldSize - 1; step++)
        {
            int sendChunkIdx = (_rank - step + 1 + _worldSize) % _worldSize;
            int recvChunkIdx = (_rank - step + _worldSize) % _worldSize;

            Memory<float> sendSlice = vectorBuffer.Slice(_chunkOffsets[sendChunkIdx], _chunkLengths[sendChunkIdx]);
            Memory<float> targetRecvSlice = vectorBuffer.Slice(_chunkOffsets[recvChunkIdx], _chunkLengths[recvChunkIdx]);

            Task sendTask = SendChunkAsync(
                _successorPipe.Output,
                sessionId,
                step,
                phase: 1, // AllGather
                sendChunkIdx,
                sendSlice,
                cancellationToken);

            Task recvTask = ReceiveAndCopyChunkAsync(
                _predecessorPipe.Input,
                sessionId,
                step,
                expectedPhase: 1,
                expectedChunkIdx: recvChunkIdx,
                targetChunk: targetRecvSlice,
                ct: cancellationToken);

            await Task.WhenAll(sendTask, recvTask).ConfigureAwait(false);
        }
    }

    private static async Task SendChunkAsync(
        PipeWriter writer,
        Guid sessionId,
        int step,
        byte phase,
        int chunkIdx,
        Memory<float> data,
        CancellationToken ct)
    {
        int elementCount = data.Length;
        int payloadBytes = 32 + (elementCount * sizeof(float));
        int totalFrameSize = ClusterFrameHeader.HeaderSize + payloadBytes;

        Memory<byte> memory = writer.GetMemory(totalFrameSize);
        Span<byte> span = memory.Span;

        // Pack payload
        Span<byte> payloadSpan = span.Slice(ClusterFrameHeader.HeaderSize, payloadBytes);
        sessionId.TryWriteBytes(payloadSpan.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(payloadSpan.Slice(16, 4), step);
        payloadSpan[20] = phase;
        payloadSpan[21] = 0;
        payloadSpan[22] = 0;
        payloadSpan[23] = 0;
        BinaryPrimitives.WriteInt32LittleEndian(payloadSpan.Slice(24, 4), chunkIdx);
        BinaryPrimitives.WriteInt32LittleEndian(payloadSpan.Slice(28, 4), elementCount);

        if (elementCount > 0)
        {
            ReadOnlySpan<byte> floatBytes = MemoryMarshal.AsBytes(data.Span);
            floatBytes.CopyTo(payloadSpan.Slice(32, floatBytes.Length));
        }

        uint crc = ClusterCrc32.Compute(payloadSpan);
        var header = new ClusterFrameHeader(
            ClusterMessageType.RingAllReduceStep,
            correlationId: (ulong)step,
            payloadLength: (ulong)payloadBytes,
            crc32: crc,
            flags: ClusterFrameFlags.None);
        header.WriteTo(span.Slice(0, ClusterFrameHeader.HeaderSize));

        writer.Advance(totalFrameSize);
        FlushResult flush = await writer.FlushAsync(ct).ConfigureAwait(false);
        if (flush.IsCanceled || flush.IsCompleted)
            throw new InvalidOperationException("PipeWriter completed or cancelled while sending chunk.");
    }

    private static async Task ReceiveAndReduceChunkAsync(
        PipeReader reader,
        Guid expectedSessionId,
        int expectedStep,
        byte expectedPhase,
        int expectedChunkIdx,
        Memory<float> targetChunk,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            ReadResult result = await reader.ReadAsync(ct).ConfigureAwait(false);
            ReadOnlySequence<byte> buffer = result.Buffer;

            while (ClusterFrameReader.TryReadFrame(ref buffer, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
            {
                if (header.MessageType == ClusterMessageType.RingAllReduceStep)
                {
                    Span<byte> metaSpan = stackalloc byte[32];
                    payload.Slice(0, 32).CopyTo(metaSpan);

                    int elementCount = BinaryPrimitives.ReadInt32LittleEndian(metaSpan.Slice(28, 4));

                    if (elementCount > 0)
                    {
                        ReadOnlySequence<byte> floatSeq = payload.Slice(32, elementCount * sizeof(float));
                        using var tempOwner = MemoryPool<float>.Shared.Rent(elementCount);
                        Span<float> tempSpan = tempOwner.Memory.Span.Slice(0, elementCount);
                        Span<byte> tempByteSpan = MemoryMarshal.AsBytes(tempSpan);
                        floatSeq.CopyTo(tempByteSpan);

                        VectorReductionKernels.AddInPlace(targetChunk.Span, tempSpan);
                    }

                    reader.AdvanceTo(buffer.Start, buffer.Start);
                    return;
                }
            }

            reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted)
                throw new InvalidOperationException("PipeReader completed before expected frame arrived.");
        }
    }

    private static async Task ReceiveAndCopyChunkAsync(
        PipeReader reader,
        Guid expectedSessionId,
        int expectedStep,
        byte expectedPhase,
        int expectedChunkIdx,
        Memory<float> targetChunk,
        CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            ReadResult result = await reader.ReadAsync(ct).ConfigureAwait(false);
            ReadOnlySequence<byte> buffer = result.Buffer;

            while (ClusterFrameReader.TryReadFrame(ref buffer, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
            {
                if (header.MessageType == ClusterMessageType.RingAllReduceStep)
                {
                    Span<byte> metaSpan = stackalloc byte[32];
                    payload.Slice(0, 32).CopyTo(metaSpan);

                    int elementCount = BinaryPrimitives.ReadInt32LittleEndian(metaSpan.Slice(28, 4));

                    if (elementCount > 0)
                    {
                        ReadOnlySequence<byte> floatSeq = payload.Slice(32, elementCount * sizeof(float));
                        Span<byte> targetBytes = MemoryMarshal.AsBytes(targetChunk.Span);
                        floatSeq.CopyTo(targetBytes);
                    }

                    reader.AdvanceTo(buffer.Start, buffer.Start);
                    return;
                }
            }

            reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted)
                throw new InvalidOperationException("PipeReader completed before expected frame arrived.");
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
