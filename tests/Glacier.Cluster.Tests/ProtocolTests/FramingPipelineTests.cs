namespace Glacier.Cluster.Tests.ProtocolTests;

using System;
using System.Buffers;
using Glacier.Cluster.Protocol;
using Xunit;

public sealed class FramingPipelineTests
{
    [Fact]
    public void TryReadFrame_BufferShorterThan32Bytes_ReturnsFalse_DoesNotAdvance()
    {
        byte[] raw = new byte[16];
        var sequence = new ReadOnlySequence<byte>(raw);

        bool result = ClusterFrameReader.TryReadFrame(ref sequence, out var header, out var payload);

        Assert.False(result);
        Assert.Equal(default, header);
        Assert.Equal(0, payload.Length);
        Assert.Equal(16, sequence.Length);
    }

    [Fact]
    public void TryReadFrame_IncompletePayload_ReturnsFalse_DoesNotAdvance()
    {
        byte[] raw = new byte[32 + 50]; // Header + 50 bytes, but header claims 100 bytes
        var header = new ClusterFrameHeader(ClusterMessageType.StageDispatch, correlationId: 1, payloadLength: 100);
        header.WriteTo(raw.AsSpan(0, 32));

        var sequence = new ReadOnlySequence<byte>(raw);

        bool result = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedHeader, out var payload);

        Assert.False(result);
        Assert.Equal(default, parsedHeader);
        Assert.Equal(0, payload.Length);
        Assert.Equal(82, sequence.Length);
    }

    [Fact]
    public void TryReadFrame_CompletePayload_ReturnsTrue_AdvancesBuffer()
    {
        byte[] raw = new byte[32 + 100];
        for (int i = 0; i < 100; i++) raw[32 + i] = (byte)(i & 0xFF);

        var header = new ClusterFrameHeader(ClusterMessageType.StageDispatch, correlationId: 99, payloadLength: 100);
        header.WriteTo(raw.AsSpan(0, 32));

        var sequence = new ReadOnlySequence<byte>(raw);

        bool result = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedHeader, out var payload);

        Assert.True(result);
        Assert.Equal(header, parsedHeader);
        Assert.Equal(100, payload.Length);
        Assert.Equal(0, sequence.Length);

        // Verify payload content
        Span<byte> readBack = stackalloc byte[100];
        payload.CopyTo(readBack);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal((byte)(i & 0xFF), readBack[i]);
        }
    }

    [Fact]
    public void TryReadFrame_ZeroPayloadLength_ReturnsTrue()
    {
        byte[] raw = new byte[32];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, correlationId: 777, payloadLength: 0);
        header.WriteTo(raw.AsSpan(0, 32));

        var sequence = new ReadOnlySequence<byte>(raw);

        bool result = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedHeader, out var payload);

        Assert.True(result);
        Assert.Equal(header, parsedHeader);
        Assert.Equal(0, payload.Length);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void TryReadFrame_SegmentedSequence_ParsesViaStackAlloc()
    {
        // Construct a multi-segment sequence where the 32-byte header is split
        // Segment 1: 16 bytes (first half of header)
        // Segment 2: 16 bytes (second half of header) + 50 bytes payload
        byte[] fullFrame = new byte[32 + 50];
        var header = new ClusterFrameHeader(ClusterMessageType.PartitionFetchResponse, correlationId: 55, payloadLength: 50);
        header.WriteTo(fullFrame.AsSpan(0, 32));
        for (int i = 0; i < 50; i++) fullFrame[32 + i] = (byte)i;

        byte[] seg1Data = new byte[16];
        Array.Copy(fullFrame, 0, seg1Data, 0, 16);

        byte[] seg2Data = new byte[16 + 50];
        Array.Copy(fullFrame, 16, seg2Data, 0, 16 + 50);

        var seg1 = new TestBufferSegment<byte>(seg1Data);
        var seg2 = seg1.Append(seg2Data);

        var multiSegmentSequence = new ReadOnlySequence<byte>(seg1, 0, seg2, seg2.Memory.Length);

        Assert.False(multiSegmentSequence.IsSingleSegment);
        Assert.Equal(82, multiSegmentSequence.Length);

        bool result = ClusterFrameReader.TryReadFrame(ref multiSegmentSequence, out var parsedHeader, out var payload);

        Assert.True(result);
        Assert.Equal(header, parsedHeader);
        Assert.Equal(50, payload.Length);
        Assert.Equal(0, multiSegmentSequence.Length);
    }

    [Fact]
    public void TryReadFrame_MultipleConsecutiveFrames_ParsesAllSequentially()
    {
        // Frame 1: 32 header + 20 payload = 52 bytes
        // Frame 2: 32 header + 40 payload = 72 bytes
        // Total: 124 bytes
        byte[] stream = new byte[124];

        var h1 = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 101, 20);
        h1.WriteTo(stream.AsSpan(0, 32));
        stream.AsSpan(32, 20).Fill(0xAA);

        var h2 = new ClusterFrameHeader(ClusterMessageType.HeartbeatPong, 102, 40);
        h2.WriteTo(stream.AsSpan(52, 32));
        stream.AsSpan(84, 40).Fill(0xBB);

        var sequence = new ReadOnlySequence<byte>(stream);

        // First frame
        bool r1 = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedH1, out var p1);
        Assert.True(r1);
        Assert.Equal(h1, parsedH1);
        Assert.Equal(20, p1.Length);
        Assert.Equal(72, sequence.Length);

        // Second frame
        bool r2 = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedH2, out var p2);
        Assert.True(r2);
        Assert.Equal(h2, parsedH2);
        Assert.Equal(40, p2.Length);
        Assert.Equal(0, sequence.Length);

        // Third attempt on exhausted stream
        bool r3 = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedH3, out var p3);
        Assert.False(r3);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void TryReadFrame_CorruptedMagic_ThrowsInvalidOperationException()
    {
        byte[] raw = new byte[64];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 32);
        header.WriteTo(raw.AsSpan(0, 32));
        raw[0] = 0xFF; // Corrupt magic

        var sequence = new ReadOnlySequence<byte>(raw);
        Assert.Throws<InvalidOperationException>(() => ClusterFrameReader.TryReadFrame(ref sequence, out _, out _));
    }

    [Fact]
    public void TryReadFrame_ZeroAllocation_MeasuredEmpirically()
    {
        byte[] frameBytes = new byte[32 + 64];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 64);
        header.WriteTo(frameBytes.AsSpan(0, 32));

        // Warm up JIT
        for (int i = 0; i < 100; i++)
        {
            var seq = new ReadOnlySequence<byte>(frameBytes);
            ClusterFrameReader.TryReadFrame(ref seq, out _, out _);
        }

        // Measure allocations
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        const int iterations = 10_000;
        for (int i = 0; i < iterations; i++)
        {
            var seq = new ReadOnlySequence<byte>(frameBytes);
            bool success = ClusterFrameReader.TryReadFrame(ref seq, out var h, out var p);
            if (!success || p.Length != 64) throw new InvalidOperationException();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();
        long allocatedBytes = allocatedAfter - allocatedBefore;

        Assert.Equal(0, allocatedBytes);
    }

    private sealed class TestBufferSegment<T> : ReadOnlySequenceSegment<T>
    {
        public TestBufferSegment(ReadOnlyMemory<T> memory)
        {
            Memory = memory;
        }

        public TestBufferSegment<T> Append(ReadOnlyMemory<T> memory)
        {
            var nextSegment = new TestBufferSegment<T>(memory)
            {
                RunningIndex = RunningIndex + Memory.Length
            };
            Next = nextSegment;
            return nextSegment;
        }
    }
}
