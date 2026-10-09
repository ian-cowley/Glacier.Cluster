namespace Glacier.Cluster.Tests.ProtocolTests;

using System;
using System.Buffers;
using System.Collections.Generic;
using Glacier.Cluster.Protocol;
using Xunit;

public sealed class ClusterFrameReaderAdversarialTests
{
    private sealed class SegmentNode<T> : ReadOnlySequenceSegment<T>
    {
        public SegmentNode(ReadOnlyMemory<T> memory)
        {
            Memory = memory;
        }

        public SegmentNode<T> Append(ReadOnlyMemory<T> memory)
        {
            var nextSegment = new SegmentNode<T>(memory)
            {
                RunningIndex = RunningIndex + Memory.Length
            };
            Next = nextSegment;
            return nextSegment;
        }
    }

    private static ReadOnlySequence<byte> CreateSequence(params ReadOnlyMemory<byte>[] chunks)
    {
        if (chunks.Length == 0) return ReadOnlySequence<byte>.Empty;
        if (chunks.Length == 1) return new ReadOnlySequence<byte>(chunks[0]);

        var first = new SegmentNode<byte>(chunks[0]);
        var current = first;
        for (int i = 1; i < chunks.Length; i++)
        {
            current = current.Append(chunks[i]);
        }
        return new ReadOnlySequence<byte>(first, 0, current, current.Memory.Length);
    }

    private static ReadOnlySequence<byte> FragmentData(byte[] data, int chunkSize)
    {
        if (data.Length == 0) return ReadOnlySequence<byte>.Empty;
        if (chunkSize <= 0) throw new ArgumentOutOfRangeException(nameof(chunkSize));

        var chunks = new List<ReadOnlyMemory<byte>>();
        int offset = 0;
        while (offset < data.Length)
        {
            int take = Math.Min(chunkSize, data.Length - offset);
            chunks.Add(new ReadOnlyMemory<byte>(data, offset, take));
            offset += take;
        }
        return CreateSequence(chunks.ToArray());
    }

    private static ReadOnlySequence<byte> FragmentDataPattern(byte[] data, int[] pattern)
    {
        if (data.Length == 0) return ReadOnlySequence<byte>.Empty;
        var chunks = new List<ReadOnlyMemory<byte>>();
        int offset = 0;
        int patternIdx = 0;
        while (offset < data.Length)
        {
            int size = pattern[patternIdx % pattern.Length];
            patternIdx++;
            int take = Math.Min(size, data.Length - offset);
            chunks.Add(new ReadOnlyMemory<byte>(data, offset, take));
            offset += take;
        }
        return CreateSequence(chunks.ToArray());
    }

    [Theory]
    [InlineData(1)]  // 32 segments of 1 byte for header
    [InlineData(2)]  // 16 segments of 2 bytes for header
    [InlineData(3)]  // 3-byte segments
    [InlineData(4)]  // 4-byte segments
    [InlineData(5)]  // 5-byte segments
    public void TryReadFrame_HeaderFragmentedIntoSmallSegments_ParsesCorrectly(int segmentSize)
    {
        const int payloadSize = 60;
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + payloadSize];
        var header = new ClusterFrameHeader(ClusterMessageType.StageDispatch, 4242, payloadSize, 0x12345678, (byte)ClusterFrameFlags.Spilled);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        for (int i = 0; i < payloadSize; i++)
        {
            frameBytes[ClusterFrameHeader.HeaderSize + i] = (byte)((i * 37) & 0xFF);
        }

        var sequence = FragmentData(frameBytes, segmentSize);
        Assert.False(sequence.IsSingleSegment);

        bool success = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedHeader, out var payload);

        Assert.True(success);
        Assert.Equal(header, parsedHeader);
        Assert.Equal(payloadSize, payload.Length);
        Assert.Equal(0, sequence.Length);

        byte[] extractedPayload = payload.ToArray();
        for (int i = 0; i < payloadSize; i++)
        {
            Assert.Equal(frameBytes[ClusterFrameHeader.HeaderSize + i], extractedPayload[i]);
        }
    }

    [Fact]
    public void TryReadFrame_HeaderFragmentedArbitraryIrregularPattern_ParsesCorrectly()
    {
        // Irregular segment sizes: 1, 2, 3, 4, 5, 2, 1, 3, 4, 2, 5...
        int[] pattern = [1, 2, 3, 4, 5, 2, 1, 3, 4, 2, 5];
        const int payloadSize = 77;
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + payloadSize];
        var header = new ClusterFrameHeader(ClusterMessageType.RingAllReduceHandshake, 8888, payloadSize);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        for (int i = 0; i < payloadSize; i++)
        {
            frameBytes[ClusterFrameHeader.HeaderSize + i] = (byte)(i + 1);
        }

        var sequence = FragmentDataPattern(frameBytes, pattern);
        Assert.False(sequence.IsSingleSegment);

        bool success = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedHeader, out var payload);

        Assert.True(success);
        Assert.Equal(header, parsedHeader);
        Assert.Equal(payloadSize, payload.Length);
        Assert.Equal(0, sequence.Length);
        Assert.Equal(frameBytes.AsSpan(ClusterFrameHeader.HeaderSize, payloadSize).ToArray(), payload.ToArray());
    }

    [Fact]
    public void TryReadFrame_HeaderSplitAtEverySingleOffsetFrom1To31_ParsesCorrectly()
    {
        const int payloadSize = 25;
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + payloadSize];
        var header = new ClusterFrameHeader(ClusterMessageType.JobSubmission, 999, payloadSize);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));
        for (int i = 0; i < payloadSize; i++) frameBytes[ClusterFrameHeader.HeaderSize + i] = (byte)(i ^ 0xAA);

        for (int split = 1; split < ClusterFrameHeader.HeaderSize; split++)
        {
            var chunk1 = new ReadOnlyMemory<byte>(frameBytes, 0, split);
            var chunk2 = new ReadOnlyMemory<byte>(frameBytes, split, frameBytes.Length - split);
            var sequence = CreateSequence(chunk1, chunk2);

            Assert.False(sequence.IsSingleSegment);
            bool success = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedHeader, out var payload);

            Assert.True(success, $"Failed for header split at offset {split}");
            Assert.Equal(header, parsedHeader);
            Assert.Equal(payloadSize, payload.Length);
            Assert.Equal(0, sequence.Length);
            Assert.Equal(frameBytes.AsSpan(ClusterFrameHeader.HeaderSize, payloadSize).ToArray(), payload.ToArray());
        }
    }

    [Fact]
    public void TryReadFrame_ZeroBytePayload_SingleAndSegmented_ReturnsTrueAndAdvances()
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 100, 0);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        // 1. Single contiguous segment
        var singleSeq = new ReadOnlySequence<byte>(frameBytes);
        bool s1 = ClusterFrameReader.TryReadFrame(ref singleSeq, out var h1, out var p1);
        Assert.True(s1);
        Assert.Equal(header, h1);
        Assert.Equal(0, p1.Length);
        Assert.True(p1.IsEmpty);
        Assert.Equal(0, singleSeq.Length);

        // 2. 32 segments of 1 byte each
        var fragmentedSeq = FragmentData(frameBytes, 1);
        Assert.Equal(32, fragmentedSeq.Length);
        bool s2 = ClusterFrameReader.TryReadFrame(ref fragmentedSeq, out var h2, out var p2);
        Assert.True(s2);
        Assert.Equal(header, h2);
        Assert.Equal(0, p2.Length);
        Assert.True(p2.IsEmpty);
        Assert.Equal(0, fragmentedSeq.Length);
    }

    [Fact]
    public void TryReadFrame_OneBytePayload_SingleAndSegmented_ReturnsTrue()
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + 1];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPong, 200, 1);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));
        frameBytes[32] = 0xFE;

        // 1. Single segment
        var singleSeq = new ReadOnlySequence<byte>(frameBytes);
        bool s1 = ClusterFrameReader.TryReadFrame(ref singleSeq, out var h1, out var p1);
        Assert.True(s1);
        Assert.Equal(header, h1);
        Assert.Equal(1, p1.Length);
        Assert.Equal(0xFE, p1.FirstSpan[0]);
        Assert.Equal(0, singleSeq.Length);

        // 2. Header in seg 1, payload in seg 2
        var segSeq = CreateSequence(new ReadOnlyMemory<byte>(frameBytes, 0, 32), new ReadOnlyMemory<byte>(frameBytes, 32, 1));
        bool s2 = ClusterFrameReader.TryReadFrame(ref segSeq, out var h2, out var p2);
        Assert.True(s2);
        Assert.Equal(header, h2);
        Assert.Equal(1, p2.Length);
        Assert.Equal(0xFE, p2.FirstSpan[0]);
        Assert.Equal(0, segSeq.Length);

        // 3. 33 segments of 1 byte each
        var allSingleBytes = FragmentData(frameBytes, 1);
        bool s3 = ClusterFrameReader.TryReadFrame(ref allSingleBytes, out var h3, out var p3);
        Assert.True(s3);
        Assert.Equal(header, h3);
        Assert.Equal(1, p3.Length);
        Assert.Equal(0xFE, p3.FirstSpan[0]);
        Assert.Equal(0, allSingleBytes.Length);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(1023)]
    [InlineData(1024)]
    [InlineData(4095)]
    [InlineData(4096)]
    [InlineData(65535)]
    [InlineData(65536)]
    public void TryReadFrame_ExactBoundaryPayloads_ParsesWithoutCorruption(int payloadLength)
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + payloadLength];
        var header = new ClusterFrameHeader(ClusterMessageType.PartitionFetchResponse, 5555, (ulong)payloadLength);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        for (int i = 0; i < payloadLength; i++)
        {
            frameBytes[ClusterFrameHeader.HeaderSize + i] = (byte)(i & 0xFF);
        }

        // Test single segment
        var singleSeq = new ReadOnlySequence<byte>(frameBytes);
        bool s1 = ClusterFrameReader.TryReadFrame(ref singleSeq, out var h1, out var p1);
        Assert.True(s1);
        Assert.Equal(header, h1);
        Assert.Equal(payloadLength, p1.Length);
        Assert.Equal(0, singleSeq.Length);

        // Test multi-segment (header split into 16+16, payload split into 64-byte chunks or single)
        var chunks = new List<ReadOnlyMemory<byte>>
        {
            new(frameBytes, 0, 16),
            new(frameBytes, 16, 16)
        };
        if (payloadLength > 0)
        {
            int offset = 32;
            while (offset < frameBytes.Length)
            {
                int take = Math.Min(512, frameBytes.Length - offset);
                chunks.Add(new ReadOnlyMemory<byte>(frameBytes, offset, take));
                offset += take;
            }
        }
        var multiSeq = CreateSequence(chunks.ToArray());
        bool s2 = ClusterFrameReader.TryReadFrame(ref multiSeq, out var h2, out var p2);
        Assert.True(s2);
        Assert.Equal(header, h2);
        Assert.Equal(payloadLength, p2.Length);
        Assert.Equal(0, multiSeq.Length);

        // Verify data fidelity
        if (payloadLength > 0)
        {
            byte[] extracted = p2.ToArray();
            for (int i = 0; i < payloadLength; i++)
            {
                Assert.Equal((byte)(i & 0xFF), extracted[i]);
            }
        }
    }

    [Fact]
    public void TryReadFrame_IncompleteHeaderAllLengthsFrom0To31_ReturnsFalseAndNeverAdvances()
    {
        byte[] template = new byte[ClusterFrameHeader.HeaderSize];
        var header = new ClusterFrameHeader(ClusterMessageType.StageDispatch, 777, 100);
        header.WriteTo(template.AsSpan(0, ClusterFrameHeader.HeaderSize));

        for (int length = 0; length < ClusterFrameHeader.HeaderSize; length++)
        {
            // 1. Single segment
            byte[] partial = new byte[length];
            Array.Copy(template, 0, partial, 0, length);
            var singleSeq = new ReadOnlySequence<byte>(partial);

            bool s1 = ClusterFrameReader.TryReadFrame(ref singleSeq, out var h1, out var p1);
            Assert.False(s1, $"Expected false for single segment header length {length}");
            Assert.Equal(default, h1);
            Assert.Equal(0, p1.Length);
            Assert.Equal(length, singleSeq.Length);

            // 2. Highly fragmented (1 byte per segment)
            if (length > 1)
            {
                var fragSeq = FragmentData(partial, 1);
                bool s2 = ClusterFrameReader.TryReadFrame(ref fragSeq, out var h2, out var p2);
                Assert.False(s2, $"Expected false for fragmented header length {length}");
                Assert.Equal(default, h2);
                Assert.Equal(0, p2.Length);
                Assert.Equal(length, fragSeq.Length);
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(32)]
    [InlineData(100)]
    public void TryReadFrame_IncompletePayloadAllSubLengths_ReturnsFalseAndNeverAdvances(int payloadLength)
    {
        byte[] fullFrame = new byte[ClusterFrameHeader.HeaderSize + payloadLength];
        var header = new ClusterFrameHeader(ClusterMessageType.StageDispatch, 9090, (ulong)payloadLength);
        header.WriteTo(fullFrame.AsSpan(0, ClusterFrameHeader.HeaderSize));

        for (int availablePayload = 0; availablePayload < payloadLength; availablePayload++)
        {
            int totalBytes = ClusterFrameHeader.HeaderSize + availablePayload;
            byte[] truncated = new byte[totalBytes];
            Array.Copy(fullFrame, 0, truncated, 0, totalBytes);

            // Single segment
            var seq = new ReadOnlySequence<byte>(truncated);
            bool success = ClusterFrameReader.TryReadFrame(ref seq, out var parsedHeader, out var payload);

            Assert.False(success, $"Expected false when available payload is {availablePayload} of {payloadLength}");
            Assert.Equal(default, parsedHeader);
            Assert.Equal(0, payload.Length);
            Assert.Equal(totalBytes, seq.Length); // Buffer NOT advanced

            // Multi-segment: header split into two 16-byte segments + payload
            var seg1 = new ReadOnlyMemory<byte>(truncated, 0, 16);
            var seg2 = new ReadOnlyMemory<byte>(truncated, 16, totalBytes - 16);
            var multiSeq = CreateSequence(seg1, seg2);

            bool multiSuccess = ClusterFrameReader.TryReadFrame(ref multiSeq, out var multiH, out var multiP);
            Assert.False(multiSuccess);
            Assert.Equal(default, multiH);
            Assert.Equal(0, multiP.Length);
            Assert.Equal(totalBytes, multiSeq.Length);
        }
    }

    [Fact]
    public void TryReadFrame_IncompletePayloadExactOneByteShort_ReturnsFalseAndNeverAdvances()
    {
        const int payloadLength = 500;
        int totalLength = ClusterFrameHeader.HeaderSize + payloadLength - 1; // Exactly 1 byte missing
        byte[] truncated = new byte[totalLength];
        var header = new ClusterFrameHeader(ClusterMessageType.PartitionFetchResponse, 1234, (ulong)payloadLength);
        header.WriteTo(truncated.AsSpan(0, ClusterFrameHeader.HeaderSize));

        var seq = new ReadOnlySequence<byte>(truncated);
        bool success = ClusterFrameReader.TryReadFrame(ref seq, out var h, out var p);

        Assert.False(success);
        Assert.Equal(default, h);
        Assert.Equal(0, p.Length);
        Assert.Equal(totalLength, seq.Length);
    }

    [Theory]
    [InlineData(0, 0x00)] // Corrupt byte 0
    [InlineData(0, 0xFF)]
    [InlineData(1, 0x00)] // Corrupt byte 1
    [InlineData(2, 0x00)] // Corrupt byte 2
    [InlineData(3, 0x00)] // Corrupt byte 3
    [InlineData(0, 0x47)] // Byte swap
    public void TryReadFrame_CorruptedMagic_ThrowsInvalidOperationException(int byteOffset, byte badByte)
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + 20];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 20);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        frameBytes[byteOffset] = badByte;

        // Single segment
        var singleSeq = new ReadOnlySequence<byte>(frameBytes);
        var exSingle = Assert.Throws<InvalidOperationException>(() =>
            ClusterFrameReader.TryReadFrame(ref singleSeq, out _, out _));
        Assert.Contains("Invalid Glacier Cluster frame magic", exSingle.Message);

        // Multi-segment
        var multiSeq = FragmentData(frameBytes, 5);
        var exMulti = Assert.Throws<InvalidOperationException>(() =>
            ClusterFrameReader.TryReadFrame(ref multiSeq, out _, out _));
        Assert.Contains("Invalid Glacier Cluster frame magic", exMulti.Message);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(31)]
    public void TryReadFrame_IncompleteHeaderWithCorruptedMagic_ReturnsFalseWithoutThrowing(int bufferLength)
    {
        // If buffer is shorter than 32 bytes, reader must NOT throw even if magic is corrupt
        byte[] partial = new byte[bufferLength];
        partial[0] = 0xFF; // Corrupt magic

        var seq = new ReadOnlySequence<byte>(partial);
        bool success = ClusterFrameReader.TryReadFrame(ref seq, out var header, out var payload);

        Assert.False(success);
        Assert.Equal(default, header);
        Assert.Equal(0, payload.Length);
        Assert.Equal(bufferLength, seq.Length);
    }

    [Theory]
    [InlineData(ulong.MaxValue)]
    [InlineData((ulong)long.MaxValue)]
    [InlineData((ulong)(long.MaxValue - 31))] // Exactly overflows long.MaxValue when adding 32
    public void TryReadFrame_PayloadLengthIntegerOverflow_ThrowsInvalidOperationException(ulong overflowPayloadLength)
    {
        byte[] raw = new byte[ClusterFrameHeader.HeaderSize];
        var header = new ClusterFrameHeader(ClusterMessageType.StageDispatch, 99, overflowPayloadLength);
        header.WriteTo(raw.AsSpan(0, ClusterFrameHeader.HeaderSize));

        var seq = new ReadOnlySequence<byte>(raw);
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ClusterFrameReader.TryReadFrame(ref seq, out _, out _));
        Assert.Contains("exceeds maximum allowable sequence size", ex.Message);
    }

    [Fact]
    public void TryReadFrame_ConsecutiveFramesWithVaryingFragmentation_ParsesAllFramesCorrectly()
    {
        // Stream containing 6 consecutive frames of varying sizes
        int[] payloadSizes = [0, 1, 17, 0, 100, 3];
        var headers = new List<ClusterFrameHeader>();
        var fullStream = new List<byte>();

        for (int i = 0; i < payloadSizes.Length; i++)
        {
            int pLen = payloadSizes[i];
            var h = new ClusterFrameHeader(ClusterMessageType.TaskStatusUpdate, (ulong)(i + 100), (ulong)pLen);
            headers.Add(h);

            byte[] hBytes = new byte[32];
            h.WriteTo(hBytes.AsSpan(0, 32));
            fullStream.AddRange(hBytes);

            for (int p = 0; p < pLen; p++)
            {
                fullStream.Add((byte)((i * 10 + p) & 0xFF));
            }
        }

        // Highly fragmented stream (segments of 7 bytes each, splitting across headers and payloads randomly)
        var sequence = FragmentData(fullStream.ToArray(), 7);

        for (int i = 0; i < payloadSizes.Length; i++)
        {
            bool success = ClusterFrameReader.TryReadFrame(ref sequence, out var parsedH, out var parsedPayload);
            Assert.True(success, $"Failed to read frame index {i}");
            Assert.Equal(headers[i], parsedH);
            Assert.Equal(payloadSizes[i], parsedPayload.Length);

            if (payloadSizes[i] > 0)
            {
                byte[] extracted = parsedPayload.ToArray();
                for (int p = 0; p < payloadSizes[i]; p++)
                {
                    Assert.Equal((byte)((i * 10 + p) & 0xFF), extracted[p]);
                }
            }
        }

        // Exhausted stream
        Assert.Equal(0, sequence.Length);
        bool finalAttempt = ClusterFrameReader.TryReadFrame(ref sequence, out var hEnd, out var pEnd);
        Assert.False(finalAttempt);
        Assert.Equal(0, sequence.Length);
    }

    [Fact]
    public void TryReadFrame_SingleSegment_ZeroHeapAllocation_EmpiricalVerification()
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + 64];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 64);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        var baseSeq = new ReadOnlySequence<byte>(frameBytes);

        // Warm up JIT
        for (int i = 0; i < 500; i++)
        {
            var seq = baseSeq;
            ClusterFrameReader.TryReadFrame(ref seq, out _, out _);
        }

        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        const int iterations = 20_000;
        for (int i = 0; i < iterations; i++)
        {
            var seq = baseSeq;
            bool success = ClusterFrameReader.TryReadFrame(ref seq, out var h, out var p);
            if (!success || p.Length != 64) throw new InvalidOperationException();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();
        long allocatedBytes = allocatedAfter - allocatedBefore;

        Assert.Equal(0, allocatedBytes);
    }

    [Fact]
    public void TryReadFrame_MultiSegmentHeader_ZeroHeapAllocation_EmpiricalVerification()
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + 64];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 64);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        // Pre-allocate segments: 16 + 16 + 64
        var seg1 = new SegmentNode<byte>(new ReadOnlyMemory<byte>(frameBytes, 0, 16));
        var seg2 = seg1.Append(new ReadOnlyMemory<byte>(frameBytes, 16, 16));
        var seg3 = seg2.Append(new ReadOnlyMemory<byte>(frameBytes, 32, 64));
        var baseSeq = new ReadOnlySequence<byte>(seg1, 0, seg3, seg3.Memory.Length);

        // Warm up JIT
        for (int i = 0; i < 500; i++)
        {
            var seq = baseSeq;
            ClusterFrameReader.TryReadFrame(ref seq, out _, out _);
        }

        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        const int iterations = 20_000;
        for (int i = 0; i < iterations; i++)
        {
            var seq = baseSeq;
            bool success = ClusterFrameReader.TryReadFrame(ref seq, out var h, out var p);
            if (!success || p.Length != 64) throw new InvalidOperationException();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();
        long allocatedBytes = allocatedAfter - allocatedBefore;

        Assert.Equal(0, allocatedBytes);
    }

    [Fact]
    public void TryReadFrame_HighlyFragmentedHeader_ZeroHeapAllocation_EmpiricalVerification()
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + 64];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 64);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        // 8 segments of 4 bytes each for header + 1 segment of 64 bytes for payload
        var chunks = new List<ReadOnlyMemory<byte>>();
        for (int i = 0; i < 8; i++)
        {
            chunks.Add(new ReadOnlyMemory<byte>(frameBytes, i * 4, 4));
        }
        chunks.Add(new ReadOnlyMemory<byte>(frameBytes, 32, 64));

        var baseSeq = CreateSequence(chunks.ToArray());

        // Warm up JIT
        for (int i = 0; i < 500; i++)
        {
            var seq = baseSeq;
            ClusterFrameReader.TryReadFrame(ref seq, out _, out _);
        }

        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        const int iterations = 20_000;
        for (int i = 0; i < iterations; i++)
        {
            var seq = baseSeq;
            bool success = ClusterFrameReader.TryReadFrame(ref seq, out var h, out var p);
            if (!success || p.Length != 64) throw new InvalidOperationException();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();
        long allocatedBytes = allocatedAfter - allocatedBefore;

        Assert.Equal(0, allocatedBytes);
    }

    [Fact]
    public void TryReadFrame_ZeroBytePayload_ZeroHeapAllocation_EmpiricalVerification()
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 0);
        header.WriteTo(frameBytes.AsSpan(0, ClusterFrameHeader.HeaderSize));

        var baseSeq = new ReadOnlySequence<byte>(frameBytes);

        // Warm up JIT
        for (int i = 0; i < 500; i++)
        {
            var seq = baseSeq;
            ClusterFrameReader.TryReadFrame(ref seq, out _, out _);
        }

        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        const int iterations = 20_000;
        for (int i = 0; i < iterations; i++)
        {
            var seq = baseSeq;
            bool success = ClusterFrameReader.TryReadFrame(ref seq, out var h, out var p);
            if (!success || p.Length != 0) throw new InvalidOperationException();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();
        long allocatedBytes = allocatedAfter - allocatedBefore;

        Assert.Equal(0, allocatedBytes);
    }

    [Fact]
    public void TryReadFrame_IncompleteBuffer_ZeroHeapAllocation_EmpiricalVerification()
    {
        byte[] frameBytes = new byte[16]; // Incomplete header
        var baseSeq = new ReadOnlySequence<byte>(frameBytes);

        // Warm up JIT
        for (int i = 0; i < 500; i++)
        {
            var seq = baseSeq;
            ClusterFrameReader.TryReadFrame(ref seq, out _, out _);
        }

        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        const int iterations = 20_000;
        for (int i = 0; i < iterations; i++)
        {
            var seq = baseSeq;
            bool success = ClusterFrameReader.TryReadFrame(ref seq, out _, out _);
            if (success) throw new InvalidOperationException();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();
        long allocatedBytes = allocatedAfter - allocatedBefore;

        Assert.Equal(0, allocatedBytes);
    }
}
