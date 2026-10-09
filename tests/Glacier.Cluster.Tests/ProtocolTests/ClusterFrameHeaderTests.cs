namespace Glacier.Cluster.Tests.ProtocolTests;

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Glacier.Cluster.Protocol;
using Xunit;

public sealed class ClusterFrameHeaderTests
{
    [Fact]
    public void StructSize_IsExactly32Bytes()
    {
        Assert.Equal(32, Unsafe.SizeOf<ClusterFrameHeader>());
        Assert.Equal(32, Marshal.SizeOf<ClusterFrameHeader>());
        Assert.Equal(32, ClusterFrameHeader.HeaderSize);
    }

    [Fact]
    public void FieldOffsets_MatchExactSpecification()
    {
        Assert.Equal(0, (int)Marshal.OffsetOf<ClusterFrameHeader>(nameof(ClusterFrameHeader.Magic)));
        Assert.Equal(4, (int)Marshal.OffsetOf<ClusterFrameHeader>(nameof(ClusterFrameHeader.Version)));
        Assert.Equal(6, (int)Marshal.OffsetOf<ClusterFrameHeader>(nameof(ClusterFrameHeader.MessageType)));
        Assert.Equal(7, (int)Marshal.OffsetOf<ClusterFrameHeader>(nameof(ClusterFrameHeader.Flags)));
        Assert.Equal(8, (int)Marshal.OffsetOf<ClusterFrameHeader>(nameof(ClusterFrameHeader.CorrelationId)));
        Assert.Equal(16, (int)Marshal.OffsetOf<ClusterFrameHeader>(nameof(ClusterFrameHeader.PayloadLength)));
        Assert.Equal(24, (int)Marshal.OffsetOf<ClusterFrameHeader>(nameof(ClusterFrameHeader.PayloadCrc32)));
        Assert.Equal(28, (int)Marshal.OffsetOf<ClusterFrameHeader>(nameof(ClusterFrameHeader.Reserved)));
    }

    [Fact]
    public void RoundTrip_SpanBlitting_PreservesExactValues()
    {
        var header = new ClusterFrameHeader(
            messageType: ClusterMessageType.StageDispatch,
            correlationId: 0x0123456789ABCDEFU,
            payloadLength: 1048576UL,
            crc32: 0xDEADBEEFU,
            flags: (byte)(ClusterFrameFlags.Compressed | ClusterFrameFlags.ArrowIpc),
            version: 1,
            reserved: 0);

        Span<byte> destination = stackalloc byte[32];
        header.WriteTo(destination);

        var parsed = ClusterFrameHeader.ReadFrom(destination);

        Assert.Equal(header, parsed);
        Assert.Equal(ClusterFrameHeader.MagicConstant, parsed.Magic);
        Assert.Equal(1, parsed.Version);
        Assert.Equal(ClusterMessageType.StageDispatch, parsed.MessageType);
        Assert.Equal((byte)(ClusterFrameFlags.Compressed | ClusterFrameFlags.ArrowIpc), parsed.Flags);
        Assert.True(parsed.FrameFlags.HasFlag(ClusterFrameFlags.Compressed));
        Assert.True(parsed.FrameFlags.HasFlag(ClusterFrameFlags.ArrowIpc));
        Assert.Equal(0x0123456789ABCDEFU, parsed.CorrelationId);
        Assert.Equal(1048576UL, parsed.PayloadLength);
        Assert.Equal(0xDEADBEEFU, parsed.PayloadCrc32);
        Assert.Equal(0U, parsed.Reserved);
    }

    [Fact]
    public void RoundTrip_IBufferWriter_PreservesExactValues()
    {
        var header = new ClusterFrameHeader(
            messageType: ClusterMessageType.TaskStatusUpdate,
            correlationId: 42UL,
            payloadLength: 2048UL,
            flags: ClusterFrameFlags.Error);

        var writer = new ArrayBufferWriter<byte>();
        header.WriteTo(writer);

        Assert.Equal(32, writer.WrittenCount);

        var parsed = ClusterFrameHeader.ReadFrom(writer.WrittenSpan);
        Assert.Equal(header, parsed);
        Assert.Equal(ClusterFrameFlags.Error, parsed.FrameFlags);
    }

    [Fact]
    public void LittleEndian_ByteOrder_VerifiedEmpirically()
    {
        // Magic = 0x47434C55 ("GCLU"). In Little-Endian: 0x55, 0x4C, 0x43, 0x47
        // CorrelationId = 0x0102030405060708: byte[8] = 0x08, ..., byte[15] = 0x01
        var header = new ClusterFrameHeader(
            messageType: ClusterMessageType.HeartbeatPing,
            correlationId: 0x0102030405060708UL,
            payloadLength: 0x1122334455667788UL,
            crc32: 0xAABBCCDD,
            flags: (byte)0);

        Span<byte> bytes = stackalloc byte[32];
        header.WriteTo(bytes);

        // Verify magic bytes Little-Endian
        Assert.Equal(0x55, bytes[0]);
        Assert.Equal(0x4C, bytes[1]);
        Assert.Equal(0x43, bytes[2]);
        Assert.Equal(0x47, bytes[3]);

        // Version = 1 (ushort LE)
        Assert.Equal(0x01, bytes[4]);
        Assert.Equal(0x00, bytes[5]);

        // MessageType = 0x01 (HeartbeatPing)
        Assert.Equal(0x01, bytes[6]);

        // CorrelationId LE
        Assert.Equal(0x08, bytes[8]);
        Assert.Equal(0x07, bytes[9]);
        Assert.Equal(0x06, bytes[10]);
        Assert.Equal(0x05, bytes[11]);
        Assert.Equal(0x04, bytes[12]);
        Assert.Equal(0x03, bytes[13]);
        Assert.Equal(0x02, bytes[14]);
        Assert.Equal(0x01, bytes[15]);

        // PayloadCrc32 LE
        Assert.Equal(0xDD, bytes[24]);
        Assert.Equal(0xCC, bytes[25]);
        Assert.Equal(0xBB, bytes[26]);
        Assert.Equal(0xAA, bytes[27]);
    }

    [Fact]
    public void CorruptMagic_ThrowsInvalidOperationException()
    {
        byte[] bytes = new byte[32];
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 0);
        header.WriteTo(bytes);

        // Mutate first byte
        bytes[0] = 0x00;

        Assert.Throws<InvalidOperationException>(() => ClusterFrameHeader.ReadFrom(bytes));
    }

    [Fact]
    public void DestinationTooSmall_ThrowsArgumentException()
    {
        var header = new ClusterFrameHeader(ClusterMessageType.HeartbeatPing, 1, 0);
        byte[] tooSmall = new byte[31];

        Assert.Throws<ArgumentException>(() => header.WriteTo(tooSmall));
    }

    [Fact]
    public void SourceTooSmall_ThrowsArgumentException()
    {
        byte[] tooSmall = new byte[31];
        Assert.Throws<ArgumentException>(() => ClusterFrameHeader.ReadFrom(tooSmall));
    }

    [Fact]
    public void EqualityAndOperators_FunctionCorrectly()
    {
        var h1 = new ClusterFrameHeader(ClusterMessageType.JobSubmission, 100, 50, crc32: 123);
        var h2 = new ClusterFrameHeader(ClusterMessageType.JobSubmission, 100, 50, crc32: 123);
        var h3 = new ClusterFrameHeader(ClusterMessageType.JobSubmission, 101, 50, crc32: 123);

        Assert.True(h1 == h2);
        Assert.False(h1 != h2);
        Assert.True(h1.Equals(h2));
        Assert.Equal(h1.GetHashCode(), h2.GetHashCode());

        Assert.False(h1 == h3);
        Assert.True(h1 != h3);
        Assert.False(h1.Equals(h3));

        Assert.Contains("Type=JobSubmission", h1.ToString());
        Assert.Contains("CorrId=100", h1.ToString());
    }
}
