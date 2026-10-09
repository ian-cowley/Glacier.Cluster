namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// Immutable 32-byte wire protocol header for Glacier.Cluster inter-node communications.
/// Stored in strict Little-Endian format with 64-bit natural machine alignment.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 32)]
public readonly struct ClusterFrameHeader : IEquatable<ClusterFrameHeader>
{
    /// <summary>
    /// Magic constant identifying Glacier.Cluster frames: 0x47434C55 ("GCLU").
    /// </summary>
    public const uint MagicConstant = 0x47434C55;

    /// <summary>
    /// Current protocol version.
    /// </summary>
    public const ushort CurrentVersion = 1;

    /// <summary>
    /// Size of the header in bytes.
    /// </summary>
    public const int HeaderSize = 32;

    public readonly uint Magic;                     // Offset 0..3
    public readonly ushort Version;                 // Offset 4..5
    public readonly ClusterMessageType MessageType; // Offset 6
    public readonly byte Flags;                     // Offset 7
    public readonly ulong CorrelationId;            // Offset 8..15  (8-byte aligned)
    public readonly ulong PayloadLength;            // Offset 16..23 (8-byte aligned)
    public readonly uint PayloadCrc32;              // Offset 24..27 (4-byte aligned)
    public readonly uint Reserved;                  // Offset 28..31 (4-byte aligned)

    /// <summary>
    /// Constructs a new ClusterFrameHeader with the specified message metadata.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ClusterFrameHeader(
        ClusterMessageType messageType,
        ulong correlationId,
        ulong payloadLength,
        uint crc32 = 0,
        byte flags = 0,
        ushort version = CurrentVersion,
        uint reserved = 0)
    {
        Magic = MagicConstant;
        Version = version;
        MessageType = messageType;
        Flags = flags;
        CorrelationId = correlationId;
        PayloadLength = payloadLength;
        PayloadCrc32 = crc32;
        Reserved = reserved;
    }

    /// <summary>
    /// Constructs a new ClusterFrameHeader using typed <see cref="ClusterFrameFlags"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ClusterFrameHeader(
        ClusterMessageType messageType,
        ulong correlationId,
        ulong payloadLength,
        uint crc32,
        ClusterFrameFlags flags,
        ushort version = CurrentVersion,
        uint reserved = 0)
        : this(messageType, correlationId, payloadLength, crc32, (byte)flags, version, reserved)
    {
    }

    /// <summary>
    /// Constructs a new ClusterFrameHeader using typed <see cref="ClusterFrameFlags"/> with default CRC32.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ClusterFrameHeader(
        ClusterMessageType messageType,
        ulong correlationId,
        ulong payloadLength,
        ClusterFrameFlags flags)
        : this(messageType, correlationId, payloadLength, 0, (byte)flags, CurrentVersion, 0)
    {
    }

    /// <summary>
    /// Typed access to the flags bitmask.
    /// </summary>
    public ClusterFrameFlags FrameFlags => (ClusterFrameFlags)Flags;

    /// <summary>
    /// Writes the header to a destination byte span in Little-Endian format.
    /// </summary>
    /// <param name="destination">Destination span of at least 32 bytes.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < HeaderSize)
        {
            ThrowDestinationTooSmall(destination.Length);
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(0, 4), Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), Version);
        destination[6] = (byte)MessageType;
        destination[7] = Flags;
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(8, 8), CorrelationId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(16, 8), PayloadLength);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(24, 4), PayloadCrc32);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(28, 4), Reserved);
    }

    /// <summary>
    /// Writes the header to an <see cref="IBufferWriter{T}"/> and advances the writer by 32 bytes.
    /// </summary>
    /// <param name="writer">Target buffer writer.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteTo(IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        Span<byte> destination = writer.GetSpan(HeaderSize);
        WriteTo(destination);
        writer.Advance(HeaderSize);
    }

    /// <summary>
    /// Reads and deserializes a ClusterFrameHeader from a 32-byte source span.
    /// </summary>
    /// <param name="source">Input span containing at least 32 bytes.</param>
    /// <returns>Deserialized <see cref="ClusterFrameHeader"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ClusterFrameHeader ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < HeaderSize)
        {
            ThrowSourceTooSmall(source.Length);
        }

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(0, 4));
        if (magic != MagicConstant)
        {
            ThrowInvalidMagic(magic);
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(4, 2));
        var messageType = (ClusterMessageType)source[6];
        byte flags = source[7];
        ulong correlationId = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(8, 8));
        ulong payloadLength = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(16, 8));
        uint crc32 = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(24, 4));
        uint reserved = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(28, 4));

        return new ClusterFrameHeader(
            messageType: messageType,
            correlationId: correlationId,
            payloadLength: payloadLength,
            crc32: crc32,
            flags: flags,
            version: version,
            reserved: reserved);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowDestinationTooSmall(int actualLength)
    {
        throw new ArgumentException($"Destination span length ({actualLength}) is smaller than required HeaderSize ({HeaderSize}).", nameof(actualLength));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowSourceTooSmall(int actualLength)
    {
        throw new ArgumentException($"Source span length ({actualLength}) is smaller than required HeaderSize ({HeaderSize}).", nameof(actualLength));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowInvalidMagic(uint actualMagic)
    {
        throw new InvalidOperationException($"Invalid Glacier Cluster frame magic: 0x{actualMagic:X8}. Expected: 0x{MagicConstant:X8}");
    }

    public bool Equals(ClusterFrameHeader other)
    {
        return Magic == other.Magic &&
               Version == other.Version &&
               MessageType == other.MessageType &&
               Flags == other.Flags &&
               CorrelationId == other.CorrelationId &&
               PayloadLength == other.PayloadLength &&
               PayloadCrc32 == other.PayloadCrc32 &&
               Reserved == other.Reserved;
    }

    public override bool Equals(object? obj) => obj is ClusterFrameHeader other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Magic);
        hash.Add(Version);
        hash.Add((byte)MessageType);
        hash.Add(Flags);
        hash.Add(CorrelationId);
        hash.Add(PayloadLength);
        hash.Add(PayloadCrc32);
        hash.Add(Reserved);
        return hash.ToHashCode();
    }

    public static bool operator ==(ClusterFrameHeader left, ClusterFrameHeader right) => left.Equals(right);
    public static bool operator !=(ClusterFrameHeader left, ClusterFrameHeader right) => !left.Equals(right);

    public override string ToString() =>
        $"ClusterFrameHeader [Type={MessageType}, CorrId={CorrelationId}, Len={PayloadLength}, Flags=0x{Flags:X2}, CRC=0x{PayloadCrc32:X8}]";
}
