namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

/// <summary>
/// Zero-allocation UTF-8 string serialization helper with 2-byte little-endian length prefixes.
/// </summary>
public static class BinaryStringHelper
{
    /// <summary>
    /// Serializes a string directly into the destination span as:
    /// [2-byte ushort length][UTF-8 byte sequence].
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteUtf8String(Span<byte> destination, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            if (destination.Length < 2)
                throw new ArgumentException("Destination span too small for string length prefix.", nameof(destination));
            BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(0, 2), 0);
            return 2;
        }

        int maxBytes = Encoding.UTF8.GetByteCount(value);
        if (maxBytes > ushort.MaxValue)
            throw new ArgumentException($"String byte count ({maxBytes}) exceeds ushort.MaxValue.", nameof(value));

        if (destination.Length < 2 + maxBytes)
            throw new ArgumentException($"Destination span too small. Required: {2 + maxBytes}, Available: {destination.Length}.", nameof(destination));

        int bytesWritten = Encoding.UTF8.GetBytes(value.AsSpan(), destination.Slice(2));
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(0, 2), (ushort)bytesWritten);
        return 2 + bytesWritten;
    }

    /// <summary>
    /// Reads a UTF-8 string prefixed by a 2-byte ushort length from the source span.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string ReadUtf8String(ReadOnlySpan<byte> source, out int bytesConsumed)
    {
        if (source.Length < 2)
            throw new ArgumentException("Source span too small for string length prefix.", nameof(source));

        ushort length = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(0, 2));
        bytesConsumed = 2 + length;

        if (length == 0)
            return string.Empty;

        if (source.Length < 2 + length)
            throw new ArgumentException($"Source span too small for string payload. Required: {2 + length}, Available: {source.Length}.", nameof(source));

        return Encoding.UTF8.GetString(source.Slice(2, length));
    }

    /// <summary>
    /// Calculates the exact byte count required to serialize the string (2 bytes prefix + UTF-8 bytes).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetUtf8ByteCount(string? value)
    {
        return string.IsNullOrEmpty(value) ? 2 : 2 + Encoding.UTF8.GetByteCount(value);
    }
}
