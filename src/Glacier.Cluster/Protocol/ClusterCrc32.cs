namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

/// <summary>
/// High-throughput, zero-allocation IEEE 802.3 CRC-32 checksum calculator (polynomial 0xEDB88320).
/// Delegates to hardware-accelerated <see cref="Crc32"/>.
/// </summary>
public static class ClusterCrc32
{
    /// <summary>
    /// Computes the IEEE 802.3 CRC-32 checksum of the provided read-only byte span.
    /// </summary>
    /// <param name="data">Input byte sequence.</param>
    /// <returns>32-bit unsigned CRC-32 checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return 0;

        return Crc32.HashToUInt32(data);
    }

    /// <summary>
    /// Computes the IEEE 802.3 CRC-32 checksum of a potentially segmented <see cref="ReadOnlySequence{T}"/>
    /// with zero heap allocations and zero intermediate buffer copies.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Compute(in ReadOnlySequence<byte> data)
    {
        if (data.IsEmpty)
            return 0;

        if (data.IsSingleSegment)
            return Crc32.HashToUInt32(data.FirstSpan);

        var crc = new Crc32();
        foreach (ReadOnlyMemory<byte> segment in data)
        {
            if (!segment.IsEmpty)
            {
                crc.Append(segment.Span);
            }
        }

        return crc.GetCurrentHashAsUInt32();
    }
}
