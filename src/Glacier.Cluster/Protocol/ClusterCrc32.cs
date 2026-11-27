namespace Glacier.Cluster.Protocol;

using System;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

/// <summary>
/// High-throughput, zero-allocation IEEE 802.3 CRC-32 checksum calculator (polynomial 0xEDB88320).
/// Delegates to hardware-accelerated <see cref="Crc32.HashToUInt32"/>.
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
}
