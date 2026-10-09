namespace Glacier.Cluster.Protocol;

using System;
using System.Runtime.CompilerServices;
using ArmCrc32 = System.Runtime.Intrinsics.Arm.Crc32;

/// <summary>
/// High-throughput, zero-allocation IEEE 802.3 CRC-32 checksum calculator (polynomial 0xEDB88320).
/// Uses hardware intrinsics where available, with fast table lookup fallback.
/// </summary>
public static class ClusterCrc32
{
    private static readonly uint[] Table = GenerateTable();

    private static uint[] GenerateTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint entry = i;
            for (int j = 0; j < 8; j++)
            {
                if ((entry & 1) != 0)
                    entry = 0xEDB88320u ^ (entry >> 1);
                else
                    entry >>= 1;
            }
            table[i] = entry;
        }
        return table;
    }

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

        uint crc = 0xFFFFFFFFu;

        if (ArmCrc32.Arm64.IsSupported && data.Length >= 8)
        {
            int index = 0;
            while (index <= data.Length - 8)
            {
                ulong val = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(index, 8));
                crc = ArmCrc32.Arm64.ComputeCrc32(crc, val);
                index += 8;
            }
            while (index < data.Length)
            {
                crc = ArmCrc32.ComputeCrc32(crc, data[index]);
                index++;
            }
            return crc ^ 0xFFFFFFFFu;
        }

        uint[] table = Table;
        for (int i = 0; i < data.Length; i++)
        {
            byte b = data[i];
            crc = (crc >> 8) ^ table[(crc ^ b) & 0xFF];
        }

        return crc ^ 0xFFFFFFFFu;
    }
}
