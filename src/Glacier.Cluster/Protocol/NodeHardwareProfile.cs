namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// 64-byte sequentially packed hardware telemetry and capability descriptor.
/// Dynamically probed on worker boot and transmitted to coordinator for Two-Phase scheduling.
/// Fits in a single 64-byte CPU cache line.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 8)]
public partial struct NodeHardwareProfile : IEquatable<NodeHardwareProfile>
{
    /// <summary>Total size in bytes of the serialized struct.</summary>
    public const int SizeInBytes = 64;

    /// <summary>Unique 128-bit node identifier (Offset 0..15).</summary>
    public Guid NodeId;

    /// <summary>Logical CPU thread count (Offset 16..19).</summary>
    public uint CpuLogicalCores;

    /// <summary>CPU SIMD vector flags (Offset 20..23).</summary>
    public SimdArchitectureFlags SimdFlags;

    /// <summary>GPU compute backend flags (Offset 24..27).</summary>
    public GpuBackendFlags GpuFlags;

    /// <summary>Reserved / Network descriptor (Host IPv4 address) (Offset 28..31).</summary>
    public uint Reserved1;

    /// <summary>Total physical RAM in bytes (Offset 32..39).</summary>
    public ulong RamTotalBytes;

    /// <summary>Available physical RAM in bytes (Offset 40..47).</summary>
    public ulong RamAvailableBytes;

    /// <summary>Total dedicated GPU VRAM in bytes (Offset 48..55).</summary>
    public ulong VramTotalBytes;

    /// <summary>Available GPU VRAM in bytes (Offset 56..63).</summary>
    public ulong VramAvailableBytes;

    // =========================================================================
    // Convenience Properties
    // =========================================================================

    public readonly bool HasAvx512 => (SimdFlags & SimdArchitectureFlags.Avx512) != 0;
    public readonly bool HasAvx2 => (SimdFlags & SimdArchitectureFlags.Avx2) != 0;
    public readonly bool HasCuda => (GpuFlags & GpuBackendFlags.Cuda) != 0;
    public readonly bool HasRocm => (GpuFlags & GpuBackendFlags.Rocm) != 0;
    public readonly bool HasDirect3D12 => (GpuFlags & GpuBackendFlags.Direct3D12) != 0;
    public readonly bool HasVulkan => (GpuFlags & GpuBackendFlags.Vulkan) != 0;
    public readonly bool HasMetal => (GpuFlags & GpuBackendFlags.Metal) != 0;
    public readonly bool HasGpu => GpuFlags != GpuBackendFlags.None;

    public uint HostIpv4Address
    {
        readonly get => Reserved1;
        set => Reserved1 = value;
    }

    public readonly string HostIpAddressString
    {
        get
        {
            if (Reserved1 == 0) return "0.0.0.0";
            Span<byte> bytes = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(bytes, Reserved1);
            return $"{bytes[0]}.{bytes[1]}.{bytes[2]}.{bytes[3]}";
        }
    }

    // =========================================================================
    // Constructors
    // =========================================================================

    public NodeHardwareProfile(
        Guid nodeId,
        uint cpuLogicalCores,
        SimdArchitectureFlags simdFlags,
        GpuBackendFlags gpuFlags,
        ulong ramTotalBytes,
        ulong ramAvailableBytes,
        ulong vramTotalBytes,
        ulong vramAvailableBytes,
        uint hostIpv4Address = 0)
    {
        NodeId = nodeId;
        CpuLogicalCores = cpuLogicalCores;
        SimdFlags = simdFlags;
        GpuFlags = gpuFlags;
        Reserved1 = hostIpv4Address;
        RamTotalBytes = ramTotalBytes;
        RamAvailableBytes = ramAvailableBytes;
        VramTotalBytes = vramTotalBytes;
        VramAvailableBytes = vramAvailableBytes;
    }

    // =========================================================================
    // Zero-Allocation Binary Serialization
    // =========================================================================

    /// <summary>
    /// Serializes the profile to a destination byte span in Little-Endian byte order.
    /// Requires destination.Length >= 64.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly void WriteTo(Span<byte> destination)
    {
        if (destination.Length < SizeInBytes)
            throw new ArgumentException($"Destination span too small ({destination.Length} < {SizeInBytes}).", nameof(destination));

        // Offset 0..15: Guid
        NodeId.TryWriteBytes(destination.Slice(0, 16));

        // Offset 16..31: 4x 32-bit integers
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(16, 4), CpuLogicalCores);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(20, 4), (uint)SimdFlags);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(24, 4), (uint)GpuFlags);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(28, 4), Reserved1);

        // Offset 32..63: 4x 64-bit integers
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(32, 8), RamTotalBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(40, 8), RamAvailableBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(48, 8), VramTotalBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(56, 8), VramAvailableBytes);
    }

    /// <summary>
    /// Deserializes a profile from a source byte span in Little-Endian byte order.
    /// Requires source.Length >= 64.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static NodeHardwareProfile ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < SizeInBytes)
            throw new ArgumentException($"Source span too small ({source.Length} < {SizeInBytes}).", nameof(source));

        Guid nodeId = new Guid(source.Slice(0, 16));
        uint cpuLogicalCores = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(16, 4));
        var simdFlags = (SimdArchitectureFlags)BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(20, 4));
        var gpuFlags = (GpuBackendFlags)BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(24, 4));
        uint reserved1 = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(28, 4));

        ulong ramTotal = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(32, 8));
        ulong ramAvail = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(40, 8));
        ulong vramTotal = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(48, 8));
        ulong vramAvail = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(56, 8));

        return new NodeHardwareProfile
        {
            NodeId = nodeId,
            CpuLogicalCores = cpuLogicalCores,
            SimdFlags = simdFlags,
            GpuFlags = gpuFlags,
            Reserved1 = reserved1,
            RamTotalBytes = ramTotal,
            RamAvailableBytes = ramAvail,
            VramTotalBytes = vramTotal,
            VramAvailableBytes = vramAvail
        };
    }

    // =========================================================================
    // Value Equality
    // =========================================================================

    public readonly bool Equals(NodeHardwareProfile other) =>
        NodeId.Equals(other.NodeId) &&
        CpuLogicalCores == other.CpuLogicalCores &&
        SimdFlags == other.SimdFlags &&
        GpuFlags == other.GpuFlags &&
        Reserved1 == other.Reserved1 &&
        RamTotalBytes == other.RamTotalBytes &&
        RamAvailableBytes == other.RamAvailableBytes &&
        VramTotalBytes == other.VramTotalBytes &&
        VramAvailableBytes == other.VramAvailableBytes;

    public override readonly bool Equals(object? obj) => obj is NodeHardwareProfile other && Equals(other);

    public override readonly int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(NodeId);
        hash.Add(CpuLogicalCores);
        hash.Add((uint)SimdFlags);
        hash.Add((uint)GpuFlags);
        hash.Add(Reserved1);
        hash.Add(RamTotalBytes);
        hash.Add(RamAvailableBytes);
        hash.Add(VramTotalBytes);
        hash.Add(VramAvailableBytes);
        return hash.ToHashCode();
    }

    public static bool operator ==(NodeHardwareProfile left, NodeHardwareProfile right) => left.Equals(right);
    public static bool operator !=(NodeHardwareProfile left, NodeHardwareProfile right) => !left.Equals(right);

    public override readonly string ToString() =>
        $"NodeHardwareProfile [Id={NodeId}, Cores={CpuLogicalCores}, SIMD={SimdFlags}, GPU={GpuFlags}, IP={HostIpAddressString}, RAM={RamTotalBytes / (1024 * 1024 * 1024)}GB, VRAM={VramTotalBytes / (1024 * 1024 * 1024)}GB]";
}
