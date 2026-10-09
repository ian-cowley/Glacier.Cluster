namespace Glacier.Cluster.Tests.ProtocolTests;

using System;
using System.Runtime.InteropServices;
using Glacier.Cluster.Protocol;
using Xunit;

public sealed class NodeHardwareProfileTests
{
    [Fact]
    public void StructSize_MustBeExactly64Bytes()
    {
        Assert.Equal(64, Marshal.SizeOf<NodeHardwareProfile>());
        Assert.Equal(64, NodeHardwareProfile.SizeInBytes);
    }

    [Fact]
    public void FieldOffsets_MustMatchNaturalAlignment()
    {
        Assert.Equal(0, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.NodeId)));
        Assert.Equal(16, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.CpuLogicalCores)));
        Assert.Equal(20, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.SimdFlags)));
        Assert.Equal(24, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.GpuFlags)));
        Assert.Equal(28, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.Reserved1)));
        Assert.Equal(32, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.RamTotalBytes)));
        Assert.Equal(40, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.RamAvailableBytes)));
        Assert.Equal(48, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.VramTotalBytes)));
        Assert.Equal(56, (int)Marshal.OffsetOf<NodeHardwareProfile>(nameof(NodeHardwareProfile.VramAvailableBytes)));
    }

    [Fact]
    public void RoundTripSerialization_PreservesAllFields()
    {
        var original = new NodeHardwareProfile
        {
            NodeId = Guid.NewGuid(),
            CpuLogicalCores = 24,
            SimdFlags = SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
            GpuFlags = GpuBackendFlags.Cuda | GpuBackendFlags.Direct3D12 | GpuBackendFlags.Vulkan,
            Reserved1 = 0xC0A8016F, // 192.168.1.111
            RamTotalBytes = 64UL * 1024 * 1024 * 1024,
            RamAvailableBytes = 48UL * 1024 * 1024 * 1024,
            VramTotalBytes = 8UL * 1024 * 1024 * 1024,
            VramAvailableBytes = 7UL * 1024 * 1024 * 1024
        };

        Span<byte> buffer = stackalloc byte[64];
        original.WriteTo(buffer);

        NodeHardwareProfile deserialized = NodeHardwareProfile.ReadFrom(buffer);

        Assert.Equal(original, deserialized);
        Assert.Equal(original.NodeId, deserialized.NodeId);
        Assert.Equal(original.CpuLogicalCores, deserialized.CpuLogicalCores);
        Assert.Equal(original.SimdFlags, deserialized.SimdFlags);
        Assert.Equal(original.GpuFlags, deserialized.GpuFlags);
        Assert.Equal(original.Reserved1, deserialized.Reserved1);
        Assert.Equal(original.RamTotalBytes, deserialized.RamTotalBytes);
        Assert.Equal(original.RamAvailableBytes, deserialized.RamAvailableBytes);
        Assert.Equal(original.VramTotalBytes, deserialized.VramTotalBytes);
        Assert.Equal(original.VramAvailableBytes, deserialized.VramAvailableBytes);
        Assert.Equal("192.168.1.111", deserialized.HostIpAddressString);
        Assert.True(deserialized.HasAvx512);
        Assert.True(deserialized.HasAvx2);
        Assert.True(deserialized.HasCuda);
        Assert.True(deserialized.HasDirect3D12);
        Assert.True(deserialized.HasVulkan);
        Assert.False(deserialized.HasRocm);
        Assert.False(deserialized.HasMetal);
        Assert.True(deserialized.HasGpu);
    }

    [Fact]
    public void Serialization_BufferTooSmall_ThrowsArgumentException()
    {
        var profile = new NodeHardwareProfile();
        byte[] buffer = new byte[63]; // 1 byte short

        Assert.Throws<ArgumentException>(() => profile.WriteTo(buffer));
        Assert.Throws<ArgumentException>(() => NodeHardwareProfile.ReadFrom(buffer));
    }

    [Fact]
    public void ProbeLocal_OnPhysicalHardware_ReturnsSensibleValues()
    {
        var profile = NodeHardwareProfile.ProbeLocal();

        Assert.NotEqual(Guid.Empty, profile.NodeId);
        Assert.True(profile.CpuLogicalCores >= 1, "CPU logical cores must be >= 1");
        Assert.NotEqual(SimdArchitectureFlags.None, profile.SimdFlags);
        Assert.True(profile.RamTotalBytes > 0, "Total RAM must be > 0");
        Assert.True(profile.RamAvailableBytes > 0, "Available RAM must be > 0");
        Assert.True(profile.RamAvailableBytes <= profile.RamTotalBytes, "Available RAM cannot exceed Total RAM");
    }

    [Fact]
    public void ProbeLocal_WithExplicitNodeId_PreservesId()
    {
        var fixedId = Guid.NewGuid();
        var profile = NodeHardwareProfile.ProbeLocal(nodeId: fixedId);

        Assert.Equal(fixedId, profile.NodeId);
    }

    [Fact]
    public void HeadlessSimulatedNode_HasZeroGpuAndVramWithoutCrashing()
    {
        var headless = new NodeHardwareProfile
        {
            NodeId = Guid.NewGuid(),
            CpuLogicalCores = 4,
            SimdFlags = SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
            GpuFlags = GpuBackendFlags.None,
            Reserved1 = 0x7F000001,
            RamTotalBytes = 16UL * 1024 * 1024 * 1024,
            RamAvailableBytes = 12UL * 1024 * 1024 * 1024,
            VramTotalBytes = 0,
            VramAvailableBytes = 0
        };

        Assert.False(headless.HasGpu);
        Assert.False(headless.HasCuda);
        Assert.Equal(0UL, headless.VramTotalBytes);
        Assert.Equal("127.0.0.1", headless.HostIpAddressString);
    }

    [Fact]
    public void FlagsExtensions_IdentifyCapabilitiesAccurately()
    {
        var simd = SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.AdvSimd;
        Assert.True(simd.HasAvx512());
        Assert.False(simd.HasAvx2());
        Assert.True(simd.HasAdvSimd());
        Assert.False(simd.HasSve());

        var gpu = GpuBackendFlags.Rocm | GpuBackendFlags.Metal;
        Assert.False(gpu.HasCuda());
        Assert.True(gpu.HasRocm());
        Assert.True(gpu.HasMetal());
        Assert.False(gpu.HasDirect3D12());
        Assert.False(gpu.HasVulkan());
        Assert.True(gpu.HasAnyGpu());
    }
}
