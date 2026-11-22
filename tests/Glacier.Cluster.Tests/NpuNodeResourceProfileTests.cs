namespace Glacier.Cluster.Tests;

using Glacier.Cluster.Inference.Pipeline;
using Xunit;

public sealed class NpuNodeResourceProfileTests
{
    [Fact]
    public void NpuProfile_PrioritySitsBetweenCpuAndIntegratedGpu()
    {
        var cpu = new NodeResourceProfile { IsGpu = false, IsNpu = false, IsLocal = true, EstimatedMsPerLayer = 10.0 };
        var npu = new NodeResourceProfile
        {
            IsGpu = false,
            IsNpu = true,
            IsLocal = true,
            NpuSidecarEndpoint = "http://127.0.0.1:52625",
            EstimatedMsPerLayer = 10.0
        };
        var igpu = new NodeResourceProfile { IsGpu = true, VramBytes = 2UL * 1024 * 1024 * 1024, IsLocal = true, EstimatedMsPerLayer = 10.0 };

        double cpuScore = cpu.ComputePriorityScore();
        double npuScore = npu.ComputePriorityScore();
        double igpuScore = igpu.ComputePriorityScore();

        Assert.True(npuScore > cpuScore, $"Expected NPU ({npuScore}) > CPU ({cpuScore})");
        Assert.True(igpuScore > npuScore, $"Expected iGPU ({igpuScore}) > NPU ({npuScore})");
    }

    [Fact]
    public void NpuProfile_CalculateComfortableCapacity_Respects50PercentRamCap()
    {
        ulong bytesPerLayer = 1_000_000_000UL; // 1 GB per layer
        var npu32Gb = new NodeResourceProfile
        {
            IsGpu = false,
            IsNpu = true,
            SharedMemoryBytes = 32UL * 1024 * 1024 * 1024
        };
        var npu64Gb = new NodeResourceProfile
        {
            IsGpu = false,
            IsNpu = true,
            SharedMemoryBytes = 64UL * 1024 * 1024 * 1024
        };

        int cap32 = npu32Gb.CalculateComfortableCapacity(bytesPerLayer, totalModelLayers: 40);
        int cap64 = npu64Gb.CalculateComfortableCapacity(bytesPerLayer, totalModelLayers: 40);

        // 32 GiB / 2 = ~17.18 GB - 1 GB cushion = 16 layers
        Assert.Equal(16, cap32);
        // 64 GiB / 2 = ~34.36 GB - 1 GB cushion = 33 layers
        Assert.Equal(33, cap64);
    }
}
