namespace Glacier.Cluster.Tests;

using Glacier.Cluster.Inference.Pipeline;
using Xunit;

public sealed class Tier5M7Phase2HardeningTests
{
    [Fact]
    public void NodeResourceProfile_CalculateComfortableCapacity_BoundaryAndOverflowSafety()
    {
        // 1. bytesPerLayer = 0 -> returns totalModelLayers cleanly (no DivideByZero)
        var gpu80 = new NodeResourceProfile
        {
            Name = "H100-80GB",
            IsGpu = true,
            VramBytes = 80UL * 1024 * 1024 * 1024,
            SharedMemoryBytes = 128UL * 1024 * 1024 * 1024
        };
        Assert.Equal(64, gpu80.CalculateComfortableCapacity(bytesPerLayer: 0, totalModelLayers: 64));
        Assert.Equal(0, gpu80.CalculateComfortableCapacity(bytesPerLayer: 1024, totalModelLayers: 0));
        Assert.Equal(0, gpu80.CalculateComfortableCapacity(bytesPerLayer: 1024, totalModelLayers: -5));

        // 2. bytesPerLayer = 1 with 80 GB VRAM must clamp cleanly to totalModelLayers without signed int32 overflow -> 0
        Assert.Equal(128, gpu80.CalculateComfortableCapacity(bytesPerLayer: 1, totalModelLayers: 128));

        // 3. Dedicated GPU with VramBytes < 1.5 GB safety cushion (still <3 GB -> UMA branch, test 3.1 GB minus 1.5 GB vs < safety cushion)
        var lowVramDedicated = new NodeResourceProfile
        {
            Name = "TinyDedicated",
            IsGpu = true,
            VramBytes = 1_000_000_000UL,
            SharedMemoryBytes = 0UL
        };
        Assert.True(lowVramDedicated.CalculateComfortableCapacity(bytesPerLayer: 1, totalModelLayers: 32) >= 0);

        // 4. Explicit 3.1 GB GPU just above 3.0 GB threshold -> clamps cleanly without underflow
        var gpuJustOver3Gb = new NodeResourceProfile
        {
            Name = "Gpu3.1GB",
            IsGpu = true,
            VramBytes = 3_300_000_000UL
        };
        int cap = gpuJustOver3Gb.CalculateComfortableCapacity(bytesPerLayer: 500_000_000UL, totalModelLayers: 32);
        Assert.Equal(3, cap); // (3.3B - 1.5B) / 0.5B = 3

        // 5. NPU & CPU profiles with SharedMemoryBytes = 0 and bytesPerLayer = 1 (no underflow or overflow)
        var npuZeroRam = new NodeResourceProfile { Name = "Npu0", IsNpu = true, SharedMemoryBytes = 0 };
        Assert.True(npuZeroRam.CalculateComfortableCapacity(bytesPerLayer: 1, totalModelLayers: 24) >= 0);

        var npu64Gb = new NodeResourceProfile { Name = "Npu64", IsNpu = true, SharedMemoryBytes = 64UL * 1024 * 1024 * 1024 };
        Assert.Equal(40, npu64Gb.CalculateComfortableCapacity(bytesPerLayer: 1, totalModelLayers: 40));

        var cpuZeroRam = new NodeResourceProfile { Name = "Cpu0", SharedMemoryBytes = 0 };
        Assert.Equal(0, cpuZeroRam.CalculateComfortableCapacity(bytesPerLayer: 1, totalModelLayers: 40));

        var cpu64Gb = new NodeResourceProfile { Name = "Cpu64", SharedMemoryBytes = 64UL * 1024 * 1024 * 1024 };
        Assert.Equal(48, cpu64Gb.CalculateComfortableCapacity(bytesPerLayer: 1, totalModelLayers: 48));
    }
}
