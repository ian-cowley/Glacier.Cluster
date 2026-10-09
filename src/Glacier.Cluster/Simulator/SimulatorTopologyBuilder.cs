namespace Glacier.Cluster.Simulator;

using System;
using Glacier.Cluster.Protocol;

/// <summary>
/// Factory generating realistic heterogeneous hardware profiles matching the physical hardware fleet
/// and custom cluster configurations.
/// </summary>
public static class SimulatorTopologyBuilder
{
    /// <summary>
    /// Generates hardware profile matching the physical 4-machine cluster fleet cyclically.
    /// </summary>
    public static NodeHardwareProfile CreatePhysicalFleetProfile(int nodeIndex, Guid nodeId)
    {
        return (nodeIndex % 4) switch
        {
            // Node 0: Host Laptop (AMD Zen 5, 16 cores, AVX-512, CUDA, 32GB RAM, 8GB VRAM)
            0 => new NodeHardwareProfile(
                nodeId,
                cpuLogicalCores: 16,
                simdFlags: SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                gpuFlags: GpuBackendFlags.Cuda | GpuBackendFlags.Vulkan,
                ramTotalBytes: 32UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 28UL * 1024 * 1024 * 1024,
                vramTotalBytes: 8UL * 1024 * 1024 * 1024,
                vramAvailableBytes: 7UL * 1024 * 1024 * 1024),

            // Node 1: Beast (AMD Zen 3, 12 cores, AVX2, CUDA, 64GB RAM, 12GB VRAM)
            1 => new NodeHardwareProfile(
                nodeId,
                cpuLogicalCores: 12,
                simdFlags: SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                gpuFlags: GpuBackendFlags.Cuda | GpuBackendFlags.Vulkan,
                ramTotalBytes: 64UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 58UL * 1024 * 1024 * 1024,
                vramTotalBytes: 12UL * 1024 * 1024 * 1024,
                vramAvailableBytes: 11UL * 1024 * 1024 * 1024),

            // Node 2: Fedora Linux (AMD Zen 5, 16 cores, AVX-512, Vulkan/ROCm, 32GB RAM, 16GB VRAM)
            2 => new NodeHardwareProfile(
                nodeId,
                cpuLogicalCores: 16,
                simdFlags: SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                gpuFlags: GpuBackendFlags.Vulkan | GpuBackendFlags.Rocm,
                ramTotalBytes: 32UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 29UL * 1024 * 1024 * 1024,
                vramTotalBytes: 16UL * 1024 * 1024 * 1024,
                vramAvailableBytes: 14UL * 1024 * 1024 * 1024),

            // Node 3: MinisForum64 (AMD Zen 3+, 8 cores, AVX2, Vulkan, 64GB RAM, 8GB VRAM)
            _ => new NodeHardwareProfile(
                nodeId,
                cpuLogicalCores: 8,
                simdFlags: SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                gpuFlags: GpuBackendFlags.Vulkan,
                ramTotalBytes: 64UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 60UL * 1024 * 1024 * 1024,
                vramTotalBytes: 8UL * 1024 * 1024 * 1024,
                vramAvailableBytes: 7UL * 1024 * 1024 * 1024)
        };
    }

    /// <summary>
    /// Generates identical baseline profile across all nodes.
    /// </summary>
    public static NodeHardwareProfile CreateHomogeneousProfile(int nodeIndex, Guid nodeId)
    {
        return new NodeHardwareProfile(
            nodeId,
            cpuLogicalCores: 8,
            simdFlags: SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
            gpuFlags: GpuBackendFlags.Vulkan,
            ramTotalBytes: 32UL * 1024 * 1024 * 1024,
            ramAvailableBytes: 28UL * 1024 * 1024 * 1024,
            vramTotalBytes: 8UL * 1024 * 1024 * 1024,
            vramAvailableBytes: 7UL * 1024 * 1024 * 1024);
    }

    /// <summary>
    /// Generates profile based on selected strategy.
    /// </summary>
    public static NodeHardwareProfile CreateProfile(
        TopologyAssignmentStrategy strategy,
        int nodeIndex,
        Guid nodeId,
        Func<int, Guid, NodeHardwareProfile>? customFactory = null)
    {
        return strategy switch
        {
            TopologyAssignmentStrategy.PhysicalFleetCyclic => CreatePhysicalFleetProfile(nodeIndex, nodeId),
            TopologyAssignmentStrategy.Homogeneous => CreateHomogeneousProfile(nodeIndex, nodeId),
            TopologyAssignmentStrategy.TieredHpc => CreatePhysicalFleetProfile(nodeIndex, nodeId),
            TopologyAssignmentStrategy.GpuAccelerated => (nodeIndex < 2)
                ? CreatePhysicalFleetProfile(0, nodeId)
                : CreatePhysicalFleetProfile(3, nodeId),
            TopologyAssignmentStrategy.Custom => customFactory != null
                ? customFactory(nodeIndex, nodeId)
                : CreateHomogeneousProfile(nodeIndex, nodeId),
            _ => CreateHomogeneousProfile(nodeIndex, nodeId)
        };
    }
}
