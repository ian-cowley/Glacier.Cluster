namespace Glacier.Cluster.Protocol;

using System;

/// <summary>
/// Bitwise flags representing operational GPU compute backends.
/// Used by the Two-Phase Scheduler for Phase 1 hard predicate constraint filtering.
/// </summary>
[Flags]
public enum GpuBackendFlags : uint
{
    /// <summary>No hardware GPU compute acceleration detected (CPU-only worker).</summary>
    None = 0,

    /// <summary>NVIDIA CUDA Bare-Metal driver (nvcuda.dll / libcuda.so).</summary>
    Cuda = 1 << 0,          // 0x01

    /// <summary>AMD ROCm / HIP driver (amdhip64.dll / libamdhip64.so).</summary>
    Rocm = 1 << 1,          // 0x02

    /// <summary>Apple Metal Compute framework (macOS / Apple Silicon).</summary>
    Metal = 1 << 2,         // 0x04

    /// <summary>Khronos Vulkan compute pipeline (vulkan-1.dll / libvulkan.so).</summary>
    Vulkan = 1 << 3,        // 0x08

    /// <summary>Microsoft Direct3D 12 compute queue (Windows d3d12.dll / dxgi.dll).</summary>
    Direct3D12 = 1 << 4     // 0x10
}

/// <summary>
/// Extension methods for testing GPU backend availability flags.
/// </summary>
public static class GpuBackendExtensions
{
    public static bool HasCuda(this GpuBackendFlags flags) => (flags & GpuBackendFlags.Cuda) != 0;
    public static bool HasRocm(this GpuBackendFlags flags) => (flags & GpuBackendFlags.Rocm) != 0;
    public static bool HasDirect3D12(this GpuBackendFlags flags) => (flags & GpuBackendFlags.Direct3D12) != 0;
    public static bool HasVulkan(this GpuBackendFlags flags) => (flags & GpuBackendFlags.Vulkan) != 0;
    public static bool HasMetal(this GpuBackendFlags flags) => (flags & GpuBackendFlags.Metal) != 0;
    public static bool HasAnyGpu(this GpuBackendFlags flags) => flags != GpuBackendFlags.None;
}
