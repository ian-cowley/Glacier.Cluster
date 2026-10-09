namespace Glacier.Cluster.Protocol;

using System;

/// <summary>
/// Bitwise flags representing CPU SIMD vector execution capabilities.
/// Used by the Two-Phase Scheduler for Phase 2 affinity scoring.
/// </summary>
[Flags]
public enum SimdArchitectureFlags : uint
{
    /// <summary>No hardware SIMD vector acceleration detected or enabled.</summary>
    None = 0,

    /// <summary>x86/x64 Streaming SIMD Extensions 4.2 (128-bit integer/string intrinsics).</summary>
    Sse42 = 1 << 0,     // 0x01

    /// <summary>x86/x64 Advanced Vector Extensions (256-bit floating-point intrinsics).</summary>
    Avx = 1 << 1,       // 0x02

    /// <summary>x86/x64 Advanced Vector Extensions 2 (256-bit integer + FMA intrinsics).</summary>
    Avx2 = 1 << 2,      // 0x04

    /// <summary>x86/x64 AVX-512 Foundation (512-bit vector registers, Zen 5 / Intel Xeon).</summary>
    Avx512 = 1 << 3,    // 0x08

    /// <summary>ARM64 Advanced SIMD / NEON (128-bit vector execution).</summary>
    AdvSimd = 1 << 4,   // 0x10

    /// <summary>ARM64 Scalable Vector Extension (SVE / SVE2 vector execution).</summary>
    Sve = 1 << 5        // 0x20
}

/// <summary>
/// Extension methods for testing SIMD capability flags.
/// </summary>
public static class SimdArchitectureExtensions
{
    public static bool HasAvx512(this SimdArchitectureFlags flags) => (flags & SimdArchitectureFlags.Avx512) != 0;
    public static bool HasAvx2(this SimdArchitectureFlags flags) => (flags & SimdArchitectureFlags.Avx2) != 0;
    public static bool HasAdvSimd(this SimdArchitectureFlags flags) => (flags & SimdArchitectureFlags.AdvSimd) != 0;
    public static bool HasSve(this SimdArchitectureFlags flags) => (flags & SimdArchitectureFlags.Sve) != 0;
}
