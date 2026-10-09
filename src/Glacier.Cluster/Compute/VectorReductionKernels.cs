namespace Glacier.Cluster.Compute;

using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

/// <summary>
/// Hardware-accelerated SIMD vector addition kernels for Ring AllReduce.
/// Saturated memory-bandwidth in-place reduction.
/// </summary>
public static unsafe class VectorReductionKernels
{
    /// <summary>
    /// Performs in-place element-wise vector addition: destination[i] += source[i].
    /// Automatically selects AVX-512, AVX2, or SSE/NEON depending on CPU hardware acceleration.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static void AddInPlace(Span<float> destination, ReadOnlySpan<float> source)
    {
        if (destination.Length != source.Length)
            throw new ArgumentException("Destination and source spans must have identical lengths.");

        int length = destination.Length;
        if (length == 0) return;

        fixed (float* pDest = destination)
        fixed (float* pSrc = source)
        {
            int i = 0;

            // AVX-512: Process 16 floats (64 bytes = 1 cache line) per instruction
            if (Vector512.IsHardwareAccelerated && length >= 16)
            {
                for (; i <= length - 16; i += 16)
                {
                    var vd = Vector512.Load(pDest + i);
                    var vs = Vector512.Load(pSrc + i);
                    Vector512.Store(Vector512.Add(vd, vs), pDest + i);
                }
            }

            // AVX2: Process 8 floats (32 bytes = half cache line) per instruction
            if (Vector256.IsHardwareAccelerated && i <= length - 8)
            {
                for (; i <= length - 8; i += 8)
                {
                    var vd = Vector256.Load(pDest + i);
                    var vs = Vector256.Load(pSrc + i);
                    Vector256.Store(Vector256.Add(vd, vs), pDest + i);
                }
            }

            // SSE / Neon: Process 4 floats (16 bytes) per instruction
            if (Vector128.IsHardwareAccelerated && i <= length - 4)
            {
                for (; i <= length - 4; i += 4)
                {
                    var vd = Vector128.Load(pDest + i);
                    var vs = Vector128.Load(pSrc + i);
                    Vector128.Store(Vector128.Add(vd, vs), pDest + i);
                }
            }

            // Remainder scalar loop
            for (; i < length; i++)
            {
                pDest[i] += pSrc[i];
            }
        }
    }
}
