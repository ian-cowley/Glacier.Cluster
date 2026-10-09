namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers;
using System.Runtime.CompilerServices;

/// <summary>
/// High-performance zero-allocation wire frame parser operating over System.IO.Pipelines sequences.
/// </summary>
public static class ClusterFrameReader
{
    /// <summary>
    /// Attempts to read a complete Glacier.Cluster frame from the input sequence buffer.
    /// </summary>
    /// <param name="buffer">The sequence buffer to inspect and slice upon complete frame consumption.</param>
    /// <param name="header">The parsed 32-byte frame header.</param>
    /// <param name="payload">The sliced payload sequence corresponding to this frame.</param>
    /// <returns>
    /// <c>true</c> if a complete frame (header + entire payload) was parsed and buffer was advanced;
    /// otherwise <c>false</c> without advancing buffer.
    /// </returns>
    /// <exception cref="InvalidOperationException">Thrown if frame magic is corrupt or payload length overflows sequence capacity.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadFrame(
        ref ReadOnlySequence<byte> buffer,
        out ClusterFrameHeader header,
        out ReadOnlySequence<byte> payload)
    {
        header = default;
        payload = default;

        if (buffer.Length < ClusterFrameHeader.HeaderSize)
        {
            return false;
        }

        // Fast-path: Header is fully contiguous in the first memory span
        if (buffer.FirstSpan.Length >= ClusterFrameHeader.HeaderSize)
        {
            header = ClusterFrameHeader.ReadFrom(buffer.FirstSpan.Slice(0, ClusterFrameHeader.HeaderSize));
        }
        else
        {
            // Segmented slow-path: Header spans across buffer segments.
            // Copy 32 bytes to stackalloc with zero GC allocation.
            Span<byte> stackBuffer = stackalloc byte[ClusterFrameHeader.HeaderSize];
            buffer.Slice(0, ClusterFrameHeader.HeaderSize).CopyTo(stackBuffer);
            header = ClusterFrameHeader.ReadFrom(stackBuffer);
        }

        // Guard against 64-bit integer overflow when calculating total frame size
        if (header.PayloadLength > (ulong)(long.MaxValue - ClusterFrameHeader.HeaderSize))
        {
            ThrowPayloadTooLarge(header.PayloadLength);
        }

        long payloadLen = (long)header.PayloadLength;
        long totalFrameLength = ClusterFrameHeader.HeaderSize + payloadLen;

        if (buffer.Length < totalFrameLength)
        {
            // Incomplete frame: full payload has not arrived yet.
            // Do not advance buffer; return false so PipeReader can wait for more bytes.
            header = default;
            payload = default;
            return false;
        }

        // Frame is complete: slice payload and advance buffer past this frame
        payload = buffer.Slice(ClusterFrameHeader.HeaderSize, payloadLen);
        buffer = buffer.Slice(totalFrameLength);
        return true;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowPayloadTooLarge(ulong payloadLength)
    {
        throw new InvalidOperationException($"Frame payload length ({payloadLength} bytes) exceeds maximum allowable sequence size.");
    }
}
