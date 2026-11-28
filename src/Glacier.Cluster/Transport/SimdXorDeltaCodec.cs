namespace Glacier.Cluster.Transport;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

/// <summary>
/// Hardware-accelerated Vector256 / Vector128 XOR kernels for zero-allocation delta transport.
/// </summary>
public static class SimdXor
{
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static void Xor(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b, Span<byte> destination)
    {
        int len = a.Length;
        if (b.Length < len || destination.Length < len)
        {
            throw new ArgumentException("Span lengths must match.");
        }

        if (len == 0) return;

        ref byte refA = ref MemoryMarshal.GetReference(a);
        ref byte refB = ref MemoryMarshal.GetReference(b);
        ref byte refDst = ref MemoryMarshal.GetReference(destination);

        nuint i = 0;
        nuint n = (nuint)len;

        if (Vector256.IsHardwareAccelerated && n >= 32)
        {
            nuint limit = n - 32;
            while (i <= limit)
            {
                var va = Vector256.LoadUnsafe(ref refA, i);
                var vb = Vector256.LoadUnsafe(ref refB, i);
                Vector256.StoreUnsafe(va ^ vb, ref refDst, i);
                i += 32;
            }
        }
        else if (Vector128.IsHardwareAccelerated && n >= 16)
        {
            nuint limit = n - 16;
            while (i <= limit)
            {
                var va = Vector128.LoadUnsafe(ref refA, i);
                var vb = Vector128.LoadUnsafe(ref refB, i);
                Vector128.StoreUnsafe(va ^ vb, ref refDst, i);
                i += 16;
            }
        }

        while (i < n)
        {
            Unsafe.Add(ref refDst, i) = (byte)(Unsafe.Add(ref refA, i) ^ Unsafe.Add(ref refB, i));
            i++;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static void XorInPlace(Span<byte> target, ReadOnlySpan<byte> delta)
    {
        int len = target.Length;
        if (delta.Length < len)
        {
            throw new ArgumentException("Delta span is shorter than target.");
        }

        if (len == 0) return;

        ref byte refT = ref MemoryMarshal.GetReference(target);
        ref byte refD = ref MemoryMarshal.GetReference(delta);

        nuint i = 0;
        nuint n = (nuint)len;

        if (Vector256.IsHardwareAccelerated && n >= 32)
        {
            nuint limit = n - 32;
            while (i <= limit)
            {
                var vt = Vector256.LoadUnsafe(ref refT, i);
                var vd = Vector256.LoadUnsafe(ref refD, i);
                Vector256.StoreUnsafe(vt ^ vd, ref refT, i);
                i += 32;
            }
        }
        else if (Vector128.IsHardwareAccelerated && n >= 16)
        {
            nuint limit = n - 16;
            while (i <= limit)
            {
                var vt = Vector128.LoadUnsafe(ref refT, i);
                var vd = Vector128.LoadUnsafe(ref refD, i);
                Vector128.StoreUnsafe(vt ^ vd, ref refT, i);
                i += 16;
            }
        }

        while (i < n)
        {
            Unsafe.Add(ref refT, i) ^= Unsafe.Add(ref refD, i);
            i++;
        }
    }
}

/// <summary>
/// Pure-C# LZ4-compatible block compressor and decompressor with zero third-party dependencies.
/// Optimized for sparse/zero-run SIMD XOR deltas and columnar tensor/Arrow IPC payloads.
/// </summary>
public static class ClusterBlockCodec
{
    private const int HashLog = 12;
    private const int HashTableSize = 1 << HashLog;
    private const int MinMatch = 4;
    private const int LastLiterals = 5;
    private const int MfLimit = 12;
    private const int MaxDistance = 65535;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetMaxCompressedLength(int inputLength)
    {
        if (inputLength < 0) throw new ArgumentOutOfRangeException(nameof(inputLength));
        return inputLength + (inputLength / 255) + 16;
    }

    public static int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (source.IsEmpty)
        {
            return 0;
        }

        int srcLen = source.Length;
        int dstCap = destination.Length;
        int ip = 0;
        int anchor = 0;
        int op = 0;

        if (srcLen >= MfLimit + 1)
        {
            int[] hashTable = ArrayPool<int>.Shared.Rent(HashTableSize);
            try
            {
                hashTable.AsSpan(0, HashTableSize).Fill(-1);
                int mfLimit = srcLen - MfLimit;
                int matchLimit = srcLen - LastLiterals;

                // Prime index 0
                uint h0 = Hash4(BinaryPrimitives.ReadUInt32LittleEndian(source));
                hashTable[h0] = 0;
                ip = 1;

                while (ip <= mfLimit)
                {
                    uint val = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(ip, 4));
                    uint h = Hash4(val);
                    int matchIdx = hashTable[h];
                    hashTable[h] = ip;

                    int distance = ip - matchIdx;
                    if (matchIdx < 0 || distance <= 0 || distance > MaxDistance ||
                        BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(matchIdx, 4)) != val)
                    {
                        ip++;
                        continue;
                    }

                    // Match found: extend forward up to matchLimit
                    int matchLen = MinMatch;
                    while (ip + matchLen < matchLimit && source[matchIdx + matchLen] == source[ip + matchLen])
                    {
                        matchLen++;
                    }

                    // Emit token + literal run
                    int litLen = ip - anchor;
                    if (op >= dstCap) return -1;
                    int tokenPos = op++;
                    byte litNibble = (byte)(litLen >= 15 ? 15 : litLen);

                    if (litLen >= 15)
                    {
                        int rem = litLen - 15;
                        while (rem >= 255)
                        {
                            if (op >= dstCap) return -1;
                            destination[op++] = 255;
                            rem -= 255;
                        }
                        if (op >= dstCap) return -1;
                        destination[op++] = (byte)rem;
                    }

                    if (op + litLen + 2 > dstCap) return -1;
                    if (litLen > 0)
                    {
                        source.Slice(anchor, litLen).CopyTo(destination.Slice(op, litLen));
                        op += litLen;
                    }

                    // Emit 2-byte LE distance
                    BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(op, 2), (ushort)distance);
                    op += 2;

                    // Emit match length nibble + optional extension bytes
                    int matchCode = matchLen - MinMatch;
                    byte matchNibble = (byte)(matchCode >= 15 ? 15 : matchCode);
                    destination[tokenPos] = (byte)((litNibble << 4) | matchNibble);

                    if (matchCode >= 15)
                    {
                        int rem = matchCode - 15;
                        while (rem >= 255)
                        {
                            if (op >= dstCap) return -1;
                            destination[op++] = 255;
                            rem -= 255;
                        }
                        if (op >= dstCap) return -1;
                        destination[op++] = (byte)rem;
                    }

                    ip += matchLen;
                    anchor = ip;

                    if (ip <= mfLimit)
                    {
                        uint nextHash = Hash4(BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(ip - 2, 4)));
                        hashTable[nextHash] = ip - 2;
                    }
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(hashTable);
            }
        }

        // Emit trailing literals
        int lastLitLen = srcLen - anchor;
        if (op >= dstCap) return -1;
        destination[op++] = (byte)((lastLitLen >= 15 ? 15 : lastLitLen) << 4);
        if (lastLitLen >= 15)
        {
            int rem = lastLitLen - 15;
            while (rem >= 255)
            {
                if (op >= dstCap) return -1;
                destination[op++] = 255;
                rem -= 255;
            }
            if (op >= dstCap) return -1;
            destination[op++] = (byte)rem;
        }

        if (op + lastLitLen > dstCap) return -1;
        if (lastLitLen > 0)
        {
            source.Slice(anchor, lastLitLen).CopyTo(destination.Slice(op, lastLitLen));
            op += lastLitLen;
        }

        return op;
    }

    public static int Decompress(ReadOnlySpan<byte> compressed, Span<byte> destination)
    {
        if (compressed.IsEmpty)
        {
            return 0;
        }

        int srcLen = compressed.Length;
        int dstCap = destination.Length;
        int ip = 0;
        int op = 0;

        while (ip < srcLen)
        {
            byte token = compressed[ip++];
            int litLen = token >> 4;
            if (litLen == 15)
            {
                byte s;
                do
                {
                    if (ip >= srcLen) return -1;
                    s = compressed[ip++];
                    if (litLen > int.MaxValue - s) return -1;
                    litLen += s;
                }
                while (s == 255);
            }

            if ((long)ip + litLen > srcLen || (long)op + litLen > dstCap)
            {
                return -1;
            }

            if (litLen > 0)
            {
                compressed.Slice(ip, litLen).CopyTo(destination.Slice(op, litLen));
                ip += litLen;
                op += litLen;
            }

            if (ip == srcLen)
            {
                break;
            }

            if (ip + 2 > srcLen)
            {
                return -1;
            }

            int offset = BinaryPrimitives.ReadUInt16LittleEndian(compressed.Slice(ip, 2));
            ip += 2;
            if (offset == 0 || offset > op)
            {
                return -1;
            }

            int matchLen = (token & 0x0F) + MinMatch;
            if ((token & 0x0F) == 15)
            {
                byte s;
                do
                {
                    if (ip >= srcLen) return -1;
                    s = compressed[ip++];
                    if (matchLen > int.MaxValue - s) return -1;
                    matchLen += s;
                }
                while (s == 255);
            }

            if ((long)op + matchLen > dstCap)
            {
                return -1;
            }

            int matchPos = op - offset;
            if (offset >= matchLen)
            {
                destination.Slice(matchPos, matchLen).CopyTo(destination.Slice(op, matchLen));
                op += matchLen;
            }
            else if (offset == 1)
            {
                byte fill = destination[matchPos];
                destination.Slice(op, matchLen).Fill(fill);
                op += matchLen;
            }
            else
            {
                for (int k = 0; k < matchLen; k++)
                {
                    destination[op++] = destination[matchPos++];
                }
            }
        }

        return op;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Hash4(uint value) => (value * 2654435761u) >> (32 - HashLog);
}

/// <summary>
/// Stateful SIMD-XOR delta + pure-C# block encoder for activation / KV cache / frame streaming.
/// </summary>
public sealed class SimdXorDeltaCodec : IDisposable
{
    private byte[]? _previousPayload;
    private byte[]? _xorScratch;
    private int _frameByteLength;
    private bool _hasPrevious;
    private bool _disposed;

    public int GetMaxEncodedSize(int rawByteLength) => ClusterBlockCodec.GetMaxCompressedLength(rawByteLength);

    public int Encode(
        ReadOnlySpan<byte> payload,
        bool forceKeyFrame,
        Span<byte> outputBuffer,
        out bool isKeyFrame)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        int len = payload.Length;
        EnsureBuffers(len);

        isKeyFrame = forceKeyFrame || !_hasPrevious;
        Span<byte> prevSpan = _previousPayload.AsSpan(0, len);
        ReadOnlySpan<byte> toCompress;

        if (isKeyFrame)
        {
            toCompress = payload;
        }
        else
        {
            Span<byte> xorSpan = _xorScratch.AsSpan(0, len);
            SimdXor.Xor(payload, prevSpan, xorSpan);
            toCompress = xorSpan;
        }

        payload.CopyTo(prevSpan);
        _hasPrevious = true;

        int encoded = ClusterBlockCodec.Compress(toCompress, outputBuffer);
        if (encoded < 0)
        {
            throw new InvalidOperationException($"Block compression failed (output buffer {outputBuffer.Length} bytes may be too small).");
        }

        return encoded;
    }

    public bool Decode(
        ReadOnlySpan<byte> encodedData,
        bool isKeyFrame,
        int expectedByteLength,
        Span<byte> outputBuffer)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (outputBuffer.Length < expectedByteLength)
        {
            throw new ArgumentException($"Destination buffer too small ({outputBuffer.Length} < {expectedByteLength}).", nameof(outputBuffer));
        }

        if (!isKeyFrame && (!_hasPrevious || _frameByteLength != expectedByteLength))
        {
            return false;
        }

        Span<byte> dst = outputBuffer.Slice(0, expectedByteLength);

        if (isKeyFrame)
        {
            int decoded = ClusterBlockCodec.Decompress(encodedData, dst);
            if (decoded != expectedByteLength) return false;

            EnsureBuffers(expectedByteLength);
            dst.CopyTo(_previousPayload.AsSpan(0, expectedByteLength));
            _hasPrevious = true;
            return true;
        }

        EnsureBuffers(expectedByteLength);
        Span<byte> scratch = _xorScratch.AsSpan(0, expectedByteLength);
        int deltaLen = ClusterBlockCodec.Decompress(encodedData, scratch);
        if (deltaLen != expectedByteLength) return false;

        Span<byte> prev = _previousPayload.AsSpan(0, expectedByteLength);
        prev.CopyTo(dst);
        SimdXor.XorInPlace(dst, scratch);
        dst.CopyTo(prev);
        return true;
    }

    public void Reset() => _hasPrevious = false;

    private void EnsureBuffers(int byteLength)
    {
        if (_frameByteLength == byteLength && _previousPayload is not null && _xorScratch is not null)
        {
            return;
        }

        ReturnBuffers();
        int allocLen = Math.Max(1, byteLength);
        _previousPayload = ArrayPool<byte>.Shared.Rent(allocLen);
        _xorScratch = ArrayPool<byte>.Shared.Rent(allocLen);
        _frameByteLength = byteLength;
        _hasPrevious = false;
    }

    private void ReturnBuffers()
    {
        if (_previousPayload is not null)
        {
            ArrayPool<byte>.Shared.Return(_previousPayload);
            _previousPayload = null;
        }
        if (_xorScratch is not null)
        {
            ArrayPool<byte>.Shared.Return(_xorScratch);
            _xorScratch = null;
        }
        _frameByteLength = 0;
        _hasPrevious = false;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        ReturnBuffers();
    }
}
