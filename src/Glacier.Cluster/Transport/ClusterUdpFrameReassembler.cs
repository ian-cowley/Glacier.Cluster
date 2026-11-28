namespace Glacier.Cluster.Transport;

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Glacier.Cluster.Protocol;

public enum ClusterUdpReassembleStatus
{
    Pending = 0,
    FrameComplete = 1,
    Ignored = 2
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 24)]
public readonly struct ClusterUdpSliceHeader
{
    public const ushort MagicConstant = 0x4755; // "GU"

    public readonly ushort Magic;
    public readonly ushort Flags;
    public readonly uint FrameId;
    public readonly ushort TotalPackets;
    public readonly ushort PacketIndex;
    public readonly uint TotalPayloadBytes;
    public readonly uint PayloadCrc32;
    public readonly uint Reserved;

    public ClusterUdpSliceHeader(
        uint frameId,
        ushort totalPackets,
        ushort packetIndex,
        uint totalPayloadBytes,
        ushort flags = 0,
        uint payloadCrc32 = 0)
    {
        Magic = MagicConstant;
        Flags = flags;
        FrameId = frameId;
        TotalPackets = totalPackets;
        PacketIndex = packetIndex;
        TotalPayloadBytes = totalPayloadBytes;
        PayloadCrc32 = payloadCrc32;
        Reserved = 0;
    }
}

public readonly ref struct ClusterReassembledFrame
{
    public uint FrameId { get; init; }
    public ushort Flags { get; init; }
    public ReadOnlySpan<byte> Payload { get; init; }
}

/// <summary>
/// Pre-allocated ring-buffer UDP datagram slice reassembler with ulong[] bitmask deduplication,
/// wraparound-safe sequence tracking, and dropped-frame resync signaling.
/// Zero heap allocations on steady-state frame reassembly.
/// </summary>
public sealed class ClusterUdpFrameReassembler : IDisposable
{
    private sealed class FrameSlot
    {
        public bool Active;
        public uint FrameId;
        public ushort TotalPackets;
        public ushort ReceivedPackets;
        public uint TotalPayloadBytes;
        public ushort Flags;
        public uint ExpectedCrc32;
        public byte[] Buffer = Array.Empty<byte>();
        public ulong[] ReceivedBitmask = Array.Empty<ulong>();
    }

    public const int DefaultMaxDatagramBytes = 1400;
    public const int DefaultMaxFramePayloadBytes = 16 * 1024 * 1024;
    public const ushort FlagKeyFrame = 0x01;

    private readonly FrameSlot[] _slots;
    private readonly int _maxSlicePayloadBytes;
    private readonly int _maxFramePayloadBytes;
    private uint _lastCompletedFrameId;
    private bool _hasCompletedFrame;
    private bool _disposed;

    public bool NeedsResync { get; private set; }
    public uint LastDroppedFrameId { get; private set; }
    public long TotalFramesCompleted { get; private set; }
    public long TotalFramesDropped { get; private set; }
    public int MaxSlicePayloadBytes => _maxSlicePayloadBytes;

    public ClusterUdpFrameReassembler(
        int slotCount = 4,
        int maxDatagramBytes = DefaultMaxDatagramBytes,
        int maxFramePayloadBytes = DefaultMaxFramePayloadBytes)
    {
        if (slotCount < 2) throw new ArgumentOutOfRangeException(nameof(slotCount));
        if (maxDatagramBytes <= Unsafe.SizeOf<ClusterUdpSliceHeader>())
            throw new ArgumentOutOfRangeException(nameof(maxDatagramBytes));
        if (maxFramePayloadBytes <= 0) throw new ArgumentOutOfRangeException(nameof(maxFramePayloadBytes));

        _maxSlicePayloadBytes = maxDatagramBytes - Unsafe.SizeOf<ClusterUdpSliceHeader>();
        _maxFramePayloadBytes = maxFramePayloadBytes;

        _slots = new FrameSlot[slotCount];
        for (int i = 0; i < slotCount; i++)
        {
            _slots[i] = new FrameSlot();
        }
    }

    public void ClearResyncFlag() => NeedsResync = false;

    public static int BuildSlice(
        Span<byte> destinationDatagram,
        uint frameId,
        ushort totalPackets,
        ushort packetIndex,
        uint totalPayloadBytes,
        ReadOnlySpan<byte> slicePayload,
        ushort flags = 0,
        uint fullFrameCrc32 = 0)
    {
        int headerSize = Unsafe.SizeOf<ClusterUdpSliceHeader>();
        if (destinationDatagram.Length < headerSize + slicePayload.Length)
            throw new ArgumentException("Destination datagram buffer too small.", nameof(destinationDatagram));

        var hdr = new ClusterUdpSliceHeader(frameId, totalPackets, packetIndex, totalPayloadBytes, flags, fullFrameCrc32);
        MemoryMarshal.Write(destinationDatagram, in hdr);
        if (!slicePayload.IsEmpty)
        {
            slicePayload.CopyTo(destinationDatagram.Slice(headerSize));
        }

        return headerSize + slicePayload.Length;
    }

    public ClusterUdpReassembleStatus ProcessDatagram(ReadOnlySpan<byte> datagram, out ClusterReassembledFrame completedFrame)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        completedFrame = default;

        int headerSize = Unsafe.SizeOf<ClusterUdpSliceHeader>();
        if (datagram.Length < headerSize)
        {
            return ClusterUdpReassembleStatus.Ignored;
        }

        var sliceHeader = MemoryMarshal.Read<ClusterUdpSliceHeader>(datagram);
        if (sliceHeader.Magic != ClusterUdpSliceHeader.MagicConstant ||
            sliceHeader.TotalPackets == 0 ||
            sliceHeader.PacketIndex >= sliceHeader.TotalPackets)
        {
            return ClusterUdpReassembleStatus.Ignored;
        }

        if (_hasCompletedFrame && IsOlderOrEqual(sliceHeader.FrameId, _lastCompletedFrameId))
        {
            return ClusterUdpReassembleStatus.Ignored;
        }

        if (sliceHeader.TotalPayloadBytes == 0 || sliceHeader.TotalPayloadBytes > (uint)_maxFramePayloadBytes)
        {
            return ClusterUdpReassembleStatus.Ignored;
        }

        int chunkLen = datagram.Length - headerSize;
        long destOffsetLong = (long)sliceHeader.PacketIndex * _maxSlicePayloadBytes;
        if (destOffsetLong < 0 || destOffsetLong + chunkLen > sliceHeader.TotalPayloadBytes)
        {
            return ClusterUdpReassembleStatus.Ignored;
        }
        int destOffset = (int)destOffsetLong;

        FrameSlot slot = FindOrAllocateSlot(in sliceHeader);
        if (sliceHeader.TotalPackets != slot.TotalPackets ||
            sliceHeader.TotalPayloadBytes != slot.TotalPayloadBytes ||
            sliceHeader.Flags != slot.Flags ||
            sliceHeader.PayloadCrc32 != slot.ExpectedCrc32 ||
            destOffsetLong + chunkLen > slot.TotalPayloadBytes ||
            slot.Buffer is null ||
            destOffset + chunkLen > slot.Buffer.Length)
        {
            return ClusterUdpReassembleStatus.Ignored;
        }

        // Deduplicate via ulong[] bitmask
        int wordIdx = sliceHeader.PacketIndex >> 6;
        ulong bit = 1UL << (sliceHeader.PacketIndex & 63);
        if ((slot.ReceivedBitmask[wordIdx] & bit) == 0)
        {
            slot.ReceivedBitmask[wordIdx] |= bit;
            slot.ReceivedPackets++;

            if (chunkLen > 0)
            {
                datagram.Slice(headerSize, chunkLen).CopyTo(slot.Buffer.AsSpan(destOffset, chunkLen));
            }
        }

        if (slot.ReceivedPackets == slot.TotalPackets)
        {
            ReadOnlySpan<byte> assembled = slot.Buffer.AsSpan(0, (int)slot.TotalPayloadBytes);
            if (slot.ExpectedCrc32 != 0 && ClusterCrc32.Compute(assembled) != slot.ExpectedCrc32)
            {
                slot.Active = false;
                TotalFramesDropped++;
                LastDroppedFrameId = slot.FrameId;
                NeedsResync = true;
                return ClusterUdpReassembleStatus.Ignored;
            }

            bool isKeyFrame = (slot.Flags & FlagKeyFrame) != 0;
            uint expectedNextId = unchecked(_lastCompletedFrameId + 1);
            if (_hasCompletedFrame && IsOlder(expectedNextId, slot.FrameId) && !isKeyFrame)
            {
                TotalFramesDropped += unchecked(slot.FrameId - _lastCompletedFrameId - 1);
                LastDroppedFrameId = unchecked(slot.FrameId - 1);
                NeedsResync = true;
            }

            for (int i = 0; i < _slots.Length; i++)
            {
                var other = _slots[i];
                if (other.Active && other != slot && IsOlderOrEqual(other.FrameId, slot.FrameId))
                {
                    other.Active = false;
                    TotalFramesDropped++;
                    if (!isKeyFrame)
                    {
                        LastDroppedFrameId = other.FrameId;
                        NeedsResync = true;
                    }
                }
            }

            slot.Active = false;
            _lastCompletedFrameId = slot.FrameId;
            _hasCompletedFrame = true;
            TotalFramesCompleted++;

            if (isKeyFrame)
            {
                NeedsResync = false;
            }

            completedFrame = new ClusterReassembledFrame
            {
                FrameId = slot.FrameId,
                Flags = slot.Flags,
                Payload = assembled
            };

            return ClusterUdpReassembleStatus.FrameComplete;
        }

        return ClusterUdpReassembleStatus.Pending;
    }

    private FrameSlot FindOrAllocateSlot(in ClusterUdpSliceHeader slice)
    {
        FrameSlot? freeSlot = null;
        FrameSlot? oldestSlot = null;

        for (int i = 0; i < _slots.Length; i++)
        {
            var s = _slots[i];
            if (s.Active && s.FrameId == slice.FrameId)
            {
                return s;
            }

            if (!s.Active && freeSlot is null)
            {
                freeSlot = s;
            }

            if (s.Active && (oldestSlot is null || IsOlder(s.FrameId, oldestSlot.FrameId)))
            {
                oldestSlot = s;
            }
        }

        FrameSlot chosen;
        if (freeSlot is not null)
        {
            chosen = freeSlot;
        }
        else
        {
            chosen = oldestSlot!;
            TotalFramesDropped++;
            LastDroppedFrameId = chosen.FrameId;
            NeedsResync = true;
        }

        InitializeSlot(chosen, in slice);
        return chosen;
    }

    private static void InitializeSlot(FrameSlot slot, in ClusterUdpSliceHeader slice)
    {
        int requiredBytes = Math.Max(1, (int)slice.TotalPayloadBytes);
        if (slot.Buffer.Length < requiredBytes)
        {
            if (slot.Buffer.Length > 0)
            {
                ArrayPool<byte>.Shared.Return(slot.Buffer);
            }
            slot.Buffer = ArrayPool<byte>.Shared.Rent(requiredBytes);
        }

        int requiredWords = (slice.TotalPackets + 63) >> 6;
        if (slot.ReceivedBitmask.Length < requiredWords)
        {
            slot.ReceivedBitmask = new ulong[requiredWords];
        }
        else
        {
            Array.Clear(slot.ReceivedBitmask, 0, requiredWords);
        }

        slot.Active = true;
        slot.FrameId = slice.FrameId;
        slot.TotalPackets = slice.TotalPackets;
        slot.ReceivedPackets = 0;
        slot.TotalPayloadBytes = slice.TotalPayloadBytes;
        slot.Flags = slice.Flags;
        slot.ExpectedCrc32 = slice.PayloadCrc32;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsOlder(uint a, uint b) => unchecked((int)(a - b)) < 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsOlderOrEqual(uint a, uint b) => unchecked((int)(a - b)) <= 0;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        for (int i = 0; i < _slots.Length; i++)
        {
            if (_slots[i].Buffer.Length > 0)
            {
                ArrayPool<byte>.Shared.Return(_slots[i].Buffer);
                _slots[i].Buffer = Array.Empty<byte>();
            }
            _slots[i].Active = false;
        }
    }
}
