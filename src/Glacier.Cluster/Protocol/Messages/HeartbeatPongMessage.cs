namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// Fixed 40-byte blittable heartbeat pong sent by coordinator to worker.
/// Zero heap allocations when sent or received.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 8, Size = 40)]
public readonly struct HeartbeatPongMessage : IClusterMessage<HeartbeatPongMessage>, IEquatable<HeartbeatPongMessage>
{
    public const int PayloadSize = 40;

    public ClusterMessageType MessageType => ClusterMessageType.HeartbeatPong;

    public readonly Guid CoordinatorId;
    public readonly ulong TimestampNs;
    public readonly ulong ClusterEpoch;
    public readonly uint CoordinatorStatusFlags;
    public readonly uint ActiveNodeCount;

    public HeartbeatPongMessage(
        Guid coordinatorId,
        ulong timestampNs,
        ulong clusterEpoch,
        uint coordinatorStatusFlags,
        uint activeNodeCount)
    {
        CoordinatorId = coordinatorId;
        TimestampNs = timestampNs;
        ClusterEpoch = clusterEpoch;
        CoordinatorStatusFlags = coordinatorStatusFlags;
        ActiveNodeCount = activeNodeCount;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount() => PayloadSize;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Serialize(Span<byte> destination)
    {
        if (destination.Length < PayloadSize)
            throw new ArgumentException("Destination span too small for HeartbeatPongMessage.", nameof(destination));

        CoordinatorId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(16, 8), TimestampNs);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(24, 8), ClusterEpoch);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(32, 4), CoordinatorStatusFlags);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(36, 4), ActiveNodeCount);
        return PayloadSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static HeartbeatPongMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < PayloadSize)
            throw new ArgumentException("Source span too small for HeartbeatPongMessage.", nameof(source));

        Guid coordinatorId = new Guid(source.Slice(0, 16));
        ulong timestampNs = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(16, 8));
        ulong clusterEpoch = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(24, 8));
        uint flags = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(32, 4));
        uint nodeCount = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(36, 4));
        return new HeartbeatPongMessage(coordinatorId, timestampNs, clusterEpoch, flags, nodeCount);
    }

    public bool Equals(HeartbeatPongMessage other) =>
        CoordinatorId.Equals(other.CoordinatorId) &&
        TimestampNs == other.TimestampNs &&
        ClusterEpoch == other.ClusterEpoch &&
        CoordinatorStatusFlags == other.CoordinatorStatusFlags &&
        ActiveNodeCount == other.ActiveNodeCount;

    public override bool Equals(object? obj) => obj is HeartbeatPongMessage other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(CoordinatorId, TimestampNs, ClusterEpoch, CoordinatorStatusFlags, ActiveNodeCount);

    public static bool operator ==(HeartbeatPongMessage left, HeartbeatPongMessage right) => left.Equals(right);
    public static bool operator !=(HeartbeatPongMessage left, HeartbeatPongMessage right) => !left.Equals(right);
}
