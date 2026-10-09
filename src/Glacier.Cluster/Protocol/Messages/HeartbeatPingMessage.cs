namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// Fixed 40-byte blittable heartbeat ping sent by workers to coordinator.
/// Zero heap allocations when sent or received.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 8, Size = 40)]
public readonly struct HeartbeatPingMessage : IClusterMessage<HeartbeatPingMessage>, IEquatable<HeartbeatPingMessage>
{
    public const int PayloadSize = 40;

    public ClusterMessageType MessageType => ClusterMessageType.HeartbeatPing;

    public readonly Guid NodeId;
    public readonly ulong TimestampNs;
    public readonly uint ActiveTaskCount;
    public readonly float CpuUsagePercentage;
    public readonly ulong AvailablePhysicalMemoryBytes;

    public HeartbeatPingMessage(
        Guid nodeId,
        ulong timestampNs,
        uint activeTaskCount,
        float cpuUsagePercentage,
        ulong availablePhysicalMemoryBytes)
    {
        NodeId = nodeId;
        TimestampNs = timestampNs;
        ActiveTaskCount = activeTaskCount;
        CpuUsagePercentage = cpuUsagePercentage;
        AvailablePhysicalMemoryBytes = availablePhysicalMemoryBytes;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount() => PayloadSize;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Serialize(Span<byte> destination)
    {
        if (destination.Length < PayloadSize)
            throw new ArgumentException("Destination span too small for HeartbeatPingMessage.", nameof(destination));

        NodeId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(16, 8), TimestampNs);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(24, 4), ActiveTaskCount);
        BinaryPrimitives.WriteSingleLittleEndian(destination.Slice(28, 4), CpuUsagePercentage);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(32, 8), AvailablePhysicalMemoryBytes);
        return PayloadSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static HeartbeatPingMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < PayloadSize)
            throw new ArgumentException("Source span too small for HeartbeatPingMessage.", nameof(source));

        Guid nodeId = new Guid(source.Slice(0, 16));
        ulong timestampNs = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(16, 8));
        uint activeTasks = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(24, 4));
        float cpuUsage = BinaryPrimitives.ReadSingleLittleEndian(source.Slice(28, 4));
        ulong availMem = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(32, 8));
        return new HeartbeatPingMessage(nodeId, timestampNs, activeTasks, cpuUsage, availMem);
    }

    public bool Equals(HeartbeatPingMessage other) =>
        NodeId.Equals(other.NodeId) &&
        TimestampNs == other.TimestampNs &&
        ActiveTaskCount == other.ActiveTaskCount &&
        CpuUsagePercentage.Equals(other.CpuUsagePercentage) &&
        AvailablePhysicalMemoryBytes == other.AvailablePhysicalMemoryBytes;

    public override bool Equals(object? obj) => obj is HeartbeatPingMessage other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(NodeId, TimestampNs, ActiveTaskCount, CpuUsagePercentage, AvailablePhysicalMemoryBytes);

    public static bool operator ==(HeartbeatPingMessage left, HeartbeatPingMessage right) => left.Equals(right);
    public static bool operator !=(HeartbeatPingMessage left, HeartbeatPingMessage right) => !left.Equals(right);
}
