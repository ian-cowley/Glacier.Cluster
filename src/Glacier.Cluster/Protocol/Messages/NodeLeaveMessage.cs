namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Graceful departure notification sent by a worker before terminating.
/// </summary>
public sealed class NodeLeaveMessage : IClusterMessage<NodeLeaveMessage>, IEquatable<NodeLeaveMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.NodeLeave;

    public Guid NodeId { get; set; }
    public uint LeaveReason { get; set; }
    public ulong DrainTimeoutMs { get; set; }
    public string ReasonDetails { get; set; } = string.Empty;

    public NodeLeaveMessage() { }

    public NodeLeaveMessage(Guid nodeId, uint leaveReason, ulong drainTimeoutMs, string? reasonDetails = null)
    {
        NodeId = nodeId;
        LeaveReason = leaveReason;
        DrainTimeoutMs = drainTimeoutMs;
        ReasonDetails = reasonDetails ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 28 + BinaryStringHelper.GetUtf8ByteCount(ReasonDetails);
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for NodeLeaveMessage.", nameof(destination));

        NodeId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(16, 4), LeaveReason);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(20, 8), DrainTimeoutMs);
        int strBytes = BinaryStringHelper.WriteUtf8String(destination.Slice(28), ReasonDetails);
        return 28 + strBytes;
    }

    public static NodeLeaveMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 30)
            throw new ArgumentException("Source span too small for NodeLeaveMessage.", nameof(source));

        Guid nodeId = new Guid(source.Slice(0, 16));
        uint leaveReason = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(16, 4));
        ulong timeoutMs = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(20, 8));
        string details = BinaryStringHelper.ReadUtf8String(source.Slice(28), out _);

        return new NodeLeaveMessage(nodeId, leaveReason, timeoutMs, details);
    }

    public bool Equals(NodeLeaveMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return NodeId.Equals(other.NodeId) &&
               LeaveReason == other.LeaveReason &&
               DrainTimeoutMs == other.DrainTimeoutMs &&
               ReasonDetails == other.ReasonDetails;
    }

    public override bool Equals(object? obj) => Equals(obj as NodeLeaveMessage);

    public override int GetHashCode() =>
        HashCode.Combine(NodeId, LeaveReason, DrainTimeoutMs, ReasonDetails);
}
