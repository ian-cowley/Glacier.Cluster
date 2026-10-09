namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Coordinator response to NodeJoinRequest with rank assignment and membership status.
/// </summary>
public sealed class NodeJoinResponseMessage : IClusterMessage<NodeJoinResponseMessage>, IEquatable<NodeJoinResponseMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.NodeJoinResponse;

    public NodeJoinStatus Status { get; set; }
    public Guid AssignedNodeId { get; set; }
    public int AssignedRank { get; set; }
    public int ActiveClusterSize { get; set; }
    public ulong ClusterEpoch { get; set; }
    public string RejectionReason { get; set; } = string.Empty;

    public NodeJoinResponseMessage() { }

    public NodeJoinResponseMessage(
        NodeJoinStatus status,
        Guid assignedNodeId,
        int assignedRank,
        int activeClusterSize,
        ulong clusterEpoch,
        string? rejectionReason = null)
    {
        Status = status;
        AssignedNodeId = assignedNodeId;
        AssignedRank = assignedRank;
        ActiveClusterSize = activeClusterSize;
        ClusterEpoch = clusterEpoch;
        RejectionReason = rejectionReason ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 33 + BinaryStringHelper.GetUtf8ByteCount(RejectionReason);
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for NodeJoinResponseMessage.", nameof(destination));

        destination[0] = (byte)Status;
        AssignedNodeId.TryWriteBytes(destination.Slice(1, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(17, 4), AssignedRank);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(21, 4), ActiveClusterSize);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(25, 8), ClusterEpoch);
        int strBytes = BinaryStringHelper.WriteUtf8String(destination.Slice(33), RejectionReason);
        return 33 + strBytes;
    }

    public static NodeJoinResponseMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 35)
            throw new ArgumentException("Source span too small for NodeJoinResponseMessage.", nameof(source));

        NodeJoinStatus status = (NodeJoinStatus)source[0];
        Guid assignedNodeId = new Guid(source.Slice(1, 16));
        int rank = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(17, 4));
        int clusterSize = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(21, 4));
        ulong epoch = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(25, 8));
        string reason = BinaryStringHelper.ReadUtf8String(source.Slice(33), out _);

        return new NodeJoinResponseMessage(status, assignedNodeId, rank, clusterSize, epoch, reason);
    }

    public bool Equals(NodeJoinResponseMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return Status == other.Status &&
               AssignedNodeId.Equals(other.AssignedNodeId) &&
               AssignedRank == other.AssignedRank &&
               ActiveClusterSize == other.ActiveClusterSize &&
               ClusterEpoch == other.ClusterEpoch &&
               RejectionReason == other.RejectionReason;
    }

    public override bool Equals(object? obj) => Equals(obj as NodeJoinResponseMessage);

    public override int GetHashCode() =>
        HashCode.Combine(Status, AssignedNodeId, AssignedRank, ActiveClusterSize, ClusterEpoch, RejectionReason);
}
