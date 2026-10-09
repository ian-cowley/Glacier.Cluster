namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// Fixed 40-byte blittable acknowledgment returning flow-control credits back to producer.
/// Zero heap allocations when sent or received.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 8, Size = 40)]
public readonly struct PartitionAcknowledgeMessage : IClusterMessage<PartitionAcknowledgeMessage>, IEquatable<PartitionAcknowledgeMessage>
{
    public const int PayloadSize = 40;

    public ClusterMessageType MessageType => ClusterMessageType.PartitionAcknowledge;

    public readonly Guid JobId;
    public readonly int StageId;
    public readonly int PartitionId;
    public readonly uint TokensReleased;
    public readonly ulong BytesReceived;
    public readonly byte IsComplete;

    public PartitionAcknowledgeMessage(
        Guid jobId,
        int stageId,
        int partitionId,
        uint tokensReleased,
        ulong bytesReceived,
        bool isComplete)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
        TokensReleased = tokensReleased;
        BytesReceived = bytesReceived;
        IsComplete = isComplete ? (byte)1 : (byte)0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount() => PayloadSize;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Serialize(Span<byte> destination)
    {
        if (destination.Length < PayloadSize)
            throw new ArgumentException("Destination span too small for PartitionAcknowledgeMessage.", nameof(destination));

        JobId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), StageId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), PartitionId);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(24, 4), TokensReleased);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(28, 8), BytesReceived);
        destination[36] = IsComplete;
        destination[37] = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(38, 2), 0);
        return PayloadSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PartitionAcknowledgeMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < PayloadSize)
            throw new ArgumentException("Source span too small for PartitionAcknowledgeMessage.", nameof(source));

        Guid jobId = new Guid(source.Slice(0, 16));
        int stageId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        int partitionId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4));
        uint tokens = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(24, 4));
        ulong bytes = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(28, 8));
        bool complete = source[36] != 0;
        return new PartitionAcknowledgeMessage(jobId, stageId, partitionId, tokens, bytes, complete);
    }

    public bool Equals(PartitionAcknowledgeMessage other) =>
        JobId.Equals(other.JobId) &&
        StageId == other.StageId &&
        PartitionId == other.PartitionId &&
        TokensReleased == other.TokensReleased &&
        BytesReceived == other.BytesReceived &&
        IsComplete == other.IsComplete;

    public override bool Equals(object? obj) => obj is PartitionAcknowledgeMessage other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(JobId, StageId, PartitionId, TokensReleased, BytesReceived, IsComplete);

    public static bool operator ==(PartitionAcknowledgeMessage left, PartitionAcknowledgeMessage right) => left.Equals(right);
    public static bool operator !=(PartitionAcknowledgeMessage left, PartitionAcknowledgeMessage right) => !left.Equals(right);
}
