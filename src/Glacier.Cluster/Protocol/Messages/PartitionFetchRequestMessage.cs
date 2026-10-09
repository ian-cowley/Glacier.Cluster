namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// Fixed 40-byte blittable P2P pull request sent by consumer worker to producer worker.
/// Employs credit-based flow control to prevent TCP incast during wide shuffles.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 8, Size = 40)]
public readonly struct PartitionFetchRequestMessage : IClusterMessage<PartitionFetchRequestMessage>, IEquatable<PartitionFetchRequestMessage>
{
    public const int PayloadSize = 40;

    public ClusterMessageType MessageType => ClusterMessageType.PartitionFetchRequest;

    public readonly Guid JobId;
    public readonly int StageId;
    public readonly int PartitionId;
    public readonly uint CreditTokens;
    public readonly ulong ByteOffset;
    public readonly uint MaxChunkSizeBytes;

    public PartitionFetchRequestMessage(
        Guid jobId,
        int stageId,
        int partitionId,
        uint creditTokens,
        ulong byteOffset = 0,
        uint maxChunkSizeBytes = 65536)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
        CreditTokens = creditTokens;
        ByteOffset = byteOffset;
        MaxChunkSizeBytes = maxChunkSizeBytes;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount() => PayloadSize;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Serialize(Span<byte> destination)
    {
        if (destination.Length < PayloadSize)
            throw new ArgumentException("Destination span too small for PartitionFetchRequestMessage.", nameof(destination));

        JobId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), StageId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), PartitionId);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(24, 4), CreditTokens);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(28, 8), ByteOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(36, 4), MaxChunkSizeBytes);
        return PayloadSize;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PartitionFetchRequestMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < PayloadSize)
            throw new ArgumentException("Source span too small for PartitionFetchRequestMessage.", nameof(source));

        Guid jobId = new Guid(source.Slice(0, 16));
        int stageId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        int partitionId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4));
        uint credits = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(24, 4));
        ulong offset = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(28, 8));
        uint maxChunk = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(36, 4));
        return new PartitionFetchRequestMessage(jobId, stageId, partitionId, credits, offset, maxChunk);
    }

    public bool Equals(PartitionFetchRequestMessage other) =>
        JobId.Equals(other.JobId) &&
        StageId == other.StageId &&
        PartitionId == other.PartitionId &&
        CreditTokens == other.CreditTokens &&
        ByteOffset == other.ByteOffset &&
        MaxChunkSizeBytes == other.MaxChunkSizeBytes;

    public override bool Equals(object? obj) => obj is PartitionFetchRequestMessage other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(JobId, StageId, PartitionId, CreditTokens, ByteOffset, MaxChunkSizeBytes);

    public static bool operator ==(PartitionFetchRequestMessage left, PartitionFetchRequestMessage right) => left.Equals(right);
    public static bool operator !=(PartitionFetchRequestMessage left, PartitionFetchRequestMessage right) => !left.Equals(right);
}
