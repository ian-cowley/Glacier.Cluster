namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// P2P shuffle response header and data stream payload.
/// Supports zero-copy memory slicing over unmanaged partition buffers.
/// </summary>
public sealed class PartitionFetchResponseMessage : IClusterMessage<PartitionFetchResponseMessage>, IEquatable<PartitionFetchResponseMessage>
{
    public const int HeaderSize = 48;

    public ClusterMessageType MessageType => ClusterMessageType.PartitionFetchResponse;

    public Guid JobId { get; set; }
    public int StageId { get; set; }
    public int PartitionId { get; set; }
    public ulong DataByteLength { get; set; }
    public PartitionFetchStatus Status { get; set; }
    public PartitionDataFlags Flags { get; set; }
    public ulong ChunkOffset { get; set; }
    public ReadOnlyMemory<byte> Data { get; set; }

    public PartitionFetchResponseMessage() { }

    public PartitionFetchResponseMessage(
        Guid jobId,
        int stageId,
        int partitionId,
        ulong dataByteLength,
        PartitionFetchStatus status,
        PartitionDataFlags flags,
        ulong chunkOffset,
        ReadOnlyMemory<byte> data)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
        DataByteLength = dataByteLength;
        Status = status;
        Flags = flags;
        ChunkOffset = chunkOffset;
        Data = data;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount() => HeaderSize + Data.Length;

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for PartitionFetchResponseMessage.", nameof(destination));

        JobId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), StageId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), PartitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(24, 8), DataByteLength);
        destination[32] = (byte)Status;
        destination[33] = (byte)Flags;
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(34, 2), 0); // Reserved
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(36, 8), ChunkOffset);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(44, 4), Data.Length);

        if (Data.Length > 0)
        {
            Data.Span.CopyTo(destination.Slice(HeaderSize, Data.Length));
        }

        return required;
    }

    public static PartitionFetchResponseMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < HeaderSize)
            throw new ArgumentException("Source span too small for PartitionFetchResponseMessage header.", nameof(source));

        Guid jobId = new Guid(source.Slice(0, 16));
        int stageId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        int partitionId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4));
        ulong totalDataLen = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(24, 8));
        PartitionFetchStatus status = (PartitionFetchStatus)source[32];
        PartitionDataFlags flags = (PartitionDataFlags)source[33];
        ulong chunkOffset = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(36, 8));
        int chunkLen = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(44, 4));

        if (chunkLen < 0 || source.Length < HeaderSize + chunkLen)
            throw new ArgumentException("Invalid chunk payload length in PartitionFetchResponseMessage.", nameof(source));

        byte[] chunk = chunkLen > 0 ? source.Slice(HeaderSize, chunkLen).ToArray() : Array.Empty<byte>();

        return new PartitionFetchResponseMessage(jobId, stageId, partitionId, totalDataLen, status, flags, chunkOffset, chunk);
    }

    public bool Equals(PartitionFetchResponseMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return JobId.Equals(other.JobId) &&
               StageId == other.StageId &&
               PartitionId == other.PartitionId &&
               DataByteLength == other.DataByteLength &&
               Status == other.Status &&
               Flags == other.Flags &&
               ChunkOffset == other.ChunkOffset &&
               Data.Span.SequenceEqual(other.Data.Span);
    }

    public override bool Equals(object? obj) => Equals(obj as PartitionFetchResponseMessage);

    public override int GetHashCode() =>
        HashCode.Combine(JobId, StageId, PartitionId, DataByteLength, Status, Flags, ChunkOffset);
}
