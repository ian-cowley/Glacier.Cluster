namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Task execution manifest dispatched by coordinator to worker for execution.
/// </summary>
public sealed class StageDispatchMessage : IClusterMessage<StageDispatchMessage>, IEquatable<StageDispatchMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.StageDispatch;

    public Guid JobId { get; set; }
    public int StageId { get; set; }
    public int PartitionId { get; set; }
    public StageOperatorType OperatorType { get; set; }
    public ReadOnlyMemory<byte> OperatorPayload { get; set; }
    public UpstreamPartitionLocation[] UpstreamPartitions { get; set; } = Array.Empty<UpstreamPartitionLocation>();

    public StageDispatchMessage() { }

    public StageDispatchMessage(
        Guid jobId,
        int stageId,
        int partitionId,
        StageOperatorType operatorType,
        ReadOnlyMemory<byte> operatorPayload,
        UpstreamPartitionLocation[]? upstreamPartitions = null)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
        OperatorType = operatorType;
        OperatorPayload = operatorPayload;
        UpstreamPartitions = upstreamPartitions ?? Array.Empty<UpstreamPartitionLocation>();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        int bytes = 32 + OperatorPayload.Length; // 16 + 4 + 4 + 4 + 4 + Payload.Length
        bytes += 4; // UpstreamPartitionCount
        for (int i = 0; i < UpstreamPartitions.Length; i++)
        {
            bytes += UpstreamPartitions[i].GetByteCount();
        }
        return bytes;
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for StageDispatchMessage.", nameof(destination));

        JobId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), StageId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), PartitionId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(24, 4), (int)OperatorType);

        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(28, 4), OperatorPayload.Length);
        OperatorPayload.Span.CopyTo(destination.Slice(32, OperatorPayload.Length));
        int offset = 32 + OperatorPayload.Length;

        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, 4), UpstreamPartitions.Length);
        offset += 4;

        for (int i = 0; i < UpstreamPartitions.Length; i++)
        {
            offset += UpstreamPartitions[i].Serialize(destination.Slice(offset));
        }

        return offset;
    }

    public static StageDispatchMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 36) // 32 base + 4 upstream count
            throw new ArgumentException("Source span too small for StageDispatchMessage.", nameof(source));

        Guid jobId = new Guid(source.Slice(0, 16));
        int stageId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        int partitionId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4));
        StageOperatorType opType = (StageOperatorType)BinaryPrimitives.ReadInt32LittleEndian(source.Slice(24, 4));

        int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(28, 4));
        if (payloadLen < 0 || payloadLen > source.Length - 36)
            throw new ArgumentException("Invalid operator payload length in StageDispatchMessage.", nameof(source));

        byte[] payloadCopy = payloadLen > 0 ? source.Slice(32, payloadLen).ToArray() : Array.Empty<byte>();
        int offset = 32 + payloadLen;

        int upstreamCount = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(offset, 4));
        offset += 4;

        // Each UpstreamPartitionLocation requires at least 28 bytes (26 fixed + 2-byte string length prefix).
        if (upstreamCount < 0 || upstreamCount > (source.Length - offset) / 28)
            throw new ArgumentException("Invalid upstream partition count in StageDispatchMessage.", nameof(source));

        var upstreams = upstreamCount > 0 ? new UpstreamPartitionLocation[upstreamCount] : Array.Empty<UpstreamPartitionLocation>();
        for (int i = 0; i < upstreamCount; i++)
        {
            upstreams[i] = UpstreamPartitionLocation.Deserialize(source.Slice(offset), out int locBytes);
            offset += locBytes;
        }

        return new StageDispatchMessage(jobId, stageId, partitionId, opType, payloadCopy, upstreams);
    }

    public bool Equals(StageDispatchMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        if (!JobId.Equals(other.JobId) ||
            StageId != other.StageId ||
            PartitionId != other.PartitionId ||
            OperatorType != other.OperatorType ||
            !OperatorPayload.Span.SequenceEqual(other.OperatorPayload.Span) ||
            UpstreamPartitions.Length != other.UpstreamPartitions.Length)
        {
            return false;
        }

        for (int i = 0; i < UpstreamPartitions.Length; i++)
        {
            if (!UpstreamPartitions[i].Equals(other.UpstreamPartitions[i]))
                return false;
        }

        return true;
    }

    public override bool Equals(object? obj) => Equals(obj as StageDispatchMessage);

    public override int GetHashCode() =>
        HashCode.Combine(JobId, StageId, PartitionId, OperatorType);
}
