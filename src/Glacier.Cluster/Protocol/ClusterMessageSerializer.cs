namespace Glacier.Cluster.Protocol;

using System;

/// <summary>
/// High-throughput, Native AOT compatible polymorphic message serializer and dispatcher.
/// </summary>
public static class ClusterMessageSerializer
{
    /// <summary>
    /// Deserializes any cluster message based on its ClusterMessageType opcode.
    /// </summary>
    public static IClusterMessage Deserialize(ClusterMessageType type, ReadOnlySpan<byte> source)
    {
        return type switch
        {
            ClusterMessageType.HeartbeatPing => HeartbeatPingMessage.Deserialize(source),
            ClusterMessageType.HeartbeatPong => HeartbeatPongMessage.Deserialize(source),
            ClusterMessageType.NodeJoinRequest => NodeJoinRequestMessage.Deserialize(source),
            ClusterMessageType.NodeJoinResponse => NodeJoinResponseMessage.Deserialize(source),
            ClusterMessageType.NodeLeave => NodeLeaveMessage.Deserialize(source),
            ClusterMessageType.StageDispatch => StageDispatchMessage.Deserialize(source),
            ClusterMessageType.TaskStatusUpdate => TaskStatusUpdateMessage.Deserialize(source),
            ClusterMessageType.PartitionFetchRequest => PartitionFetchRequestMessage.Deserialize(source),
            ClusterMessageType.PartitionFetchResponse => PartitionFetchResponseMessage.Deserialize(source),
            ClusterMessageType.PartitionAcknowledge => PartitionAcknowledgeMessage.Deserialize(source),
            ClusterMessageType.RingAllReduceHandshake => RingAllReduceHandshakeMessage.Deserialize(source),
            ClusterMessageType.RingAllReduceStep => AllReduceStepMessage.Deserialize(source),
            ClusterMessageType.RingAllReduceCompletion => AllReduceCompletionMessage.Deserialize(source),
            ClusterMessageType.JobSubmission => JobSubmissionMessage.Deserialize(source),
            ClusterMessageType.JobCompletion => JobCompletionMessage.Deserialize(source),
            _ => throw new NotSupportedException($"Unsupported cluster message type opcode: 0x{(byte)type:X2}")
        };
    }

    /// <summary>
    /// Serializes an entire frame: [32-byte ClusterFrameHeader][Payload Bytes] directly into destination span.
    /// </summary>
    public static int PackFrame(
        IClusterMessage message,
        ulong correlationId,
        byte flags,
        Span<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(message);

        int payloadSize = message.GetByteCount();
        int totalFrameSize = ClusterFrameHeader.HeaderSize + payloadSize;

        if (destination.Length < totalFrameSize)
            throw new ArgumentException($"Destination span too small. Required: {totalFrameSize}, Available: {destination.Length}.", nameof(destination));

        // 1. Serialize payload into destination span at offset 32
        Span<byte> payloadSpan = destination.Slice(ClusterFrameHeader.HeaderSize, payloadSize);
        int written = message.Serialize(payloadSpan);

        // 2. Compute CRC32 of payload bytes
        uint crc32 = ClusterCrc32.Compute(payloadSpan.Slice(0, written));

        // 3. Write 32-byte header
        var header = new ClusterFrameHeader(message.MessageType, correlationId, (ulong)written, crc32, flags);
        header.WriteTo(destination.Slice(0, ClusterFrameHeader.HeaderSize));

        return totalFrameSize;
    }

    /// <summary>
    /// Serializes an entire frame using typed <see cref="ClusterFrameFlags"/>.
    /// </summary>
    public static int PackFrame(
        IClusterMessage message,
        ulong correlationId,
        ClusterFrameFlags flags,
        Span<byte> destination)
    {
        return PackFrame(message, correlationId, (byte)flags, destination);
    }
}
