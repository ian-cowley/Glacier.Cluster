namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Identifies the network location of an upstream partition that a downstream task depends on.
/// </summary>
public sealed class UpstreamPartitionLocation : IEquatable<UpstreamPartitionLocation>
{
    public Guid ProducerNodeId { get; set; }
    public int StageId { get; set; }
    public int PartitionId { get; set; }
    public ushort ShufflePort { get; set; }
    public string Hostname { get; set; } = string.Empty;

    public UpstreamPartitionLocation() { }

    public UpstreamPartitionLocation(Guid producerNodeId, int stageId, int partitionId, ushort shufflePort, string? hostname)
    {
        ProducerNodeId = producerNodeId;
        StageId = stageId;
        PartitionId = partitionId;
        ShufflePort = shufflePort;
        Hostname = hostname ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 26 + BinaryStringHelper.GetUtf8ByteCount(Hostname);
    }

    public int Serialize(Span<byte> destination)
    {
        int minRequired = GetByteCount();
        if (destination.Length < minRequired)
            throw new ArgumentException("Destination span too small for UpstreamPartitionLocation.", nameof(destination));

        ProducerNodeId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), StageId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), PartitionId);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(24, 2), ShufflePort);
        int strBytes = BinaryStringHelper.WriteUtf8String(destination.Slice(26), Hostname);
        return 26 + strBytes;
    }

    public static UpstreamPartitionLocation Deserialize(ReadOnlySpan<byte> source, out int bytesConsumed)
    {
        if (source.Length < 28) // 26 fixed + at least 2 bytes string length
            throw new ArgumentException("Source span too small for UpstreamPartitionLocation.", nameof(source));

        Guid producerNodeId = new Guid(source.Slice(0, 16));
        int stageId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        int partitionId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4));
        ushort shufflePort = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(24, 2));
        string hostname = BinaryStringHelper.ReadUtf8String(source.Slice(26), out int strBytes);

        bytesConsumed = 26 + strBytes;
        return new UpstreamPartitionLocation(producerNodeId, stageId, partitionId, shufflePort, hostname);
    }

    public bool Equals(UpstreamPartitionLocation? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return ProducerNodeId.Equals(other.ProducerNodeId) &&
               StageId == other.StageId &&
               PartitionId == other.PartitionId &&
               ShufflePort == other.ShufflePort &&
               Hostname == other.Hostname;
    }

    public override bool Equals(object? obj) => Equals(obj as UpstreamPartitionLocation);

    public override int GetHashCode() =>
        HashCode.Combine(ProducerNodeId, StageId, PartitionId, ShufflePort, Hostname);
}
