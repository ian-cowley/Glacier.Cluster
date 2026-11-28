namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Leader lease replication message exchanged between Active and Standby coordinators.
/// Uses relative <see cref="RemainingLeaseMs"/> to eliminate cross-machine wall-clock skew.
/// </summary>
public sealed class LeaderLeaseMessage : IClusterMessage<LeaderLeaseMessage>, IEquatable<LeaderLeaseMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.LeaderLease;

    public Guid LeaderId { get; set; }
    public ulong Epoch { get; set; }
    public long RemainingLeaseMs { get; set; }
    public string ClusterSecret { get; set; } = string.Empty;

    public LeaderLeaseMessage() { }

    public LeaderLeaseMessage(
        Guid leaderId,
        ulong epoch,
        long remainingLeaseMs,
        string? clusterSecret = null)
    {
        LeaderId = leaderId;
        Epoch = epoch;
        RemainingLeaseMs = remainingLeaseMs;
        ClusterSecret = clusterSecret ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 32 + BinaryStringHelper.GetUtf8ByteCount(ClusterSecret);
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for LeaderLeaseMessage.", nameof(destination));

        LeaderId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(16, 8), Epoch);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(24, 8), RemainingLeaseMs);
        int secretBytes = BinaryStringHelper.WriteUtf8String(destination.Slice(32), ClusterSecret);
        return 32 + secretBytes;
    }

    public static LeaderLeaseMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 34)
            throw new ArgumentException("Source span too small for LeaderLeaseMessage.", nameof(source));

        Guid leaderId = new Guid(source.Slice(0, 16));
        ulong epoch = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(16, 8));
        long remainingMs = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(24, 8));
        string secret = BinaryStringHelper.ReadUtf8String(source.Slice(32), out _);

        return new LeaderLeaseMessage(leaderId, epoch, remainingMs, secret);
    }

    public bool Equals(LeaderLeaseMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return LeaderId.Equals(other.LeaderId) &&
               Epoch == other.Epoch &&
               RemainingLeaseMs == other.RemainingLeaseMs &&
               ClusterSecret == other.ClusterSecret;
    }

    public override bool Equals(object? obj) => Equals(obj as LeaderLeaseMessage);

    public override int GetHashCode() =>
        HashCode.Combine(LeaderId, Epoch, RemainingLeaseMs, ClusterSecret);
}
