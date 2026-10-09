namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Worker registration request containing probed NodeHardwareProfile and network hostname.
/// </summary>
public sealed class NodeJoinRequestMessage : IClusterMessage<NodeJoinRequestMessage>, IEquatable<NodeJoinRequestMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.NodeJoinRequest;

    public NodeHardwareProfile HardwareProfile { get; set; }
    public string Hostname { get; set; } = string.Empty;
    public string ClusterSecret { get; set; } = string.Empty;

    public NodeJoinRequestMessage() { }

    public NodeJoinRequestMessage(NodeHardwareProfile hardwareProfile, string? hostname, string? clusterSecret = null)
    {
        HardwareProfile = hardwareProfile;
        Hostname = hostname ?? string.Empty;
        ClusterSecret = clusterSecret ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 64 + BinaryStringHelper.GetUtf8ByteCount(Hostname) + BinaryStringHelper.GetUtf8ByteCount(ClusterSecret);
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for NodeJoinRequestMessage.", nameof(destination));

        // Serialize 64-byte NodeHardwareProfile
        HardwareProfile.WriteTo(destination.Slice(0, 64));
        int offset = 64;

        offset += BinaryStringHelper.WriteUtf8String(destination.Slice(offset), Hostname);
        offset += BinaryStringHelper.WriteUtf8String(destination.Slice(offset), ClusterSecret);
        return offset;
    }

    public static NodeJoinRequestMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 68) // 64 profile + 2 hostname length + 2 secret length
            throw new ArgumentException("Source span too small for NodeJoinRequestMessage.", nameof(source));

        NodeHardwareProfile profile = NodeHardwareProfile.ReadFrom(source.Slice(0, 64));
        int offset = 64;

        string hostname = BinaryStringHelper.ReadUtf8String(source.Slice(offset), out int hostBytes);
        offset += hostBytes;

        string secret = BinaryStringHelper.ReadUtf8String(source.Slice(offset), out _);

        return new NodeJoinRequestMessage(profile, hostname, secret);
    }

    public bool Equals(NodeJoinRequestMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return HardwareProfile.Equals(other.HardwareProfile) &&
               Hostname == other.Hostname &&
               ClusterSecret == other.ClusterSecret;
    }

    public override bool Equals(object? obj) => Equals(obj as NodeJoinRequestMessage);

    public override int GetHashCode() =>
        HashCode.Combine(HardwareProfile, Hostname, ClusterSecret);
}
