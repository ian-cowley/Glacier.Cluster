namespace Glacier.Cluster.Protocol;

using System;
using System.Runtime.CompilerServices;

/// <summary>
/// Authentication handshake message for P2P shuffle connections when ClusterSecret is configured.
/// </summary>
public sealed class ShuffleAuthMessage : IClusterMessage<ShuffleAuthMessage>, IEquatable<ShuffleAuthMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.ShuffleAuth;

    public Guid NodeId { get; set; }
    public bool IsResponse { get; set; }
    public bool Accepted { get; set; }
    public string ClusterSecret { get; set; } = string.Empty;

    public ShuffleAuthMessage() { }

    public ShuffleAuthMessage(
        Guid nodeId,
        string? clusterSecret = null,
        bool isResponse = false,
        bool accepted = false)
    {
        NodeId = nodeId;
        ClusterSecret = clusterSecret ?? string.Empty;
        IsResponse = isResponse;
        Accepted = accepted;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 18 + BinaryStringHelper.GetUtf8ByteCount(ClusterSecret);
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for ShuffleAuthMessage.", nameof(destination));

        NodeId.TryWriteBytes(destination.Slice(0, 16));
        destination[16] = IsResponse ? (byte)1 : (byte)0;
        destination[17] = Accepted ? (byte)1 : (byte)0;
        int secretBytes = BinaryStringHelper.WriteUtf8String(destination.Slice(18), ClusterSecret);
        return 18 + secretBytes;
    }

    public static ShuffleAuthMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 20)
            throw new ArgumentException("Source span too small for ShuffleAuthMessage.", nameof(source));

        Guid nodeId = new Guid(source.Slice(0, 16));
        bool isResponse = source[16] != 0;
        bool accepted = source[17] != 0;
        string secret = BinaryStringHelper.ReadUtf8String(source.Slice(18), out _);

        return new ShuffleAuthMessage(nodeId, secret, isResponse, accepted);
    }

    public bool Equals(ShuffleAuthMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return NodeId.Equals(other.NodeId) &&
               IsResponse == other.IsResponse &&
               Accepted == other.Accepted &&
               ClusterSecret == other.ClusterSecret;
    }

    public override bool Equals(object? obj) => Equals(obj as ShuffleAuthMessage);

    public override int GetHashCode() =>
        HashCode.Combine(NodeId, IsResponse, Accepted, ClusterSecret);
}
