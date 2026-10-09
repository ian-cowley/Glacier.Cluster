namespace Glacier.Cluster.Protocol;

using System;

/// <summary>
/// Common contract for all serializable binary cluster messages.
/// </summary>
public interface IClusterMessage
{
    /// <summary>Gets the authoritative message opcode.</summary>
    ClusterMessageType MessageType { get; }

    /// <summary>Gets the exact byte count required to serialize this message payload.</summary>
    int GetByteCount();

    /// <summary>
    /// Serializes this message into the destination span using explicit Little-Endian blitting.
    /// </summary>
    /// <param name="destination">Destination memory span.</param>
    /// <returns>Exact number of bytes written.</returns>
    int Serialize(Span<byte> destination);
}

/// <summary>
/// Generic contract supporting static abstract deserialization in C# 11+ / .NET 10.
/// </summary>
/// <typeparam name="TSelf">The concrete message type.</typeparam>
public interface IClusterMessage<TSelf> : IClusterMessage
    where TSelf : IClusterMessage<TSelf>
{
    /// <summary>
    /// Deserializes an instance of <typeparamref name="TSelf"/> from the provided read-only byte span.
    /// </summary>
    /// <param name="source">Read-only memory span containing the serialized message payload.</param>
    /// <returns>Deserialized message instance.</returns>
    static abstract TSelf Deserialize(ReadOnlySpan<byte> source);
}
