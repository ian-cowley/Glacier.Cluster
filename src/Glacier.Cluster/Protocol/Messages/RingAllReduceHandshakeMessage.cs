namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Collective communication topology handshake setting up Ring AllReduce neighbors.
/// </summary>
public sealed class RingAllReduceHandshakeMessage : IClusterMessage<RingAllReduceHandshakeMessage>, IEquatable<RingAllReduceHandshakeMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.RingAllReduceHandshake;

    public Guid SessionId { get; set; }
    public int Rank { get; set; }
    public int WorldSize { get; set; }
    public Guid PredecessorId { get; set; }
    public Guid SuccessorId { get; set; }
    public ushort PredecessorPort { get; set; }
    public ushort SuccessorPort { get; set; }
    public string PredecessorEndpoint { get; set; } = string.Empty;
    public string SuccessorEndpoint { get; set; } = string.Empty;

    public RingAllReduceHandshakeMessage() { }

    public RingAllReduceHandshakeMessage(
        Guid sessionId,
        int rank,
        int worldSize,
        Guid predecessorId,
        Guid successorId,
        ushort predecessorPort,
        ushort successorPort,
        string? predecessorEndpoint = null,
        string? successorEndpoint = null)
    {
        SessionId = sessionId;
        Rank = rank;
        WorldSize = worldSize;
        PredecessorId = predecessorId;
        SuccessorId = successorId;
        PredecessorPort = predecessorPort;
        SuccessorPort = successorPort;
        PredecessorEndpoint = predecessorEndpoint ?? string.Empty;
        SuccessorEndpoint = successorEndpoint ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 60 + BinaryStringHelper.GetUtf8ByteCount(PredecessorEndpoint) + BinaryStringHelper.GetUtf8ByteCount(SuccessorEndpoint);
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for RingAllReduceHandshakeMessage.", nameof(destination));

        SessionId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), Rank);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), WorldSize);
        PredecessorId.TryWriteBytes(destination.Slice(24, 16));
        SuccessorId.TryWriteBytes(destination.Slice(40, 16));
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(56, 2), PredecessorPort);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(58, 2), SuccessorPort);

        int offset = 60;
        offset += BinaryStringHelper.WriteUtf8String(destination.Slice(offset), PredecessorEndpoint);
        offset += BinaryStringHelper.WriteUtf8String(destination.Slice(offset), SuccessorEndpoint);
        return offset;
    }

    public static RingAllReduceHandshakeMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 64)
            throw new ArgumentException("Source span too small for RingAllReduceHandshakeMessage.", nameof(source));

        Guid sessionId = new Guid(source.Slice(0, 16));
        int rank = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        int worldSize = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4));
        Guid predId = new Guid(source.Slice(24, 16));
        Guid succId = new Guid(source.Slice(40, 16));
        ushort predPort = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(56, 2));
        ushort succPort = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(58, 2));

        int offset = 60;
        string predEp = BinaryStringHelper.ReadUtf8String(source.Slice(offset), out int predBytes);
        offset += predBytes;

        string succEp = BinaryStringHelper.ReadUtf8String(source.Slice(offset), out _);

        return new RingAllReduceHandshakeMessage(sessionId, rank, worldSize, predId, succId, predPort, succPort, predEp, succEp);
    }

    public bool Equals(RingAllReduceHandshakeMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return SessionId.Equals(other.SessionId) &&
               Rank == other.Rank &&
               WorldSize == other.WorldSize &&
               PredecessorId.Equals(other.PredecessorId) &&
               SuccessorId.Equals(other.SuccessorId) &&
               PredecessorPort == other.PredecessorPort &&
               SuccessorPort == other.SuccessorPort &&
               PredecessorEndpoint == other.PredecessorEndpoint &&
               SuccessorEndpoint == other.SuccessorEndpoint;
    }

    public override bool Equals(object? obj) => Equals(obj as RingAllReduceHandshakeMessage);

    public override int GetHashCode() =>
        HashCode.Combine(SessionId, Rank, WorldSize, PredecessorId, SuccessorId, PredecessorPort, SuccessorPort);
}
