namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Signals completion of a collective Ring AllReduce round.
/// </summary>
public sealed class AllReduceCompletionMessage : IClusterMessage<AllReduceCompletionMessage>, IEquatable<AllReduceCompletionMessage>
{
    public const int PayloadSize = 40;

    public ClusterMessageType MessageType => ClusterMessageType.RingAllReduceCompletion;

    public Guid SessionId { get; set; }
    public int Rank { get; set; }
    public byte Status { get; set; } // 0 = Succeeded, 1 = Failed
    public long TotalElementsReduced { get; set; }
    public long ElapsedMicroseconds { get; set; }

    public AllReduceCompletionMessage() { }

    public AllReduceCompletionMessage(
        Guid sessionId,
        int rank,
        byte status,
        long totalElementsReduced,
        long elapsedMicroseconds)
    {
        SessionId = sessionId;
        Rank = rank;
        Status = status;
        TotalElementsReduced = totalElementsReduced;
        ElapsedMicroseconds = elapsedMicroseconds;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount() => PayloadSize;

    public int Serialize(Span<byte> destination)
    {
        if (destination.Length < PayloadSize)
            throw new ArgumentException($"Destination span too small. Required: {PayloadSize}, Available: {destination.Length}.", nameof(destination));

        SessionId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), Rank);
        destination[20] = Status;
        destination[21] = 0;
        destination[22] = 0;
        destination[23] = 0;
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(24, 8), TotalElementsReduced);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(32, 8), ElapsedMicroseconds);

        return PayloadSize;
    }

    public static AllReduceCompletionMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < PayloadSize)
            throw new ArgumentException($"Source span too small for AllReduceCompletionMessage. Expected at least {PayloadSize}, got {source.Length}.", nameof(source));

        var sessionId = new Guid(source.Slice(0, 16));
        int rank = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        byte status = source[20];
        long totalElementsReduced = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(24, 8));
        long elapsedMicroseconds = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(32, 8));

        return new AllReduceCompletionMessage(sessionId, rank, status, totalElementsReduced, elapsedMicroseconds);
    }


    public bool Equals(AllReduceCompletionMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return SessionId == other.SessionId &&
               Rank == other.Rank &&
               Status == other.Status &&
               TotalElementsReduced == other.TotalElementsReduced &&
               ElapsedMicroseconds == other.ElapsedMicroseconds;
    }

    public override bool Equals(object? obj) => Equals(obj as AllReduceCompletionMessage);

    public override int GetHashCode() => HashCode.Combine(SessionId, Rank, Status, TotalElementsReduced, ElapsedMicroseconds);
}
