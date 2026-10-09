namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// Transfers a single vector chunk during Ring AllReduce scatter-reduce or allgather phases.
/// </summary>
public sealed class AllReduceStepMessage : IClusterMessage<AllReduceStepMessage>, IEquatable<AllReduceStepMessage>
{
    public const int HeaderSize = 32;

    public ClusterMessageType MessageType => ClusterMessageType.RingAllReduceStep;

    public Guid SessionId { get; set; }
    public int Step { get; set; }
    public byte Phase { get; set; } // 0 = Scatter-Reduce, 1 = AllGather
    public int ChunkIndex { get; set; }
    public int ElementCount => Data.Length;
    public float[] Data { get; set; } = Array.Empty<float>();

    public AllReduceStepMessage() { }

    public AllReduceStepMessage(
        Guid sessionId,
        int step,
        byte phase,
        int chunkIndex,
        float[] data)
    {
        SessionId = sessionId;
        Step = step;
        Phase = phase;
        ChunkIndex = chunkIndex;
        Data = data ?? Array.Empty<float>();
    }

    public AllReduceStepMessage(
        Guid sessionId,
        int step,
        byte phase,
        int chunkIndex,
        ReadOnlySpan<float> dataSpan)
    {
        SessionId = sessionId;
        Step = step;
        Phase = phase;
        ChunkIndex = chunkIndex;
        Data = dataSpan.ToArray();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount() => HeaderSize + (Data.Length * sizeof(float));

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException($"Destination span too small. Required: {required}, Available: {destination.Length}.", nameof(destination));

        SessionId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), Step);
        destination[20] = Phase;
        destination[21] = 0;
        destination[22] = 0;
        destination[23] = 0;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(24, 4), ChunkIndex);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(28, 4), Data.Length);

        if (Data.Length > 0)
        {
            ReadOnlySpan<byte> byteSpan = MemoryMarshal.AsBytes(Data.AsSpan());
            byteSpan.CopyTo(destination.Slice(HeaderSize, byteSpan.Length));
        }

        return required;
    }

    public static AllReduceStepMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < HeaderSize)
            throw new ArgumentException($"Source span too small for AllReduceStepMessage header. Expected at least {HeaderSize}, got {source.Length}.", nameof(source));

        var sessionId = new Guid(source.Slice(0, 16));
        int step = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        byte phase = source[20];
        int chunkIndex = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(24, 4));
        int elementCount = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(28, 4));

        int dataBytes = elementCount * sizeof(float);
        if (source.Length < HeaderSize + dataBytes)
            throw new ArgumentException($"Source span too small for float payload. Expected {HeaderSize + dataBytes}, got {source.Length}.", nameof(source));

        float[] floats = new float[elementCount];
        if (elementCount > 0)
        {
            Span<byte> destBytes = MemoryMarshal.AsBytes(floats.AsSpan());
            source.Slice(HeaderSize, dataBytes).CopyTo(destBytes);
        }

        return new AllReduceStepMessage(sessionId, step, phase, chunkIndex, floats);
    }


    public bool Equals(AllReduceStepMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        if (SessionId != other.SessionId ||
            Step != other.Step ||
            Phase != other.Phase ||
            ChunkIndex != other.ChunkIndex ||
            Data.Length != other.Data.Length)
        {
            return false;
        }

        return Data.AsSpan().SequenceEqual(other.Data.AsSpan());
    }

    public override bool Equals(object? obj) => Equals(obj as AllReduceStepMessage);

    public override int GetHashCode() => HashCode.Combine(SessionId, Step, Phase, ChunkIndex, Data.Length);
}
