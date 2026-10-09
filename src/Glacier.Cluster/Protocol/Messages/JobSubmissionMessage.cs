namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Distributed query / computation DAG job submission sent by client to coordinator.
/// </summary>
public sealed class JobSubmissionMessage : IClusterMessage<JobSubmissionMessage>, IEquatable<JobSubmissionMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.JobSubmission;

    public Guid JobId { get; set; }
    public string JobName { get; set; } = string.Empty;
    public int TotalStages { get; set; }
    public uint TargetPartitionCount { get; set; }
    public ulong TimeoutMs { get; set; }
    public ReadOnlyMemory<byte> DagPayload { get; set; }

    public JobSubmissionMessage() { }

    public JobSubmissionMessage(
        Guid jobId,
        string? jobName,
        int totalStages,
        uint targetPartitionCount,
        ulong timeoutMs,
        ReadOnlyMemory<byte> dagPayload)
    {
        JobId = jobId;
        JobName = jobName ?? string.Empty;
        TotalStages = totalStages;
        TargetPartitionCount = targetPartitionCount;
        TimeoutMs = timeoutMs;
        DagPayload = dagPayload;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 16 + BinaryStringHelper.GetUtf8ByteCount(JobName) + 4 + 4 + 8 + 4 + DagPayload.Length;
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for JobSubmissionMessage.", nameof(destination));

        JobId.TryWriteBytes(destination.Slice(0, 16));
        int offset = 16;

        offset += BinaryStringHelper.WriteUtf8String(destination.Slice(offset), JobName);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset, 4), TotalStages);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset + 4, 4), TargetPartitionCount);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(offset + 8, 8), TimeoutMs);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(offset + 16, 4), DagPayload.Length);
        offset += 20;

        if (DagPayload.Length > 0)
        {
            DagPayload.Span.CopyTo(destination.Slice(offset, DagPayload.Length));
            offset += DagPayload.Length;
        }

        return offset;
    }

    public static JobSubmissionMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 38)
            throw new ArgumentException("Source span too small for JobSubmissionMessage.", nameof(source));

        Guid jobId = new Guid(source.Slice(0, 16));
        int offset = 16;

        string jobName = BinaryStringHelper.ReadUtf8String(source.Slice(offset), out int nameBytes);
        offset += nameBytes;

        int totalStages = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(offset, 4));
        uint partitions = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(offset + 4, 4));
        ulong timeoutMs = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(offset + 8, 8));
        int dagLen = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(offset + 16, 4));
        offset += 20;

        if (dagLen < 0 || source.Length < offset + dagLen)
            throw new ArgumentException("Invalid DAG payload length in JobSubmissionMessage.", nameof(source));

        byte[] dag = dagLen > 0 ? source.Slice(offset, dagLen).ToArray() : Array.Empty<byte>();

        return new JobSubmissionMessage(jobId, jobName, totalStages, partitions, timeoutMs, dag);
    }

    public bool Equals(JobSubmissionMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return JobId.Equals(other.JobId) &&
               JobName == other.JobName &&
               TotalStages == other.TotalStages &&
               TargetPartitionCount == other.TargetPartitionCount &&
               TimeoutMs == other.TimeoutMs &&
               DagPayload.Span.SequenceEqual(other.DagPayload.Span);
    }

    public override bool Equals(object? obj) => Equals(obj as JobSubmissionMessage);

    public override int GetHashCode() =>
        HashCode.Combine(JobId, JobName, TotalStages, TargetPartitionCount, TimeoutMs);
}
