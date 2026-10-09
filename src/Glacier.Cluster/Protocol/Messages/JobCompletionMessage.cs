namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Final distributed job completion telemetry returned by coordinator to client.
/// </summary>
public sealed class JobCompletionMessage : IClusterMessage<JobCompletionMessage>, IEquatable<JobCompletionMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.JobCompletion;

    public Guid JobId { get; set; }
    public JobCompletionStatus Status { get; set; }
    public double ElapsedMs { get; set; }
    public ulong TotalOutputRows { get; set; }
    public ulong TotalOutputBytes { get; set; }
    public string ErrorMessage { get; set; } = string.Empty;

    public JobCompletionMessage() { }

    public JobCompletionMessage(
        Guid jobId,
        JobCompletionStatus status,
        double elapsedMs,
        ulong totalOutputRows,
        ulong totalOutputBytes,
        string? errorMessage = null)
    {
        JobId = jobId;
        Status = status;
        ElapsedMs = elapsedMs;
        TotalOutputRows = totalOutputRows;
        TotalOutputBytes = totalOutputBytes;
        ErrorMessage = errorMessage ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 41 + BinaryStringHelper.GetUtf8ByteCount(ErrorMessage);
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for JobCompletionMessage.", nameof(destination));

        JobId.TryWriteBytes(destination.Slice(0, 16));
        destination[16] = (byte)Status;
        BinaryPrimitives.WriteDoubleLittleEndian(destination.Slice(17, 8), ElapsedMs);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(25, 8), TotalOutputRows);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(33, 8), TotalOutputBytes);
        int strBytes = BinaryStringHelper.WriteUtf8String(destination.Slice(41), ErrorMessage);
        return 41 + strBytes;
    }

    public static JobCompletionMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 43)
            throw new ArgumentException("Source span too small for JobCompletionMessage.", nameof(source));

        Guid jobId = new Guid(source.Slice(0, 16));
        JobCompletionStatus status = (JobCompletionStatus)source[16];
        double elapsedMs = BinaryPrimitives.ReadDoubleLittleEndian(source.Slice(17, 8));
        ulong rows = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(25, 8));
        ulong bytes = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(33, 8));
        string error = BinaryStringHelper.ReadUtf8String(source.Slice(41), out _);

        return new JobCompletionMessage(jobId, status, elapsedMs, rows, bytes, error);
    }

    public bool Equals(JobCompletionMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return JobId.Equals(other.JobId) &&
               Status == other.Status &&
               ElapsedMs.Equals(other.ElapsedMs) &&
               TotalOutputRows == other.TotalOutputRows &&
               TotalOutputBytes == other.TotalOutputBytes &&
               ErrorMessage == other.ErrorMessage;
    }

    public override bool Equals(object? obj) => Equals(obj as JobCompletionMessage);

    public override int GetHashCode() =>
        HashCode.Combine(JobId, Status, ElapsedMs, TotalOutputRows, TotalOutputBytes);
}
