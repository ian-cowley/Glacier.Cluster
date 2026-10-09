namespace Glacier.Cluster.Protocol;

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

/// <summary>
/// Execution status telemetry dispatched by worker to coordinator upon task finish or failure.
/// </summary>
public sealed class TaskStatusUpdateMessage : IClusterMessage<TaskStatusUpdateMessage>, IEquatable<TaskStatusUpdateMessage>
{
    public ClusterMessageType MessageType => ClusterMessageType.TaskStatusUpdate;

    public Guid JobId { get; set; }
    public int StageId { get; set; }
    public int PartitionId { get; set; }
    public TaskExecutionStatus Status { get; set; }
    public double ElapsedMs { get; set; }
    public ulong OutputBytes { get; set; }
    public ulong OutputRowCount { get; set; }
    public string ErrorMessage { get; set; } = string.Empty;

    public TaskStatusUpdateMessage() { }

    public TaskStatusUpdateMessage(
        Guid jobId,
        int stageId,
        int partitionId,
        TaskExecutionStatus status,
        double elapsedMs,
        ulong outputBytes,
        ulong outputRowCount,
        string? errorMessage = null)
    {
        JobId = jobId;
        StageId = stageId;
        PartitionId = partitionId;
        Status = status;
        ElapsedMs = elapsedMs;
        OutputBytes = outputBytes;
        OutputRowCount = outputRowCount;
        ErrorMessage = errorMessage ?? string.Empty;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetByteCount()
    {
        return 49 + BinaryStringHelper.GetUtf8ByteCount(ErrorMessage);
    }

    public int Serialize(Span<byte> destination)
    {
        int required = GetByteCount();
        if (destination.Length < required)
            throw new ArgumentException("Destination span too small for TaskStatusUpdateMessage.", nameof(destination));

        JobId.TryWriteBytes(destination.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), StageId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), PartitionId);
        destination[24] = (byte)Status;
        BinaryPrimitives.WriteDoubleLittleEndian(destination.Slice(25, 8), ElapsedMs);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(33, 8), OutputBytes);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(41, 8), OutputRowCount);
        int strBytes = BinaryStringHelper.WriteUtf8String(destination.Slice(49), ErrorMessage);
        return 49 + strBytes;
    }

    public static TaskStatusUpdateMessage Deserialize(ReadOnlySpan<byte> source)
    {
        if (source.Length < 51)
            throw new ArgumentException("Source span too small for TaskStatusUpdateMessage.", nameof(source));

        Guid jobId = new Guid(source.Slice(0, 16));
        int stageId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        int partitionId = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4));
        TaskExecutionStatus status = (TaskExecutionStatus)source[24];
        double elapsedMs = BinaryPrimitives.ReadDoubleLittleEndian(source.Slice(25, 8));
        ulong outputBytes = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(33, 8));
        ulong rowCount = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(41, 8));
        string error = BinaryStringHelper.ReadUtf8String(source.Slice(49), out _);

        return new TaskStatusUpdateMessage(jobId, stageId, partitionId, status, elapsedMs, outputBytes, rowCount, error);
    }

    public bool Equals(TaskStatusUpdateMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return JobId.Equals(other.JobId) &&
               StageId == other.StageId &&
               PartitionId == other.PartitionId &&
               Status == other.Status &&
               ElapsedMs.Equals(other.ElapsedMs) &&
               OutputBytes == other.OutputBytes &&
               OutputRowCount == other.OutputRowCount &&
               ErrorMessage == other.ErrorMessage;
    }

    public override bool Equals(object? obj) => Equals(obj as TaskStatusUpdateMessage);

    public override int GetHashCode() =>
        HashCode.Combine(JobId, StageId, PartitionId, Status, ElapsedMs, OutputBytes, OutputRowCount);
}
