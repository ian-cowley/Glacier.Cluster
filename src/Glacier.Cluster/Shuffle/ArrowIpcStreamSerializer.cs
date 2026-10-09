namespace Glacier.Cluster.Shuffle;

using System;
using System.IO;
using Glacier.Storage.Arrow;

/// <summary>
/// High-performance pure C# serializer converting <see cref="ArrowRecordBatch"/> instances into
/// Arrow IPC stream bytes backed by 64-byte cache-line aligned unmanaged memory buffers.
/// Strictly zero third-party dependencies (no Apache.Arrow).
/// </summary>
public static class ArrowIpcStreamSerializer
{
    /// <summary>
    /// Serializes an ArrowRecordBatch directly into a new 64-byte aligned <see cref="RefCountedPartitionBuffer"/>.
    /// </summary>
    public static RefCountedPartitionBuffer SerializeToPartitionBuffer(ArrowRecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, leaveOpen: true))
        {
            writer.WriteSchema(batch.Schema);
            writer.WriteRecordBatch(batch);
            writer.WriteEndOfStream();
        }

        int length = (int)ms.Length;
        var buffer = RefCountedPartitionBuffer.Allocate(length);
        ms.Position = 0;
        int read = ms.Read(buffer.Span);
        if (read != length)
        {
            buffer.Release();
            throw new InvalidOperationException($"Failed to copy complete serialized stream. Expected {length} bytes, read {read}.");
        }

        return buffer;
    }

    /// <summary>
    /// Serializes an ArrowRecordBatch into the target stream.
    /// </summary>
    public static void SerializeToStream(ArrowRecordBatch batch, Stream stream, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(stream);

        using var writer = new ArrowStreamWriter(stream, leaveOpen);
        writer.WriteSchema(batch.Schema);
        writer.WriteRecordBatch(batch);
        writer.WriteEndOfStream();
    }
}
