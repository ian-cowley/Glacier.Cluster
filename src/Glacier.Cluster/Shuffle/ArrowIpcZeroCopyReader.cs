namespace Glacier.Cluster.Shuffle;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Glacier.Storage.Arrow;

/// <summary>
/// Zero-allocation, pure C# reader deserializing <see cref="ArrowRecordBatch"/> instances directly
/// from unmanaged memory slices. Avoids allocating managed byte[] arrays for column buffers.
/// </summary>
public static class ArrowIpcZeroCopyReader
{
    /// <summary>
    /// Deserializes an <see cref="ArrowRecordBatch"/> directly from a <see cref="RefCountedPartitionBuffer"/>.
    /// </summary>
    public static ArrowRecordBatch ReadRecordBatch(RefCountedPartitionBuffer buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return ReadRecordBatch(buffer.Memory);
    }

    /// <summary>
    /// Deserializes an <see cref="ArrowRecordBatch"/> directly from a memory slice.
    /// Column buffers maintain zero-copy views into this memory.
    /// </summary>
    public static ArrowRecordBatch ReadRecordBatch(ReadOnlyMemory<byte> memory)
    {
        ReadOnlySpan<byte> span = memory.Span;
        int offset = 0;

        // 1. Read Schema Message
        if (!ArrowIpcParser.TryReadPrefix(span.Slice(offset), out int schemaMetaLen))
        {
            throw new InvalidDataException("Invalid Arrow IPC Schema prefix.");
        }
        offset += 8;

        int schemaMetaAligned = (schemaMetaLen + 7) & ~7;
        if (span.Length < offset + 16)
        {
            throw new InvalidDataException("Unexpected end of data in Arrow IPC Schema metadata.");
        }

        int schemaPayloadLen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset + 12, 4));
        var schema = ParseSchema(span.Slice(offset + 16, schemaPayloadLen));
        offset += schemaMetaAligned;

        // 2. Read RecordBatch Message
        if (!ArrowIpcParser.TryReadPrefix(span.Slice(offset), out int rbMetaLen))
        {
            throw new InvalidDataException("Invalid Arrow IPC RecordBatch prefix.");
        }
        offset += 8;

        int rbMetaAligned = (rbMetaLen + 7) & ~7;
        if (span.Length < offset + 16)
        {
            throw new InvalidDataException("Unexpected end of data in Arrow IPC RecordBatch metadata.");
        }

        long bodyLen = BinaryPrimitives.ReadInt64LittleEndian(span.Slice(offset + 4, 8));
        int rbMetaPayloadLen = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset + 12, 4));
        ReadOnlySpan<byte> rbMetaPayload = span.Slice(offset + 16, rbMetaPayloadLen);
        offset += rbMetaAligned;

        if (rbMetaPayload.Length < 8)
        {
            throw new InvalidDataException("Invalid RecordBatch metadata payload size.");
        }

        int rowCount = BinaryPrimitives.ReadInt32LittleEndian(rbMetaPayload[..4]);
        int colCount = BinaryPrimitives.ReadInt32LittleEndian(rbMetaPayload.Slice(4, 4));

        ReadOnlyMemory<byte> bodyMemory = memory.Slice(offset, (int)bodyLen);

        var columns = new List<ArrowColumn>(colCount);
        int descOffset = 8;

        for (int i = 0; i < colCount; i++)
        {
            if (rbMetaPayload.Length < descOffset + ArrowColumnDescriptor.Size)
            {
                throw new InvalidDataException("Truncated column descriptor in RecordBatch metadata.");
            }

            var desc = ArrowColumnDescriptor.ReadFrom(rbMetaPayload.Slice(descOffset, ArrowColumnDescriptor.Size));
            descOffset += ArrowColumnDescriptor.Size;

            var field = schema.GetField(desc.FieldIndex);

            // Zero-copy: Memory slices point directly to the underlying unmanaged buffer
            ReadOnlyMemory<byte> nullBitmap = desc.NullBitmapLength > 0
                ? bodyMemory.Slice(desc.NullBitmapOffset, desc.NullBitmapLength)
                : ReadOnlyMemory<byte>.Empty;

            ReadOnlyMemory<byte> offsets = desc.OffsetsLength > 0
                ? bodyMemory.Slice(desc.OffsetsOffset, desc.OffsetsLength)
                : ReadOnlyMemory<byte>.Empty;

            ReadOnlyMemory<byte> data = desc.DataLength > 0
                ? bodyMemory.Slice(desc.DataOffset, desc.DataLength)
                : ReadOnlyMemory<byte>.Empty;

            columns.Add(new ArrowColumn(field, desc.Length, desc.NullCount, nullBitmap, offsets, data));
        }

        return new ArrowRecordBatch(schema, rowCount, columns);
    }

    private static ArrowSchema ParseSchema(ReadOnlySpan<byte> payload)
    {
        var fields = new List<ArrowField>();
        if (payload.Length < 2)
        {
            fields.Add(new ArrowField("column0", ArrowType.Int32));
            return new ArrowSchema(fields);
        }

        ushort count = BinaryPrimitives.ReadUInt16LittleEndian(payload[..2]);
        int offset = 2;

        for (int i = 0; i < count && offset < payload.Length; i++)
        {
            byte nameLen = payload[offset++];
            string name = Encoding.UTF8.GetString(payload.Slice(offset, nameLen));
            offset += nameLen;
            var typeId = (ArrowTypeId)payload[offset++];
            bool isNullable = payload[offset++] != 0;

            fields.Add(new ArrowField(name, new ArrowType(typeId), isNullable));
        }

        return new ArrowSchema(fields);
    }
}
