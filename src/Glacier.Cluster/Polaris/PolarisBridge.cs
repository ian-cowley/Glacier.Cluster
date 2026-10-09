namespace Glacier.Cluster.Polaris;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Polaris.IO;
using Glacier.Polaris.Memory;
using Glacier.Storage.Arrow;

/// <summary>
/// High-performance zero-copy bridge between <see cref="DataFrame"/> and Arrow RecordBatches /
/// <see cref="RefCountedPartitionBuffer"/> for distributed cluster execution.
/// Strictly 100% pure C# first-party dependencies (strictly ZERO Apache.Arrow references).
/// </summary>
public static unsafe class PolarisBridge
{
    /// <summary>
    /// Serializes a <see cref="DataFrame"/> directly into a 64-byte aligned <see cref="RefCountedPartitionBuffer"/>
    /// using pure C# Arrow IPC streaming with zero intermediate managed heap copies.
    /// </summary>
    public static RefCountedPartitionBuffer DataFrameToPartitionBuffer(DataFrame df)
    {
        ArgumentNullException.ThrowIfNull(df);

        // 1. Obtain pure first-party ArrowRecordBatch with zero-copy column views
        var batch = df.ToGlacierRecordBatch();

        // 2. Pre-calculate exact byte size to allocate unmanaged memory in a single pass
        int exactLength = CalculateIpcByteLength(batch);
        var partitionBuffer = RefCountedPartitionBuffer.Allocate(exactLength);

        try
        {
            // 3. Write Arrow IPC stream directly into unmanaged memory span
            using (var unmanagedStream = new UnmanagedMemoryStream(
                (byte*)partitionBuffer.UnmanagedPointer,
                exactLength,
                exactLength,
                FileAccess.Write))
            using (var writer = new ArrowStreamWriter(unmanagedStream, leaveOpen: true))
            {
                writer.WriteSchema(batch.Schema);
                writer.WriteRecordBatch(batch);
                writer.WriteEndOfStream();
            }

            return partitionBuffer;
        }
        catch
        {
            partitionBuffer.Release();
            throw;
        }
    }

    /// <summary>
    /// Deserializes a <see cref="DataFrame"/> directly from a <see cref="RefCountedPartitionBuffer"/>.
    /// In zero-copy mode, columns directly wrap the unmanaged partition memory via atomic reference counting.
    /// </summary>
    public static DataFrame PartitionBufferToDataFrame(RefCountedPartitionBuffer buffer, bool zeroCopy = true)
    {
        ArgumentNullException.ThrowIfNull(buffer);

        // 1. Parse ArrowRecordBatch using zero-allocation memory slices
        var batch = ArrowIpcZeroCopyReader.ReadRecordBatch(buffer);

        if (!zeroCopy)
        {
            // Fallback: Copy column data into independent NativeMemoryOwners
            return GlacierStorageBridge.FromGlacierRecordBatch(batch);
        }

        // 2. True zero-copy: Construct DataFrame columns backed by partition buffer references
        int colCount = batch.Columns.Count;
        var columns = new List<ISeries>(colCount);

        for (int i = 0; i < colCount; i++)
        {
            var arrowCol = batch.Columns[i];
            columns.Add(CreateZeroCopySeries(buffer, arrowCol));
        }

        return new DataFrame(columns);
    }

    /// <summary>
    /// Calculates the exact byte size required for an Arrow IPC stream containing the specified record batch.
    /// </summary>
    public static int CalculateIpcByteLength(ArrowRecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        var schema = batch.Schema;
        int colCount = batch.Columns.Count;

        // --- Schema Message ---
        int schemaPayloadLen = 2; // FieldCount (u16)
        for (int i = 0; i < schema.FieldCount; i++)
        {
            var field = schema.GetField(i);
            int nameLen = System.Text.Encoding.UTF8.GetByteCount(field.Name);
            schemaPayloadLen += 1 + nameLen + 1 + 1; // [len(1B)][name][type(1B)][nullable(1B)]
        }
        int schemaMetaLen = 16 + schemaPayloadLen;
        int schemaMetaAligned = (schemaMetaLen + 7) & ~7;
        int schemaTotal = 8 + schemaMetaAligned;

        // --- RecordBatch Message ---
        int rbMetaPayloadLen = 8 + (colCount * ArrowColumnDescriptor.Size);
        int rbMetaLen = 16 + rbMetaPayloadLen;
        int rbMetaAligned = (rbMetaLen + 7) & ~7;

        long bodyLen = 0;
        for (int i = 0; i < colCount; i++)
        {
            var col = batch.Columns[i];
            if (col.Field.IsNullable && !col.NullBitmap.IsEmpty)
            {
                bodyLen += (col.NullBitmap.Length + 7) & ~7;
            }
            if (!col.OffsetsBuffer.IsEmpty)
            {
                bodyLen += (col.OffsetsBuffer.Length + 7) & ~7;
            }
            if (!col.DataBuffer.IsEmpty)
            {
                bodyLen += (col.DataBuffer.Length + 7) & ~7;
            }
        }
        int rbTotal = 8 + rbMetaAligned + (int)bodyLen;

        // --- EOS Marker ---
        int eosTotal = 8;

        return schemaTotal + rbTotal + eosTotal;
    }

    /// <summary>
    /// Creates a typed <see cref="ISeries"/> that maintains zero-copy views into the underlying
    /// <see cref="RefCountedPartitionBuffer"/> with thread-safe atomic reference retention.
    /// </summary>
    private static ISeries CreateZeroCopySeries(RefCountedPartitionBuffer buffer, ArrowColumn col)
    {
        int length = col.Length;
        var typeId = col.Field.DataType.Id;
        string name = col.Field.Name;

        // Build ValidityMask from NullBitmap slice
        ValidityMask mask;
        if (col.NullCount > 0 && !col.NullBitmap.IsEmpty)
        {
            mask = ValidityMask.FromNullBitmap(length, col.NullBitmap.Span);
        }
        else
        {
            mask = new ValidityMask(length, setAllValid: true);
        }

        switch (typeId)
        {
            case ArrowTypeId.Int8:
                return new Int8Series(name, length, new PartitionBufferMemoryOwner<sbyte>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.UInt8:
                return new UInt8Series(name, length, new PartitionBufferMemoryOwner<byte>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Int16:
                return new Int16Series(name, length, new PartitionBufferMemoryOwner<short>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.UInt16:
                return new UInt16Series(name, length, new PartitionBufferMemoryOwner<ushort>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Int32:
                return new Int32Series(name, length, new PartitionBufferMemoryOwner<int>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.UInt32:
                return new UInt32Series(name, length, new PartitionBufferMemoryOwner<uint>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Int64:
            case ArrowTypeId.Date64:
                return new Int64Series(name, length, new PartitionBufferMemoryOwner<long>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.UInt64:
                return new UInt64Series(name, length, new PartitionBufferMemoryOwner<ulong>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Float:
                return new Float32Series(name, length, new PartitionBufferMemoryOwner<float>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Double:
                return new Float64Series(name, length, new PartitionBufferMemoryOwner<double>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Date32:
                return new DateSeries(name, length, new PartitionBufferMemoryOwner<int>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Timestamp:
                return new DatetimeSeries(name, length, new PartitionBufferMemoryOwner<long>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Duration:
                return new DurationSeries(name, length, new PartitionBufferMemoryOwner<long>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Time64:
                return new TimeSeries(name, length, new PartitionBufferMemoryOwner<long>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Decimal128:
                return new DecimalSeries(name, length, new PartitionBufferMemoryOwner<decimal>(buffer, col.DataBuffer), mask);

            case ArrowTypeId.Boolean:
            {
                // Booleans in Arrow are bit-packed; unpack into NativeMemoryOwner<bool> byte-per-bool
                var owner = new NativeMemoryOwner<bool>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    var dataSpan = col.DataBuffer.Span;
                    var boolSpan = owner.Span;
                    for (int i = 0; i < length; i++)
                    {
                        boolSpan[i] = (dataSpan[i >> 3] & (1 << (i & 7))) != 0;
                    }
                }
                return new BooleanSeries(name, length, owner, mask);
            }

            case ArrowTypeId.Utf8:
            {
                var offsetsOwner = new PartitionBufferMemoryOwner<int>(buffer, col.OffsetsBuffer);
                var dataOwner = new PartitionBufferMemoryOwner<byte>(buffer, col.DataBuffer);
                return new Utf8StringSeries(name, length, offsetsOwner, dataOwner, mask);
            }

            case ArrowTypeId.Binary:
            {
                var offsetsOwner = new PartitionBufferMemoryOwner<int>(buffer, col.OffsetsBuffer);
                var dataOwner = new PartitionBufferMemoryOwner<byte>(buffer, col.DataBuffer);
                return new BinarySeries(name, length, offsetsOwner, dataOwner, mask);
            }

            case ArrowTypeId.Null:
                return new NullSeries(name, length);

            default:
                throw new NotSupportedException($"Unsupported Arrow column type: {typeId}");
        }
    }
}
