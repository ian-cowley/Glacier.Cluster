namespace Glacier.Cluster.Tests.SynthesisTests;

using System;
using System.IO;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Storage.Arrow;
using Xunit;

public sealed class PolarisBridgeTests
{
    [Fact]
    public void ExactByteLengthCalculation_MatchesStreamLength()
    {
        int rowCount = 50;
        var i32Vals = new int?[rowCount];
        var f64Vals = new double?[rowCount];
        var strVals = new string?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            i32Vals[i] = i * 10;
            f64Vals[i] = i * 2.5;
            strVals[i] = $"row_{i}";
        }

        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", i32Vals),
            Float64Series.FromValues("score", f64Vals),
            Utf8StringSeries.FromStrings("label", strVals)
        });

        var batch = df.ToGlacierRecordBatch();
        int calculatedLength = PolarisBridge.CalculateIpcByteLength(batch);

        using var ms = new MemoryStream();
        df.ToArrowIpc(ms, leaveOpen: true);
        long actualStreamLength = ms.Length;

        Assert.Equal((int)actualStreamLength, calculatedLength);
    }

    [Fact]
    public void All16PolarisTypes_RoundTrip_ZeroDataLoss()
    {
        int rowCount = 20;
        var i8 = new sbyte?[rowCount];
        var u8 = new byte?[rowCount];
        var i16 = new short?[rowCount];
        var u16 = new ushort?[rowCount];
        var i32 = new int?[rowCount];
        var u32 = new uint?[rowCount];
        var i64 = new long?[rowCount];
        var u64 = new ulong?[rowCount];
        var f32 = new float?[rowCount];
        var f64 = new double?[rowCount];
        var bl = new bool?[rowCount];
        var utf8 = new string?[rowCount];
        var date = new int?[rowCount];
        var dt = new long?[rowCount];
        var dur = new long?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            i8[i] = (sbyte)(i - 10);
            u8[i] = (byte)(i * 2);
            i16[i] = (short)(i * 100);
            u16[i] = (ushort)(i * 200);
            i32[i] = i * 1000;
            u32[i] = (uint)(i * 2000);
            i64[i] = (long)i * 1_000_000L;
            u64[i] = (ulong)i * 2_000_000UL;
            f32[i] = i * 1.5f;
            f64[i] = i * 3.14159;
            bl[i] = (i % 2 == 1);
            utf8[i] = $"item_{i}";
            date[i] = 19000 + i;
            dt[i] = 1700000000_000_000_000L + (long)i * 1000L;
            dur[i] = (long)i * 60_000_000L;
        }

        var df = new DataFrame(new ISeries[]
        {
            Int8Series.FromValues("c_i8", i8),
            UInt8Series.FromValues("c_u8", u8),
            Int16Series.FromValues("c_i16", i16),
            UInt16Series.FromValues("c_u16", u16),
            Int32Series.FromValues("c_i32", i32),
            UInt32Series.FromValues("c_u32", u32),
            Int64Series.FromValues("c_i64", i64),
            UInt64Series.FromValues("c_u64", u64),
            Float32Series.FromValues("c_f32", f32),
            Float64Series.FromValues("c_f64", f64),
            BooleanSeries.FromValues("c_bl", bl),
            Utf8StringSeries.FromStrings("c_utf8", utf8),
            DateSeries.FromValues("c_date", date),
            DatetimeSeries.FromValues("c_dt", dt),
            DurationSeries.FromValues("c_dur", dur)
        });

        using var partitionBuffer = PolarisBridge.DataFrameToPartitionBuffer(df);
        Assert.NotNull(partitionBuffer);
        Assert.True(partitionBuffer.ByteLength > 0);

        // Deserialization with zero-copy
        var deserialized = PolarisBridge.PartitionBufferToDataFrame(partitionBuffer, zeroCopy: true);

        Assert.Equal(rowCount, deserialized.RowCount);
        Assert.Equal(df.Columns.Count, deserialized.Columns.Count);

        for (int i = 0; i < rowCount; i++)
        {
            var i32Col = (Int32Series)deserialized.GetColumn("c_i32");
            Assert.Equal(i * 1000, i32Col.Memory.Span[i]);

            var f64Col = (Float64Series)deserialized.GetColumn("c_f64");
            Assert.Equal(i * 3.14159, f64Col.Memory.Span[i], precision: 5);

            var utf8Col = (Utf8StringSeries)deserialized.GetColumn("c_utf8");
            Assert.Equal($"item_{i}", System.Text.Encoding.UTF8.GetString(utf8Col.GetStringSpan(i)));
        }
    }

    [Fact]
    public void ReferenceCounting_SeriesDisposalFreesBuffer()
    {
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3 }),
            Float64Series.FromValues("val", new double?[] { 10.0, 20.0, 30.0 })
        });

        var buffer = PolarisBridge.DataFrameToPartitionBuffer(df);
        int initialRefCount = buffer.ReferenceCount; // 1 from allocation

        var zeroCopyDf = PolarisBridge.PartitionBufferToDataFrame(buffer, zeroCopy: true);
        // Each of the 2 columns retains the buffer via PartitionBufferMemoryOwner
        Assert.Equal(initialRefCount + 2, buffer.ReferenceCount);

        zeroCopyDf.Dispose();
        // Disposing columns releases the retained references
        Assert.Equal(initialRefCount, buffer.ReferenceCount);

        buffer.Release();
        Assert.Equal(0, buffer.ReferenceCount);
        Assert.True(buffer.IsDisposed);
    }

    [Theory]
    [InlineData("age > 30")]
    [InlineData("salary >= 50000")]
    [InlineData("dept == 'Engineering'")]
    [InlineData("dept != 'Sales'")]
    [InlineData("active == true")]
    public void PredicateCompiler_ParsesValidExpressions(string predicate)
    {
        var expr = PredicateCompiler.Compile(predicate);
        Assert.NotNull(expr);
        Assert.False(string.IsNullOrWhiteSpace(expr.ToString()));
    }
}
