namespace Glacier.Cluster.Tests.ShuffleTests;

using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Polaris.IO;
using Glacier.Storage.Arrow;
using Xunit;

public sealed class ArrowShuffleExchangeTests : IDisposable
{
    private readonly Guid _nodeId = Guid.NewGuid();
    private readonly LocalPartitionStore _store;
    private readonly ArrowShuffleServer _server;
    private readonly ArrowShuffleClient _client;
    private readonly IDuplexPipe _clientPipe;
    private readonly IDuplexPipe _serverPipe;
    private readonly Task _serverLoopTask;
    private readonly CancellationTokenSource _cts = new();

    public ArrowShuffleExchangeTests()
    {
        _store = new LocalPartitionStore(_nodeId);
        _server = new ArrowShuffleServer(null, _store);
        _client = new ArrowShuffleClient();

        (_clientPipe, _serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        _serverLoopTask = Task.Run(() => _server.ProcessConnectionAsync(_serverPipe, _cts.Token));
    }

    public void Dispose()
    {
        _cts.Cancel();
        _serverPipe.Output.Complete();
        _serverPipe.Input.Complete();
        _clientPipe.Output.Complete();
        _clientPipe.Input.Complete();

        _client.Dispose();
        _server.Dispose();
        _store.Dispose();
        _cts.Dispose();
    }

    [Fact]
    public async Task EndToEnd_P2PShuffle_StreamingOverInProcessPipe()
    {
        var jobId = Guid.NewGuid();
        int stageId = 1;
        int partitionId = 0;

        // Create test partition
        int byteCount = 4096;
        var originalBuffer = RefCountedPartitionBuffer.Allocate(byteCount);
        for (int i = 0; i < byteCount; i++)
        {
            originalBuffer.Span[i] = (byte)(i % 256);
        }

        _store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), originalBuffer);
        originalBuffer.Release();

        // Client fetches partition from server over duplex pipe
        using var fetched = await _client.FetchPartitionAsync(_clientPipe, jobId, stageId, partitionId);

        Assert.NotNull(fetched);
        Assert.Equal(byteCount, fetched.ByteLength);
        Assert.True((ulong)fetched.Pointer % 64 == 0, "Fetched buffer must be 64-byte aligned.");

        for (int i = 0; i < byteCount; i++)
        {
            Assert.Equal((byte)(i % 256), fetched.Span[i]);
        }
    }

    [Fact]
    public async Task EndToEnd_ArrowRecordBatch_RoundTrip()
    {
        var jobId = Guid.NewGuid();
        int stageId = 2;
        int partitionId = 1;

        // 1. Build an ArrowRecordBatch with Int32 and Double columns
        var fields = new List<ArrowField>
        {
            new ArrowField("id", ArrowType.Int32, isNullable: false),
            new ArrowField("score", ArrowType.Double, isNullable: false)
        };
        var schema = new ArrowSchema(fields);

        int rowCount = 1000;
        byte[] idData = new byte[rowCount * sizeof(int)];
        byte[] scoreData = new byte[rowCount * sizeof(double)];

        var idSpan = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(idData.AsSpan());
        var scoreSpan = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, double>(scoreData.AsSpan());

        for (int i = 0; i < rowCount; i++)
        {
            idSpan[i] = i * 10;
            scoreSpan[i] = i * 1.5;
        }

        var col0 = new ArrowColumn(fields[0], rowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, idData);
        var col1 = new ArrowColumn(fields[1], rowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, scoreData);

        var originalBatch = new ArrowRecordBatch(schema, rowCount, new[] { col0, col1 });

        // 2. Serialize to unmanaged partition buffer and register
        var partitionBuffer = ArrowIpcStreamSerializer.SerializeToPartitionBuffer(originalBatch);
        _store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), partitionBuffer);
        partitionBuffer.Release();

        // 3. Client fetches and deserializes ArrowRecordBatch
        var fetchedBatch = await _client.FetchRecordBatchAsync(_clientPipe, jobId, stageId, partitionId);

        Assert.NotNull(fetchedBatch);
        Assert.Equal(rowCount, fetchedBatch.RowCount);
        Assert.Equal(2, fetchedBatch.ColumnCount);
        Assert.Equal("id", fetchedBatch.Column(0).Field.Name);
        Assert.Equal("score", fetchedBatch.Column(1).Field.Name);

        for (int i = 0; i < rowCount; i++)
        {
            Assert.Equal(i * 10, fetchedBatch.Column(0).GetInt32(i));
            Assert.Equal(i * 1.5, fetchedBatch.Column(1).GetDouble(i));
        }
    }

    [Fact]
    public async Task EndToEnd_DataFrame_RoundTrip()
    {
        var jobId = Guid.NewGuid();
        int stageId = 3;
        int partitionId = 0;

        // Build a Polaris DataFrame
        int rows = 500;
        int[] ids = new int[rows];
        double[] vals = new double[rows];
        for (int i = 0; i < rows; i++)
        {
            ids[i] = i + 1;
            vals[i] = (i + 1) * 2.5;
        }

        var idSeries = new Int32Series("id", ids);
        var valSeries = new Float64Series("val", vals);

        var originalDf = new DataFrame(new ISeries[] { idSeries, valSeries });
        var batch = originalDf.ToGlacierRecordBatch();

        var partitionBuffer = ArrowIpcStreamSerializer.SerializeToPartitionBuffer(batch);
        _store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), partitionBuffer);
        partitionBuffer.Release();

        // Client fetches as DataFrame
        var fetchedDf = await _client.FetchDataFrameAsync(_clientPipe, jobId, stageId, partitionId);

        Assert.NotNull(fetchedDf);
        Assert.Equal(rows, fetchedDf.RowCount);
        Assert.Equal(2, fetchedDf.Columns.Count);

        var colId = fetchedDf.Columns[0];
        var colVal = fetchedDf.Columns[1];

        for (int i = 0; i < rows; i++)
        {
            Assert.Equal(i + 1, colId.Get(i));
            Assert.Equal((i + 1) * 2.5, colVal.Get(i));
        }
    }

    [Fact]
    public async Task ChunkedTransfer_LargePartition_ReassemblesCorrectly()
    {
        var jobId = Guid.NewGuid();
        int stageId = 4;
        int partitionId = 0;

        // 200,000 bytes with 64KB max chunk forces multi-chunk transfer
        int size = 200_000;
        var buffer = RefCountedPartitionBuffer.Allocate(size);
        for (int i = 0; i < size; i++)
        {
            buffer.Span[i] = (byte)(i & 0xFF);
        }

        _store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), buffer);
        buffer.Release();

        using var fetched = await _client.FetchPartitionAsync(_clientPipe, jobId, stageId, partitionId);

        Assert.NotNull(fetched);
        Assert.Equal(size, fetched.ByteLength);

        for (int i = 0; i < size; i++)
        {
            Assert.Equal((byte)(i & 0xFF), fetched.Span[i]);
        }
    }

    [Fact]
    public async Task FetchNonExistentPartition_ThrowsPartitionNotFoundException()
    {
        var jobId = Guid.NewGuid();
        int stageId = 99;
        int partitionId = 99;

        await Assert.ThrowsAsync<PartitionNotFoundException>(async () =>
        {
            await _client.FetchPartitionAsync(_clientPipe, jobId, stageId, partitionId);
        });
    }

    [Fact]
    public async Task ZeroAllocation_HotPathChunkStreaming()
    {
        var jobId = Guid.NewGuid();
        int stageId = 5;
        int partitionId = 0;

        int size = 64 * 1024;
        var buffer = RefCountedPartitionBuffer.Allocate(size);
        buffer.Span.Fill(0x42);
        _store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), buffer);
        buffer.Release();

        // Warmup
        using (var warmup = await _client.FetchPartitionAsync(_clientPipe, jobId, stageId, partitionId))
        {
            Assert.Equal(size, warmup.ByteLength);
        }

        // Measure allocations on 5 consecutive fetches on a dedicated thread to isolate
        // against ThreadPool thread-hopping and parallel test execution interference
        long allocatedBytes = MeasureDedicatedThreadAllocations(() =>
        {
            for (int i = 0; i < 5; i++)
            {
                using var fetched = _client.FetchPartitionAsync(_clientPipe, jobId, stageId, partitionId).AsTask().GetAwaiter().GetResult();
                Assert.Equal(size, fetched.ByteLength);
            }
        });

        // Allocation per fetch should be strictly minimal (only Task/async machinery, zero payload byte copying)
        long allocPerFetch = allocatedBytes / 5;
        Assert.True(allocPerFetch < 64 * 1024, $"Expected minimal allocations per fetch, observed {allocPerFetch} bytes.");
    }

    private static long MeasureDedicatedThreadAllocations(Action action)
    {
        long allocated = 0;
        Exception? error = null;

        var thread = new Thread(() =>
        {
            try
            {
                long before = GC.GetAllocatedBytesForCurrentThread();
                action();
                long after = GC.GetAllocatedBytesForCurrentThread();
                allocated = after - before;
            }
            catch (Exception ex)
            {
                error = ex;
            }
        })
        {
            IsBackground = true
        };

        thread.Start();
        thread.Join();

        if (error != null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
        }

        return allocated;
    }
}
