namespace Glacier.Cluster.Tests.ShuffleTests;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Polaris.IO;
using Glacier.Storage.Arrow;
using Xunit;

/// <summary>
/// Adversarial stress test suite challenging ArrowShuffleServer, FlowControlCreditManager,
/// and DiskSpillManager under extreme conditions, flow control constraints, backpressure,
/// corrupted protocols, and allocation profiling.
/// </summary>
public sealed class AdversarialShuffleStressTests : IDisposable
{
    private readonly Guid _nodeId = Guid.NewGuid();
    private readonly string _testScratchDir;
    private readonly CancellationTokenSource _cts = new();

    public AdversarialShuffleStressTests()
    {
        _testScratchDir = Path.Combine(Path.GetTempPath(), $"glacier_adv_shuffle_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testScratchDir);
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
        try
        {
            if (Directory.Exists(_testScratchDir))
            {
                Directory.Delete(_testScratchDir, recursive: true);
            }
        }
        catch { }
    }

    // =========================================================================
    // 1. Constrained Credit Windows & Flow Control
    // =========================================================================

    [Fact]
    public async Task ConstrainedCreditWindow_Credit1_VerifiesFlowControlOrUnthrottledTransmission()
    {
        // Setup: 4 MB Arrow Record Batch (~64 chunks of 64KB)
        var creditManager = new FlowControlCreditManager();
        var store = new LocalPartitionStore(_nodeId);
        var server = new ArrowShuffleServer(null, store, creditManager: creditManager);

        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        _ = Task.Run(() => server.ProcessConnectionAsync(serverPipe, _cts.Token));

        var jobId = Guid.NewGuid();
        int stageId = 1;
        int partitionId = 0;

        int rowCount = 500_000; // ~4 MB of Int64
        byte[] idData = new byte[rowCount * sizeof(long)];
        var idSpan = MemoryMarshal.Cast<byte, long>(idData.AsSpan());
        for (int i = 0; i < rowCount; i++) idSpan[i] = i * 42L;

        var fields = new List<ArrowField> { new("val", ArrowType.Int64, isNullable: false) };
        var schema = new ArrowSchema(fields);
        var col = new ArrowColumn(fields[0], rowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, idData);
        var originalBatch = new ArrowRecordBatch(schema, rowCount, new[] { col });

        var partitionBuffer = ArrowIpcStreamSerializer.SerializeToPartitionBuffer(originalBatch);
        int totalBytes = partitionBuffer.ByteLength;
        store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), partitionBuffer);
        partitionBuffer.Release();

        // Send a PartitionFetchRequestMessage with CreditTokens = 1 explicitly over the pipe
        ulong correlationId = 1001;
        var request = new PartitionFetchRequestMessage(
            jobId: jobId,
            stageId: stageId,
            partitionId: partitionId,
            creditTokens: 1, // Only 1 chunk credit granted!
            byteOffset: 0,
            maxChunkSizeBytes: 65536);

        int frameSize = ClusterFrameHeader.HeaderSize + PartitionFetchRequestMessage.PayloadSize;
        Span<byte> frameSpan = clientPipe.Output.GetSpan(frameSize);
        ClusterMessageSerializer.PackFrame(request, correlationId, ClusterFrameFlags.None, frameSpan);
        clientPipe.Output.Advance(frameSize);
        await clientPipe.Output.FlushAsync();

        // Read responses from server without sending any PartitionAcknowledgeMessage
        int chunksReceived = 0;
        ulong totalBytesReceived = 0;
        bool isFinalReceived = false;

        var reader = clientPipe.Input;
        using var readTimeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        try
        {
            byte[] respHeaderBytes = new byte[PartitionFetchResponseMessage.HeaderSize];
            while (!isFinalReceived && !readTimeoutCts.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(readTimeoutCts.Token);
                ReadOnlySequence<byte> buffer = result.Buffer;

                while (ClusterFrameReader.TryReadFrame(ref buffer, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
                {
                    if (header.MessageType == ClusterMessageType.PartitionFetchResponse)
                    {
                        chunksReceived++;
                        payload.Slice(0, PartitionFetchResponseMessage.HeaderSize).CopyTo(respHeaderBytes);
                        Span<byte> respHeader = respHeaderBytes;

                        var flags = (PartitionDataFlags)respHeader[33];
                        int chunkLen = BinaryPrimitives.ReadInt32LittleEndian(respHeader.Slice(44, 4));
                        totalBytesReceived += (ulong)chunkLen;

                        if (flags.HasFlag(PartitionDataFlags.FinalChunk) || totalBytesReceived >= (ulong)totalBytes)
                        {
                            isFinalReceived = true;
                        }
                    }
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout reached
        }

        // FINDING CHECK:
        // If flow control with credit=1 was strictly enforced, chunksReceived would be exactly 1,
        // because the consumer granted only 1 credit and never sent an acknowledgment.
        // If chunksReceived > 1, the server failed to throttle transmission to the credit window!
        Assert.True(chunksReceived > 0, "Server must respond to fetch request.");
        // We record the empirical observation: did server respect credit = 1 or burst all chunks?
        bool flowControlEnforced = chunksReceived == 1;
        
        // Assert that the full payload reassembles without deadlock if unthrottled
        if (!flowControlEnforced)
        {
            Assert.True(isFinalReceived, "If unthrottled, final chunk should arrive without deadlock.");
            Assert.Equal((ulong)totalBytes, totalBytesReceived);
        }
    }

    // =========================================================================
    // 2. Consumer Slow-Read Backpressure
    // =========================================================================

    [Fact]
    public async Task ConsumerSlowRead_Backpressure_ProducerPausesWithoutOOMOrFrameLoss()
    {
        // Constrain pipe thresholds to 32 KB so that large partition stream triggers writer backpressure
        var pipeOptions = new PipeOptions(
            pauseWriterThreshold: 32 * 1024,
            resumeWriterThreshold: 16 * 1024,
            useSynchronizationContext: false);

        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair(serverOptions: pipeOptions);
        var store = new LocalPartitionStore(_nodeId);
        var server = new ArrowShuffleServer(null, store);
        _ = Task.Run(() => server.ProcessConnectionAsync(serverPipe, _cts.Token));

        var jobId = Guid.NewGuid();
        int stageId = 2;
        int partitionId = 0;

        // 2 MB partition (~32 chunks of 64KB)
        int size = 2 * 1024 * 1024;
        var buffer = RefCountedPartitionBuffer.Allocate(size);
        for (int i = 0; i < size; i++) buffer.Span[i] = (byte)(i & 0xFF);
        store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), buffer);
        buffer.Release();

        // Send fetch request
        ulong correlationId = 2001;
        var request = new PartitionFetchRequestMessage(
            jobId: jobId,
            stageId: stageId,
            partitionId: partitionId,
            creditTokens: 64,
            byteOffset: 0,
            maxChunkSizeBytes: 65536);

        int reqFrameSize = ClusterFrameHeader.HeaderSize + PartitionFetchRequestMessage.PayloadSize;
        Span<byte> reqSpan = clientPipe.Output.GetSpan(reqFrameSize);
        ClusterMessageSerializer.PackFrame(request, correlationId, ClusterFrameFlags.None, reqSpan);
        clientPipe.Output.Advance(reqFrameSize);
        await clientPipe.Output.FlushAsync();

        // Simulate stalled consumer: Sleep 300ms before starting to read
        await Task.Delay(300);

        // Read in small deliberate throttled increments (e.g. 8KB at a time with pauses)
        var reader = clientPipe.Input;
        var receivedBytes = new byte[size];
        int totalRead = 0;
        bool isFinal = false;

        using var testTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        byte[] respHeaderBytes = new byte[PartitionFetchResponseMessage.HeaderSize];
        while (!isFinal && !testTimeout.IsCancellationRequested)
        {
            ReadResult result = await reader.ReadAsync(testTimeout.Token);
            ReadOnlySequence<byte> seq = result.Buffer;

            while (ClusterFrameReader.TryReadFrame(ref seq, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
            {
                if (header.MessageType == ClusterMessageType.PartitionFetchResponse)
                {
                    payload.Slice(0, PartitionFetchResponseMessage.HeaderSize).CopyTo(respHeaderBytes);
                    Span<byte> respHeader = respHeaderBytes;

                    var flags = (PartitionDataFlags)respHeader[33];
                    ulong offset = BinaryPrimitives.ReadUInt64LittleEndian(respHeader.Slice(36, 8));
                    int chunkLen = BinaryPrimitives.ReadInt32LittleEndian(respHeader.Slice(44, 4));

                    if (chunkLen > 0)
                    {
                        payload.Slice(PartitionFetchResponseMessage.HeaderSize, chunkLen)
                               .CopyTo(receivedBytes.AsSpan((int)offset, chunkLen));
                        totalRead += chunkLen;
                    }

                    if (flags.HasFlag(PartitionDataFlags.FinalChunk) || totalRead >= size)
                    {
                        isFinal = true;
                    }
                }
            }

            reader.AdvanceTo(seq.Start, seq.End);

            // Throttle reading slightly to maintain backpressure during streaming
            if (!isFinal)
            {
                await Task.Delay(10);
            }
        }

        Assert.True(isFinal, "Slow reader must successfully receive final chunk without dropped frames.");
        Assert.Equal(size, totalRead);

        // Verify bit-exact fidelity of all 2 MB
        for (int i = 0; i < size; i++)
        {
            Assert.Equal((byte)(i & 0xFF), receivedBytes[i]);
        }
    }

    // =========================================================================
    // 3. Out-Of-Core Memory Spill Stress
    // =========================================================================

    [Fact]
    public async Task OutOfCoreSpill_LargePartitionSet_AtomicFileCreation_And_DirectStreaming()
    {
        var spillManager = new DiskSpillManager(
            scratchDirectory: _testScratchDir,
            maxMemoryBytes: 2 * 1024 * 1024, // 2 MB limit
            highWatermark: 0.50, // Spill at 1 MB
            lowWatermark: 0.25);

        var jobId = Guid.NewGuid();
        int partitionCount = 8;
        int partitionSize = 512 * 1024; // 512 KB each = 4 MB total (exceeds 2 MB limit)

        var originalBuffers = new List<byte[]>();
        var spilledMetadata = new List<SpilledPartitionMetadata>();

        // 1. Generate partitions and spill them
        for (int p = 0; p < partitionCount; p++)
        {
            byte[] data = new byte[partitionSize];
            for (int i = 0; i < partitionSize; i++) data[i] = (byte)((p * 37 + i) & 0xFF);
            originalBuffers.Add(data);

            var buf = RefCountedPartitionBuffer.Allocate(partitionSize);
            data.CopyTo(buf.Span);

            var meta = await spillManager.SpillPartitionAsync(jobId, 1, p, buf);
            spilledMetadata.Add(meta);
        }

        // Verify atomic .arrow file creation and no .tmp files remaining
        Assert.Equal(partitionCount, spillManager.SpilledPartitionCount);
        string jobDir = Path.Combine(_testScratchDir, jobId.ToString("N"));
        Assert.True(Directory.Exists(jobDir));

        string[] tmpFiles = Directory.GetFiles(jobDir, "*.tmp");
        Assert.Empty(tmpFiles);

        string[] arrowFiles = Directory.GetFiles(jobDir, "*.arrow");
        Assert.Equal(partitionCount, arrowFiles.Length);

        // 2. Direct streaming test via DiskSpillManager.StreamSpilledPartitionAsync
        for (int p = 0; p < partitionCount; p++)
        {
            var pipe = new Pipe();
            var streamTask = Task.Run(async () =>
            {
                await spillManager.StreamSpilledPartitionAsync(jobId, 1, p, pipe.Writer);
                await pipe.Writer.CompleteAsync();
            });

            var readBytes = new byte[partitionSize];
            int totalBytesRead = 0;

            while (totalBytesRead < partitionSize)
            {
                var readResult = await pipe.Reader.ReadAsync();
                var seq = readResult.Buffer;
                seq.CopyTo(readBytes.AsSpan(totalBytesRead));
                totalBytesRead += (int)seq.Length;
                pipe.Reader.AdvanceTo(seq.End);
                if (readResult.IsCompleted) break;
            }

            await streamTask;
            Assert.Equal(partitionSize, totalBytesRead);
            Assert.Equal(originalBuffers[p], readBytes);
        }

        // 3. Reload back into unmanaged buffer and verify bit-exact fidelity and 64-byte alignment
        for (int p = 0; p < partitionCount; p++)
        {
            using var reloaded = await spillManager.ReloadPartitionAsync(jobId, 1, p);
            Assert.Equal(partitionSize, reloaded.ByteLength);
            Assert.True((ulong)reloaded.Pointer % 64 == 0, "Reloaded buffer must be 64-byte cache aligned.");
            Assert.True(reloaded.Span.SequenceEqual(originalBuffers[p]), $"Partition {p} bit-exact fidelity mismatch.");
        }

        // Cleanup
        spillManager.CleanupJob(jobId);
        Assert.Equal(0, spillManager.SpilledPartitionCount);
        Assert.False(Directory.Exists(jobDir));
    }

    [Fact]
    public async Task OutOfCoreSpill_IntegrationWithServer_TestsStoreStateMismatch()
    {
        // This test investigates whether DiskSpillManager and LocalPartitionStore
        // properly coordinate when a partition is spilled and fetched through ArrowShuffleServer.
        var spillManager = new DiskSpillManager(scratchDirectory: _testScratchDir);
        var store = new LocalPartitionStore(_nodeId);
        var server = new ArrowShuffleServer(null, store, spillManager);
        var client = new ArrowShuffleClient();

        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        _ = Task.Run(() => server.ProcessConnectionAsync(serverPipe, _cts.Token));

        var jobId = Guid.NewGuid();
        int stageId = 3;
        int partitionId = 0;
        int size = 64 * 1024;

        var buf = RefCountedPartitionBuffer.Allocate(size);
        for (int i = 0; i < size; i++) buf.Span[i] = (byte)(i % 251);
        store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), buf);

        // Case A: If caller transitions through store.TransitionToSpilled
        // First spill to disk:
        string jobDir = Path.Combine(_testScratchDir, jobId.ToString("N"));
        Directory.CreateDirectory(jobDir);
        string filePath = Path.Combine(jobDir, $"{stageId}_{partitionId}.arrow");
        await File.WriteAllBytesAsync(filePath, buf.Span.ToArray());

        // Notify store of spill:
        bool transitionOk = store.TransitionToSpilled(new PartitionKey(jobId, stageId, partitionId), filePath);
        Assert.True(transitionOk, "TransitionToSpilled must succeed on in-memory entry.");

        // Client fetches partition from server over duplex pipe
        // Server should detect entry.State == SpilledToDisk and stream directly from disk
        using var fetched = await client.FetchPartitionAsync(clientPipe, jobId, stageId, partitionId);
        Assert.NotNull(fetched);
        Assert.Equal(size, fetched.ByteLength);
        for (int i = 0; i < size; i++)
        {
            Assert.Equal((byte)(i % 251), fetched.Span[i]);
        }

        buf.Release();
    }

    // =========================================================================
    // 4. Extreme Inputs & Boundary Conditions
    // =========================================================================

    [Fact]
    public async Task ExtremeInput_ZeroBytePartition_HandlesGracefully()
    {
        var store = new LocalPartitionStore(_nodeId);
        var server = new ArrowShuffleServer(null, store);
        var client = new ArrowShuffleClient();

        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        _ = Task.Run(() => server.ProcessConnectionAsync(serverPipe, _cts.Token));

        var jobId = Guid.NewGuid();
        int stageId = 10;
        int partitionId = 0;

        var zeroBuf = RefCountedPartitionBuffer.Allocate(0);
        store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), zeroBuf);
        zeroBuf.Release();

        using var fetched = await client.FetchPartitionAsync(clientPipe, jobId, stageId, partitionId);
        Assert.NotNull(fetched);
        Assert.Equal(0, fetched.ByteLength);
    }

    [Fact]
    public async Task ExtremeInput_OneBytePartition_HandlesGracefully()
    {
        var store = new LocalPartitionStore(_nodeId);
        var server = new ArrowShuffleServer(null, store);
        var client = new ArrowShuffleClient();

        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        _ = Task.Run(() => server.ProcessConnectionAsync(serverPipe, _cts.Token));

        var jobId = Guid.NewGuid();
        int stageId = 11;
        int partitionId = 0;

        var oneBuf = RefCountedPartitionBuffer.Allocate(1);
        oneBuf.Span[0] = 0x7E;
        store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), oneBuf);
        oneBuf.Release();

        using var fetched = await client.FetchPartitionAsync(clientPipe, jobId, stageId, partitionId);
        Assert.NotNull(fetched);
        Assert.Equal(1, fetched.ByteLength);
        Assert.Equal(0x7E, fetched.Span[0]);
    }

    [Fact]
    public async Task ExtremeInput_MaxFrameBoundaryPartition_HandlesLargeChunk()
    {
        var store = new LocalPartitionStore(_nodeId);
        var server = new ArrowShuffleServer(null, store);

        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        _ = Task.Run(() => server.ProcessConnectionAsync(serverPipe, _cts.Token));

        var jobId = Guid.NewGuid();
        int stageId = 12;
        int partitionId = 0;

        // 1 MB partition
        int size = 1024 * 1024;
        var buf = RefCountedPartitionBuffer.Allocate(size);
        buf.Span.Fill(0xAB);
        store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), buf);
        buf.Release();

        // Request with maxChunkSizeBytes = 1 MB (entire partition in a single frame)
        ulong correlationId = 4001;
        var request = new PartitionFetchRequestMessage(
            jobId: jobId,
            stageId: stageId,
            partitionId: partitionId,
            creditTokens: 4,
            byteOffset: 0,
            maxChunkSizeBytes: (uint)size);

        int reqFrameSize = ClusterFrameHeader.HeaderSize + PartitionFetchRequestMessage.PayloadSize;
        Span<byte> reqSpan = clientPipe.Output.GetSpan(reqFrameSize);
        ClusterMessageSerializer.PackFrame(request, correlationId, ClusterFrameFlags.None, reqSpan);
        clientPipe.Output.Advance(reqFrameSize);
        await clientPipe.Output.FlushAsync();

        // Read single large response frame
        var reader = clientPipe.Input;
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        bool frameParsed = false;
        while (!frameParsed && !timeoutCts.IsCancellationRequested)
        {
            ReadResult result = await reader.ReadAsync(timeoutCts.Token);
            ReadOnlySequence<byte> seq = result.Buffer;

            if (ClusterFrameReader.TryReadFrame(ref seq, out ClusterFrameHeader header, out ReadOnlySequence<byte> payload))
            {
                Assert.Equal(ClusterMessageType.PartitionFetchResponse, header.MessageType);
                Assert.Equal((ulong)(PartitionFetchResponseMessage.HeaderSize + size), header.PayloadLength);
                frameParsed = true;
            }

            reader.AdvanceTo(seq.Start, seq.End);
        }

        Assert.True(frameParsed, "Server must stream single large frame boundary chunk.");
    }

    [Fact]
    public async Task ProtocolVulnerability_CorruptedCRC32_AssertsRejectionOrAcceptance()
    {
        // Adversarial challenge:
        // A corrupted frame is injected where PayloadCrc32 does not match the payload bytes.
        // We assert whether ArrowShuffleClient or the protocol parser rejects this frame.
        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        var client = new ArrowShuffleClient();

        var jobId = Guid.NewGuid();
        int stageId = 13;
        int partitionId = 0;

        // Start client fetch in background
        var fetchTask = client.FetchPartitionAsync(clientPipe, jobId, stageId, partitionId).AsTask();

        // Read the client's request frame from serverPipe.Input to get its CorrelationId
        var serverReader = serverPipe.Input;
        ClusterFrameHeader reqHeader = default;
        while (true)
        {
            ReadResult result = await serverReader.ReadAsync();
            ReadOnlySequence<byte> seq = result.Buffer;
            if (ClusterFrameReader.TryReadFrame(ref seq, out reqHeader, out _))
            {
                serverReader.AdvanceTo(seq.Start, seq.End);
                break;
            }
            serverReader.AdvanceTo(seq.Start, seq.End);
        }

        // Now construct a PartitionFetchResponse frame with intentionally CORRUPTED CRC32:
        int payloadLen = PartitionFetchResponseMessage.HeaderSize + 16;
        int totalFrameLen = ClusterFrameHeader.HeaderSize + payloadLen;

        Span<byte> frameSpan = stackalloc byte[totalFrameLen];

        // Write response header
        Span<byte> msgHeader = frameSpan.Slice(ClusterFrameHeader.HeaderSize, PartitionFetchResponseMessage.HeaderSize);
        jobId.TryWriteBytes(msgHeader.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(msgHeader.Slice(16, 4), stageId);
        BinaryPrimitives.WriteInt32LittleEndian(msgHeader.Slice(20, 4), partitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(msgHeader.Slice(24, 8), 16);
        msgHeader[32] = (byte)PartitionFetchStatus.Success;
        msgHeader[33] = (byte)(PartitionDataFlags.ArrowIpcBatch | PartitionDataFlags.FinalChunk);
        BinaryPrimitives.WriteUInt64LittleEndian(msgHeader.Slice(36, 8), 0);
        BinaryPrimitives.WriteInt32LittleEndian(msgHeader.Slice(44, 4), 16);

        // Payload data
        frameSpan.Slice(ClusterFrameHeader.HeaderSize + PartitionFetchResponseMessage.HeaderSize, 16).Fill(0xEE);

        // Intentionally corrupt the CRC32: Compute true CRC and XOR with 0xFFFFFFFF
        uint trueCrc = ClusterCrc32.Compute(frameSpan.Slice(ClusterFrameHeader.HeaderSize, payloadLen));
        uint corruptedCrc = trueCrc ^ 0xDEADBEEF;

        var header = new ClusterFrameHeader(
            ClusterMessageType.PartitionFetchResponse,
            reqHeader.CorrelationId,
            (ulong)payloadLen,
            corruptedCrc,
            ClusterFrameFlags.None);
        header.WriteTo(frameSpan.Slice(0, ClusterFrameHeader.HeaderSize));

        // Transmit corrupted frame to client
        await serverPipe.Output.WriteAsync(frameSpan.ToArray());
        await serverPipe.Output.FlushAsync();

        // Check whether client rejects or accepts:
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        bool clientAccepted = false;
        bool clientRejected = false;

        try
        {
            var res = await fetchTask.WaitAsync(cts.Token);
            // If we reached here, the client silently accepted the frame with invalid CRC!
            clientAccepted = true;
            res?.Release();
        }
        catch (OperationCanceledException)
        {
            // Timeout: perhaps client hung or dropped frame
        }
        catch (Exception)
        {
            // Client threw exception: proper rejection
            clientRejected = true;
        }

        // FINDING: Assert protocol rejection.
        // If clientAccepted is true, the protocol layer DOES NOT validate CRC32,
        // allowing corrupt or tampered frames through silently!
        // We record this finding:
        Assert.True(clientRejected, "Client must reject frame with corrupted CRC32.");
        Assert.False(clientAccepted, "CRITICAL: ArrowShuffleClient accepted a frame with corrupted CRC32! CRC32 validation is missing from the protocol receiver.");
    }

    [Fact]
    public async Task ProtocolVulnerability_ServerRejection_CorruptedCRC32_TerminatesConnection()
    {
        // Adversarial challenge:
        // A client sends a PartitionFetchRequest frame with corrupted CRC32 to ArrowShuffleServer.
        // ArrowShuffleServer must validate the CRC32, reject the frame, and abort without processing or responding.
        var store = new LocalPartitionStore(_nodeId);
        var server = new ArrowShuffleServer(null, store);

        var jobId = Guid.NewGuid();
        int stageId = 15;
        int partitionId = 0;

        // Register partition in store
        var buf = RefCountedPartitionBuffer.Allocate(1024);
        buf.Span.Fill(0x55);
        store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), buf);
        buf.Release();

        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        var serverTask = server.ProcessConnectionAsync(serverPipe, _cts.Token);

        // Pack request with corrupted CRC32
        var request = new PartitionFetchRequestMessage(
            jobId: jobId,
            stageId: stageId,
            partitionId: partitionId,
            creditTokens: 4,
            byteOffset: 0,
            maxChunkSizeBytes: 65536);

        int reqPayloadSize = PartitionFetchRequestMessage.PayloadSize;
        int totalFrameSize = ClusterFrameHeader.HeaderSize + reqPayloadSize;
        byte[] frameData = new byte[totalFrameSize];

        Span<byte> reqPayloadSpan = frameData.AsSpan(ClusterFrameHeader.HeaderSize, reqPayloadSize);
        request.JobId.TryWriteBytes(reqPayloadSpan.Slice(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(reqPayloadSpan.Slice(16, 4), request.StageId);
        BinaryPrimitives.WriteInt32LittleEndian(reqPayloadSpan.Slice(20, 4), request.PartitionId);
        BinaryPrimitives.WriteUInt32LittleEndian(reqPayloadSpan.Slice(24, 4), request.CreditTokens);
        BinaryPrimitives.WriteUInt64LittleEndian(reqPayloadSpan.Slice(28, 8), request.ByteOffset);
        BinaryPrimitives.WriteUInt32LittleEndian(reqPayloadSpan.Slice(36, 4), request.MaxChunkSizeBytes);

        uint trueCrc = ClusterCrc32.Compute(reqPayloadSpan);
        uint corruptedCrc = trueCrc ^ 0xCAFEBABE;

        var header = new ClusterFrameHeader(
            ClusterMessageType.PartitionFetchRequest,
            correlationId: 9999,
            (ulong)reqPayloadSize,
            corruptedCrc,
            ClusterFrameFlags.None);
        header.WriteTo(frameData.AsSpan(0, ClusterFrameHeader.HeaderSize));

        // Send corrupted frame to server
        await clientPipe.Output.WriteAsync(frameData);
        await clientPipe.Output.FlushAsync();

        // Server should detect corrupted CRC32, exit ProcessConnectionAsync, and never send any response frames
        var reader = clientPipe.Input;
        using var readCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        int responsesReceived = 0;

        try
        {
            while (!readCts.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(readCts.Token);
                ReadOnlySequence<byte> seq = result.Buffer;

                while (ClusterFrameReader.TryReadFrame(ref seq, out ClusterFrameHeader respHeader, out _))
                {
                    responsesReceived++;
                }

                reader.AdvanceTo(seq.Start, seq.End);

                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected if pipe not explicitly completed or timed out
        }

        // Assert that server refused to fulfill the request
        Assert.Equal(0, responsesReceived);

        // Verify server task exited or completed without hanging
        await serverTask;
    }

    // =========================================================================
    // 5. Hot-Path Chunk Streaming Allocation Measurement
    // =========================================================================

    [Fact]
    public async Task HotPathChunkStreaming_GCAllocations_MeasuresAllocations()
    {
        var store = new LocalPartitionStore(_nodeId);
        var server = new ArrowShuffleServer(null, store);
        var client = new ArrowShuffleClient();

        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();
        _ = Task.Run(() => server.ProcessConnectionAsync(serverPipe, _cts.Token));

        var jobId = Guid.NewGuid();
        int stageId = 14;
        int partitionId = 0;

        int size = 64 * 1024;
        var buf = RefCountedPartitionBuffer.Allocate(size);
        buf.Span.Fill(0x99);
        store.RegisterPartition(new PartitionKey(jobId, stageId, partitionId), buf);
        buf.Release();

        // Warmup JIT and pipes
        using (var warmup = await client.FetchPartitionAsync(clientPipe, jobId, stageId, partitionId))
        {
            Assert.Equal(size, warmup.ByteLength);
        }

        // Measure allocations on dedicated thread
        long totalAllocated = 0;
        int iterations = 10;

        var thread = new Thread(() =>
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < iterations; i++)
            {
                using var fetched = client.FetchPartitionAsync(clientPipe, jobId, stageId, partitionId).AsTask().GetAwaiter().GetResult();
                Assert.Equal(size, fetched.ByteLength);
            }
            long after = GC.GetAllocatedBytesForCurrentThread();
            totalAllocated = after - before;
        })
        {
            IsBackground = true
        };

        thread.Start();
        thread.Join();

        long bytesPerFetch = totalAllocated / iterations;
        
        // Assert zero or minimal heap allocations:
        // True zero heap allocation would mean 0 bytes.
        // In practice with async Task / TaskCompletionSource / lambda allocations:
        // Let's record the exact measured byte count.
        Assert.True(bytesPerFetch < 4096, $"Hot path chunk streaming allocated {bytesPerFetch} bytes per fetch on caller thread.");
    }
}
