namespace Glacier.Cluster.Tests.ProtocolTests;

using System;
using System.Text;
using Glacier.Cluster.Protocol;
using Xunit;

public sealed class MessageSerializationTests
{
    // =========================================================================
    // 1. Fixed-Size 40-byte Struct Roundtrip & Zero-Allocation Tests
    // =========================================================================

    [Fact]
    public void HeartbeatPing_RoundtripAndZeroAlloc()
    {
        var original = new HeartbeatPingMessage(
            nodeId: Guid.NewGuid(),
            timestampNs: 1_234_567_890_123UL,
            activeTaskCount: 8,
            cpuUsagePercentage: 74.5f,
            availablePhysicalMemoryBytes: 32UL * 1024 * 1024 * 1024);

        Span<byte> buffer = stackalloc byte[HeartbeatPingMessage.PayloadSize];
        int written = original.Serialize(buffer);
        Assert.Equal(HeartbeatPingMessage.PayloadSize, written);

        var deserialized = HeartbeatPingMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(original.NodeId, deserialized.NodeId);
        Assert.Equal(original.TimestampNs, deserialized.TimestampNs);
        Assert.Equal(original.ActiveTaskCount, deserialized.ActiveTaskCount);
        Assert.Equal(original.CpuUsagePercentage, deserialized.CpuUsagePercentage);
        Assert.Equal(original.AvailablePhysicalMemoryBytes, deserialized.AvailablePhysicalMemoryBytes);

        // Zero-allocation verification
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 5000; i++)
        {
            original.Serialize(buffer);
            var d = HeartbeatPingMessage.Deserialize(buffer);
            if (d.ActiveTaskCount != 8) throw new InvalidOperationException();
        }
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
    }

    [Fact]
    public void HeartbeatPong_RoundtripAndZeroAlloc()
    {
        var original = new HeartbeatPongMessage(
            coordinatorId: Guid.NewGuid(),
            timestampNs: 9_876_543_210UL,
            clusterEpoch: 42UL,
            coordinatorStatusFlags: 0x01,
            activeNodeCount: 16);

        Span<byte> buffer = stackalloc byte[HeartbeatPongMessage.PayloadSize];
        int written = original.Serialize(buffer);
        Assert.Equal(HeartbeatPongMessage.PayloadSize, written);

        var deserialized = HeartbeatPongMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(original.CoordinatorId, deserialized.CoordinatorId);
        Assert.Equal(original.ClusterEpoch, deserialized.ClusterEpoch);
        Assert.Equal(original.ActiveNodeCount, deserialized.ActiveNodeCount);

        // Zero-allocation verification
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 5000; i++)
        {
            original.Serialize(buffer);
            var d = HeartbeatPongMessage.Deserialize(buffer);
            if (d.ActiveNodeCount != 16) throw new InvalidOperationException();
        }
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
    }

    [Fact]
    public void PartitionFetchRequest_RoundtripAndZeroAlloc()
    {
        var original = new PartitionFetchRequestMessage(
            jobId: Guid.NewGuid(),
            stageId: 3,
            partitionId: 7,
            creditTokens: 100,
            byteOffset: 65536UL,
            maxChunkSizeBytes: 32768);

        Span<byte> buffer = stackalloc byte[PartitionFetchRequestMessage.PayloadSize];
        int written = original.Serialize(buffer);
        Assert.Equal(PartitionFetchRequestMessage.PayloadSize, written);

        var deserialized = PartitionFetchRequestMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(original.JobId, deserialized.JobId);
        Assert.Equal(original.StageId, deserialized.StageId);
        Assert.Equal(original.PartitionId, deserialized.PartitionId);
        Assert.Equal(original.CreditTokens, deserialized.CreditTokens);
        Assert.Equal(original.ByteOffset, deserialized.ByteOffset);
        Assert.Equal(original.MaxChunkSizeBytes, deserialized.MaxChunkSizeBytes);

        // Zero-allocation verification
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 5000; i++)
        {
            original.Serialize(buffer);
            var d = PartitionFetchRequestMessage.Deserialize(buffer);
            if (d.CreditTokens != 100) throw new InvalidOperationException();
        }
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
    }

    [Fact]
    public void PartitionAcknowledge_RoundtripAndZeroAlloc()
    {
        var original = new PartitionAcknowledgeMessage(
            jobId: Guid.NewGuid(),
            stageId: 2,
            partitionId: 5,
            tokensReleased: 50,
            bytesReceived: 1048576UL,
            isComplete: true);

        Span<byte> buffer = stackalloc byte[PartitionAcknowledgeMessage.PayloadSize];
        int written = original.Serialize(buffer);
        Assert.Equal(PartitionAcknowledgeMessage.PayloadSize, written);

        var deserialized = PartitionAcknowledgeMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(original.JobId, deserialized.JobId);
        Assert.Equal(original.TokensReleased, deserialized.TokensReleased);
        Assert.Equal(original.BytesReceived, deserialized.BytesReceived);
        Assert.Equal((byte)1, deserialized.IsComplete);

        // Zero-allocation verification
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 5000; i++)
        {
            original.Serialize(buffer);
            var d = PartitionAcknowledgeMessage.Deserialize(buffer);
            if (d.TokensReleased != 50) throw new InvalidOperationException();
        }
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
    }

    // =========================================================================
    // 2. Variable-Sized Message Roundtrip Tests
    // =========================================================================

    [Fact]
    public void NodeJoinRequest_RoundtripPreservesData()
    {
        var profile = new NodeHardwareProfile
        {
            NodeId = Guid.NewGuid(),
            CpuLogicalCores = 16,
            SimdFlags = SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
            GpuFlags = GpuBackendFlags.Cuda,
            Reserved1 = 0xC0A8016C, // 192.168.1.108
            RamTotalBytes = 32UL * 1024 * 1024 * 1024,
            RamAvailableBytes = 24UL * 1024 * 1024 * 1024,
            VramTotalBytes = 12UL * 1024 * 1024 * 1024,
            VramAvailableBytes = 11UL * 1024 * 1024 * 1024
        };

        var original = new NodeJoinRequestMessage(profile, "beast.local", "cluster-auth-token-1234");

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = NodeJoinRequestMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal("beast.local", deserialized.Hostname);
        Assert.Equal("cluster-auth-token-1234", deserialized.ClusterSecret);
        Assert.Equal(profile, deserialized.HardwareProfile);
    }

    [Fact]
    public void NodeJoinResponse_RoundtripPreservesData()
    {
        var original = new NodeJoinResponseMessage(
            status: NodeJoinStatus.Accepted,
            assignedNodeId: Guid.NewGuid(),
            assignedRank: 3,
            activeClusterSize: 8,
            clusterEpoch: 101UL,
            rejectionReason: null);

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = NodeJoinResponseMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(NodeJoinStatus.Accepted, deserialized.Status);
        Assert.Equal(3, deserialized.AssignedRank);
        Assert.Equal(8, deserialized.ActiveClusterSize);
        Assert.Equal(string.Empty, deserialized.RejectionReason);
    }

    [Fact]
    public void NodeLeave_RoundtripPreservesData()
    {
        var original = new NodeLeaveMessage(
            nodeId: Guid.NewGuid(),
            leaveReason: 1, // Drain
            drainTimeoutMs: 30000UL,
            reasonDetails: "Maintenance rolling restart");

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = NodeLeaveMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(1U, deserialized.LeaveReason);
        Assert.Equal(30000UL, deserialized.DrainTimeoutMs);
        Assert.Equal("Maintenance rolling restart", deserialized.ReasonDetails);
    }

    [Fact]
    public void StageDispatch_RoundtripWithUpstreamDependencies()
    {
        byte[] payloadBytes = new byte[] { 0x10, 0x20, 0x30, 0x40, 0x50 };
        var upstreams = new[]
        {
            new UpstreamPartitionLocation(Guid.NewGuid(), stageId: 0, partitionId: 0, shufflePort: 9001, "node1.cluster"),
            new UpstreamPartitionLocation(Guid.NewGuid(), stageId: 0, partitionId: 1, shufflePort: 9002, "node2.cluster")
        };

        var original = new StageDispatchMessage(
            jobId: Guid.NewGuid(),
            stageId: 1,
            partitionId: 2,
            operatorType: StageOperatorType.HashJoinProbe,
            operatorPayload: payloadBytes,
            upstreamPartitions: upstreams);

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = StageDispatchMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(StageOperatorType.HashJoinProbe, deserialized.OperatorType);
        Assert.Equal(2, deserialized.UpstreamPartitions.Length);
        Assert.Equal("node1.cluster", deserialized.UpstreamPartitions[0].Hostname);
        Assert.Equal("node2.cluster", deserialized.UpstreamPartitions[1].Hostname);
    }

    [Fact]
    public void TaskStatusUpdate_RoundtripPreservesData()
    {
        var original = new TaskStatusUpdateMessage(
            jobId: Guid.NewGuid(),
            stageId: 2,
            partitionId: 4,
            status: TaskExecutionStatus.Completed,
            elapsedMs: 142.85,
            outputBytes: 2048576UL,
            outputRowCount: 100000UL,
            errorMessage: null);

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = TaskStatusUpdateMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(TaskExecutionStatus.Completed, deserialized.Status);
        Assert.Equal(142.85, deserialized.ElapsedMs);
        Assert.Equal(2048576UL, deserialized.OutputBytes);
        Assert.Equal(100000UL, deserialized.OutputRowCount);
        Assert.Equal(string.Empty, deserialized.ErrorMessage);
    }

    [Fact]
    public void PartitionFetchResponse_RoundtripWithDataChunk()
    {
        byte[] chunk = new byte[] { 0xCA, 0xFE, 0xBA, 0xBE, 0x01, 0x02, 0x03, 0x04 };
        var original = new PartitionFetchResponseMessage(
            jobId: Guid.NewGuid(),
            stageId: 5,
            partitionId: 12,
            dataByteLength: 1024UL,
            status: PartitionFetchStatus.Success,
            flags: PartitionDataFlags.ArrowIpcBatch | PartitionDataFlags.FinalChunk,
            chunkOffset: 0UL,
            data: chunk);

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = PartitionFetchResponseMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(PartitionFetchStatus.Success, deserialized.Status);
        Assert.True(deserialized.Flags.HasFlag(PartitionDataFlags.ArrowIpcBatch));
        Assert.True(deserialized.Flags.HasFlag(PartitionDataFlags.FinalChunk));
        Assert.True(chunk.AsSpan().SequenceEqual(deserialized.Data.Span));
    }

    [Fact]
    public void RingAllReduceHandshake_RoundtripPreservesTopology()
    {
        var original = new RingAllReduceHandshakeMessage(
            sessionId: Guid.NewGuid(),
            rank: 2,
            worldSize: 4,
            predecessorId: Guid.NewGuid(),
            successorId: Guid.NewGuid(),
            predecessorPort: 8081,
            successorPort: 8082,
            predecessorEndpoint: "192.168.1.108",
            successorEndpoint: "192.168.1.200");

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = RingAllReduceHandshakeMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(2, deserialized.Rank);
        Assert.Equal(4, deserialized.WorldSize);
        Assert.Equal(original.PredecessorId, deserialized.PredecessorId);
        Assert.Equal(original.SuccessorId, deserialized.SuccessorId);
        Assert.Equal("192.168.1.108", deserialized.PredecessorEndpoint);
        Assert.Equal("192.168.1.200", deserialized.SuccessorEndpoint);
    }

    [Fact]
    public void JobSubmission_RoundtripWithDagBlob()
    {
        byte[] dag = Encoding.UTF8.GetBytes("{\"stages\": [{\"id\": 1, \"type\": \"scan\"}]}");
        var original = new JobSubmissionMessage(
            jobId: Guid.NewGuid(),
            jobName: "Polaris-Distributed-TPCH-Q1",
            totalStages: 3,
            targetPartitionCount: 64,
            timeoutMs: 60000UL,
            dagPayload: dag);

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = JobSubmissionMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal("Polaris-Distributed-TPCH-Q1", deserialized.JobName);
        Assert.Equal(3, deserialized.TotalStages);
        Assert.Equal(64U, deserialized.TargetPartitionCount);
        Assert.Equal(60000UL, deserialized.TimeoutMs);
        Assert.True(dag.AsSpan().SequenceEqual(deserialized.DagPayload.Span));
    }

    [Fact]
    public void JobCompletion_RoundtripPreservesMetrics()
    {
        var original = new JobCompletionMessage(
            jobId: Guid.NewGuid(),
            status: JobCompletionStatus.Succeeded,
            elapsedMs: 2450.75,
            totalOutputRows: 10_000_000UL,
            totalOutputBytes: 800_000_000UL,
            errorMessage: null);

        byte[] buffer = new byte[original.GetByteCount()];
        int written = original.Serialize(buffer);
        Assert.Equal(buffer.Length, written);

        var deserialized = JobCompletionMessage.Deserialize(buffer);
        Assert.Equal(original, deserialized);
        Assert.Equal(JobCompletionStatus.Succeeded, deserialized.Status);
        Assert.Equal(2450.75, deserialized.ElapsedMs);
        Assert.Equal(10_000_000UL, deserialized.TotalOutputRows);
        Assert.Equal(800_000_000UL, deserialized.TotalOutputBytes);
        Assert.Equal(string.Empty, deserialized.ErrorMessage);
    }

    // =========================================================================
    // 3. Polymorphic Dispatch & PackFrame Tests
    // =========================================================================

    [Fact]
    public void ClusterMessageSerializer_PolymorphicDispatch_All13Types()
    {
        // 1. HeartbeatPing
        var m1 = new HeartbeatPingMessage(Guid.NewGuid(), 1, 2, 3.0f, 4);
        AssertPolyDispatch(m1, ClusterMessageType.HeartbeatPing);

        // 2. HeartbeatPong
        var m2 = new HeartbeatPongMessage(Guid.NewGuid(), 1, 2, 3, 4);
        AssertPolyDispatch(m2, ClusterMessageType.HeartbeatPong);

        // 3. NodeJoinRequest
        var m3 = new NodeJoinRequestMessage(NodeHardwareProfile.ProbeLocal(), "host1", "secret");
        AssertPolyDispatch(m3, ClusterMessageType.NodeJoinRequest);

        // 4. NodeJoinResponse
        var m4 = new NodeJoinResponseMessage(NodeJoinStatus.Accepted, Guid.NewGuid(), 1, 2, 3, "ok");
        AssertPolyDispatch(m4, ClusterMessageType.NodeJoinResponse);

        // 5. NodeLeave
        var m5 = new NodeLeaveMessage(Guid.NewGuid(), 1, 2, "draining");
        AssertPolyDispatch(m5, ClusterMessageType.NodeLeave);

        // 6. StageDispatch
        var m6 = new StageDispatchMessage(Guid.NewGuid(), 1, 2, StageOperatorType.Filter, new byte[] { 1, 2, 3 });
        AssertPolyDispatch(m6, ClusterMessageType.StageDispatch);

        // 7. TaskStatusUpdate
        var m7 = new TaskStatusUpdateMessage(Guid.NewGuid(), 1, 2, TaskExecutionStatus.Completed, 1.0, 2, 3, "done");
        AssertPolyDispatch(m7, ClusterMessageType.TaskStatusUpdate);

        // 8. PartitionFetchRequest
        var m8 = new PartitionFetchRequestMessage(Guid.NewGuid(), 1, 2, 3, 4, 5);
        AssertPolyDispatch(m8, ClusterMessageType.PartitionFetchRequest);

        // 9. PartitionFetchResponse
        var m9 = new PartitionFetchResponseMessage(Guid.NewGuid(), 1, 2, 3, PartitionFetchStatus.Success, PartitionDataFlags.FinalChunk, 0, new byte[] { 4, 5 });
        AssertPolyDispatch(m9, ClusterMessageType.PartitionFetchResponse);

        // 10. PartitionAcknowledge
        var m10 = new PartitionAcknowledgeMessage(Guid.NewGuid(), 1, 2, 3, 4, true);
        AssertPolyDispatch(m10, ClusterMessageType.PartitionAcknowledge);

        // 11. RingAllReduceHandshake
        var m11 = new RingAllReduceHandshakeMessage(Guid.NewGuid(), 1, 2, Guid.NewGuid(), Guid.NewGuid(), 3, 4, "pred", "succ");
        AssertPolyDispatch(m11, ClusterMessageType.RingAllReduceHandshake);

        // 12. JobSubmission
        var m12 = new JobSubmissionMessage(Guid.NewGuid(), "job", 1, 2, 3, new byte[] { 9, 8, 7 });
        AssertPolyDispatch(m12, ClusterMessageType.JobSubmission);

        // 13. JobCompletion
        var m13 = new JobCompletionMessage(Guid.NewGuid(), JobCompletionStatus.Succeeded, 10.0, 100, 1000, "all good");
        AssertPolyDispatch(m13, ClusterMessageType.JobCompletion);
    }

    private static void AssertPolyDispatch(IClusterMessage msg, ClusterMessageType expectedType)
    {
        byte[] payload = new byte[msg.GetByteCount()];
        msg.Serialize(payload);

        var restored = ClusterMessageSerializer.Deserialize(expectedType, payload);
        Assert.Equal(expectedType, restored.MessageType);
        Assert.Equal(msg.GetByteCount(), restored.GetByteCount());
    }

    [Fact]
    public void PackFrame_ProducesValidWireFrameWithChecksum()
    {
        var msg = new HeartbeatPingMessage(Guid.NewGuid(), 123456789UL, 4, 55.0f, 16UL * 1024 * 1024 * 1024);

        int totalSize = ClusterFrameHeader.HeaderSize + msg.GetByteCount();
        byte[] wireFrame = new byte[totalSize];

        int packedBytes = ClusterMessageSerializer.PackFrame(msg, correlationId: 8888, ClusterFrameFlags.None, wireFrame);
        Assert.Equal(totalSize, packedBytes);

        var header = ClusterFrameHeader.ReadFrom(wireFrame.AsSpan(0, 32));
        Assert.Equal(ClusterMessageType.HeartbeatPing, header.MessageType);
        Assert.Equal(8888UL, header.CorrelationId);
        Assert.Equal((ulong)HeartbeatPingMessage.PayloadSize, header.PayloadLength);
        Assert.NotEqual(0U, header.PayloadCrc32);

        // Verify CRC32 matches computed payload CRC
        uint expectedCrc = ClusterCrc32.Compute(wireFrame.AsSpan(32, HeartbeatPingMessage.PayloadSize));
        Assert.Equal(expectedCrc, header.PayloadCrc32);

        // Verify payload deserialization
        var unpackedMsg = (HeartbeatPingMessage)ClusterMessageSerializer.Deserialize(header.MessageType, wireFrame.AsSpan(32));
        Assert.Equal(msg, unpackedMsg);
    }

    // =========================================================================
    // 4. BinaryStringHelper Tests
    // =========================================================================

    [Fact]
    public void BinaryStringHelper_NullAndEmpty_EncodeAsZeroLength()
    {
        Span<byte> span = stackalloc byte[16];

        int writtenNull = BinaryStringHelper.WriteUtf8String(span, null);
        Assert.Equal(2, writtenNull);
        string readNull = BinaryStringHelper.ReadUtf8String(span, out int consumedNull);
        Assert.Equal(string.Empty, readNull);
        Assert.Equal(2, consumedNull);

        int writtenEmpty = BinaryStringHelper.WriteUtf8String(span, "");
        Assert.Equal(2, writtenEmpty);
        string readEmpty = BinaryStringHelper.ReadUtf8String(span, out int consumedEmpty);
        Assert.Equal(string.Empty, readEmpty);
        Assert.Equal(2, consumedEmpty);
    }

    [Fact]
    public void BinaryStringHelper_UnicodeAndSpecialCharacters_RoundtripAccurately()
    {
        string unicode = "Glacier ❄️ 分散コンピューティング — High Performance 🚀";
        int required = BinaryStringHelper.GetUtf8ByteCount(unicode);

        byte[] buffer = new byte[required];
        int written = BinaryStringHelper.WriteUtf8String(buffer, unicode);
        Assert.Equal(required, written);

        string readBack = BinaryStringHelper.ReadUtf8String(buffer, out int consumed);
        Assert.Equal(unicode, readBack);
        Assert.Equal(required, consumed);
    }

    // =========================================================================
    // 5. Buffer Boundary & Error Handling Tests
    // =========================================================================

    [Fact]
    public void UnsupportedOpcode_ThrowsNotSupportedException()
    {
        byte[] payload = new byte[32];
        Assert.Throws<NotSupportedException>(() => ClusterMessageSerializer.Deserialize((ClusterMessageType)0xFF, payload));
    }

    [Fact]
    public void PackFrame_DestinationTooSmall_ThrowsArgumentException()
    {
        var msg = new HeartbeatPingMessage(Guid.NewGuid(), 1, 1, 1f, 1);
        byte[] tooSmall = new byte[ClusterFrameHeader.HeaderSize + msg.GetByteCount() - 1];
        Assert.Throws<ArgumentException>(() => ClusterMessageSerializer.PackFrame(msg, 1, ClusterFrameFlags.None, tooSmall));
    }

    [Fact]
    public void Deserialization_TruncatedSource_ThrowsArgumentException()
    {
        // For each message type, test truncated buffer
        Assert.Throws<ArgumentException>(() => HeartbeatPingMessage.Deserialize(new byte[HeartbeatPingMessage.PayloadSize - 1]));
        Assert.Throws<ArgumentException>(() => HeartbeatPongMessage.Deserialize(new byte[HeartbeatPongMessage.PayloadSize - 1]));
        Assert.Throws<ArgumentException>(() => NodeJoinRequestMessage.Deserialize(new byte[60]));
        Assert.Throws<ArgumentException>(() => NodeJoinResponseMessage.Deserialize(new byte[30]));
        Assert.Throws<ArgumentException>(() => NodeLeaveMessage.Deserialize(new byte[20]));
        Assert.Throws<ArgumentException>(() => StageDispatchMessage.Deserialize(new byte[20]));
        Assert.Throws<ArgumentException>(() => TaskStatusUpdateMessage.Deserialize(new byte[40]));
        Assert.Throws<ArgumentException>(() => PartitionFetchRequestMessage.Deserialize(new byte[PartitionFetchRequestMessage.PayloadSize - 1]));
        Assert.Throws<ArgumentException>(() => PartitionFetchResponseMessage.Deserialize(new byte[PartitionFetchResponseMessage.HeaderSize - 1]));
        Assert.Throws<ArgumentException>(() => PartitionAcknowledgeMessage.Deserialize(new byte[PartitionAcknowledgeMessage.PayloadSize - 1]));
        Assert.Throws<ArgumentException>(() => RingAllReduceHandshakeMessage.Deserialize(new byte[50]));
        Assert.Throws<ArgumentException>(() => JobSubmissionMessage.Deserialize(new byte[30]));
        Assert.Throws<ArgumentException>(() => JobCompletionMessage.Deserialize(new byte[35]));
    }

    [Fact]
    public void Serialization_DestinationTooSmall_ThrowsArgumentException()
    {
        var ping = new HeartbeatPingMessage(Guid.NewGuid(), 1, 1, 1f, 1);
        Assert.Throws<ArgumentException>(() => ping.Serialize(new byte[ping.GetByteCount() - 1]));

        var pong = new HeartbeatPongMessage(Guid.NewGuid(), 1, 1, 1, 1);
        Assert.Throws<ArgumentException>(() => pong.Serialize(new byte[pong.GetByteCount() - 1]));

        var joinReq = new NodeJoinRequestMessage(new NodeHardwareProfile(), "host", "secret");
        Assert.Throws<ArgumentException>(() => joinReq.Serialize(new byte[joinReq.GetByteCount() - 1]));

        var joinResp = new NodeJoinResponseMessage(NodeJoinStatus.Accepted, Guid.NewGuid(), 1, 1, 1, "ok");
        Assert.Throws<ArgumentException>(() => joinResp.Serialize(new byte[joinResp.GetByteCount() - 1]));

        var leave = new NodeLeaveMessage(Guid.NewGuid(), 1, 100, "leaving");
        Assert.Throws<ArgumentException>(() => leave.Serialize(new byte[leave.GetByteCount() - 1]));

        var dispatch = new StageDispatchMessage(Guid.NewGuid(), 1, 1, StageOperatorType.Scan, new byte[] { 1 });
        Assert.Throws<ArgumentException>(() => dispatch.Serialize(new byte[dispatch.GetByteCount() - 1]));

        var status = new TaskStatusUpdateMessage(Guid.NewGuid(), 1, 1, TaskExecutionStatus.Completed, 1.0, 1, 1, "err");
        Assert.Throws<ArgumentException>(() => status.Serialize(new byte[status.GetByteCount() - 1]));

        var fetchReq = new PartitionFetchRequestMessage(Guid.NewGuid(), 1, 1, 1);
        Assert.Throws<ArgumentException>(() => fetchReq.Serialize(new byte[fetchReq.GetByteCount() - 1]));

        var fetchResp = new PartitionFetchResponseMessage(Guid.NewGuid(), 1, 1, 1, PartitionFetchStatus.Success, PartitionDataFlags.None, 0, new byte[] { 1 });
        Assert.Throws<ArgumentException>(() => fetchResp.Serialize(new byte[fetchResp.GetByteCount() - 1]));

        var ack = new PartitionAcknowledgeMessage(Guid.NewGuid(), 1, 1, 1, 1, true);
        Assert.Throws<ArgumentException>(() => ack.Serialize(new byte[ack.GetByteCount() - 1]));

        var ring = new RingAllReduceHandshakeMessage(Guid.NewGuid(), 0, 1, Guid.NewGuid(), Guid.NewGuid(), 1, 2, "e1", "e2");
        Assert.Throws<ArgumentException>(() => ring.Serialize(new byte[ring.GetByteCount() - 1]));

        var sub = new JobSubmissionMessage(Guid.NewGuid(), "sub", 1, 1, 100, new byte[] { 1 });
        Assert.Throws<ArgumentException>(() => sub.Serialize(new byte[sub.GetByteCount() - 1]));

        var comp = new JobCompletionMessage(Guid.NewGuid(), JobCompletionStatus.Succeeded, 1.0, 1, 1, "err");
        Assert.Throws<ArgumentException>(() => comp.Serialize(new byte[comp.GetByteCount() - 1]));
    }
}
