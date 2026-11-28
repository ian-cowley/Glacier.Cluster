namespace Glacier.Cluster.Tests;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.Inference.Pipeline;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Protocol.Inference;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Glacier.Cluster.Serialization;
using Glacier.Cluster.Shuffle;
using Glacier.Cluster.Simulator;
using Glacier.Cluster.Transport;
using Glacier.Cluster.Worker;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

public sealed class HardeningM35Tests
{
    // =========================================================================
    // Item 1: Coordinator-Side Heartbeat Clock Stamping (Skew & Silent Expiry)
    // =========================================================================

    [Fact]
    public async Task Item1_MembershipManager_IgnoresWorkerClockSkew_AndDetectsSilentExpiry()
    {
        var detector = new PhiAccrualFailureDetector(
            suspectThreshold: 8.0,
            evictionThreshold: 12.0,
            minStdDevMs: 25.0,
            historyWindowSize: 1000);
        using var membership = new MembershipManager(detector);

        var workerId = Guid.NewGuid();
        var profile = NodeHardwareProfile.ProbeLocal(workerId, 0x7F000001);
        var joinResp = await membership.RegisterWorkerAsync(
            new NodeJoinRequestMessage(profile, "worker-skew", shufflePort: 51100),
            new IPEndPoint(IPAddress.Loopback, 51100),
            shufflePort: 51100);
        Assert.Equal(NodeJoinStatus.Accepted, joinResp.Status);

        // Worker sends wildly skewed / frozen / MaxValue timestamps; coordinator stamps local monotonic time.
        for (int i = 0; i < 8; i++)
        {
            ulong bogusWorkerTs = i switch
            {
                0 => 0UL,
                1 => ulong.MaxValue,
                2 => 12345UL,
                _ => (ulong)(1_000_000_000_000L - i * 1_000_000L) // backwards timestamps!
            };
            var ping = new HeartbeatPingMessage(workerId, bogusWorkerTs, activeTaskCount: 0, cpuUsagePercentage: 10f, availablePhysicalMemoryBytes: 1024UL);
            membership.RecordHeartbeat(ping);
            await Task.Delay(20);
        }

        // Despite hostile worker timestamps, real arrival cadence was 20ms -> node must remain Healthy.
        Assert.Equal(NodeHealthState.Healthy, membership.FailureDetector.GetHealthState(workerId));

        // Now go completely silent and verify coordinator detects expiry on its own clock.
        await Task.Delay(350);
        Assert.True(membership.FailureDetector.GetHealthState(workerId) >= NodeHealthState.Suspect);
    }

    // =========================================================================
    // Item 2: Auth, Forged Identity Blocking, CRC32 & Non-Blocking Duplex
    // =========================================================================

    [Fact]
    public async Task Item2_Coordinator_RejectsUnauthenticatedFrames_WrongSecrets_AndForgedIdentities()
    {
        const string secret = "m35-strong-cluster-secret-value";
        await using var coordinator = new ClusterCoordinator(new ClusterCoordinatorOptions
        {
            BindAddress = IPAddress.Loopback,
            Port = 0,
            ClusterSecret = secret
        });
        await coordinator.StartAsync();

        // 1. Unauthenticated connection sending non-NodeJoin frame must be rejected and closed.
        using (var rawSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
        {
            await rawSocket.ConnectAsync(IPAddress.Loopback, coordinator.Port);
            using var netStream = new NetworkStream(rawSocket, ownsSocket: false);
            var writer = PipeWriter.Create(netStream);
            byte[] buf = new byte[128];
            int len = ClusterMessageSerializer.PackFrame(
                new HeartbeatPingMessage(Guid.NewGuid(), 1UL, 0, 0f, 0UL),
                correlationId: 1,
                flags: ClusterFrameFlags.None,
                buf);
            await writer.WriteAsync(buf.AsMemory(0, len));
            await writer.FlushAsync();

            byte[] rx = new byte[64];
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            int read = await rawSocket.ReceiveAsync(rx, SocketFlags.None, cts.Token);
            Assert.Equal(0, read); // Server closed unauthenticated socket
        }

        // 2. Wrong secret (both same length and shorter length) receives Unauthorized and socket closes.
        foreach (string badSecret in new[] { "m35-strong-cluster-secret-wrong", "x" })
        {
            var badSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await badSocket.ConnectAsync(IPAddress.Loopback, coordinator.Port);
            await using var pipe = new SocketDuplexPipe(badSocket);
            await using var badConn = new DuplexConnection(Guid.Empty, pipe);
            badConn.Start();

            var badProfile = NodeHardwareProfile.ProbeLocal(Guid.NewGuid(), 0x7F000001);
            var joinReq = new NodeJoinRequestMessage(badProfile, "bad-worker", badSecret, 52001);
            using var respFrame = await badConn.SendRequestAsync(joinReq, correlationId: 1, TimeSpan.FromSeconds(3));
            var resp = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(respFrame.Header.MessageType, respFrame.Payload.Span));

            Assert.Equal(NodeJoinStatus.Unauthorized, resp.Status);
        }

        // 3. Connect two legitimate workers W1 and W2, then attempt forged NodeLeave from W1 against W2.
        var w1Profile = NodeHardwareProfile.ProbeLocal(Guid.NewGuid(), 0x7F000001);
        var w2Profile = NodeHardwareProfile.ProbeLocal(Guid.NewGuid(), 0x7F000001);

        var s1 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await s1.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var p1 = new SocketDuplexPipe(s1);
        await using var c1 = new DuplexConnection(Guid.Empty, p1);
        c1.Start();
        using (var join1 = await c1.SendRequestAsync(
            new NodeJoinRequestMessage(w1Profile, "w1", secret, 52002),
            correlationId: 1,
            TimeSpan.FromSeconds(3)))
        {
            var r1 = (NodeJoinResponseMessage)ClusterMessageSerializer.Deserialize(join1.Header.MessageType, join1.Payload.Span);
            Assert.Equal(NodeJoinStatus.Accepted, r1.Status);
        }

        var s2 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await s2.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var p2 = new SocketDuplexPipe(s2);
        await using var c2 = new DuplexConnection(Guid.Empty, p2);
        c2.Start();
        using (var join2 = await c2.SendRequestAsync(
            new NodeJoinRequestMessage(w2Profile, "w2", secret, 52003),
            correlationId: 1,
            TimeSpan.FromSeconds(3)))
        {
            var r2 = (NodeJoinResponseMessage)ClusterMessageSerializer.Deserialize(join2.Header.MessageType, join2.Payload.Span);
            Assert.Equal(NodeJoinStatus.Accepted, r2.Status);
        }

        Assert.Equal(2, coordinator.Membership.ActiveNodeCount);

        // W1 sends forged NodeLeave naming W2 -> W2 must NOT be removed!
        await c1.SendFrameAsync(new NodeLeaveMessage(w2Profile.NodeId, 0u, 0UL, "forged"), correlationId: 99);
        await Task.Delay(80);
        Assert.Equal(2, coordinator.Membership.ActiveNodeCount);
    }

    [Fact]
    public async Task Item2_ArrowShuffle_SecretAuth_AcceptsValidAndRejectsUnauthorized()
    {
        const string secret = "shuffle-m35-secret";
        using var store = new LocalPartitionStore(Guid.NewGuid());
        var jobId = Guid.NewGuid();
        var key = new PartitionKey(jobId, 1, 0);
        byte[] expected = new byte[4096];
        Random.Shared.NextBytes(expected);

        using (var buf = RefCountedPartitionBuffer.Allocate(expected.Length))
        {
            expected.CopyTo(buf.Span);
            store.RegisterPartition(key, buf);
        }

        await using var server = new ArrowShuffleServer(null, store, port: 0, clusterSecret: secret);
        server.Start();

        // Unauthorized client fails fast
        await using var badClient = new ArrowShuffleClient(clusterSecret: "wrong-secret");
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await badClient.FetchPartitionAsync("127.0.0.1", server.Port, jobId, 1, 0));

        // Authorized client succeeds
        await using var goodClient = new ArrowShuffleClient(clusterSecret: secret);
        using var fetched = await goodClient.FetchPartitionAsync("127.0.0.1", server.Port, jobId, 1, 0);
        Assert.True(expected.AsSpan().SequenceEqual(fetched.Span));
    }

    [Fact]
    public async Task Item2_DuplexConnection_Crc32Verification_AndNonBlockingUnsolicitedDispatch()
    {
        // 1. Non-blocking unsolicited dispatch: unsolicited handler sends a request back on SAME connection!
        var pC2S = new Pipe();
        var pS2C = new Pipe();
        DuplexConnection? srvRef = null;
        DuplexConnection? clientConnRef = null;
        var nestedResultTcs = new TaskCompletionSource<ClusterMessageType>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var srvConn = new DuplexConnection(
            Guid.NewGuid(),
            new PairDuplexPipe(pC2S.Reader, pS2C.Writer),
            unsolicitedFrameHandler: async (frame) =>
            {
                if (frame.Header.MessageType == ClusterMessageType.HeartbeatPing)
                {
                    await srvRef!.SendFrameAsync(
                        new HeartbeatPongMessage(Guid.NewGuid(), 0UL, 42UL, 0u, 1u),
                        frame.Header.CorrelationId);
                }
            });
        srvRef = srvConn;
        srvConn.Start();

        await using var cliConn = new DuplexConnection(
            Guid.NewGuid(),
            new PairDuplexPipe(pS2C.Reader, pC2S.Writer),
            unsolicitedFrameHandler: async (frame) =>
            {
                if (frame.Header.MessageType == ClusterMessageType.NodeLeave)
                {
                    using var reply = await clientConnRef!.SendRequestAsync(
                        new HeartbeatPingMessage(Guid.NewGuid(), 1UL, 0, 0f, 0UL),
                        correlationId: 500,
                        TimeSpan.FromSeconds(3));
                    nestedResultTcs.TrySetResult(reply.Header.MessageType);
                }
            });
        clientConnRef = cliConn;
        cliConn.Start();

        await srvConn.SendFrameAsync(new NodeLeaveMessage(Guid.NewGuid(), 0u, 0UL, "trigger"), correlationId: 100);
        var replyType = await nestedResultTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(ClusterMessageType.HeartbeatPong, replyType);

        // 2. Corrupted payload CRC32 throws InvalidDataException inside ReadLoop
        var crcPipeIn = new Pipe();
        var crcPipeOut = new Pipe();
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + 128];
        int packedLen = ClusterMessageSerializer.PackFrame(
            new HeartbeatPongMessage(Guid.NewGuid(), 0UL, 1UL, 0u, 1u),
            correlationId: 1,
            flags: ClusterFrameFlags.None,
            frameBytes);
        frameBytes[ClusterFrameHeader.HeaderSize] ^= 0xFF; // flip 1 payload byte after CRC computed

        await using var badConn = new DuplexConnection(Guid.NewGuid(), new PairDuplexPipe(crcPipeIn.Reader, crcPipeOut.Writer));
        badConn.Start();
        await crcPipeIn.Writer.WriteAsync(frameBytes.AsMemory(0, packedLen));
        await crcPipeIn.Writer.CompleteAsync();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await badConn.SendRequestAsync(
                new HeartbeatPingMessage(Guid.NewGuid(), 1UL, 0, 0f, 0UL),
                correlationId: 1,
                TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task Item2_DuplexConnection_Crc32Corruption_AlwaysPreservesInvalidDataInnerException()
    {
        byte[] frameBytes = new byte[ClusterFrameHeader.HeaderSize + 128];
        int packedLen = ClusterMessageSerializer.PackFrame(
            new HeartbeatPongMessage(Guid.NewGuid(), 0UL, 1UL, 0u, 1u),
            correlationId: 1,
            flags: ClusterFrameFlags.None,
            frameBytes);
        frameBytes[ClusterFrameHeader.HeaderSize] ^= 0xFF;

        for (int iter = 0; iter < 50; iter++)
        {
            var pipeIn = new Pipe();
            var pipeOut = new Pipe();
            await using var conn = new DuplexConnection(Guid.NewGuid(), new PairDuplexPipe(pipeIn.Reader, pipeOut.Writer));
            conn.Start();

            Task sendTask = conn.SendRequestAsync(
                new HeartbeatPingMessage(Guid.NewGuid(), 1UL, 0, 0f, 0UL),
                correlationId: 1,
                TimeSpan.FromSeconds(2)).AsTask();

            ReadResult outgoing = await pipeOut.Reader.ReadAsync();
            pipeOut.Reader.AdvanceTo(outgoing.Buffer.End);

            await pipeIn.Writer.WriteAsync(frameBytes.AsMemory(0, packedLen));
            await pipeIn.Writer.FlushAsync();

            var ex = await Assert.ThrowsAsync<IOException>(() => sendTask);
            Assert.IsType<InvalidDataException>(ex.InnerException);
            await pipeIn.Writer.CompleteAsync();
        }
    }

    [Fact]
    public async Task ClusterSimulator_DefaultsAndTransientJitter_KeepWorkersHealthy()
    {
        var opts = new ClusterSimulatorOptions();
        Assert.True(opts.CoordinatorOptions.DisableSocketListener);
        Assert.True(opts.FailureDetectorMinStdDevMs >= 250.0);

        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount: 4);
        Assert.Equal(4, simulator.Coordinator.Membership.ActiveNodeCount);

        await Task.Delay(300);

        var healthyProfiles = simulator.Coordinator.Membership.GetHealthyHardwareProfiles();
        Assert.Equal(4, healthyProfiles.Count);
    }

    [Fact]
    public async Task Item2_RingAllReduceWorker_ValidatesCtorAndCorruptHeaders()
    {
        var prevPipe = new Pipe();
        var nextPipe = new Pipe();
        var leftDuplex = new PairDuplexPipe(prevPipe.Reader, prevPipe.Writer);
        var rightDuplex = new PairDuplexPipe(nextPipe.Reader, nextPipe.Writer);

        Assert.Throws<ArgumentOutOfRangeException>(() => new RingAllReduceWorker(Guid.NewGuid(), rank: 0, worldSize: 0, rightDuplex, leftDuplex, totalElements: 16));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RingAllReduceWorker(Guid.NewGuid(), rank: -1, worldSize: 4, rightDuplex, leftDuplex, totalElements: 16));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RingAllReduceWorker(Guid.NewGuid(), rank: 4, worldSize: 4, rightDuplex, leftDuplex, totalElements: 16));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RingAllReduceWorker(Guid.NewGuid(), rank: 0, worldSize: 4, rightDuplex, leftDuplex, totalElements: -1));

        var sessionId = Guid.NewGuid();
        await using var worker = new RingAllReduceWorker(Guid.NewGuid(), rank: 0, worldSize: 2, rightDuplex, leftDuplex, totalElements: 4);

        // Write a malformed RingAllReduceStep frame with mismatched SessionId to predecessor pipe!
        byte[] payload = new byte[32 + 2 * sizeof(float)];
        Guid wrongSession = Guid.NewGuid();
        wrongSession.TryWriteBytes(payload.AsSpan(0, 16));
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(16, 4), 0); // Step
        payload[20] = 0; // Phase = Scatter-Reduce
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(24, 4), 1); // ChunkIndex (wrong)
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(28, 4), 2); // ElementCount

        uint crc = ClusterCrc32.Compute(payload);
        var hdr = new ClusterFrameHeader(ClusterMessageType.RingAllReduceStep, 1, (ulong)payload.Length, crc);
        byte[] frame = new byte[ClusterFrameHeader.HeaderSize + payload.Length];
        hdr.WriteTo(frame);
        payload.CopyTo(frame.AsSpan(ClusterFrameHeader.HeaderSize));
        await prevPipe.Writer.WriteAsync(frame);
        await prevPipe.Writer.FlushAsync();

        float[] localData = new float[] { 1f, 2f, 3f, 4f };
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await worker.AllReduceAsync(localData, sessionId));
    }

    // =========================================================================
    // Item 3: Split-Brain Consensus Lease Replication & Same-Epoch Hijack Guard
    // =========================================================================

    [Fact]
    public void Item3_ConsensusLedger_BlocksSameEpochHijack_AndReplicatesLease()
    {
        Guid leaderId = Guid.NewGuid();
        Guid attackerId = Guid.NewGuid();
        using var standby = new ActiveStandbyConsensusLedger(Guid.NewGuid(), new ConsensusOptions
        {
            InitialRole = ConsensusRole.Standby,
            LeaseDuration = TimeSpan.FromMilliseconds(5000),
            RenewInterval = TimeSpan.FromMilliseconds(500),
            GracePeriod = TimeSpan.FromMilliseconds(500)
        });

        // Initial lease accepted for epoch 1
        standby.UpdateLeaderLeaseFromRemote(leaderId, epoch: 1, remainingLeaseMs: 4000);
        Assert.Equal(1UL, standby.CurrentEpoch);
        Assert.Equal(leaderId, standby.CurrentLease.LeaderId);

        // Different leader attempting same-epoch takeover MUST be ignored!
        standby.UpdateLeaderLeaseFromRemote(attackerId, epoch: 1, remainingLeaseMs: 4000);
        Assert.Equal(leaderId, standby.CurrentLease.LeaderId);
        Assert.Equal(1UL, standby.CurrentEpoch);

        // Same leader renewing same epoch succeeds
        standby.UpdateLeaderLeaseFromRemote(leaderId, epoch: 1, remainingLeaseMs: 4500);
        Assert.Equal(leaderId, standby.CurrentLease.LeaderId);

        // Higher epoch transfers leadership cleanly
        standby.UpdateLeaderLeaseFromRemote(attackerId, epoch: 2, remainingLeaseMs: 4500);
        Assert.Equal(2UL, standby.CurrentEpoch);
        Assert.Equal(attackerId, standby.CurrentLease.LeaderId);
    }

    // =========================================================================
    // Item 4 & 5: Spill Isolation, Memory Cleanup & Backpressured >1 MB Shuffle
    // =========================================================================

    [Fact]
    public async Task Item4_SpillIsolation_StoreCleanup_PinnedArenaDispose_AndUnregisterSource()
    {
        Guid nodeA = Guid.NewGuid();
        Guid nodeB = Guid.NewGuid();
        using var spillA = new DiskSpillManager(nodeId: nodeA);
        using var spillB = new DiskSpillManager(nodeId: nodeB);
        Assert.NotEqual(spillA.ScratchDirectory, spillB.ScratchDirectory);

        using var storeB = new LocalPartitionStore(nodeB);
        var jobB = Guid.NewGuid();
        var keyB = new PartitionKey(jobB, 0, 0);
        using (var bufB = RefCountedPartitionBuffer.Allocate(256))
        {
            bufB.Span.Fill(0x5A);
            storeB.RegisterPartition(keyB, bufB);
            var meta = await spillB.SpillPartitionAsync(jobB, 0, 0, bufB);
            Assert.True(storeB.TransitionToSpilled(keyB, meta.FilePath));
        }
        Assert.True(storeB.TotalSpilledBytes > 0);

        // Disposing spillA must NOT wipe spillB's files!
        spillA.Dispose();
        using (var reloadedB = await storeB.FetchPartitionAsync(keyB))
        {
            Assert.Equal((byte)0x5A, reloadedB.Span[0]);
        }
        // Fetching from disk must delete spill file and decrement TotalSpilledBytes back to 0!
        Assert.Equal(0, storeB.TotalSpilledBytes);

        // PinnedBatchBufferArena releases backing arrays and throws ObjectDisposedException on Dispose
        var arena = new PinnedBatchBufferArena(maxBatchTokens: 8, hiddenDim: 16, vocabSize: 32);
        arena.Dispose();
        Assert.Throws<ObjectDisposedException>(() => arena.GetBatchSendSpan(4));
        Assert.Throws<ObjectDisposedException>(() => _ = arena.BatchWireSendArray);

        // UnregisterDataSource drops static references cleanly
        string uri = $"memory://m35_cleanup_{Guid.NewGuid():N}";
        var srcBuf = RefCountedPartitionBuffer.Allocate(64);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { srcBuf });
        srcBuf.Release();
        Assert.False(srcBuf.IsDisposed);
        Assert.True(InMemoryDistributedExecutionContext.UnregisterDataSource(uri));
        Assert.True(srcBuf.IsDisposed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Item5_ArrowShuffle_MultiChunkTransferOver1MB_WithTightCredits_CompletesWithoutDeadlock(bool enableCompression)
    {
        using var creditMgr = new FlowControlCreditManager(new FlowControlOptions
        {
            DefaultCreditTokensPerFetch = 4,
            MaxChunkSizeBytes = 65536
        });
        using var store = new LocalPartitionStore(Guid.NewGuid());
        var jobId = Guid.NewGuid();
        var key = new PartitionKey(jobId, 3, 0);

        // > 1.5 MB (> 24 chunks of 64 KB with only 4 credit tokens per window!)
        byte[] payload = new byte[1_572_864];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)((i * 31) ^ (i >> 8));
        }

        using (var buf = RefCountedPartitionBuffer.Allocate(payload.Length))
        {
            payload.CopyTo(buf.Span);
            store.RegisterPartition(key, buf);
        }

        await using var server = new ArrowShuffleServer(
            dataExchange: null,
            partitionStore: store,
            port: 0,
            creditManager: creditMgr,
            enableCompression: enableCompression);
        server.Start();

        await using var client = new ArrowShuffleClient();
        using var result = await client.FetchPartitionAsync("127.0.0.1", server.Port, jobId, 3, 0)
            .AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(payload.Length, result.ByteLength);
        Assert.True(payload.AsSpan().SequenceEqual(result.Span));
    }

    // =========================================================================
    // Item 6: PredicateCompiler, StageCompiler Fusion & Distributed Parity
    // =========================================================================

    [Fact]
    public void Item6_PredicateCompiler_CompoundAndQuotedAndNullChecks_WorkEndToEnd()
    {
        var deptCol = new Utf8StringSeries("dept", new[] { "R&D | Core", "Sales", "R&D | Core", "Ops" });
        var noteCol = new Utf8StringSeries("note", new[] { "ok", "a'b", "other", "a'b" });
        var ageCol = new Int32Series("age", new[] { 35, 22, 18, 40 });
        using var df = new DataFrame(new ISeries[] { deptCol, noteCol, ageCol });

        var expr = PredicateCompiler.Compile("(age >= 30 AND dept == 'R&D | Core') OR (note == 'a''b' AND age > 30)");
        using var filtered = df.Filter(expr);
        Assert.Equal(2, filtered.RowCount); // Row 0 (35, R&D | Core) and Row 3 (40, a'b)

        // Pipeline splitter preserves '|' inside quotes
        var split = PredicateCompiler.SplitPipelinePayload("memory://ds|dept == 'R&D | Core'|age >= 30");
        Assert.Equal(3, split.Count);
        Assert.Equal("dept == 'R&D | Core'", split[1]);

        // IS NULL / IS NOT NULL compile without error
        Assert.NotNull(PredicateCompiler.Compile("note IS NOT NULL"));
        Assert.NotNull(PredicateCompiler.Compile("NOT (age < 10 OR note IS NULL)"));

        // StageCompiler preserves Scan + Filter payload when fused with GroupByMap
        var compiler = new StageCompiler();
        var plan = new LogicalGroupByNode(
            new LogicalFilterNode(new LogicalScanNode("orders.parquet", 4), "dept == 'R&D | Core'"),
            new[] { "dept" },
            new[] { "SUM(age)" },
            targetPartitions: 4);
        var dag = compiler.Compile(Guid.NewGuid(), "FusedGroupBy", plan);
        Assert.Equal(2, dag.Stages.Count);
        var mapStage = dag.Stages.Values.First(s => s.PrimaryOperator == StageOperatorType.GroupByMap);
        string mapPayload = Encoding.UTF8.GetString(mapStage.OperatorPayload.Span);
        Assert.StartsWith("orders.parquet|dept == 'R&D | Core'\u001fGROUPBY:dept", mapPayload);

        // StageCompiler with autoRepartitionJoins: true inserts Repartition for non-co-partitioned scans,
        // and skips Repartition when inputs are already co-partitioned on JoinKey.
        var autoCompiler = new StageCompiler(autoRepartitionJoins: true);
        var unpartJoin = new LogicalHashJoinNode(
            new LogicalScanNode("a.parquet", 4),
            new LogicalScanNode("b.parquet", 4),
            "customer_id",
            targetPartitions: 4);
        var unpartDag = autoCompiler.Compile(Guid.NewGuid(), "AutoRepartJoin", unpartJoin);
        Assert.Equal(2, unpartDag.Stages.Values.Count(s => s.PrimaryOperator == StageOperatorType.Custom));

        var coPartJoin = new LogicalHashJoinNode(
            new LogicalRepartitionNode(new LogicalScanNode("a.parquet", 4), 4, new[] { "customer_id" }),
            new LogicalRepartitionNode(new LogicalScanNode("b.parquet", 4), 4, new[] { "customer_id" }),
            "customer_id",
            targetPartitions: 4);
        var coPartDag = autoCompiler.Compile(Guid.NewGuid(), "CoPartJoin", coPartJoin);
        // Should still have ONLY the 2 explicit repartition stages (no redundant repartitions added!)
        Assert.Equal(2, coPartDag.Stages.Values.Count(s => s.PrimaryOperator == StageOperatorType.Custom));
    }

    [Fact]
    public async Task Item6_DistributedParquetScanAndShardWrites_PreserveExactRows()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"glacier_m35_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        string inputParquet = Path.Combine(tempDir, "input.parquet");
        string shardOutDir = Path.Combine(tempDir, "parquet_shards");
        string arrowOutDir = Path.Combine(tempDir, "arrow_shards");

        try
        {
            var ids = new Int32Series("id", Enumerable.Range(0, 100).ToArray());
            var depts = new Utf8StringSeries("dept", Enumerable.Range(0, 100).Select(i => i % 2 == 0 ? "eng" : "ops").ToArray());
            var vals = new Float64Series("val", Enumerable.Range(0, 100).Select(i => (double)i).ToArray());
            using (var srcDf = new DataFrame(new ISeries[] { ids, depts, vals }))
            {
                srcDf.WriteParquet(inputParquet);
            }

            await using var sim = await ClusterSimulator.CreateAsync(nodeCount: 4);
            using var workerCtx = new ClusterSimulatorExecutionContext(sim);
            var schema = new Glacier.Formats.Arrow.ArrowSchema(new[]
            {
                new Glacier.Formats.Arrow.ArrowField("id", Glacier.Formats.Arrow.ArrowType.Int32, false),
                new Glacier.Formats.Arrow.ArrowField("dept", Glacier.Formats.Arrow.ArrowType.Utf8, false),
                new Glacier.Formats.Arrow.ArrowField("val", Glacier.Formats.Arrow.ArrowType.Double, false)
            });

            using var ddf = DistributedDataFrameFactory.FromDataSource(inputParquet, partitionCount: 4, schema, workerCtx, "ParquetInput");
            var filteredDdf = ddf.Filter("id >= 50");

            await filteredDdf.WriteParquetAsync(shardOutDir);
            await filteredDdf.WriteArrowIpcAsync(arrowOutDir);

            string[] pqShards = Directory.GetFiles(shardOutDir, "part-*.parquet");
            Assert.Equal(2, pqShards.Length); // Only partitions 2 & 3 have id >= 50 rows

            int totalParquetRows = 0;
            foreach (string shard in pqShards)
            {
                var reader = new Glacier.Polaris.IO.ParquetReader(shard);
                await foreach (var rg in reader.ReadAsync())
                {
                    totalParquetRows += rg.RowCount;
                    rg.Dispose();
                }
            }
            Assert.Equal(50, totalParquetRows);

            string[] arrowShards = Directory.GetFiles(arrowOutDir, "part-*.arrow");
            Assert.Equal(4, arrowShards.Length);
        }
        finally
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
        }
    }

    // =========================================================================
    // Item 7: Oversized Frame Check Before Full Buffer & FusedSocket DrainPayload
    // =========================================================================

    [Fact]
    public async Task Item7_ClusterFrameReader_RejectsOversizedBeforeBuffering_AndFusedSocketDrains()
    {
        var oversizedHeader = new ClusterFrameHeader(
            ClusterMessageType.StageDispatch,
            correlationId: 1,
            payloadLength: ClusterFrameReader.DefaultMaxFrameBytes + 1024,
            crc32: 0);
        byte[] headerOnly = new byte[ClusterFrameHeader.HeaderSize];
        oversizedHeader.WriteTo(headerOnly);

        Assert.Throws<InvalidOperationException>(() =>
        {
            var seq = new ReadOnlySequence<byte>(headerOnly);
            ClusterFrameReader.TryReadFrame(ref seq, out _, out _);
        });

        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var acceptTask = listener.AcceptSocketAsync();
        using var clientSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await clientSock.ConnectAsync(IPAddress.Loopback, port);
        using var serverSock = await acceptTask;

        // Send two result frames: first 16 floats (oversized for tiny 48B recv buf), second 2 floats
        float[] frame1 = Enumerable.Range(0, 16).Select(i => (float)i).ToArray();
        float[] frame2 = new float[] { 42.5f, 99.25f };
        byte[] staging = new byte[512];
        var hdr1 = new FusedActivationResultHeader(0, 0, 1.0, 1, 1.0f);
        var hdr2 = new FusedActivationResultHeader(1, 0, 1.0, 2, 2.0f);
        FusedSocketTransport.SendResult(serverSock, 1UL, in hdr1, MemoryMarshal.AsBytes(frame1.AsSpan()), staging);
        FusedSocketTransport.SendResult(serverSock, 2UL, in hdr2, MemoryMarshal.AsBytes(frame2.AsSpan()), staging);

        // Undersized buffer for frame #1 must return false AND drain payload cleanly
        byte[] tinyBuf = new byte[48]; // >= 32B header, < 64B combined header
        bool ok1 = FusedSocketTransport.ReceiveResult(clientSock, tinyBuf, out _, out _, out _);
        Assert.False(ok1);

        // Next ReceiveResult must read frame #2 cleanly without stream desynchronization!
        byte[] fullBuf = new byte[1024];
        bool ok2 = FusedSocketTransport.ReceiveResult(clientSock, fullBuf, out _, out var rxHdr2, out var payload2);
        Assert.True(ok2);
        Assert.Equal(2, rxHdr2.NextTokenId);
        Assert.Equal(frame2.Length * sizeof(float), payload2.Length);
    }

    // =========================================================================
    // Items 8, 9, 10: ClusterJsonContext, ProbeLocal, Ring Topology, Opt #20
    // =========================================================================

    [Fact]
    public void Item8_ClusterJsonContext_RoundTripsAllDtos_AotSafe()
    {
        var workerOpts = new ClusterWorkerOptions { ShufflePort = 51234, ClusterSecret = "s", MaxConcurrentTasks = 4 };
        string workerJson = JsonSerializer.Serialize(workerOpts, ClusterJsonContext.Default.ClusterWorkerOptions);
        var workerRt = JsonSerializer.Deserialize(workerJson, ClusterJsonContext.Default.ClusterWorkerOptions);
        Assert.Equal((ushort)51234, workerRt!.ShufflePort);
        Assert.Equal(4, workerRt.MaxConcurrentTasks);

        var lease = new LeaderLease(Guid.NewGuid(), epoch: 7, DateTimeOffset.UtcNow.AddSeconds(5));
        string leaseJson = JsonSerializer.Serialize(lease, ClusterJsonContext.Default.LeaderLease);
        var leaseRt = JsonSerializer.Deserialize(leaseJson, ClusterJsonContext.Default.LeaderLease);
        Assert.Equal(7UL, leaseRt.Epoch);

        var metrics = new SpeculativePipelineMetrics { SpeculativeAttempts = 10, SpeculativeHits = 8 };
        string metricsJson = JsonSerializer.Serialize(metrics, ClusterJsonContext.Default.SpeculativePipelineMetrics);
        var metricsRt = JsonSerializer.Deserialize(metricsJson, ClusterJsonContext.Default.SpeculativePipelineMetrics);
        Assert.Equal(8L, metricsRt!.SpeculativeHits);
    }

    [Fact]
    public void Item9_TopologyOrdering_GroupsBySubnet24_AndProbeLocalHonest()
    {
        var n1 = NodeHardwareProfile.ProbeLocal(new Guid("00000000-0000-0000-0000-000000000003"), 0x0A000205); // 10.0.2.5
        var n2 = NodeHardwareProfile.ProbeLocal(new Guid("00000000-0000-0000-0000-000000000002"), 0x0A000105); // 10.0.1.5
        var n3 = NodeHardwareProfile.ProbeLocal(new Guid("00000000-0000-0000-0000-000000000001"), 0x0A000201); // 10.0.2.1

        var ordered = RingAllReduceCoordinator.OrderNodesTopologyAware(new[] { n1, n2, n3 });
        // 10.0.1.0/24 (n2) precedes 10.0.2.0/24 (n3 then n1 by NodeId)
        Assert.Equal(n2.NodeId, ordered[0].NodeId);
        Assert.Equal(n3.NodeId, ordered[1].NodeId);
        Assert.Equal(n1.NodeId, ordered[2].NodeId);
    }

    [Fact]
    public async Task Item10_Opt20_SimdXorDeltaCodec_UdpReassembler_AndTcpControlChannel()
    {
        // 1. SimdXorDeltaCodec: Keyframe + Delta round-trip + SIMD XOR parity
        using var encoder = new SimdXorDeltaCodec();
        using var decoder = new SimdXorDeltaCodec();
        byte[] frameA = new byte[1027]; // non-power-of-2 misaligned length
        byte[] frameB = new byte[1027];
        Random.Shared.NextBytes(frameA);
        frameA.CopyTo(frameB, 0);
        for (int i = 100; i < 160; i++) frameB[i] ^= 0x3C;

        byte[] encoded = new byte[encoder.GetMaxEncodedSize(frameA.Length)];
        byte[] decoded = new byte[frameA.Length];

        int enc1 = encoder.Encode(frameA, forceKeyFrame: true, encoded, out bool kf1);
        Assert.True(kf1);
        Assert.True(decoder.Decode(encoded.AsSpan(0, enc1), kf1, frameA.Length, decoded));
        Assert.True(frameA.AsSpan().SequenceEqual(decoded));

        int enc2 = encoder.Encode(frameB, forceKeyFrame: false, encoded, out bool kf2);
        Assert.False(kf2);
        Assert.True(enc2 < enc1); // Delta compresses nearly-identical payload much tighter than keyframe!
        Assert.True(decoder.Decode(encoded.AsSpan(0, enc2), kf2, frameB.Length, decoded));
        Assert.True(frameB.AsSpan().SequenceEqual(decoded));

        // 2. ClusterUdpFrameReassembler: Out-of-order, duplicate bitmask, wraparound & resync
        using var reassembler = new ClusterUdpFrameReassembler(slotCount: 4, maxDatagramBytes: 512);
        byte[] bigFrame = new byte[1200];
        Random.Shared.NextBytes(bigFrame);
        uint crc = ClusterCrc32.Compute(bigFrame);
        int sliceCap = reassembler.MaxSlicePayloadBytes;
        ushort totalPkts = (ushort)((bigFrame.Length + sliceCap - 1) / sliceCap);

        byte[][] datagrams = new byte[totalPkts][];
        for (ushort p = 0; p < totalPkts; p++)
        {
            int offset = p * sliceCap;
            int len = Math.Min(sliceCap, bigFrame.Length - offset);
            datagrams[p] = new byte[64 + sliceCap];
            int dgLen = ClusterUdpFrameReassembler.BuildSlice(
                datagrams[p],
                frameId: 65534,
                totalPackets: totalPkts,
                packetIndex: p,
                totalPayloadBytes: (uint)bigFrame.Length,
                slicePayload: bigFrame.AsSpan(offset, len),
                flags: ClusterUdpFrameReassembler.FlagKeyFrame,
                fullFrameCrc32: crc);
            datagrams[p] = datagrams[p].AsSpan(0, dgLen).ToArray();
        }

        // Deliver out-of-order + duplicate first packet
        for (int p = totalPkts - 1; p >= 1; p--)
        {
            Assert.Equal(ClusterUdpReassembleStatus.Pending, reassembler.ProcessDatagram(datagrams[p], out _));
        }
        Assert.Equal(ClusterUdpReassembleStatus.Pending, reassembler.ProcessDatagram(datagrams[totalPkts - 1], out _)); // dup
        Assert.Equal(ClusterUdpReassembleStatus.FrameComplete, reassembler.ProcessDatagram(datagrams[0], out var completed));
        Assert.True(bigFrame.AsSpan().SequenceEqual(completed.Payload));

        // Wraparound / gap sets NeedsResync
        byte[] wrappedNext = new byte[512];
        int wrapLen = ClusterUdpFrameReassembler.BuildSlice(
            wrappedNext, frameId: 65538, totalPackets: 1, packetIndex: 0,
            totalPayloadBytes: (uint)bigFrame.Length, slicePayload: bigFrame.AsSpan(0, 100),
            flags: 0, fullFrameCrc32: ClusterCrc32.Compute(bigFrame.AsSpan(0, 100)));
        reassembler.ProcessDatagram(wrappedNext.AsSpan(0, wrapLen), out _);
        Assert.True(reassembler.NeedsResync);

        // 3. TcpControlChannel loopback messaging + CRC32 validation
        using var tcpListener = new TcpListener(IPAddress.Loopback, 0);
        tcpListener.Start();
        int ctlPort = ((IPEndPoint)tcpListener.LocalEndpoint).Port;

        var acceptCtl = tcpListener.AcceptSocketAsync();
        using var cSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await cSock.ConnectAsync(IPAddress.Loopback, ctlPort);
        using var sSock = await acceptCtl;

        await using var tx = new TcpControlChannel(cSock);
        await using var rx = new TcpControlChannel(sSock);

        var receivedTcs = new TaskCompletionSource<ClusterMessageType>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rxCts = new CancellationTokenSource();
        var rxTask = rx.ReadLoopAsync((hdr, _) => receivedTcs.TrySetResult(hdr.MessageType), cancellationToken: rxCts.Token);

        await tx.SendMessageAsync(new HeartbeatPingMessage(Guid.NewGuid(), 99UL, 2, 12.5f, 4096UL));
        var msgType = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(ClusterMessageType.HeartbeatPing, msgType);

        await rxCts.CancelAsync();
        try { await rxTask; } catch (OperationCanceledException) { }
    }

    // =========================================================================
    // M3.5 R2 Hardening Regressions (UDP Header Consistency, Auth Rebinding,
    // Guid.Empty Join, Shuffle Length Consistency, Probe Request CRC32)
    // =========================================================================

    [Fact]
    public void R2_UdpReassembler_RejectsConflictingSlotMetadata_FreshAndPooledBuffer()
    {
        // 1. Exact Challenger 8C repro on fresh reassembler + after large pooled buffer reuse
        using var reassembler = new ClusterUdpFrameReassembler(slotCount: 4, maxDatagramBytes: 256);

        // Warm slot buffers with a larger 4096-byte frame so ArrayPool buffer exceeds 16 bytes
        byte[] largePayload = new byte[4096];
        Random.Shared.NextBytes(largePayload);
        uint largeCrc = ClusterCrc32.Compute(largePayload);
        int sliceCap = reassembler.MaxSlicePayloadBytes; // 232 bytes
        ushort largePkts = (ushort)((largePayload.Length + sliceCap - 1) / sliceCap);
        byte[] dg = new byte[256];
        for (ushort p = 0; p < largePkts; p++)
        {
            int off = p * sliceCap;
            int len = Math.Min(sliceCap, largePayload.Length - off);
            int dgLen = ClusterUdpFrameReassembler.BuildSlice(
                dg, frameId: 1, totalPackets: largePkts, packetIndex: p,
                totalPayloadBytes: (uint)largePayload.Length,
                slicePayload: largePayload.AsSpan(off, len),
                flags: 0, fullFrameCrc32: largeCrc);
            var st = reassembler.ProcessDatagram(dg.AsSpan(0, dgLen), out _);
            Assert.Equal(p == largePkts - 1 ? ClusterUdpReassembleStatus.FrameComplete : ClusterUdpReassembleStatus.Pending, st);
        }

        // Now send slice 0 of frameId=100 (totalPackets=2, totalPayloadBytes=16, 8 bytes payload)
        byte[] smallSlice0 = new byte[8];
        Random.Shared.NextBytes(smallSlice0);
        int s0Len = ClusterUdpFrameReassembler.BuildSlice(
            dg, frameId: 100, totalPackets: 2, packetIndex: 0,
            totalPayloadBytes: 16, slicePayload: smallSlice0, flags: 0, fullFrameCrc32: 0);
        Assert.Equal(ClusterUdpReassembleStatus.Pending, reassembler.ProcessDatagram(dg.AsSpan(0, s0Len), out _));

        // Conflicting slice 1 claiming totalPayloadBytes=10000 MUST be Ignored (neither throw nor falsely complete!)
        byte[] bogusSlice1 = new byte[232];
        Random.Shared.NextBytes(bogusSlice1);
        int s1BadLen = ClusterUdpFrameReassembler.BuildSlice(
            dg, frameId: 100, totalPackets: 2, packetIndex: 1,
            totalPayloadBytes: 10000, slicePayload: bogusSlice1, flags: 0, fullFrameCrc32: 0);
        Assert.Equal(ClusterUdpReassembleStatus.Ignored, reassembler.ProcessDatagram(dg.AsSpan(0, s1BadLen), out _));

        // 2. Verify TotalPackets, Flags, and PayloadCrc32 mismatches are Ignored and valid slice 1 still completes
        using var r2 = new ClusterUdpFrameReassembler(slotCount: 4, maxDatagramBytes: 32); // MaxSlicePayloadBytes = 8
        byte[] exact16 = new byte[16];
        Random.Shared.NextBytes(exact16);
        uint crc16 = ClusterCrc32.Compute(exact16);
        byte[] d32 = new byte[32];

        int d0Len = ClusterUdpFrameReassembler.BuildSlice(
            d32, frameId: 100, totalPackets: 2, packetIndex: 0,
            totalPayloadBytes: 16, slicePayload: exact16.AsSpan(0, 8),
            flags: ClusterUdpFrameReassembler.FlagKeyFrame, fullFrameCrc32: crc16);
        Assert.Equal(ClusterUdpReassembleStatus.Pending, r2.ProcessDatagram(d32.AsSpan(0, d0Len), out _));

        // Mismatched TotalPackets
        int badPktsLen = ClusterUdpFrameReassembler.BuildSlice(
            d32, frameId: 100, totalPackets: 3, packetIndex: 1,
            totalPayloadBytes: 16, slicePayload: exact16.AsSpan(8, 8),
            flags: ClusterUdpFrameReassembler.FlagKeyFrame, fullFrameCrc32: crc16);
        Assert.Equal(ClusterUdpReassembleStatus.Ignored, r2.ProcessDatagram(d32.AsSpan(0, badPktsLen), out _));

        // Mismatched Flags
        int badFlagsLen = ClusterUdpFrameReassembler.BuildSlice(
            d32, frameId: 100, totalPackets: 2, packetIndex: 1,
            totalPayloadBytes: 16, slicePayload: exact16.AsSpan(8, 8),
            flags: 0, fullFrameCrc32: crc16);
        Assert.Equal(ClusterUdpReassembleStatus.Ignored, r2.ProcessDatagram(d32.AsSpan(0, badFlagsLen), out _));

        // Mismatched PayloadCrc32
        int badCrcLen = ClusterUdpFrameReassembler.BuildSlice(
            d32, frameId: 100, totalPackets: 2, packetIndex: 1,
            totalPayloadBytes: 16, slicePayload: exact16.AsSpan(8, 8),
            flags: ClusterUdpFrameReassembler.FlagKeyFrame, fullFrameCrc32: crc16 ^ 0x12345678u);
        Assert.Equal(ClusterUdpReassembleStatus.Ignored, r2.ProcessDatagram(d32.AsSpan(0, badCrcLen), out _));

        // Valid matching slice 1 completes cleanly with exact payload
        int goodD1Len = ClusterUdpFrameReassembler.BuildSlice(
            d32, frameId: 100, totalPackets: 2, packetIndex: 1,
            totalPayloadBytes: 16, slicePayload: exact16.AsSpan(8, 8),
            flags: ClusterUdpFrameReassembler.FlagKeyFrame, fullFrameCrc32: crc16);
        Assert.Equal(ClusterUdpReassembleStatus.FrameComplete, r2.ProcessDatagram(d32.AsSpan(0, goodD1Len), out var completed));
        Assert.True(exact16.AsSpan().SequenceEqual(completed.Payload));
    }

    [Fact]
    public async Task R2_Coordinator_RejectsCrossNodeIdRebinding_AndBindsEmptyNodeIdToAssignedId()
    {
        const string secret = "m35-r2-coordinator-secret";
        await using var coordinator = new ClusterCoordinator(new ClusterCoordinatorOptions
        {
            BindAddress = IPAddress.Loopback,
            Port = 0,
            ClusterSecret = secret
        });
        await coordinator.StartAsync();

        // 1. Legitimate W1 and W2 join
        var w1Id = Guid.NewGuid();
        var w2Id = Guid.NewGuid();
        var w1Profile = NodeHardwareProfile.ProbeLocal(w1Id, 0x7F000001);
        var w2Profile = NodeHardwareProfile.ProbeLocal(w2Id, 0x7F000001);

        var s1 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await s1.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var p1 = new SocketDuplexPipe(s1);
        await using var c1 = new DuplexConnection(Guid.Empty, p1);
        c1.Start();

        using (var j1 = await c1.SendRequestAsync(new NodeJoinRequestMessage(w1Profile, "w1", secret, 53001), 1, TimeSpan.FromSeconds(3)))
        {
            var r1 = (NodeJoinResponseMessage)ClusterMessageSerializer.Deserialize(j1.Header.MessageType, j1.Payload.Span);
            Assert.Equal(NodeJoinStatus.Accepted, r1.Status);
            Assert.Equal(w1Id, r1.AssignedNodeId);
        }

        var s2 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await s2.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var p2 = new SocketDuplexPipe(s2);
        await using var c2 = new DuplexConnection(Guid.Empty, p2);
        c2.Start();

        using (var j2 = await c2.SendRequestAsync(new NodeJoinRequestMessage(w2Profile, "w2", secret, 53002), 1, TimeSpan.FromSeconds(3)))
        {
            var r2 = (NodeJoinResponseMessage)ClusterMessageSerializer.Deserialize(j2.Header.MessageType, j2.Payload.Span);
            Assert.Equal(NodeJoinStatus.Accepted, r2.Status);
            Assert.Equal(w2Id, r2.AssignedNodeId);
        }

        // W1 attempts second NodeJoinRequest on the already-authenticated connection claiming w2Id -> MUST be Unauthorized & closed
        using (var rebindResp = await c1.SendRequestAsync(
            new NodeJoinRequestMessage(w2Profile, "w1-impersonating-w2", secret, 53001),
            correlationId: 2,
            TimeSpan.FromSeconds(3)))
        {
            var rej = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(rebindResp.Header.MessageType, rebindResp.Payload.Span));
            Assert.Equal(NodeJoinStatus.Unauthorized, rej.Status);
        }

        await Task.Delay(80);
        // Socket s1 must have been closed by coordinator
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await c1.SendRequestAsync(new HeartbeatPingMessage(w1Id, 1UL, 0, 0f, 0UL), correlationId: 3, TimeSpan.FromSeconds(2)));

        // W2 must still be connected & healthy via its own socket
        using (var pong2 = await c2.SendRequestAsync(new HeartbeatPingMessage(w2Id, 1UL, 0, 0f, 0UL), correlationId: 2, TimeSpan.FromSeconds(3)))
        {
            Assert.Equal(ClusterMessageType.HeartbeatPong, pong2.Header.MessageType);
        }

        // 2. Client joining with Guid.Empty gets AssignedNodeId and subsequent HeartbeatPing using AssignedNodeId succeeds
        var emptyProfile = new NodeHardwareProfile(
            nodeId: Guid.Empty,
            cpuLogicalCores: 4,
            simdFlags: SimdArchitectureFlags.None,
            gpuFlags: GpuBackendFlags.None,
            ramTotalBytes: 8UL * 1024 * 1024 * 1024,
            ramAvailableBytes: 4UL * 1024 * 1024 * 1024,
            vramTotalBytes: 0UL,
            vramAvailableBytes: 0UL,
            hostIpv4Address: 0x7F000001);

        var s3 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await s3.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var p3 = new SocketDuplexPipe(s3);
        await using var c3 = new DuplexConnection(Guid.Empty, p3);
        c3.Start();

        Guid assignedId;
        using (var j3 = await c3.SendRequestAsync(new NodeJoinRequestMessage(emptyProfile, "w-empty", secret, 53003), 1, TimeSpan.FromSeconds(3)))
        {
            var r3 = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(j3.Header.MessageType, j3.Payload.Span));
            Assert.Equal(NodeJoinStatus.Accepted, r3.Status);
            Assert.NotEqual(Guid.Empty, r3.AssignedNodeId);
            assignedId = r3.AssignedNodeId;
        }

        using (var pong3 = await c3.SendRequestAsync(
            new HeartbeatPingMessage(assignedId, 1UL, 0, 1.0f, 1024UL),
            correlationId: 2,
            TimeSpan.FromSeconds(3)))
        {
            Assert.Equal(ClusterMessageType.HeartbeatPong, pong3.Header.MessageType);
        }
    }

    [Fact]
    public async Task R2_ShuffleClient_RejectsMidStreamTotalLengthMismatch_AndCalibratorChecksProbeCrc32()
    {
        // 1. ArrowShuffleClient rejects mid-stream TotalDataLength change cleanly
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var jobId = Guid.NewGuid();
        var clientDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverTask = Task.Run(async () =>
        {
            using var srvSock = await listener.AcceptSocketAsync();
            await using var pipe = new SocketDuplexPipe(srvSock);

            // Read PartitionFetchRequest frame
            while (true)
            {
                var readRes = await pipe.Input.ReadAsync();
                var buf = readRes.Buffer;
                if (ClusterFrameReader.TryReadFrame(ref buf, out var reqHdr, out _))
                {
                    pipe.Input.AdvanceTo(buf.Start, buf.End);

                    byte[] chunkData = new byte[64];
                    var resp1 = new PartitionFetchResponseMessage(
                        jobId, 1, 0, dataByteLength: 128,
                        PartitionFetchStatus.Success, PartitionDataFlags.None,
                        chunkOffset: 0, data: chunkData);
                    var resp2 = new PartitionFetchResponseMessage(
                        jobId, 1, 0, dataByteLength: 256, // tampered mid-stream!
                        PartitionFetchStatus.Success, PartitionDataFlags.FinalChunk,
                        chunkOffset: 64, data: chunkData);

                    byte[] outBuf = new byte[512];
                    int len1 = ClusterMessageSerializer.PackFrame(resp1, reqHdr.CorrelationId, ClusterFrameFlags.None, outBuf);
                    await pipe.Output.WriteAsync(outBuf.AsMemory(0, len1));
                    int len2 = ClusterMessageSerializer.PackFrame(resp2, reqHdr.CorrelationId, ClusterFrameFlags.None, outBuf);
                    await pipe.Output.WriteAsync(outBuf.AsMemory(0, len2));
                    await pipe.Output.FlushAsync();
                    await clientDone.Task.WaitAsync(TimeSpan.FromSeconds(5));
                    break;
                }
                pipe.Input.AdvanceTo(buf.Start, buf.End);
                if (readRes.IsCompleted) break;
            }
        });

        await using var client = new ArrowShuffleClient();
        try
        {
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await client.FetchPartitionAsync("127.0.0.1", (ushort)port, jobId, 1, 0));
        }
        finally
        {
            clientDone.TrySetResult();
        }
        await serverTask;

        // 2. DynamicThroughputCalibrator.HandleProbeRequestAsync verifies inbound PayloadCrc32
        byte[] probePayload = new byte[24];
        BinaryPrimitives.WriteInt32LittleEndian(probePayload.AsSpan(0, 4), 64);
        BinaryPrimitives.WriteInt32LittleEndian(probePayload.AsSpan(4, 4), 128);
        BinaryPrimitives.WriteInt32LittleEndian(probePayload.AsSpan(8, 4), 256);
        BinaryPrimitives.WriteInt32LittleEndian(probePayload.AsSpan(12, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(probePayload.AsSpan(16, 4), 1);
        uint goodCrc = ClusterCrc32.Compute(probePayload);
        var badReqHdr = new ClusterFrameHeader(
            DynamicThroughputCalibrator.MsgCalibrateProbeRequest,
            correlationId: 7,
            payloadLength: (ulong)probePayload.Length,
            crc32: goodCrc ^ 0xFFFFFFFFu);

        using var ms = new MemoryStream(probePayload);
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await DynamicThroughputCalibrator.HandleProbeRequestAsync(ms, badReqHdr));
    }

    // =========================================================================
    // M3.5 R3 Hardening Regressions (Value-Checked Connection Removal,
    // Missing Connection Fail-Fast, Fused Operator Intermediate Cleanup)
    // =========================================================================

    [Fact]
    public async Task R3_Coordinator_OldSocketRejection_DoesNotEvictReconnectedWorker()
    {
        const string secret = "m35-r3-reconnect-secret";
        await using var coordinator = new ClusterCoordinator(new ClusterCoordinatorOptions
        {
            BindAddress = IPAddress.Loopback,
            Port = 0,
            ClusterSecret = secret
        });
        await coordinator.StartAsync();

        var w3Id = Guid.NewGuid();
        var otherId = Guid.NewGuid();
        var w3Profile = NodeHardwareProfile.ProbeLocal(w3Id, 0x7F000001);
        var otherProfile = NodeHardwareProfile.ProbeLocal(otherId, 0x7F000001);

        // 1. Connect cOld and join as W3
        var sOld = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await sOld.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var pOld = new SocketDuplexPipe(sOld);
        await using var cOld = new DuplexConnection(Guid.Empty, pOld);
        cOld.Start();

        using (var jOld = await cOld.SendRequestAsync(new NodeJoinRequestMessage(w3Profile, "w3", secret, 53101), 1, TimeSpan.FromSeconds(3)))
        {
            var rOld = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(jOld.Header.MessageType, jOld.Payload.Span));
            Assert.Equal(NodeJoinStatus.Accepted, rOld.Status);
            Assert.Equal(w3Id, rOld.AssignedNodeId);
        }

        // 2. Reconnect W3 on cNew with StageDispatch handler
        var sNew = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await sNew.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var pNew = new SocketDuplexPipe(sNew);
        DuplexConnection? cNewRef = null;
        await using var cNew = new DuplexConnection(
            Guid.Empty,
            pNew,
            unsolicitedFrameHandler: async frame =>
            {
                if (frame.Header.MessageType == ClusterMessageType.StageDispatch)
                {
                    var dispatch = (StageDispatchMessage)ClusterMessageSerializer.Deserialize(
                        frame.Header.MessageType, frame.Payload.Span);
                    var done = new TaskStatusUpdateMessage(
                        dispatch.JobId,
                        dispatch.StageId,
                        dispatch.PartitionId,
                        TaskExecutionStatus.Completed,
                        elapsedMs: 1.0,
                        outputBytes: 64UL,
                        outputRowCount: 4UL);
                    await cNewRef!.SendFrameAsync(done, frame.Header.CorrelationId).ConfigureAwait(false);
                }
            });
        cNewRef = cNew;
        cNew.Start();

        using (var jNew = await cNew.SendRequestAsync(new NodeJoinRequestMessage(w3Profile, "w3", secret, 53102), 1, TimeSpan.FromSeconds(3)))
        {
            var rNew = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(jNew.Header.MessageType, jNew.Payload.Span));
            Assert.Equal(NodeJoinStatus.Accepted, rNew.Status);
            Assert.Equal(w3Id, rNew.AssignedNodeId);
        }

        // 3. Baseline SubmitJobAsync succeeds via cNew
        Guid baselineJobId = Guid.NewGuid();
        var baselineDag = new ExecutionDag(
            baselineJobId,
            "BaselineReconnectJob",
            new[]
            {
                new ExecutionStage(
                    stageId: 0,
                    jobId: baselineJobId,
                    stageName: "Scan",
                    isShuffleBoundary: false,
                    partitionsCount: 1,
                    primaryOperator: StageOperatorType.Scan,
                    operatorPayload: Encoding.UTF8.GetBytes("memory://baseline"))
            });

        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
        {
            var baselineResult = await coordinator.SubmitJobAsync(baselineDag, cts.Token);
            Assert.Equal(JobCompletionStatus.Succeeded, baselineResult.Status);
        }

        // 4. Send unauthorized cross-NodeId NodeJoinRequest on cOld -> triggers CloseRejectedConnectionAsync(cOld)
        using (var rejResp = await cOld.SendRequestAsync(
            new NodeJoinRequestMessage(otherProfile, "w3-spoofing-other", secret, 53101),
            correlationId: 2,
            TimeSpan.FromSeconds(3)))
        {
            var rej = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(rejResp.Header.MessageType, rejResp.Payload.Span));
            Assert.Equal(NodeJoinStatus.Unauthorized, rej.Status);
        }

        await Task.Delay(80);

        // cOld must be closed by coordinator, while cNew remains alive for W3
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await cOld.SendRequestAsync(new HeartbeatPingMessage(w3Id, 1UL, 0, 0f, 0UL), correlationId: 3, TimeSpan.FromSeconds(2)));

        using (var pong = await cNew.SendRequestAsync(new HeartbeatPingMessage(w3Id, 2UL, 0, 0f, 0UL), correlationId: 2, TimeSpan.FromSeconds(3)))
        {
            Assert.Equal(ClusterMessageType.HeartbeatPong, pong.Header.MessageType);
        }

        // 5. Second SubmitJobAsync MUST still dispatch to cNew and complete Succeeded without hanging!
        Guid postRejectionJobId = Guid.NewGuid();
        var postDag = new ExecutionDag(
            postRejectionJobId,
            "PostOldSocketRejectionJob",
            new[]
            {
                new ExecutionStage(
                    stageId: 0,
                    jobId: postRejectionJobId,
                    stageName: "Scan",
                    isShuffleBoundary: false,
                    partitionsCount: 1,
                    primaryOperator: StageOperatorType.Scan,
                    operatorPayload: Encoding.UTF8.GetBytes("memory://post"))
            });

        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
        {
            var postResult = await coordinator.SubmitJobAsync(postDag, cts.Token);
            Assert.Equal(JobCompletionStatus.Succeeded, postResult.Status);
        }
    }

    [Fact]
    public async Task R3_Coordinator_MissingWorkerConnection_FailsJobCleanlyWithoutHanging()
    {
        await using var coordinator = new ClusterCoordinator(new ClusterCoordinatorOptions
        {
            DisableSocketListener = true,
            Port = 0
        });
        await coordinator.StartAsync();

        // Register healthy worker directly in MembershipManager without registering a control connection
        Guid missingConnWorkerId = Guid.NewGuid();
        var profile = NodeHardwareProfile.ProbeLocal(missingConnWorkerId, 0x7F000001);
        var joinResp = await coordinator.Membership.RegisterWorkerAsync(
            new NodeJoinRequestMessage(profile, "no-conn-worker", shufflePort: 53110),
            new IPEndPoint(IPAddress.Loopback, 53110),
            shufflePort: 53110);
        Assert.Equal(NodeJoinStatus.Accepted, joinResp.Status);

        Guid jobId = Guid.NewGuid();
        var dag = new ExecutionDag(
            jobId,
            "MissingConnectionJob",
            new[]
            {
                new ExecutionStage(
                    stageId: 0,
                    jobId: jobId,
                    stageName: "Scan",
                    isShuffleBoundary: false,
                    partitionsCount: 1,
                    primaryOperator: StageOperatorType.Scan,
                    operatorPayload: Encoding.UTF8.GetBytes("memory://missing-conn"))
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var completion = await coordinator.SubmitJobAsync(dag, cts.Token);
        Assert.Equal(JobCompletionStatus.Failed, completion.Status);
        Assert.NotNull(completion.ErrorMessage);
        Assert.Contains(missingConnWorkerId.ToString(), completion.ErrorMessage, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void R3_TaskExecutor_ApplyFusedOperators_TracksIntermediatesOnSubsequentThrow()
    {
        using var inputDf = new DataFrame(new ISeries[]
        {
            new Int32Series("id", new[] { 1, 2, 3, 4 }),
            new Float64Series("val", new[] { 10.0, 20.0, 30.0, 40.0 })
        });

        var transientDfs = new List<DataFrame>();
        try
        {
            // Op 0 allocates an intermediate DataFrame (id >= 2); Op 1 throws on nonexistent column
            Assert.ThrowsAny<Exception>(() =>
                TaskExecutor.ApplyFusedOperators(
                    inputDf,
                    new[] { "id >= 2", "nonexistent_col > 0" },
                    transientDfs));

            // Both inputDf and the intermediate DataFrame created by op 0 must already be tracked
            Assert.True(transientDfs.Count >= 2);
            Assert.Contains(transientDfs, d => ReferenceEquals(d, inputDf));
            Assert.Contains(transientDfs, d => !ReferenceEquals(d, inputDf) && d.RowCount == 3);
        }
        finally
        {
            foreach (var d in transientDfs)
            {
                if (!ReferenceEquals(d, inputDf))
                {
                    d.Dispose();
                }
            }
        }

        // Also verify null transientDfs path cleans up intermediate and rethrows cleanly
        Assert.ThrowsAny<Exception>(() =>
            TaskExecutor.ApplyFusedOperators(
                inputDf,
                new[] { "id >= 2", "PROJECT:nonexistent_col" },
                transientDfs: null));
    }

    // =========================================================================
    // M3.5 R4 Hardening Regressions (Re-join on Existing Connection,
    // Sole Worker Abrupt Mid-Job Eviction, Child Stage Zero-Healthy Failure)
    // =========================================================================

    [Fact]
    public async Task R4_Coordinator_ExistingConnRejoinAfterInterveningSocket_RestoresActiveConnection()
    {
        const string secret = "m35-r4-rejoin-secret";
        await using var coordinator = new ClusterCoordinator(new ClusterCoordinatorOptions
        {
            BindAddress = IPAddress.Loopback,
            Port = 0,
            ClusterSecret = secret
        });
        await coordinator.StartAsync();

        var w3Id = Guid.NewGuid();
        var w3Profile = NodeHardwareProfile.ProbeLocal(w3Id, 0x7F000001);

        // 1. Connect cNew and join as W3
        var sNew = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await sNew.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var pNew = new SocketDuplexPipe(sNew);
        DuplexConnection? cNewRef = null;
        await using var cNew = new DuplexConnection(
            Guid.Empty,
            pNew,
            unsolicitedFrameHandler: async frame =>
            {
                if (frame.Header.MessageType == ClusterMessageType.StageDispatch)
                {
                    var dispatch = (StageDispatchMessage)ClusterMessageSerializer.Deserialize(
                        frame.Header.MessageType, frame.Payload.Span);
                    var done = new TaskStatusUpdateMessage(
                        dispatch.JobId,
                        dispatch.StageId,
                        dispatch.PartitionId,
                        TaskExecutionStatus.Completed,
                        elapsedMs: 1.0,
                        outputBytes: 64UL,
                        outputRowCount: 4UL);
                    await cNewRef!.SendFrameAsync(done, frame.Header.CorrelationId).ConfigureAwait(false);
                }
            });
        cNewRef = cNew;
        cNew.Start();

        using (var j1 = await cNew.SendRequestAsync(new NodeJoinRequestMessage(w3Profile, "w3-new", secret, 53201), 1, TimeSpan.FromSeconds(3)))
        {
            var r1 = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(j1.Header.MessageType, j1.Payload.Span));
            Assert.Equal(NodeJoinStatus.Accepted, r1.Status);
            Assert.Equal(w3Id, r1.AssignedNodeId);
        }

        // 2. Intervening socket cOther joins as W3, temporarily overwriting _workerConnections[W3]
        var sOther = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await sOther.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var pOther = new SocketDuplexPipe(sOther);
        await using var cOther = new DuplexConnection(Guid.Empty, pOther);
        cOther.Start();

        using (var jOther = await cOther.SendRequestAsync(new NodeJoinRequestMessage(w3Profile, "w3-other", secret, 53202), 1, TimeSpan.FromSeconds(3)))
        {
            var rOther = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(jOther.Header.MessageType, jOther.Payload.Span));
            Assert.Equal(NodeJoinStatus.Accepted, rOther.Status);
            Assert.Equal(w3Id, rOther.AssignedNodeId);
        }

        // 3. Existing connection cNew re-joins W3 -> must be Accepted and restore _workerConnections[W3] = cNew
        using (var jRejoin = await cNew.SendRequestAsync(new NodeJoinRequestMessage(w3Profile, "w3-new", secret, 53201), 2, TimeSpan.FromSeconds(3)))
        {
            var rRejoin = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(jRejoin.Header.MessageType, jRejoin.Payload.Span));
            Assert.Equal(NodeJoinStatus.Accepted, rRejoin.Status);
            Assert.Equal(w3Id, rRejoin.AssignedNodeId);
        }

        // 4. cOther is rejected with WRONG_SECRET -> must NOT remove cNew from _workerConnections[W3]
        using (var jBad = await cOther.SendRequestAsync(new NodeJoinRequestMessage(w3Profile, "w3-other", "WRONG_SECRET", 53202), 2, TimeSpan.FromSeconds(3)))
        {
            var rBad = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(jBad.Header.MessageType, jBad.Payload.Span));
            Assert.Equal(NodeJoinStatus.Unauthorized, rBad.Status);
        }

        await Task.Delay(80);

        using (var pong = await cNew.SendRequestAsync(new HeartbeatPingMessage(w3Id, 1UL, 0, 0f, 0UL), 3, TimeSpan.FromSeconds(3)))
        {
            Assert.Equal(ClusterMessageType.HeartbeatPong, pong.Header.MessageType);
        }

        // 5. SubmitJobAsync dispatches to cNew and succeeds cleanly
        Guid jobId = Guid.NewGuid();
        var dag = new ExecutionDag(
            jobId,
            "RejoinActiveConnJob",
            new[]
            {
                new ExecutionStage(
                    stageId: 0,
                    jobId: jobId,
                    stageName: "Scan",
                    isShuffleBoundary: false,
                    partitionsCount: 1,
                    primaryOperator: StageOperatorType.Scan,
                    operatorPayload: Encoding.UTF8.GetBytes("memory://rejoin-test"))
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var completion = await coordinator.SubmitJobAsync(dag, cts.Token);
        Assert.Equal(JobCompletionStatus.Succeeded, completion.Status);
    }

    [Fact]
    public async Task R4_Coordinator_SoleWorkerAbruptDisconnectMidJob_FailsJobCleanly()
    {
        const string secret = "m35-r4-abrupt-secret";
        await using var coordinator = new ClusterCoordinator(new ClusterCoordinatorOptions
        {
            BindAddress = IPAddress.Loopback,
            Port = 0,
            ClusterSecret = secret,
            HeartbeatAuditInterval = TimeSpan.FromMilliseconds(50)
        });
        await coordinator.StartAsync();

        var w1Id = Guid.NewGuid();
        var w1Profile = NodeHardwareProfile.ProbeLocal(w1Id, 0x7F000001);
        var dispatchReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var s1 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await s1.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        var p1 = new SocketDuplexPipe(s1);
        var c1 = new DuplexConnection(
            Guid.Empty,
            p1,
            unsolicitedFrameHandler: frame =>
            {
                if (frame.Header.MessageType == ClusterMessageType.StageDispatch)
                {
                    dispatchReceived.TrySetResult();
                }
                return ValueTask.CompletedTask;
            });
        c1.Start();

        try
        {
            using (var j1 = await c1.SendRequestAsync(new NodeJoinRequestMessage(w1Profile, "w1-abrupt", secret, 53210), 1, TimeSpan.FromSeconds(3)))
            {
                var r1 = Assert.IsType<NodeJoinResponseMessage>(
                    ClusterMessageSerializer.Deserialize(j1.Header.MessageType, j1.Payload.Span));
                Assert.Equal(NodeJoinStatus.Accepted, r1.Status);
            }

            Guid jobId = Guid.NewGuid();
            var dag = new ExecutionDag(
                jobId,
                "SoleWorkerAbruptDisconnectJob",
                new[]
                {
                    new ExecutionStage(
                        stageId: 0,
                        jobId: jobId,
                        stageName: "Scan",
                        isShuffleBoundary: false,
                        partitionsCount: 1,
                        primaryOperator: StageOperatorType.Scan,
                        operatorPayload: Encoding.UTF8.GetBytes("memory://abrupt-disconnect"))
                });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var submitTask = coordinator.SubmitJobAsync(dag, cts.Token);

            // Wait until W1 receives StageDispatch, then abruptly close socket without NodeLeaveMessage
            await dispatchReceived.Task.WaitAsync(cts.Token);
            await c1.DisposeAsync();
            s1.Dispose();

            var completion = await submitTask;
            Assert.Equal(JobCompletionStatus.Failed, completion.Status);
            Assert.NotNull(completion.ErrorMessage);
        }
        finally
        {
            await c1.DisposeAsync();
            s1.Dispose();
        }
    }

    [Fact]
    public async Task R4_Coordinator_MultiStageZeroHealthyForChildStage_FailsJobCleanly()
    {
        const string secret = "m35-r4-multistage-secret";
        await using var coordinator = new ClusterCoordinator(new ClusterCoordinatorOptions
        {
            BindAddress = IPAddress.Loopback,
            Port = 0,
            ClusterSecret = secret
        });
        await coordinator.StartAsync();

        var w1Id = Guid.NewGuid();
        var w1Profile = NodeHardwareProfile.ProbeLocal(w1Id, 0x7F000001);

        var s1 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await s1.ConnectAsync(IPAddress.Loopback, coordinator.Port);
        await using var p1 = new SocketDuplexPipe(s1);
        DuplexConnection? c1Ref = null;
        await using var c1 = new DuplexConnection(
            Guid.Empty,
            p1,
            unsolicitedFrameHandler: async frame =>
            {
                if (frame.Header.MessageType == ClusterMessageType.StageDispatch)
                {
                    var dispatch = (StageDispatchMessage)ClusterMessageSerializer.Deserialize(
                        frame.Header.MessageType, frame.Payload.Span);
                    if (dispatch.StageId == 0)
                    {
                        // Leave cluster right before reporting Stage 0 Completed so Stage 1 sees 0 healthy workers
                        await c1Ref!.SendFrameAsync(
                            new NodeLeaveMessage(w1Id, 0u, 0UL, "leaving-before-stage1"),
                            correlationId: 90).ConfigureAwait(false);

                        var done = new TaskStatusUpdateMessage(
                            dispatch.JobId,
                            dispatch.StageId,
                            dispatch.PartitionId,
                            TaskExecutionStatus.Completed,
                            elapsedMs: 1.0,
                            outputBytes: 64UL,
                            outputRowCount: 4UL);
                        await c1Ref!.SendFrameAsync(done, frame.Header.CorrelationId).ConfigureAwait(false);
                    }
                }
            });
        c1Ref = c1;
        c1.Start();

        using (var j1 = await c1.SendRequestAsync(new NodeJoinRequestMessage(w1Profile, "w1-multistage", secret, 53220), 1, TimeSpan.FromSeconds(3)))
        {
            var r1 = Assert.IsType<NodeJoinResponseMessage>(
                ClusterMessageSerializer.Deserialize(j1.Header.MessageType, j1.Payload.Span));
            Assert.Equal(NodeJoinStatus.Accepted, r1.Status);
        }

        Guid jobId = Guid.NewGuid();
        var stage0 = new ExecutionStage(
            stageId: 0,
            jobId: jobId,
            stageName: "Stage0Scan",
            isShuffleBoundary: false,
            partitionsCount: 1,
            primaryOperator: StageOperatorType.Scan,
            operatorPayload: Encoding.UTF8.GetBytes("memory://stage0"));
        var stage1 = new ExecutionStage(
            stageId: 1,
            jobId: jobId,
            stageName: "Stage1Filter",
            isShuffleBoundary: false,
            partitionsCount: 1,
            primaryOperator: StageOperatorType.Filter,
            operatorPayload: Encoding.UTF8.GetBytes("id > 0"),
            parentStageIds: new[] { 0 });

        var dag = new ExecutionDag(jobId, "ZeroHealthyChildStageJob", new[] { stage0, stage1 });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var completion = await coordinator.SubmitJobAsync(dag, cts.Token);
        Assert.Equal(JobCompletionStatus.Failed, completion.Status);
        Assert.NotNull(completion.ErrorMessage);
    }

    private sealed class PairDuplexPipe : IDuplexPipe
    {
        public PipeReader Input { get; }
        public PipeWriter Output { get; }

        public PairDuplexPipe(PipeReader input, PipeWriter output)
        {
            Input = input;
            Output = output;
        }
    }
}
