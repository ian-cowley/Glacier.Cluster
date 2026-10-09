namespace Glacier.Cluster.Tests.SynthesisTests;

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Glacier.Cluster.Shuffle;
using Glacier.Cluster.Worker;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Storage.Arrow;
using Xunit;

public sealed class Milestone4ChallengerTests
{
    private sealed class DuplexPipePair : IDuplexPipe
    {
        public PipeReader Input { get; }
        public PipeWriter Output { get; }

        public DuplexPipePair(PipeReader input, PipeWriter output)
        {
            Input = input;
            Output = output;
        }
    }

    private sealed class NoOpShuffleClient : IArrowShuffleClient
    {
        public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
            string host,
            ushort port,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(RefCountedPartitionBuffer.Allocate(64));
        }

        public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
            IDuplexPipe pipe,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(RefCountedPartitionBuffer.Allocate(64));
        }

        public ValueTask<ArrowRecordBatch> FetchRecordBatchAsync(
            IDuplexPipe pipe,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public ValueTask<DataFrame> FetchDataFrameAsync(
            IDuplexPipe pipe,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public ValueTask<RefCountedPartitionBuffer> FetchRemotePartitionAsync(
            Guid producerNodeId,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(RefCountedPartitionBuffer.Allocate(64));
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public void Dispose() { }
    }

    // =========================================================================
    // 1. Worker Registration Stress Tests
    // =========================================================================

    [Fact]
    public async Task WorkerRegistration_HighConcurrency_ContiguousRanksAndNoDuplicates()
    {
        using var manager = new MembershipManager();
        int workerCount = 100;
        var joinResponses = new NodeJoinResponseMessage[workerCount];
        var endPoint = new IPEndPoint(IPAddress.Loopback, 9000);

        var tasks = Enumerable.Range(0, workerCount).Select(async i =>
        {
            var nodeProfile = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());
            var req = new NodeJoinRequestMessage(nodeProfile, $"worker-{i}");
            var resp = await manager.RegisterWorkerAsync(req, endPoint, (ushort)(10000 + i));
            joinResponses[i] = resp;
        }).ToArray();

        await Task.WhenAll(tasks);

        // Verify active node count
        Assert.Equal(workerCount, manager.ActiveNodeCount);

        // Verify all 100 ranks are unique and span exactly 0..99
        var ranks = joinResponses.Select(r => r.AssignedRank).OrderBy(r => r).ToArray();
        for (int i = 0; i < workerCount; i++)
        {
            Assert.Equal(i, ranks[i]);
        }

        // Test concurrent depart of 20 workers
        int departCount = 20;
        var departTasks = Enumerable.Range(0, departCount).Select(async i =>
        {
            var targetResp = joinResponses[i];
            await manager.HandleWorkerLeaveAsync(new NodeLeaveMessage(targetResp.AssignedNodeId, (uint)targetResp.AssignedRank, 1000UL, "Leaving"));
        }).ToArray();

        await Task.WhenAll(departTasks);
        Assert.Equal(workerCount - departCount, manager.ActiveNodeCount);

        // Concurrently register 20 new workers - they should recycle the departed ranks
        var recycledResponses = new NodeJoinResponseMessage[departCount];
        var reJoinTasks = Enumerable.Range(0, departCount).Select(async i =>
        {
            var nodeProfile = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());
            var req = new NodeJoinRequestMessage(nodeProfile, $"new-worker-{i}");
            var resp = await manager.RegisterWorkerAsync(req, endPoint, (ushort)(20000 + i));
            recycledResponses[i] = resp;
        }).ToArray();

        await Task.WhenAll(reJoinTasks);
        Assert.Equal(workerCount, manager.ActiveNodeCount);

        // Verify recycled ranks are within 0..19 and no duplicate ranks across entire cluster
        var allActiveNodes = manager.GetAllNodes().Where(n => n.HealthState != NodeHealthState.Evicted).ToArray();
        Assert.Equal(workerCount, allActiveNodes.Length);
        var activeRanks = allActiveNodes.Select(n => n.Rank).OrderBy(r => r).ToArray();
        for (int i = 0; i < workerCount; i++)
        {
            Assert.Equal(i, activeRanks[i]);
        }
    }

    // =========================================================================
    // 2. Heartbeat Timeout & Failure Detection Tests
    // =========================================================================

    [Fact]
    public void PhiAccrualFailureDetector_MissingHeartbeats_TransitionsToSuspectAndEvicted()
    {
        var detector = new PhiAccrualFailureDetector(
            suspectThreshold: 8.0,
            evictionThreshold: 12.0,
            minStdDevMs: 1.0);

        Guid nodeId = Guid.NewGuid();
        ulong t0 = 1_000_000_000UL; // 1 second in ns
        detector.RegisterNode(nodeId, t0);

        // Record 20 regular heartbeats at 100ms intervals (100_000_000 ns)
        ulong currentT = t0;
        for (int i = 0; i < 20; i++)
        {
            currentT += 100_000_000UL;
            detector.RecordHeartbeat(nodeId, currentT);
        }

        // Immediately after regular heartbeat: Healthy
        Assert.Equal(NodeHealthState.Healthy, detector.GetHealthState(nodeId, currentT + 10_000_000UL));

        // After 5 missed intervals (500ms): Phi should increase, transitioning to Suspect
        ulong suspectT = currentT + 600_000_000UL;
        var suspectState = detector.GetHealthState(nodeId, suspectT);
        Assert.True(suspectState == NodeHealthState.Suspect || suspectState == NodeHealthState.Evicted,
            $"Expected Suspect or Evicted after 600ms lapse, but was {suspectState}");

        // After 20 missed intervals (2000ms): Should be Evicted
        ulong evictedT = currentT + 2_500_000_000UL;
        var evictedState = detector.GetHealthState(nodeId, evictedT);
        Assert.Equal(NodeHealthState.Evicted, evictedState);
    }

    [Fact]
    public async Task ClusterCoordinator_HeartbeatAuditLoop_DetectsMissingHeartbeats()
    {
        // Challenge: Test coordinator background health audit loop on missing heartbeats
        var coordOptions = new ClusterCoordinatorOptions
        {
            Port = 0,
            HeartbeatAuditInterval = TimeSpan.FromMilliseconds(50)
        };

        var failureDetector = new PhiAccrualFailureDetector(
            suspectThreshold: 4.0,
            evictionThreshold: 8.0,
            minStdDevMs: 1.0);

        using var membership = new MembershipManager(failureDetector);
        using var coordinator = new ClusterCoordinator(coordOptions, membershipManager: membership);
        await coordinator.StartAsync();

        Guid workerId = Guid.NewGuid();
        var hardware = NodeHardwareProfile.ProbeLocal(nodeId: workerId);
        var joinReq = new NodeJoinRequestMessage(hardware, "heartbeat-test-worker");

        await membership.RegisterWorkerAsync(joinReq, new IPEndPoint(IPAddress.Loopback, 9001), 9001);
        Assert.Equal(1, coordinator.Membership.ActiveNodeCount);

        // Node is initially registered and healthy
        Assert.True(coordinator.Membership.TryGetNode(workerId, out var workerNode));
        Assert.Equal(NodeHealthState.Healthy, workerNode!.HealthState);

        // We do NOT send any heartbeats.
        // Wait 1.5 seconds for coordinator's background HealthAuditLoop to audit and evict node.
        var sw = Stopwatch.StartNew();
        bool evicted = false;
        while (sw.ElapsedMilliseconds < 15000)
        {
            if (coordinator.Membership.ActiveNodeCount == 0 || workerNode.HealthState == NodeHealthState.Evicted)
            {
                evicted = true;
                break;
            }
            await Task.Delay(50);
        }

        // Verify that the coordinator's background audit loop correctly detected the failure
        Assert.True(evicted, $"Worker was not evicted by coordinator background audit loop within 15.0s. ActiveNodeCount={coordinator.Membership.ActiveNodeCount}, HealthState={workerNode.HealthState}");
    }

    [Fact]
    public async Task ClusterCoordinator_PartialHeartbeatTimeout_EvictsOnlySilentNode()
    {
        var coordOptions = new ClusterCoordinatorOptions
        {
            Port = 0,
            HeartbeatAuditInterval = TimeSpan.FromMilliseconds(50)
        };

        var failureDetector = new PhiAccrualFailureDetector(
            suspectThreshold: 4.0,
            evictionThreshold: 8.0,
            minStdDevMs: 1.0);

        using var membership = new MembershipManager(failureDetector);
        using var coordinator = new ClusterCoordinator(coordOptions, membershipManager: membership);
        await coordinator.StartAsync();

        Guid healthyWorkerId = Guid.NewGuid();
        Guid silentWorkerId = Guid.NewGuid();

        var healthyProfile = NodeHardwareProfile.ProbeLocal(nodeId: healthyWorkerId);
        var silentProfile = NodeHardwareProfile.ProbeLocal(nodeId: silentWorkerId);

        await membership.RegisterWorkerAsync(new NodeJoinRequestMessage(healthyProfile, "worker-healthy"), new IPEndPoint(IPAddress.Loopback, 9002), 9002);
        await membership.RegisterWorkerAsync(new NodeJoinRequestMessage(silentProfile, "worker-silent"), new IPEndPoint(IPAddress.Loopback, 9003), 9003);

        Assert.Equal(2, coordinator.Membership.ActiveNodeCount);
        Assert.True(coordinator.Membership.TryGetNode(healthyWorkerId, out var healthyNode));
        Assert.True(coordinator.Membership.TryGetNode(silentWorkerId, out var silentNode));
        Assert.Equal(NodeHealthState.Healthy, healthyNode!.HealthState);
        Assert.Equal(NodeHealthState.Healthy, silentNode!.HealthState);

        // Keep sending heartbeats for healthyWorkerId every 50ms, but zero heartbeats for silentWorkerId
        using var cts = new CancellationTokenSource();
        var heartbeatTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                membership.RecordHeartbeat(new HeartbeatPingMessage(
                    healthyWorkerId,
                    PhiAccrualFailureDetector.GetCurrentTimestampNs(),
                    0,
                    10.0f,
                    1_000_000_000UL));
                try
                {
                    await Task.Delay(40, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        });

        // Wait until silentWorkerId is evicted
        var sw = Stopwatch.StartNew();
        bool silentEvicted = false;
        while (sw.ElapsedMilliseconds < 15000)
        {
            if (silentNode.HealthState == NodeHealthState.Evicted)
            {
                silentEvicted = true;
                break;
            }
            await Task.Delay(50);
        }

        cts.Cancel();
        try { await heartbeatTask; } catch (OperationCanceledException) { }

        // Assert silent node transitioned from Healthy to Evicted
        Assert.True(silentEvicted, $"Silent worker was not evicted. HealthState={silentNode.HealthState}");
        Assert.Equal(NodeHealthState.Evicted, silentNode.HealthState);

        // Assert healthy node remained Healthy and active node count is exactly 1
        Assert.Equal(NodeHealthState.Healthy, healthyNode.HealthState);
        Assert.Equal(1, coordinator.Membership.ActiveNodeCount);
    }

    // =========================================================================
    // 3. Task Execution Concurrency & Semaphore Gating Tests
    // =========================================================================

    [Fact]
    public async Task TaskExecutor_HeavyConcurrentLoad_RespectsSemaphoreAndPreventsBufferCorruption()
    {
        Guid nodeId = Guid.NewGuid();
        using var partitionStore = new LocalPartitionStore(nodeId);
        var shuffleClient = new NoOpShuffleClient();
        int maxConcurrency = 4;
        using var executor = new TaskExecutor(nodeId, partitionStore, shuffleClient, maxConcurrency: maxConcurrency);

        int taskCount = 50;
        int maxObservedActive = 0;
        Guid jobId = Guid.NewGuid();

        // Populate 50 source partitions in local store
        for (int i = 0; i < taskCount; i++)
        {
            var df = new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("id", new int?[] { i, i * 2, i * 3 }),
                Int32Series.FromValues("val", new int?[] { 10 + i, 20 + i, 30 + i })
            });
            var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
            partitionStore.RegisterPartition(new PartitionKey(jobId, 0, i), buf);
        }

        // Run 50 filter tasks concurrently on stage 1 (Filter val > 15)
        var tasks = Enumerable.Range(0, taskCount).Select(async i =>
        {
            var upstreamLoc = new UpstreamPartitionLocation(nodeId, stageId: 0, partitionId: i, 0, "localhost");
            var dispatch = new StageDispatchMessage(
                jobId,
                stageId: 1,
                partitionId: i,
                operatorType: StageOperatorType.Filter,
                operatorPayload: Encoding.UTF8.GetBytes("val > 15"),
                upstreamPartitions: new[] { upstreamLoc });

            int currentActive = executor.ActiveTaskCount;
            Interlocked.Exchange(ref maxObservedActive, Math.Max(maxObservedActive, currentActive));

            var result = await executor.ExecuteTaskAsync(dispatch);

            currentActive = executor.ActiveTaskCount;
            Interlocked.Exchange(ref maxObservedActive, Math.Max(maxObservedActive, currentActive));

            return result;
        }).ToArray();

        var results = await Task.WhenAll(tasks);

        // Verify semaphore gating: max active count never exceeded maxConcurrency
        Assert.InRange(maxObservedActive, 0, maxConcurrency);

        // Verify all tasks succeeded
        foreach (var res in results)
        {
            Assert.Equal(TaskExecutionStatus.Completed, res.Status);
        }

        // Verify all 50 output partitions are valid and uncorrupted in store
        for (int i = 0; i < taskCount; i++)
        {
            var outKey = new PartitionKey(jobId, 1, i);
            using var outBuf = await partitionStore.FetchPartitionAsync(outKey);
            var outDf = PolarisBridge.PartitionBufferToDataFrame(outBuf, zeroCopy: true);
            Assert.True(outDf.RowCount >= 2, $"Expected at least 2 rows passing filter for partition {i}, got {outDf.RowCount}");
        }
    }

    [Fact]
    public async Task TaskExecutor_MalformedPayload_ReturnsFailedWithoutCrashingOrLeakingSemaphore()
    {
        Guid nodeId = Guid.NewGuid();
        using var partitionStore = new LocalPartitionStore(nodeId);
        var shuffleClient = new NoOpShuffleClient();
        using var executor = new TaskExecutor(nodeId, partitionStore, shuffleClient, maxConcurrency: 2);

        Guid jobId = Guid.NewGuid();

        // Populate a source partition
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("x", new int?[] { 1, 2, 3 })
        });
        var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        partitionStore.RegisterPartition(new PartitionKey(jobId, 0, 0), buf);

        // Dispatch with completely invalid filter syntax
        var dispatch = new StageDispatchMessage(
            jobId,
            stageId: 1,
            partitionId: 0,
            operatorType: StageOperatorType.Filter,
            operatorPayload: Encoding.UTF8.GetBytes("INVALID @#$ EXPR SYNTAX %%%"),
            upstreamPartitions: new[] { new UpstreamPartitionLocation(nodeId, 0, 0, 0, "localhost") });

        var result = await executor.ExecuteTaskAsync(dispatch);

        Assert.Equal(TaskExecutionStatus.Failed, result.Status);
        Assert.NotNull(result.ErrorMessage);
        Assert.Equal(0UL, result.OutputRowCount);
        Assert.Equal(0UL, result.OutputBytes);

        // Ensure semaphore was cleanly released and active count is 0
        Assert.Equal(0, executor.ActiveTaskCount);
    }

    private sealed class TrackingShuffleClient : IArrowShuffleClient
    {
        private readonly RefCountedPartitionBuffer _buffer;

        public TrackingShuffleClient(RefCountedPartitionBuffer buffer)
        {
            _buffer = buffer;
        }

        public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
            string host,
            ushort port,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(_buffer);
        }

        public ValueTask<RefCountedPartitionBuffer> FetchPartitionAsync(
            IDuplexPipe pipe,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(_buffer);
        }

        public ValueTask<ArrowRecordBatch> FetchRecordBatchAsync(
            IDuplexPipe pipe,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public ValueTask<DataFrame> FetchDataFrameAsync(
            IDuplexPipe pipe,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default) => throw new NotImplementedException();

        public ValueTask<RefCountedPartitionBuffer> FetchRemotePartitionAsync(
            Guid producerNodeId,
            Guid jobId,
            int stageId,
            int partitionId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(_buffer);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public void Dispose() { }
    }

    [Fact]
    public async Task TaskExecutor_OperatorException_DisposesUpstreamBuffersWithoutMemoryLeaks()
    {
        Guid nodeId = Guid.NewGuid();
        using var partitionStore = new LocalPartitionStore(nodeId);

        // Prepare an upstream buffer
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("x", new int?[] { 1, 2, 3 })
        });
        var remoteBuffer = PolarisBridge.DataFrameToPartitionBuffer(df);
        Assert.False(remoteBuffer.IsDisposed);
        Assert.Equal(1, remoteBuffer.ReferenceCount);

        var shuffleClient = new TrackingShuffleClient(remoteBuffer);
        using var executor = new TaskExecutor(nodeId, partitionStore, shuffleClient, maxConcurrency: 2);

        Guid jobId = Guid.NewGuid();
        Guid remoteProducerId = Guid.NewGuid(); // different node => remote fetch

        var dispatch = new StageDispatchMessage(
            jobId,
            stageId: 1,
            partitionId: 0,
            operatorType: StageOperatorType.Filter,
            operatorPayload: Encoding.UTF8.GetBytes("INVALID EXPR %%%"),
            upstreamPartitions: new[] { new UpstreamPartitionLocation(remoteProducerId, 0, 0, 0, "remote-host") });

        var result = await executor.ExecuteTaskAsync(dispatch);

        // 1. Returns Failed
        Assert.Equal(TaskExecutionStatus.Failed, result.Status);
        Assert.NotNull(result.ErrorMessage);

        // 2. Returns 0 rows, 0 bytes (no dummy 100-row records)
        Assert.Equal(0UL, result.OutputRowCount);
        Assert.Equal(0UL, result.OutputBytes);

        // 3. Upstream buffer was disposed on failure path (zero memory leaks)
        Assert.True(remoteBuffer.IsDisposed, "Remote upstream partition buffer was not disposed after operator failure.");

        // 4. Semaphore released
        Assert.Equal(0, executor.ActiveTaskCount);
    }

    // =========================================================================
    // 4. Consensus Failover Tests
    // =========================================================================

    [Fact]
    public async Task ConsensusFailover_LeaderHeartbeatLapse_StandbyAssumesLeadershipCleanly()
    {
        Guid activeCoordId = Guid.NewGuid();
        Guid standbyCoordId = Guid.NewGuid();

        var options = new ConsensusOptions
        {
            InitialRole = ConsensusRole.Standby,
            LeaseDuration = TimeSpan.FromMilliseconds(150),
            RenewInterval = TimeSpan.FromMilliseconds(30),
            GracePeriod = TimeSpan.FromMilliseconds(30)
        };

        using var standbyLedger = new ActiveStandbyConsensusLedger(standbyCoordId, options);

        bool promotedFired = false;
        standbyLedger.PromotedToActive += () => promotedFired = true;

        // Standby starts
        await standbyLedger.StartAsync();
        Assert.Equal(ConsensusRole.Standby, standbyLedger.CurrentRole);

        // Active leader informs standby of its lease expiring at UtcNow + 150ms
        DateTimeOffset leaseExpiry = DateTimeOffset.UtcNow.AddMilliseconds(150);
        standbyLedger.UpdateLeaderLeaseFromRemote(activeCoordId, epoch: 1, leaseExpiry);

        Assert.Equal(ConsensusRole.Standby, standbyLedger.CurrentRole);
        Assert.Equal(activeCoordId, standbyLedger.CurrentLease.LeaderId);

        // Simulate active leader failure (no lease updates sent).
        // Standby should detect lease expiry + grace period and promote itself.
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < 15000 && (!promotedFired || standbyLedger.CurrentRole != ConsensusRole.Active))
        {
            await Task.Delay(20);
        }

        Assert.Equal(ConsensusRole.Active, standbyLedger.CurrentRole);
        Assert.True(promotedFired, "PromotedToActive event was not triggered on standby ledger.");
        Assert.Equal(standbyCoordId, standbyLedger.CurrentLease.LeaderId);
        Assert.True(standbyLedger.CurrentLease.Epoch >= 2, "Epoch should have incremented on takeover.");
    }

    [Fact]
    public void ConsensusFailover_ActiveLeaderDemotedWhenHigherEpochReceived()
    {
        Guid coordId = Guid.NewGuid();
        Guid competitorId = Guid.NewGuid();

        var options = new ConsensusOptions
        {
            InitialRole = ConsensusRole.Active,
            LeaseDuration = TimeSpan.FromMilliseconds(500)
        };

        using var ledger = new ActiveStandbyConsensusLedger(coordId, options);
        Assert.Equal(ConsensusRole.Active, ledger.CurrentRole);

        bool demotedFired = false;
        ledger.DemotedToStandby += () => demotedFired = true;

        // Remote coordinator with higher epoch asserts leadership
        ledger.UpdateLeaderLeaseFromRemote(competitorId, epoch: 5, DateTimeOffset.UtcNow.AddSeconds(5));

        Assert.Equal(ConsensusRole.Standby, ledger.CurrentRole);
        Assert.True(demotedFired, "DemotedToStandby event was not triggered when higher epoch was received.");
        Assert.Equal(competitorId, ledger.CurrentLease.LeaderId);
    }

    // =========================================================================
    // 5. Ring AllReduce Numerical Precision & Large Vector Stress (100,000+ elements)
    // =========================================================================

    [Theory]
    [InlineData(2, 100_000)]
    [InlineData(4, 100_000)]
    [InlineData(8, 100_000)]
    [InlineData(4, 104_857)] // Non-power-of-two chunk boundary with 4 nodes
    public async Task RingAllReduce_LargeVector_ComparesAgainstExactDoublePrecisionScalarReference(int worldSize, int totalElements)
    {
        var nodeIds = Enumerable.Range(0, worldSize).Select(_ => Guid.NewGuid()).ToArray();
        var pipes = Enumerable.Range(0, worldSize).Select(_ => new Pipe()).ToArray();
        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int prevIdx = (i - 1 + worldSize) % worldSize;
            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), pipes[i].Writer);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodeIds[i],
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        // Initialize float vectors with varying values and compute exact double-precision reference
        float[][] buffers = new float[worldSize][];
        double[] doubleReference = new double[totalElements];

        for (int i = 0; i < worldSize; i++)
        {
            buffers[i] = new float[totalElements];
            float baseVal = (i + 1) * 0.25f;
            for (int e = 0; e < totalElements; e++)
            {
                // Varied positive and negative float values
                float val = baseVal + ((e % 17) - 8) * 0.125f;
                buffers[i][e] = val;
                doubleReference[e] += (double)val;
            }
        }

        Guid sessionId = Guid.NewGuid();
        Task[] allReduceTasks = new Task[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int rank = i;
            allReduceTasks[i] = Task.Run(async () =>
            {
                await workers[rank].AllReduceAsync(buffers[rank], sessionId);
            });
        }

        await Task.WhenAll(allReduceTasks);

        // Verify all nodes converged to the exact same values, and compare against double-precision reference
        for (int i = 0; i < worldSize; i++)
        {
            for (int e = 0; e < totalElements; e++)
            {
                float actual = buffers[i][e];
                double expected = doubleReference[e];
                double absError = Math.Abs(actual - expected);
                double tolerance = Math.Max(1e-4, Math.Abs(expected) * 1e-4);

                Assert.True(absError <= tolerance,
                    $"Mismatch at rank {i}, index {e}: Actual={actual}, ExpectedDouble={expected}, AbsError={absError}");

                // Node consistency check: all nodes must match node 0 exactly
                if (i > 0)
                {
                    Assert.Equal(buffers[0][e], actual);
                }
            }
        }

        for (int i = 0; i < worldSize; i++)
        {
            await workers[i].DisposeAsync();
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public async Task RingAllReduce_FloatingPointExtremes_Infinities_NaNs_Subnormals(int worldSize)
    {
        int totalElements = 16;
        var nodeIds = Enumerable.Range(0, worldSize).Select(_ => Guid.NewGuid()).ToArray();
        var pipes = Enumerable.Range(0, worldSize).Select(_ => new Pipe()).ToArray();
        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int prevIdx = (i - 1 + worldSize) % worldSize;
            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), pipes[i].Writer);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodeIds[i],
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        float[][] buffers = new float[worldSize][];
        for (int i = 0; i < worldSize; i++)
        {
            buffers[i] = new float[totalElements];
        }

        // Element 0: Positive Infinity propagation
        buffers[0][0] = float.PositiveInfinity;
        for (int i = 1; i < worldSize; i++) buffers[i][0] = 100.0f;

        // Element 1: Negative Infinity propagation
        buffers[0][1] = float.NegativeInfinity;
        for (int i = 1; i < worldSize; i++) buffers[i][1] = -50.0f;

        // Element 2: NaN propagation
        buffers[0][2] = float.NaN;
        for (int i = 1; i < worldSize; i++) buffers[i][2] = 1.0f;

        // Element 3: Infinity cancellation (+Inf + -Inf = NaN)
        buffers[0][3] = float.PositiveInfinity;
        buffers[1][3] = float.NegativeInfinity;

        // Element 4: Large float extremes (+1e30f and -1e30f)
        buffers[0][4] = 1e30f;
        buffers[1][4] = -1e30f;

        Guid sessionId = Guid.NewGuid();
        Task[] tasks = new Task[worldSize];
        for (int i = 0; i < worldSize; i++)
        {
            int rank = i;
            tasks[i] = Task.Run(async () => await workers[rank].AllReduceAsync(buffers[rank], sessionId));
        }

        await Task.WhenAll(tasks);

        for (int i = 0; i < worldSize; i++)
        {
            Assert.True(float.IsPositiveInfinity(buffers[i][0]), $"Rank {i} element 0 should be +Inf");
            Assert.True(float.IsNegativeInfinity(buffers[i][1]), $"Rank {i} element 1 should be -Inf");
            Assert.True(float.IsNaN(buffers[i][2]), $"Rank {i} element 2 should be NaN");
            Assert.True(float.IsNaN(buffers[i][3]), $"Rank {i} element 3 should be NaN");
            Assert.Equal(0.0f, buffers[i][4], precision: 1); // 1e30 + -1e30 = 0
        }

        for (int i = 0; i < worldSize; i++) await workers[i].DisposeAsync();
    }

    // =========================================================================
    // 6. Buffer Edge Cases (0 elements, 1 element, odd vector lengths)
    // =========================================================================

    [Theory]
    [InlineData(2, 0)]
    [InlineData(4, 0)]
    [InlineData(8, 0)]
    public async Task RingAllReduce_ZeroElements_CompletesCleanlyWithoutErrors(int worldSize, int totalElements)
    {
        var nodeIds = Enumerable.Range(0, worldSize).Select(_ => Guid.NewGuid()).ToArray();
        var pipes = Enumerable.Range(0, worldSize).Select(_ => new Pipe()).ToArray();
        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int prevIdx = (i - 1 + worldSize) % worldSize;
            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), pipes[i].Writer);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodeIds[i],
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        float[][] buffers = new float[worldSize][];
        for (int i = 0; i < worldSize; i++) buffers[i] = Array.Empty<float>();

        Guid sessionId = Guid.NewGuid();
        Task[] tasks = new Task[worldSize];
        for (int i = 0; i < worldSize; i++)
        {
            int rank = i;
            tasks[i] = Task.Run(async () => await workers[rank].AllReduceAsync(buffers[rank], sessionId));
        }

        // Should complete without hanging or throwing exceptions
        await Task.WhenAll(tasks);

        for (int i = 0; i < worldSize; i++) await workers[i].DisposeAsync();
    }

    [Theory]
    [InlineData(2, 1)]
    [InlineData(4, 1)]
    [InlineData(8, 1)]
    public async Task RingAllReduce_SingleElement_ReachesExactSumAcrossAllNodes(int worldSize, int totalElements)
    {
        var nodeIds = Enumerable.Range(0, worldSize).Select(_ => Guid.NewGuid()).ToArray();
        var pipes = Enumerable.Range(0, worldSize).Select(_ => new Pipe()).ToArray();
        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int prevIdx = (i - 1 + worldSize) % worldSize;
            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), pipes[i].Writer);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodeIds[i],
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        float[][] buffers = new float[worldSize][];
        float expectedSum = 0;
        for (int i = 0; i < worldSize; i++)
        {
            float val = (i + 1) * 3.5f;
            buffers[i] = new[] { val };
            expectedSum += val;
        }

        Guid sessionId = Guid.NewGuid();
        Task[] tasks = new Task[worldSize];
        for (int i = 0; i < worldSize; i++)
        {
            int rank = i;
            tasks[i] = Task.Run(async () => await workers[rank].AllReduceAsync(buffers[rank], sessionId));
        }

        await Task.WhenAll(tasks);

        for (int i = 0; i < worldSize; i++)
        {
            Assert.Equal(expectedSum, buffers[i][0], precision: 4);
            await workers[i].DisposeAsync();
        }
    }

    [Theory]
    [InlineData(2, 3)]
    [InlineData(4, 7)]
    [InlineData(8, 13)]
    [InlineData(4, 31)]
    [InlineData(4, 33)]
    [InlineData(8, 100_003)] // Prime length, not divisible by SIMD width (4, 8, 16) or node count (8)
    public async Task RingAllReduce_OddVectorLengths_NotDivisibleBySimdOrNodeCount_CalculatesExactSum(int worldSize, int totalElements)
    {
        var nodeIds = Enumerable.Range(0, worldSize).Select(_ => Guid.NewGuid()).ToArray();
        var pipes = Enumerable.Range(0, worldSize).Select(_ => new Pipe()).ToArray();
        var workers = new RingAllReduceWorker[worldSize];

        for (int i = 0; i < worldSize; i++)
        {
            int prevIdx = (i - 1 + worldSize) % worldSize;
            var successorPipe = new DuplexPipePair(PipeReader.Create(Stream.Null), pipes[i].Writer);
            var predecessorPipe = new DuplexPipePair(pipes[prevIdx].Reader, PipeWriter.Create(Stream.Null));

            workers[i] = new RingAllReduceWorker(
                nodeIds[i],
                rank: i,
                worldSize: worldSize,
                successorPipe: successorPipe,
                predecessorPipe: predecessorPipe,
                totalElements: totalElements);
        }

        float[][] buffers = new float[worldSize][];
        double[] expectedSums = new double[totalElements];

        for (int i = 0; i < worldSize; i++)
        {
            buffers[i] = new float[totalElements];
            float val = (i + 1) * 1.25f;
            for (int e = 0; e < totalElements; e++)
            {
                buffers[i][e] = val;
                expectedSums[e] += (double)val;
            }
        }

        Guid sessionId = Guid.NewGuid();
        Task[] tasks = new Task[worldSize];
        for (int i = 0; i < worldSize; i++)
        {
            int rank = i;
            tasks[i] = Task.Run(async () => await workers[rank].AllReduceAsync(buffers[rank], sessionId));
        }

        await Task.WhenAll(tasks);

        for (int i = 0; i < worldSize; i++)
        {
            for (int e = 0; e < totalElements; e++)
            {
                float actual = buffers[i][e];
                double expected = expectedSums[e];
                double absError = Math.Abs(actual - expected);
                double tolerance = Math.Max(1e-4, Math.Abs(expected) * 1e-4);

                Assert.True(absError <= tolerance,
                    $"Odd length mismatch at rank {i}, index {e}: Actual={actual}, Expected={expected}");
            }
            await workers[i].DisposeAsync();
        }
    }

    // =========================================================================
    // 8. Adversarial Multi-Fused Scan Filter Tests (TaskExecutor Remediation 2)
    // =========================================================================

    [Theory]
    [InlineData("age > 50", 5)] // 1 predicate
    [InlineData("age > 20|age < 80", 5)] // 2 predicates
    [InlineData("age >= 20|age <= 90|age > 30|age < 80|id > 4", 3)] // 5 chained predicates
    public async Task TaskExecutor_FusedScanFilter_ChainedPredicates_CorrectRowCounts(string filterPayload, int expectedRowCount)
    {
        Guid nodeId = Guid.NewGuid();
        using var partitionStore = new LocalPartitionStore(nodeId);
        var shuffleClient = new NoOpShuffleClient();
        using var executor = new TaskExecutor(nodeId, partitionStore, shuffleClient, maxConcurrency: 2);

        string uri = $"memory://scan_chain_test_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }),
            Int32Series.FromValues("age", new int?[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 })
        });
        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        Guid jobId = Guid.NewGuid();
        var dispatch = new StageDispatchMessage(
            jobId,
            stageId: 0,
            partitionId: 0,
            operatorType: StageOperatorType.Scan,
            operatorPayload: Encoding.UTF8.GetBytes($"{uri}|{filterPayload}"));

        var result = await executor.ExecuteTaskAsync(dispatch);

        Assert.Equal(TaskExecutionStatus.Completed, result.Status);
        Assert.Equal((ulong)expectedRowCount, result.OutputRowCount);

        var key = new PartitionKey(jobId, 0, 0);
        Assert.True(partitionStore.TryGetPartition(key, out var entry));
        Assert.NotNull(entry);
        Assert.True(entry.TryRetainBuffer(out var outBuf));
        using (outBuf!)
        {
            var resDf = PolarisBridge.PartitionBufferToDataFrame(outBuf, zeroCopy: true);
            Assert.Equal(expectedRowCount, resDf.RowCount);
            Assert.Equal(2, resDf.Columns.Count);
        }
    }

    [Fact]
    public async Task TaskExecutor_FusedScanFilter_ZeroMatchingRows_FollowedByAnotherPredicate_PreservesSchema()
    {
        Guid nodeId = Guid.NewGuid();
        using var partitionStore = new LocalPartitionStore(nodeId);
        var shuffleClient = new NoOpShuffleClient();
        using var executor = new TaskExecutor(nodeId, partitionStore, shuffleClient, maxConcurrency: 2);

        string uri = $"memory://scan_zero_test_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5 }),
            Int32Series.FromValues("age", new int?[] { 10, 20, 30, 40, 50 })
        });
        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        Guid jobId = Guid.NewGuid();
        // First predicate results in 0 rows (age > 1000), followed by age < 2000
        var dispatch = new StageDispatchMessage(
            jobId,
            stageId: 0,
            partitionId: 0,
            operatorType: StageOperatorType.Scan,
            operatorPayload: Encoding.UTF8.GetBytes($"{uri}|age > 1000|age < 2000"));

        var result = await executor.ExecuteTaskAsync(dispatch);

        Assert.Equal(TaskExecutionStatus.Completed, result.Status);
        Assert.Equal(0UL, result.OutputRowCount);

        var key = new PartitionKey(jobId, 0, 0);
        Assert.True(partitionStore.TryGetPartition(key, out var entry));
        Assert.NotNull(entry);
        Assert.True(entry.TryRetainBuffer(out var outBuf));
        using (outBuf!)
        {
            var resDf = PolarisBridge.PartitionBufferToDataFrame(outBuf, zeroCopy: true);
            Assert.Equal(0, resDf.RowCount);
            Assert.Equal(2, resDf.Columns.Count);
            Assert.Equal("id", resDf.Columns[0].Name);
            Assert.Equal("age", resDf.Columns[1].Name);
        }
    }

    [Theory]
    [InlineData("||age > 50", 5)] // Leading empty segment
    [InlineData("age > 20|||age < 80|", 5)] // Middle and trailing empty segments
    [InlineData("|||", 10)] // All empty segments - acts as full scan
    public async Task TaskExecutor_FusedScanFilter_EmptyStringSegments_ToleratesMalformedDelimiters(string malformedSegments, int expectedRowCount)
    {
        Guid nodeId = Guid.NewGuid();
        using var partitionStore = new LocalPartitionStore(nodeId);
        var shuffleClient = new NoOpShuffleClient();
        using var executor = new TaskExecutor(nodeId, partitionStore, shuffleClient, maxConcurrency: 2);

        string uri = $"memory://scan_malformed_test_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }),
            Int32Series.FromValues("age", new int?[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 })
        });
        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        Guid jobId = Guid.NewGuid();
        var dispatch = new StageDispatchMessage(
            jobId,
            stageId: 0,
            partitionId: 0,
            operatorType: StageOperatorType.Scan,
            operatorPayload: Encoding.UTF8.GetBytes($"{uri}|{malformedSegments}"));

        var result = await executor.ExecuteTaskAsync(dispatch);

        Assert.Equal(TaskExecutionStatus.Completed, result.Status);
        Assert.Equal((ulong)expectedRowCount, result.OutputRowCount);

        var key = new PartitionKey(jobId, 0, 0);
        Assert.True(partitionStore.TryGetPartition(key, out var entry));
        Assert.NotNull(entry);
        Assert.True(entry.TryRetainBuffer(out var outBuf));
        using (outBuf!)
        {
            var resDf = PolarisBridge.PartitionBufferToDataFrame(outBuf, zeroCopy: true);
            Assert.Equal(expectedRowCount, resDf.RowCount);
        }
    }

    [Fact]
    public async Task TaskExecutor_FusedScanFilter_InvalidPredicateInChain_FailsGracefullyWithoutLeak()
    {
        Guid nodeId = Guid.NewGuid();
        using var partitionStore = new LocalPartitionStore(nodeId);
        var shuffleClient = new NoOpShuffleClient();
        using var executor = new TaskExecutor(nodeId, partitionStore, shuffleClient, maxConcurrency: 2);

        string uri = $"memory://scan_invalid_pred_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("x", new int?[] { 1, 2, 3 })
        });
        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        Guid jobId = Guid.NewGuid();
        var dispatch = new StageDispatchMessage(
            jobId,
            stageId: 0,
            partitionId: 0,
            operatorType: StageOperatorType.Scan,
            operatorPayload: Encoding.UTF8.GetBytes($"{uri}|x > 1|INVALID @@@ EXPR|x < 3"));

        var result = await executor.ExecuteTaskAsync(dispatch);

        Assert.Equal(TaskExecutionStatus.Failed, result.Status);
        Assert.NotNull(result.ErrorMessage);
        Assert.Equal(0, executor.ActiveTaskCount);
    }

    [Fact]
    public async Task ClusterCoordinator_FusedScanFilter_FiveChainedFilters_EndToEndExecution()
    {
        string uri = $"memory://coord_fused_5chain_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }),
            Int32Series.FromValues("age", new int?[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 })
        });
        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        var coordOptions = new ClusterCoordinatorOptions { Port = 0 };
        using var coordinator = new ClusterCoordinator(coordOptions);
        await coordinator.StartAsync();

        var workerOptions = new ClusterWorkerOptions
        {
            NodeId = Guid.NewGuid(),
            Hostname = "test-worker-5chain",
            ShufflePort = 0
        };
        using var worker = new ClusterWorker(workerOptions);

        var toCoordinator = new Pipe();
        var toWorker = new Pipe();
        var workerSidePipe = new DuplexPipePair(toWorker.Reader, toCoordinator.Writer);
        var coordSidePipe = new DuplexPipePair(toCoordinator.Reader, toWorker.Writer);

        await coordinator.RegisterInProcessWorkerAsync(worker.NodeId, coordSidePipe);
        await worker.StartAsync(workerSidePipe);

        Guid jobId = Guid.NewGuid();
        var scan = new LogicalScanNode(uri, partitionCount: 1);
        var f1 = new LogicalFilterNode(scan, "age >= 20");
        var f2 = new LogicalFilterNode(f1, "age <= 90");
        var f3 = new LogicalFilterNode(f2, "age > 30");
        var f4 = new LogicalFilterNode(f3, "age < 80");
        var f5 = new LogicalFilterNode(f4, "id > 4");

        var compiler = new StageCompiler();
        var dag = compiler.Compile(jobId, "FiveChainFusedJob", f5);

        // All 5 filters must be fused into 1 scan stage
        Assert.Single(dag.Stages);
        var stage = dag.Stages.Values.Single();
        Assert.Equal(StageOperatorType.Scan, stage.PrimaryOperator);

        var jobCompletion = await coordinator.SubmitJobAsync(dag);
        Assert.NotNull(jobCompletion);
        Assert.Equal(JobCompletionStatus.Succeeded, jobCompletion.Status);

        var resultKey = new PartitionKey(jobId, stage.StageId, 0);
        using var resultBuf = await worker.PartitionStore.FetchPartitionAsync(resultKey);
        var resultDf = PolarisBridge.PartitionBufferToDataFrame(resultBuf, zeroCopy: true);

        // Matching rows: age in {50, 60, 70} -> rowCount == 3
        Assert.Equal(3, resultDf.RowCount);
        Assert.Equal(2, resultDf.Columns.Count);

        await worker.StopAsync();
    }

    [Fact]
    public async Task ClusterCoordinator_FusedScanFilter_ZeroRowIntermediate_EndToEndExecution()
    {
        string uri = $"memory://coord_fused_zerorow_{Guid.NewGuid():N}";
        var df = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5 }),
            Int32Series.FromValues("age", new int?[] { 10, 20, 30, 40, 50 })
        });
        using var buf = PolarisBridge.DataFrameToPartitionBuffer(df);
        InMemoryDistributedExecutionContext.RegisterDataSource(uri, new[] { buf });

        var coordOptions = new ClusterCoordinatorOptions { Port = 0 };
        using var coordinator = new ClusterCoordinator(coordOptions);
        await coordinator.StartAsync();

        var workerOptions = new ClusterWorkerOptions
        {
            NodeId = Guid.NewGuid(),
            Hostname = "test-worker-zerorow",
            ShufflePort = 0
        };
        using var worker = new ClusterWorker(workerOptions);

        var toCoordinator = new Pipe();
        var toWorker = new Pipe();
        var workerSidePipe = new DuplexPipePair(toWorker.Reader, toCoordinator.Writer);
        var coordSidePipe = new DuplexPipePair(toCoordinator.Reader, toWorker.Writer);

        await coordinator.RegisterInProcessWorkerAsync(worker.NodeId, coordSidePipe);
        await worker.StartAsync(workerSidePipe);

        Guid jobId = Guid.NewGuid();
        var scan = new LogicalScanNode(uri, partitionCount: 1);
        var f1 = new LogicalFilterNode(scan, "age > 500");
        var f2 = new LogicalFilterNode(f1, "age < 1000");

        var compiler = new StageCompiler();
        var dag = compiler.Compile(jobId, "ZeroRowFusedJob", f2);

        Assert.Single(dag.Stages);
        var stage = dag.Stages.Values.Single();

        var jobCompletion = await coordinator.SubmitJobAsync(dag);
        Assert.NotNull(jobCompletion);
        Assert.Equal(JobCompletionStatus.Succeeded, jobCompletion.Status);

        var resultKey = new PartitionKey(jobId, stage.StageId, 0);
        using var resultBuf = await worker.PartitionStore.FetchPartitionAsync(resultKey);
        var resultDf = PolarisBridge.PartitionBufferToDataFrame(resultBuf, zeroCopy: true);

        Assert.Equal(0, resultDf.RowCount);
        Assert.Equal(2, resultDf.Columns.Count);
        Assert.Equal("id", resultDf.Columns[0].Name);
        Assert.Equal("age", resultDf.Columns[1].Name);

        await worker.StopAsync();
    }
}

