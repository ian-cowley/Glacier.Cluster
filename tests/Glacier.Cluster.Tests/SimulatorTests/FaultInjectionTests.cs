namespace Glacier.Cluster.Tests.SimulatorTests;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.FaultInjection;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;
using Glacier.Cluster.Simulator;
using Glacier.Cluster.Worker;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

public sealed class FaultInjectionTests
{
    [Fact]
    public async Task NarrowStage_WorkerCrash_Rescheduled()
    {
        await using var simulator = await ClusterSimulator.CreateAsync(4);
        var injector = new FaultInjector(simulator);

        // Crash worker 1 before query execution
        var workerToCrash = simulator.Workers[1];
        await injector.CrashWorkerAsync(workerToCrash.NodeId);

        // Evict crashed worker from membership
        var deadNode = simulator.Coordinator.Membership.GetAllNodes().FirstOrDefault(n => n.NodeId == workerToCrash.NodeId);
        if (deadNode != null)
        {
            deadNode.HealthState = NodeHealthState.Evicted;
            simulator.Coordinator.Membership.FailureDetector.UnregisterNode(workerToCrash.NodeId);
        }

        int nodeCount = 3;
        int rowsPerNode = 50;
        var (partitions, singleNodeDf) = GenerateTestDataset(nodeCount, rowsPerNode);
        var ddf = simulator.CreateDataFrame(partitions);

        var resultDf = await ddf.Filter("salary > 50000.0").CollectAsync();
        var expectedDf = singleNodeDf.Filter(Expr.Col("salary") > 50000.0);

        Assert.Equal(expectedDf.RowCount, resultDf.RowCount);
        Assert.True(resultDf.RowCount > 0);
    }

    [Fact]
    public void WideShuffle_WorkerCrash_LineageRecompute()
    {
        var jobId = Guid.NewGuid();
        var s0 = new ExecutionStage(0, jobId, "Scan", false, 4, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "Filter", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s2 = new ExecutionStage(2, jobId, "Map", false, 4, StageOperatorType.GroupByMap, Array.Empty<byte>(), parentStageIds: new[] { 1 });
        var s3 = new ExecutionStage(3, jobId, "Reduce", true, 2, StageOperatorType.GroupByReduce, Array.Empty<byte>(), parentStageIds: new[] { 2 });

        var dag = new ExecutionDag(jobId, "LineageRecomputeDAG", new[] { s0, s1, s2, s3 });

        // Simulate complete execution of all stages
        for (int p = 0; p < 4; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, p, TaskExecutionStatus.Completed, 5, 100, 10));
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, p, TaskExecutionStatus.Completed, 5, 100, 10));
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 2, p, TaskExecutionStatus.Completed, 5, 100, 10));
        }
        for (int p = 0; p < 2; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 3, p, TaskExecutionStatus.Completed, 5, 100, 10));
        }

        Assert.True(dag.IsJobCompleted);

        // Invalidate Stage 1 Partition 2 (narrow stage partition lost due to worker crash)
        dag.InvalidateStagePartition(stageId: 1, partitionId: 2);

        // Narrow stage 1: only partition 2 is invalidated
        Assert.Equal(TaskState.Pending, s1.Tasks[2].State);
        Assert.Equal(TaskState.Completed, s1.Tasks[0].State);

        // Narrow stage 2: only partition 2 is invalidated
        Assert.Equal(TaskState.Pending, s2.Tasks[2].State);
        Assert.Equal(TaskState.Completed, s2.Tasks[0].State);

        // Wide stage 3: ALL partitions are invalidated due to wide shuffle dependency
        Assert.Equal(TaskState.Pending, s3.Tasks[0].State);
        Assert.Equal(TaskState.Pending, s3.Tasks[1].State);

        // Upstream stage 0: remains completed
        Assert.All(s0.Tasks, t => Assert.Equal(TaskState.Completed, t.State));
        Assert.False(dag.IsJobCompleted);

        // Recompute s1 partition 2
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, 2, TaskExecutionStatus.Completed, 5, 100, 10));
        Assert.True(s1.IsCompleted);

        // Recompute s2 partition 2
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 2, 2, TaskExecutionStatus.Completed, 5, 100, 10));
        Assert.True(s2.IsCompleted);

        // Recompute s3 partitions
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 3, 0, TaskExecutionStatus.Completed, 5, 100, 10));
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 3, 1, TaskExecutionStatus.Completed, 5, 100, 10));

        Assert.True(dag.IsJobCompleted);
    }

    [Fact]
    public void TransientHeartbeatJitter_NoFalseEviction()
    {
        var detector = new PhiAccrualFailureDetector(
            suspectThreshold: 4.0,
            evictionThreshold: 12.0,
            minStdDevMs: 15.0,
            historyWindowSize: 50);

        var nodeId = Guid.NewGuid();

        // Feed consistent 50ms intervals in nanoseconds
        ulong baseTimeNs = 1_000_000_000UL;
        ulong stepNs = 50_000_000UL; // 50ms

        for (int i = 0; i < 30; i++)
        {
            detector.RecordHeartbeat(nodeId, baseTimeNs + ((ulong)i * stepNs));
        }

        // Current time is immediately after last heartbeat: Healthy
        ulong lastTimeNs = baseTimeNs + (29 * stepNs);
        Assert.Equal(NodeHealthState.Healthy, detector.GetHealthState(nodeId, lastTimeNs));

        // Jitter: moderate delay (120ms instead of 50ms) -> suspicion rises into Suspect but NOT Evicted
        ulong jitterTimeNs = lastTimeNs + 120_000_000UL;
        double phi = detector.ComputePhi(nodeId, jitterTimeNs);
        Assert.True(phi >= 4.0, $"Expected phi >= 4.0 but got {phi}");
        Assert.True(phi < 12.0, $"Expected phi < 12.0 but got {phi}");
        Assert.Equal(NodeHealthState.Suspect, detector.GetHealthState(nodeId, jitterTimeNs));

        // Next heartbeat arrives -> restores to Healthy
        ulong restoredTimeNs = jitterTimeNs + 10_000_000UL;
        detector.RecordHeartbeat(nodeId, restoredTimeNs);
        Assert.Equal(NodeHealthState.Healthy, detector.GetHealthState(nodeId, restoredTimeNs));
    }

    [Fact]
    public void DownstreamFetchFailure_AutoRecovery()
    {
        Guid producerId = Guid.NewGuid();
        string fetchError = $"FetchFailed: Stage=1, Partition=2, Producer={producerId}: Connection reset by peer";

        bool parsed = ClusterCoordinator.TryParseFetchFailedError(
            fetchError,
            out int upstreamStageId,
            out int upstreamPartitionId,
            out Guid upstreamProducer);

        Assert.True(parsed);
        Assert.Equal(1, upstreamStageId);
        Assert.Equal(2, upstreamPartitionId);
        Assert.Equal(producerId, upstreamProducer);

        // Verify UpstreamPartitionRegistry InvalidatePartition and RemoveLocationsForNode
        var registry = new UpstreamPartitionRegistry();
        registry.RegisterPartitionLocation(1, 2, new UpstreamPartitionLocation(producerId, 1, 2, 7000, "node1"));
        registry.RegisterPartitionLocation(1, 3, new UpstreamPartitionLocation(producerId, 1, 3, 7000, "node1"));
        registry.RegisterPartitionLocation(1, 4, new UpstreamPartitionLocation(Guid.NewGuid(), 1, 4, 7000, "node2"));

        Assert.Equal(3, registry.GetLocationsForStage(1).Length);

        registry.InvalidatePartition(1, 2);
        Assert.Equal(2, registry.GetLocationsForStage(1).Length);
        Assert.DoesNotContain(registry.GetLocationsForStage(1), loc => loc.PartitionId == 2);

        registry.RemoveLocationsForNode(producerId);
        Assert.Single(registry.GetLocationsForStage(1));
        Assert.Equal(4, registry.GetLocationsForStage(1)[0].PartitionId);
    }

    [Fact]
    public async Task TaskRetryExhaustion_CleanJobFailure()
    {
        var coordOptions = new ClusterCoordinatorOptions { Port = 0, MaxTaskRetries = 2 };
        using var coordinator = new ClusterCoordinator(coordOptions);
        await coordinator.StartAsync();

        var worker = new ClusterWorker(new ClusterWorkerOptions { Hostname = "worker-fail", DisableSocketListener = true });
        var (workerPipe, coordPipe) = DuplexPipeFactory.CreateInProcessPair();
        await coordinator.RegisterInProcessWorkerAsync(worker.NodeId, coordPipe);
        await worker.StartAsync(workerPipe);

        Guid jobId = Guid.NewGuid();
        // Stage with empty upstreams and unknown operator that causes execution failure
        var stage = new ExecutionStage(
            0,
            jobId,
            "FailingStage",
            isShuffleBoundary: false,
            partitionsCount: 1,
            primaryOperator: StageOperatorType.Filter,
            operatorPayload: Encoding.UTF8.GetBytes("unknown_column > 0"));

        var dag = new ExecutionDag(jobId, "FailingJob", new[] { stage });

        var completion = await coordinator.SubmitJobAsync(dag);
        Assert.Equal(JobCompletionStatus.Failed, completion.Status);
        Assert.NotNull(completion.ErrorMessage);

        await worker.StopAsync();
    }

    [Fact]
    public async Task ActiveStandbyConsensus_CoordinatorFailover()
    {
        var options = new ConsensusOptions
        {
            InitialRole = ConsensusRole.Standby,
            RenewInterval = TimeSpan.FromMilliseconds(30),
            LeaseDuration = TimeSpan.FromMilliseconds(150),
            GracePeriod = TimeSpan.FromMilliseconds(30)
        };

        var standbyCoordId = Guid.NewGuid();
        using var standbyLedger = new ActiveStandbyConsensusLedger(standbyCoordId, options);

        bool promotedFired = false;
        standbyLedger.PromotedToActive += () => promotedFired = true;

        await standbyLedger.StartAsync();
        Assert.Equal(ConsensusRole.Standby, standbyLedger.CurrentRole);

        // Leader was active but fails to renew lease
        var leaderId = Guid.NewGuid();
        DateTimeOffset leaseExpiry = DateTimeOffset.UtcNow.AddMilliseconds(150);
        standbyLedger.UpdateLeaderLeaseFromRemote(leaderId, epoch: 1, leaseExpiry);

        Assert.Equal(ConsensusRole.Standby, standbyLedger.CurrentRole);

        // Wait for leader lease to expire (>150ms + 30ms grace)
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < 5000 && (!promotedFired || standbyLedger.CurrentRole != ConsensusRole.Active))
        {
            await Task.Delay(20);
        }

        Assert.Equal(ConsensusRole.Active, standbyLedger.CurrentRole);
        Assert.True(promotedFired);
        Assert.Equal(standbyCoordId, standbyLedger.CurrentLease.LeaderId);
    }

    private static (List<DataFrame> Partitions, DataFrame Consolidated) GenerateTestDataset(int nodeCount, int rowsPerNode)
    {
        var partitions = new List<DataFrame>(nodeCount);
        var allIds = new List<int?>();
        var allAges = new List<int?>();
        var allSalaries = new List<double?>();
        var allDepts = new List<string?>();

        int currentId = 1;
        for (int p = 0; p < nodeCount; p++)
        {
            var pIds = new int?[rowsPerNode];
            var pAges = new int?[rowsPerNode];
            var pSalaries = new double?[rowsPerNode];
            var pDepts = new string?[rowsPerNode];

            for (int r = 0; r < rowsPerNode; r++)
            {
                int id = currentId++;
                int age = 22 + (id % 35);
                double salary = 40000.0 + (id * 300.0);
                string dept = (id % 4) switch { 0 => "Engineering", 1 => "Marketing", 2 => "Sales", _ => "Operations" };

                pIds[r] = id;
                pAges[r] = age;
                pSalaries[r] = salary;
                pDepts[r] = dept;

                allIds.Add(id);
                allAges.Add(age);
                allSalaries.Add(salary);
                allDepts.Add(dept);
            }

            partitions.Add(new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("id", pIds),
                Int32Series.FromValues("age", pAges),
                Float64Series.FromValues("salary", pSalaries),
                Utf8StringSeries.FromStrings("dept", pDepts)
            }));
        }

        var consolidated = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", allIds.ToArray()),
            Int32Series.FromValues("age", allAges.ToArray()),
            Float64Series.FromValues("salary", allSalaries.ToArray()),
            Utf8StringSeries.FromStrings("dept", allDepts.ToArray())
        });

        return (partitions, consolidated);
    }
}
