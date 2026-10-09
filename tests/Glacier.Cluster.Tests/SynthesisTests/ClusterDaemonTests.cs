namespace Glacier.Cluster.Tests.SynthesisTests;

using System;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Glacier.Cluster.Shuffle;
using Glacier.Cluster.Worker;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

public sealed class ClusterDaemonTests
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

    [Fact]
    public async Task MembershipManager_ContiguousRankAssignmentAndRecycling()
    {
        using var manager = new MembershipManager();

        var node1 = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());
        var node2 = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());
        var node3 = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());

        var endPoint = new IPEndPoint(IPAddress.Loopback, 8000);

        var resp1 = await manager.RegisterWorkerAsync(new NodeJoinRequestMessage(node1, "host1"), endPoint, 9001);
        var resp2 = await manager.RegisterWorkerAsync(new NodeJoinRequestMessage(node2, "host2"), endPoint, 9002);
        var resp3 = await manager.RegisterWorkerAsync(new NodeJoinRequestMessage(node3, "host3"), endPoint, 9003);

        Assert.Equal(0, resp1.AssignedRank);
        Assert.Equal(1, resp2.AssignedRank);
        Assert.Equal(2, resp3.AssignedRank);
        Assert.Equal(3, manager.ActiveNodeCount);

        // Depart node 2
        await manager.HandleWorkerLeaveAsync(new NodeLeaveMessage(node2.NodeId, 0, 1000, "Normal"));
        Assert.Equal(2, manager.ActiveNodeCount);

        // New node should recycle rank 1
        var node4 = NodeHardwareProfile.ProbeLocal(nodeId: Guid.NewGuid());
        var resp4 = await manager.RegisterWorkerAsync(new NodeJoinRequestMessage(node4, "host4"), endPoint, 9004);
        Assert.Equal(1, resp4.AssignedRank);
    }

    [Fact]
    public void ActiveStandbyConsensusLedger_LeaderLeaseRenewal()
    {
        Guid coordinatorId = Guid.NewGuid();
        var options = new ConsensusOptions
        {
            InitialRole = ConsensusRole.Active,
            LeaseDuration = TimeSpan.FromSeconds(5)
        };

        using var ledger = new ActiveStandbyConsensusLedger(coordinatorId, options);
        Assert.Equal(ConsensusRole.Active, ledger.CurrentRole);

        bool renewed = ledger.TryRenewLease();
        Assert.True(renewed);
        Assert.False(ledger.CurrentLease.IsExpired);
        Assert.Equal(coordinatorId, ledger.CurrentLease.LeaderId);
    }

    [Fact]
    public async Task ClusterCoordinator_And_ClusterWorker_InProcessExecution()
    {
        var coordOptions = new ClusterCoordinatorOptions
        {
            Port = 0 // Ephemeral or disabled
        };

        using var coordinator = new ClusterCoordinator(coordOptions);
        await coordinator.StartAsync();

        var workerOptions = new ClusterWorkerOptions
        {
            NodeId = Guid.NewGuid(),
            Hostname = "test-worker",
            ShufflePort = 0
        };

        using var worker = new ClusterWorker(workerOptions);

        var toCoordinator = new Pipe();
        var toWorker = new Pipe();

        var workerSidePipe = new DuplexPipePair(toWorker.Reader, toCoordinator.Writer);
        var coordSidePipe = new DuplexPipePair(toCoordinator.Reader, toWorker.Writer);

        await coordinator.RegisterInProcessWorkerAsync(worker.NodeId, coordSidePipe);
        await worker.StartAsync(workerSidePipe);

        Assert.Equal(0, worker.AssignedRank);
        Assert.Equal(1, coordinator.Membership.ActiveNodeCount);

        // Populate a local test partition in worker store
        var testDf = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("id", new int?[] { 1, 2, 3, 4, 5 }),
            Int32Series.FromValues("age", new int?[] { 20, 35, 42, 18, 50 })
        });
        var testBuf = PolarisBridge.DataFrameToPartitionBuffer(testDf);

        Guid jobId = Guid.NewGuid();
        int scanStageId = 0;
        int filterStageId = 1;

        var scanKey = new PartitionKey(jobId, scanStageId, 0);
        worker.PartitionStore.RegisterPartition(scanKey, testBuf);

        // Build DAG: Scan (stage 0) -> Filter age > 30 (stage 1)
        var scanStage = new ExecutionStage(
            scanStageId,
            jobId,
            "Scan",
            isShuffleBoundary: false,
            partitionsCount: 1,
            primaryOperator: StageOperatorType.Scan,
            operatorPayload: Encoding.UTF8.GetBytes("memory://test"));

        var filterStage = new ExecutionStage(
            filterStageId,
            jobId,
            "Filter",
            isShuffleBoundary: false,
            partitionsCount: 1,
            primaryOperator: StageOperatorType.Filter,
            operatorPayload: Encoding.UTF8.GetBytes("age > 30"),
            parentStageIds: new[] { scanStageId });

        var dag = new ExecutionDag(jobId, "TestJob", new[] { scanStage, filterStage });

        // Submit Job
        var jobCompletion = await coordinator.SubmitJobAsync(dag);
        Assert.NotNull(jobCompletion);
        Assert.Equal(JobCompletionStatus.Succeeded, jobCompletion.Status);

        // Fetch result partition from worker
        var resultKey = new PartitionKey(jobId, filterStageId, 0);
        using var resultBuf = await worker.PartitionStore.FetchPartitionAsync(resultKey);
        var resultDf = PolarisBridge.PartitionBufferToDataFrame(resultBuf, zeroCopy: true);

        Assert.Equal(3, resultDf.RowCount); // 35, 42, 50

        await worker.StopAsync();
    }
}
