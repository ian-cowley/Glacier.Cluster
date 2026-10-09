namespace Glacier.Cluster.Tests.SchedulerTests;

using System;
using System.Collections.Generic;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Xunit;

public sealed class TwoPhaseSchedulerTests
{
    private static readonly Guid LaptopNodeId = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid BeastNodeId = Guid.Parse("22222222-2222-2222-2222-222222222222");
    private static readonly Guid FedoraNodeId = Guid.Parse("33333333-3333-3333-3333-333333333333");
    private static readonly Guid MinisForumNodeId = Guid.Parse("44444444-4444-4444-4444-444444444444");

    // Fleet Hardware Profiles:
    // 1. Host Laptop: AMD Ryzen AI 9 HX 370 (12C/24T, AVX-512), RTX 4060 (8GB VRAM, CUDA), 32GB RAM
    private static NodeHardwareProfile CreateLaptopProfile() => new(
        LaptopNodeId,
        cpuLogicalCores: 24,
        simdFlags: SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
        gpuFlags: GpuBackendFlags.Cuda | GpuBackendFlags.Direct3D12 | GpuBackendFlags.Vulkan,
        ramTotalBytes: 32UL * 1024 * 1024 * 1024,
        ramAvailableBytes: 24UL * 1024 * 1024 * 1024,
        vramTotalBytes: 8UL * 1024 * 1024 * 1024,
        vramAvailableBytes: 6UL * 1024 * 1024 * 1024);

    // 2. Beast: AMD Ryzen 5 5500 (6C/12T, AVX2), RTX 3060 (12GB VRAM, CUDA), 64GB RAM
    private static NodeHardwareProfile CreateBeastProfile() => new(
        BeastNodeId,
        cpuLogicalCores: 12,
        simdFlags: SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
        gpuFlags: GpuBackendFlags.Cuda | GpuBackendFlags.Direct3D12 | GpuBackendFlags.Vulkan,
        ramTotalBytes: 64UL * 1024 * 1024 * 1024,
        ramAvailableBytes: 56UL * 1024 * 1024 * 1024,
        vramTotalBytes: 12UL * 1024 * 1024 * 1024,
        vramAvailableBytes: 10UL * 1024 * 1024 * 1024);

    // 3. Fedora: AMD Ryzen AI 9 HX 370 (12C/24T, AVX-512), Radeon 890M (Vulkan/ROCm, No CUDA), 32GB RAM
    private static NodeHardwareProfile CreateFedoraProfile() => new(
        FedoraNodeId,
        cpuLogicalCores: 24,
        simdFlags: SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
        gpuFlags: GpuBackendFlags.Vulkan | GpuBackendFlags.Rocm,
        ramTotalBytes: 32UL * 1024 * 1024 * 1024,
        ramAvailableBytes: 26UL * 1024 * 1024 * 1024,
        vramTotalBytes: 4UL * 1024 * 1024 * 1024,
        vramAvailableBytes: 4UL * 1024 * 1024 * 1024);

    // 4. MinisForum64: AMD Ryzen 9 6900HX (8C/16T, AVX2), Radeon 680M (Vulkan, No CUDA), 64GB RAM
    private static NodeHardwareProfile CreateMinisForumProfile() => new(
        MinisForumNodeId,
        cpuLogicalCores: 16,
        simdFlags: SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
        gpuFlags: GpuBackendFlags.Vulkan,
        ramTotalBytes: 64UL * 1024 * 1024 * 1024,
        ramAvailableBytes: 58UL * 1024 * 1024 * 1024,
        vramTotalBytes: 4UL * 1024 * 1024 * 1024,
        vramAvailableBytes: 4UL * 1024 * 1024 * 1024);

    [Fact]
    public void Phase1_Disqualifies_NonCuda_Nodes_When_Task_Requires_Cuda()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest
        {
            RequiresGpu = true,
            RequiredGpu = GpuBackendFlags.Cuda,
            MinVramBytes = 4UL * 1024 * 1024 * 1024
        };

        var fleet = new List<NodeHardwareProfile>
        {
            CreateLaptopProfile(),
            CreateBeastProfile(),
            CreateFedoraProfile(),
            CreateMinisForumProfile()
        };

        var decision = scheduler.ScheduleTask(task, fleet);

        Assert.True(decision.IsSuccess);
        Assert.NotNull(decision.SelectedNode);
        // Only Laptop and Beast have CUDA; Fedora and MinisForum must be disqualified
        Assert.True(decision.SelectedNode.Value.NodeId == LaptopNodeId || decision.SelectedNode.Value.NodeId == BeastNodeId);
        Assert.Equal(FilterDisqualificationReason.MissingCudaCapability, decision.DisqualifiedCandidates[FedoraNodeId]);
        Assert.Equal(FilterDisqualificationReason.MissingCudaCapability, decision.DisqualifiedCandidates[MinisForumNodeId]);
    }

    [Fact]
    public void Phase1_Disqualifies_Node_With_Insufficient_Ram()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest
        {
            RequiresGpu = false,
            MinRamBytes = 48UL * 1024 * 1024 * 1024 // 48 GB RAM requirement
        };

        var fleet = new List<NodeHardwareProfile>
        {
            CreateLaptopProfile(),     // 24 GB available -> Disqualified
            CreateBeastProfile(),      // 56 GB available -> Eligible
            CreateFedoraProfile(),     // 26 GB available -> Disqualified
            CreateMinisForumProfile()  // 58 GB available -> Eligible
        };

        var decision = scheduler.ScheduleTask(task, fleet);

        Assert.True(decision.IsSuccess);
        Assert.Equal(FilterDisqualificationReason.InsufficientRam, decision.DisqualifiedCandidates[LaptopNodeId]);
        Assert.Equal(FilterDisqualificationReason.InsufficientRam, decision.DisqualifiedCandidates[FedoraNodeId]);
        Assert.True(decision.SelectedNode!.Value.NodeId == BeastNodeId || decision.SelectedNode!.Value.NodeId == MinisForumNodeId);
    }

    [Fact]
    public void Phase1_Disqualifies_Node_With_Insufficient_Vram()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest
        {
            RequiresGpu = true,
            RequiredGpu = GpuBackendFlags.Cuda,
            MinVramBytes = 9UL * 1024 * 1024 * 1024 // 9 GB VRAM required (Laptop has 6GB avail, Beast has 10GB avail)
        };

        var fleet = new List<NodeHardwareProfile>
        {
            CreateLaptopProfile(),
            CreateBeastProfile()
        };

        var decision = scheduler.ScheduleTask(task, fleet);

        Assert.True(decision.IsSuccess);
        Assert.Equal(BeastNodeId, decision.SelectedNode!.Value.NodeId);
        Assert.Equal(FilterDisqualificationReason.InsufficientVram, decision.DisqualifiedCandidates[LaptopNodeId]);
    }

    [Fact]
    public void Phase1_Disqualifies_Suspect_And_Evicted_Nodes()
    {
        var detector = new PhiAccrualFailureDetector();
        ulong t0 = 1_000_000_000UL;

        // Register Laptop and Beast
        detector.RegisterNode(LaptopNodeId, t0);
        detector.RegisterNode(BeastNodeId, t0);

        // Laptop is silent for 20 seconds -> Evicted
        ulong tNow = t0 + 20_000_000_000UL;
        detector.RecordHeartbeat(BeastNodeId, tNow - 100_000_000UL); // Beast sent recent ping

        var scheduler = new TwoPhaseScheduler(failureDetector: detector);
        var task = new TaskPlacementRequest { RequiresGpu = false };

        var fleet = new List<NodeHardwareProfile>
        {
            CreateLaptopProfile(),
            CreateBeastProfile()
        };

        var decision = scheduler.ScheduleTask(task, fleet, currentTimestampNs: tNow);

        Assert.True(decision.IsSuccess);
        Assert.Equal(BeastNodeId, decision.SelectedNode!.Value.NodeId);
        Assert.Equal(FilterDisqualificationReason.NodeNotHealthy, decision.DisqualifiedCandidates[LaptopNodeId]);
    }

    [Fact]
    public void Phase1_Returns_Detailed_Diagnostics_When_All_Nodes_Fail()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest
        {
            RequiresGpu = true,
            RequiredGpu = GpuBackendFlags.Cuda,
            MinVramBytes = 24UL * 1024 * 1024 * 1024 // 24 GB VRAM (none has 24GB)
        };

        var fleet = new List<NodeHardwareProfile>
        {
            CreateLaptopProfile(),
            CreateBeastProfile(),
            CreateFedoraProfile()
        };

        var decision = scheduler.ScheduleTask(task, fleet);

        Assert.False(decision.IsSuccess);
        Assert.Null(decision.SelectedNode);
        Assert.Equal(3, decision.DisqualifiedCandidates.Count);
        Assert.Equal(FilterDisqualificationReason.InsufficientVram, decision.DisqualifiedCandidates[LaptopNodeId]);
        Assert.Equal(FilterDisqualificationReason.InsufficientVram, decision.DisqualifiedCandidates[BeastNodeId]);
        Assert.Equal(FilterDisqualificationReason.MissingCudaCapability, decision.DisqualifiedCandidates[FedoraNodeId]);
    }

    [Fact]
    public void Phase2_Awards_Avx512_Over_Avx2_And_Scalar()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest { RequiresGpu = false };

        var nodeAvx512 = new NodeHardwareProfile(
            Guid.NewGuid(), 16, SimdArchitectureFlags.Avx512, GpuBackendFlags.None,
            32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);

        var nodeAvx2 = new NodeHardwareProfile(
            Guid.NewGuid(), 16, SimdArchitectureFlags.Avx2, GpuBackendFlags.None,
            32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);

        var nodeScalar = new NodeHardwareProfile(
            Guid.NewGuid(), 16, SimdArchitectureFlags.None, GpuBackendFlags.None,
            32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);

        var candidates = new List<NodeHardwareProfile> { nodeScalar, nodeAvx2, nodeAvx512 };
        var decision = scheduler.ScheduleTask(task, candidates);

        Assert.True(decision.IsSuccess);
        Assert.Equal(nodeAvx512.NodeId, decision.SelectedNode!.Value.NodeId);

        double score512 = scheduler.ComputeAffinityScore(task, in nodeAvx512, 0);
        double scoreAvx2 = scheduler.ComputeAffinityScore(task, in nodeAvx2, 0);
        double scoreScalar = scheduler.ComputeAffinityScore(task, in nodeScalar, 0);

        // Score diffs: AVX-512 has +100, AVX2 has +50, Scalar has +0
        Assert.Equal(50.0, score512 - scoreAvx2, 3);
        Assert.Equal(50.0, scoreAvx2 - scoreScalar, 3);
    }

    [Fact]
    public void Phase2_Awards_AdvSimd_Over_Sse42()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest { RequiresGpu = false };

        var nodeAdvSimd = new NodeHardwareProfile(
            Guid.NewGuid(), 8, SimdArchitectureFlags.AdvSimd, GpuBackendFlags.None,
            16UL * 1024 * 1024 * 1024, 16UL * 1024 * 1024 * 1024, 0, 0);

        var nodeSse = new NodeHardwareProfile(
            Guid.NewGuid(), 8, SimdArchitectureFlags.Sse42, GpuBackendFlags.None,
            16UL * 1024 * 1024 * 1024, 16UL * 1024 * 1024 * 1024, 0, 0);

        var decision = scheduler.ScheduleTask(task, new[] { nodeSse, nodeAdvSimd });

        Assert.True(decision.IsSuccess);
        Assert.Equal(nodeAdvSimd.NodeId, decision.SelectedNode!.Value.NodeId);

        double scoreAdv = scheduler.ComputeAffinityScore(task, in nodeAdvSimd, 0);
        double scoreSse = scheduler.ComputeAffinityScore(task, in nodeSse, 0);
        Assert.Equal(40.0, scoreAdv - scoreSse, 3);
    }

    [Fact]
    public void Phase2_Data_Locality_Overrides_Simd_Advantage()
    {
        var scheduler = new TwoPhaseScheduler();

        // Node 1: AVX-512 (+100), but partition is remote
        var node1 = new NodeHardwareProfile(
            Guid.NewGuid(), 16, SimdArchitectureFlags.Avx512, GpuBackendFlags.None,
            32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);

        // Node 2: AVX2 (+50), but partition is LOCAL (+200)
        var node2 = new NodeHardwareProfile(
            Guid.NewGuid(), 16, SimdArchitectureFlags.Avx2, GpuBackendFlags.None,
            32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);

        var task = new TaskPlacementRequest
        {
            RequiresGpu = false,
            PreferredNodeId = node2.NodeId
        };

        var decision = scheduler.ScheduleTask(task, new[] { node1, node2 });

        Assert.True(decision.IsSuccess);
        Assert.Equal(node2.NodeId, decision.SelectedNode!.Value.NodeId);

        double score1 = scheduler.ComputeAffinityScore(task, in node1, 0);
        double score2 = scheduler.ComputeAffinityScore(task, in node2, 0);

        // Node 2 has +200 locality + 50 AVX2 = +250 vs Node 1 = +100 AVX-512
        Assert.True(score2 > score1);
        Assert.Equal(150.0, score2 - score1, 3);
    }

    [Fact]
    public void Phase2_Prioritizes_Idle_Cores()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest { RequiresGpu = false };

        // Two identical AVX2 nodes
        var nodeBusy = new NodeHardwareProfile(
            Guid.NewGuid(), 12, SimdArchitectureFlags.Avx2, GpuBackendFlags.None,
            32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);

        var nodeIdle = new NodeHardwareProfile(
            Guid.NewGuid(), 12, SimdArchitectureFlags.Avx2, GpuBackendFlags.None,
            32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);

        var activeTasks = new Dictionary<Guid, uint>
        {
            [nodeBusy.NodeId] = 12, // 100% loaded -> CPU load score = 0
            [nodeIdle.NodeId] = 0   // 0% loaded -> CPU load score = +50
        };

        var decision = scheduler.ScheduleTask(task, new[] { nodeBusy, nodeIdle }, activeTasks);

        Assert.True(decision.IsSuccess);
        Assert.Equal(nodeIdle.NodeId, decision.SelectedNode!.Value.NodeId);
    }

    [Fact]
    public void Phase2_Deterministic_Tie_Breaking()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest { RequiresGpu = false };

        var id1 = Guid.Parse("00000000-0000-0000-0000-000000000001");
        var id2 = Guid.Parse("00000000-0000-0000-0000-000000000002");

        var nodeA = new NodeHardwareProfile(
            id1, 8, SimdArchitectureFlags.Avx2, GpuBackendFlags.None,
            16UL * 1024 * 1024 * 1024, 16UL * 1024 * 1024 * 1024, 0, 0);

        var nodeB = new NodeHardwareProfile(
            id2, 8, SimdArchitectureFlags.Avx2, GpuBackendFlags.None,
            16UL * 1024 * 1024 * 1024, 16UL * 1024 * 1024 * 1024, 0, 0);

        for (int i = 0; i < 100; i++)
        {
            var decision = scheduler.ScheduleTask(task, new[] { nodeA, nodeB });
            Assert.True(decision.IsSuccess);
            // Lexicographical comparison id1 < id2 means nodeA is always selected
            Assert.Equal(id1, decision.SelectedNode!.Value.NodeId);
        }
    }

    [Fact]
    public void ScheduleStageTasks_Balances_Load_Across_Feasible_Nodes()
    {
        var scheduler = new TwoPhaseScheduler();

        // 3 identical nodes
        var n1 = new NodeHardwareProfile(Guid.NewGuid(), 8, SimdArchitectureFlags.Avx2, GpuBackendFlags.None, 32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);
        var n2 = new NodeHardwareProfile(Guid.NewGuid(), 8, SimdArchitectureFlags.Avx2, GpuBackendFlags.None, 32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);
        var n3 = new NodeHardwareProfile(Guid.NewGuid(), 8, SimdArchitectureFlags.Avx2, GpuBackendFlags.None, 32UL * 1024 * 1024 * 1024, 32UL * 1024 * 1024 * 1024, 0, 0);
        var nodes = new[] { n1, n2, n3 };

        // 12 tasks to schedule
        var tasks = new List<ITaskPlacementDescriptor>();
        for (int i = 0; i < 12; i++)
        {
            tasks.Add(new TaskPlacementRequest { RequiresGpu = false });
        }

        var decisions = scheduler.ScheduleStageTasks(tasks, nodes);

        Assert.Equal(12, decisions.Count);
        var counts = new Dictionary<Guid, int>();
        foreach (var d in decisions)
        {
            Assert.True(d.IsSuccess);
            var nid = d.SelectedNode!.Value.NodeId;
            counts[nid] = counts.GetValueOrDefault(nid) + 1;
        }

        // 12 tasks across 3 nodes must be 4 tasks each!
        Assert.Equal(3, counts.Count);
        Assert.Equal(4, counts[n1.NodeId]);
        Assert.Equal(4, counts[n2.NodeId]);
        Assert.Equal(4, counts[n3.NodeId]);
    }
}
