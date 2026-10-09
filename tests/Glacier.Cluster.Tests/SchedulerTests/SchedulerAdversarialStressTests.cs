namespace Glacier.Cluster.Tests.SchedulerTests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Xunit;

/// <summary>
/// Empirical Adversarial Stress Test Suite challenging TwoPhaseScheduler,
/// PhiAccrualFailureDetector, and ExecutionDag under extreme failure, scale, and jitter conditions.
/// </summary>
public sealed class SchedulerAdversarialStressTests
{
    // =========================================================================
    // SECTION 1: Massive Concurrent Task Placement Across 100+ Simulated Nodes
    // =========================================================================

    [Fact]
    public void Massive_Concurrent_Task_Placement_128_Nodes_Under_Parallel_Load()
    {
        // 1. Generate 128 heterogeneous simulated cluster nodes across 8 distinct hardware profiles
        const int nodeCount = 128;
        var nodes = new List<NodeHardwareProfile>(nodeCount);

        for (int i = 0; i < nodeCount; i++)
        {
            var nodeId = Guid.NewGuid();
            int profileType = i % 8;

            NodeHardwareProfile profile = profileType switch
            {
                // Profile 0: High-end GPU Server (64 cores, AVX-512, CUDA, 512GB RAM, 80GB VRAM)
                0 => new NodeHardwareProfile(
                    nodeId,
                    cpuLogicalCores: 64,
                    simdFlags: SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                    gpuFlags: GpuBackendFlags.Cuda | GpuBackendFlags.Direct3D12 | GpuBackendFlags.Vulkan,
                    ramTotalBytes: 512UL * 1024 * 1024 * 1024,
                    ramAvailableBytes: 400UL * 1024 * 1024 * 1024,
                    vramTotalBytes: 80UL * 1024 * 1024 * 1024,
                    vramAvailableBytes: 70UL * 1024 * 1024 * 1024),

                // Profile 1: Mid-tier GPU Workstation (24 cores, AVX-512, CUDA, 64GB RAM, 16GB VRAM)
                1 => new NodeHardwareProfile(
                    nodeId,
                    cpuLogicalCores: 24,
                    simdFlags: SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                    gpuFlags: GpuBackendFlags.Cuda | GpuBackendFlags.Vulkan,
                    ramTotalBytes: 64UL * 1024 * 1024 * 1024,
                    ramAvailableBytes: 48UL * 1024 * 1024 * 1024,
                    vramTotalBytes: 16UL * 1024 * 1024 * 1024,
                    vramAvailableBytes: 12UL * 1024 * 1024 * 1024),

                // Profile 2: Low-tier GPU Machine (12 cores, AVX2, CUDA, 32GB RAM, 8GB VRAM)
                2 => new NodeHardwareProfile(
                    nodeId,
                    cpuLogicalCores: 12,
                    simdFlags: SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                    gpuFlags: GpuBackendFlags.Cuda,
                    ramTotalBytes: 32UL * 1024 * 1024 * 1024,
                    ramAvailableBytes: 24UL * 1024 * 1024 * 1024,
                    vramTotalBytes: 8UL * 1024 * 1024 * 1024,
                    vramAvailableBytes: 6UL * 1024 * 1024 * 1024),

                // Profile 3: AMD ROCm Server (64 cores, AVX-512, ROCm, 256GB RAM, 64GB VRAM)
                3 => new NodeHardwareProfile(
                    nodeId,
                    cpuLogicalCores: 64,
                    simdFlags: SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2,
                    gpuFlags: GpuBackendFlags.Rocm | GpuBackendFlags.Vulkan,
                    ramTotalBytes: 256UL * 1024 * 1024 * 1024,
                    ramAvailableBytes: 200UL * 1024 * 1024 * 1024,
                    vramTotalBytes: 64UL * 1024 * 1024 * 1024,
                    vramAvailableBytes: 50UL * 1024 * 1024 * 1024),

                // Profile 4: Apple / ARM Compute Node (32 cores, AdvSIMD, Vulkan, 128GB RAM, 32GB VRAM)
                4 => new NodeHardwareProfile(
                    nodeId,
                    cpuLogicalCores: 32,
                    simdFlags: SimdArchitectureFlags.AdvSimd,
                    gpuFlags: GpuBackendFlags.Vulkan,
                    ramTotalBytes: 128UL * 1024 * 1024 * 1024,
                    ramAvailableBytes: 96UL * 1024 * 1024 * 1024,
                    vramTotalBytes: 32UL * 1024 * 1024 * 1024,
                    vramAvailableBytes: 24UL * 1024 * 1024 * 1024),

                // Profile 5: Heavy CPU Compute Node (128 cores, AVX-512, No GPU, 512GB RAM, 0 VRAM)
                5 => new NodeHardwareProfile(
                    nodeId,
                    cpuLogicalCores: 128,
                    simdFlags: SimdArchitectureFlags.Avx512 | SimdArchitectureFlags.Avx2,
                    gpuFlags: GpuBackendFlags.None,
                    ramTotalBytes: 512UL * 1024 * 1024 * 1024,
                    ramAvailableBytes: 450UL * 1024 * 1024 * 1024,
                    vramTotalBytes: 0,
                    vramAvailableBytes: 0),

                // Profile 6: Standard CPU Worker (16 cores, AVX2, No GPU, 32GB RAM, 0 VRAM)
                6 => new NodeHardwareProfile(
                    nodeId,
                    cpuLogicalCores: 16,
                    simdFlags: SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                    gpuFlags: GpuBackendFlags.None,
                    ramTotalBytes: 32UL * 1024 * 1024 * 1024,
                    ramAvailableBytes: 28UL * 1024 * 1024 * 1024,
                    vramTotalBytes: 0,
                    vramAvailableBytes: 0),

                // Profile 7: Edge Worker Node (4 cores, SSE4.2, No GPU, 8GB RAM, 0 VRAM)
                _ => new NodeHardwareProfile(
                    nodeId,
                    cpuLogicalCores: 4,
                    simdFlags: SimdArchitectureFlags.Sse42,
                    gpuFlags: GpuBackendFlags.None,
                    ramTotalBytes: 8UL * 1024 * 1024 * 1024,
                    ramAvailableBytes: 6UL * 1024 * 1024 * 1024,
                    vramTotalBytes: 0,
                    vramAvailableBytes: 0)
            };

            nodes.Add(profile);
        }

        var scheduler = new TwoPhaseScheduler();

        // 2. Generate 2,000 tasks with heterogeneous requirements
        const int taskCount = 2000;
        var tasks = new ITaskPlacementDescriptor[taskCount];
        for (int i = 0; i < taskCount; i++)
        {
            int taskKind = i % 4;
            tasks[i] = taskKind switch
            {
                // Kind 0: Heavy CUDA Task (Requires CUDA GPU, 10GB VRAM, 16GB RAM)
                0 => new TaskPlacementRequest
                {
                    RequiresGpu = true,
                    RequiredGpu = GpuBackendFlags.Cuda,
                    MinVramBytes = 10UL * 1024 * 1024 * 1024,
                    MinRamBytes = 16UL * 1024 * 1024 * 1024
                },
                // Kind 1: ROCm GPU Task (Requires ROCm GPU, 16GB VRAM, 32GB RAM)
                1 => new TaskPlacementRequest
                {
                    RequiresGpu = true,
                    RequiredGpu = GpuBackendFlags.Rocm,
                    MinVramBytes = 16UL * 1024 * 1024 * 1024,
                    MinRamBytes = 32UL * 1024 * 1024 * 1024
                },
                // Kind 2: High-RAM CPU In-Memory Analytics (Requires 100GB RAM, No GPU)
                2 => new TaskPlacementRequest
                {
                    RequiresGpu = false,
                    MinRamBytes = 100UL * 1024 * 1024 * 1024
                },
                // Kind 3: General Small Task (Requires 4GB RAM)
                _ => new TaskPlacementRequest
                {
                    RequiresGpu = false,
                    MinRamBytes = 4UL * 1024 * 1024 * 1024
                }
            };
        }

        // 3. Execute massive concurrent task placement across multiple threads
        var placementDecisions = new PlacementDecision[taskCount];
        Parallel.For(0, taskCount, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 }, i =>
        {
            placementDecisions[i] = scheduler.ScheduleTask(tasks[i], nodes);
        });

        // 4. Assert rigorous correctness on all 2,000 placements
        for (int i = 0; i < taskCount; i++)
        {
            var task = tasks[i];
            var decision = placementDecisions[i];

            Assert.True(decision.IsSuccess, $"Task {i} failed placement unexpected: disqualified count {decision.DisqualifiedCandidates.Count}");
            Assert.NotNull(decision.SelectedNode);

            var chosen = decision.SelectedNode.Value;

            // Invariant 1: Hardware acceleration predicate
            if (task.RequiresGpu)
            {
                Assert.True((chosen.GpuFlags & task.RequiredGpu) != 0,
                    $"Task {i} requires {task.RequiredGpu} but got node {chosen.NodeId} with {chosen.GpuFlags}");

                if (task.MinVramBytes > 0)
                {
                    Assert.True(chosen.VramAvailableBytes >= task.MinVramBytes,
                        $"Task {i} requires {task.MinVramBytes} VRAM but got {chosen.VramAvailableBytes}");
                }
            }

            // Invariant 2: RAM predicate
            if (task.MinRamBytes > 0)
            {
                Assert.True(chosen.RamAvailableBytes >= task.MinRamBytes,
                    $"Task {i} requires {task.MinRamBytes} RAM but got {chosen.RamAvailableBytes}");
            }

            // Invariant 3: CPU cores predicate
            Assert.True(chosen.CpuLogicalCores > 0, $"Node {chosen.NodeId} has 0 CPU cores.");
        }
    }

    // =========================================================================
    // SECTION 2: Complete Resource Exhaustion & Detailed Disqualification Diagnostics
    // =========================================================================

    [Fact]
    public void Resource_Exhaustion_Excessive_Ram_Asserts_Exact_Disqualification_Diagnostics()
    {
        var scheduler = new TwoPhaseScheduler();

        // 100 nodes with RAM up to 256 GB
        const int nodeCount = 100;
        var nodes = new List<NodeHardwareProfile>(nodeCount);
        for (int i = 0; i < nodeCount; i++)
        {
            nodes.Add(new NodeHardwareProfile(
                Guid.NewGuid(),
                cpuLogicalCores: 16,
                simdFlags: SimdArchitectureFlags.Avx2,
                gpuFlags: GpuBackendFlags.None,
                ramTotalBytes: 256UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 200UL * 1024 * 1024 * 1024,
                vramTotalBytes: 0,
                vramAvailableBytes: 0));
        }

        // Task requests 1024 GB RAM (exceeds all nodes)
        var task = new TaskPlacementRequest
        {
            RequiresGpu = false,
            MinRamBytes = 1024UL * 1024 * 1024 * 1024
        };

        var decision = scheduler.ScheduleTask(task, nodes);

        Assert.False(decision.IsSuccess);
        Assert.Null(decision.SelectedNode);
        Assert.Equal(nodeCount, decision.DisqualifiedCandidates.Count);

        // Every single node MUST be diagnosed with InsufficientRam
        foreach (var (nodeId, reason) in decision.DisqualifiedCandidates)
        {
            Assert.Equal(FilterDisqualificationReason.InsufficientRam, reason);
        }
    }

    [Fact]
    public void Resource_Exhaustion_Excessive_Vram_Asserts_Exact_Disqualification_Diagnostics()
    {
        var scheduler = new TwoPhaseScheduler();

        // 100 nodes: 30 CUDA nodes (max 48GB VRAM), 20 ROCm nodes (16GB VRAM), 50 CPU-only nodes
        var nodes = new List<NodeHardwareProfile>(100);
        var cudaNodeIds = new HashSet<Guid>();
        var nonCudaNodeIds = new HashSet<Guid>();

        for (int i = 0; i < 30; i++)
        {
            var id = Guid.NewGuid();
            cudaNodeIds.Add(id);
            nodes.Add(new NodeHardwareProfile(
                id,
                cpuLogicalCores: 32,
                simdFlags: SimdArchitectureFlags.Avx512,
                gpuFlags: GpuBackendFlags.Cuda,
                ramTotalBytes: 128UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 100UL * 1024 * 1024 * 1024,
                vramTotalBytes: 48UL * 1024 * 1024 * 1024,
                vramAvailableBytes: 40UL * 1024 * 1024 * 1024));
        }

        for (int i = 0; i < 20; i++)
        {
            var id = Guid.NewGuid();
            nonCudaNodeIds.Add(id);
            nodes.Add(new NodeHardwareProfile(
                id,
                cpuLogicalCores: 32,
                simdFlags: SimdArchitectureFlags.Avx512,
                gpuFlags: GpuBackendFlags.Rocm,
                ramTotalBytes: 128UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 100UL * 1024 * 1024 * 1024,
                vramTotalBytes: 16UL * 1024 * 1024 * 1024,
                vramAvailableBytes: 16UL * 1024 * 1024 * 1024));
        }

        for (int i = 0; i < 50; i++)
        {
            var id = Guid.NewGuid();
            nonCudaNodeIds.Add(id);
            nodes.Add(new NodeHardwareProfile(
                id,
                cpuLogicalCores: 16,
                simdFlags: SimdArchitectureFlags.Avx2,
                gpuFlags: GpuBackendFlags.None,
                ramTotalBytes: 64UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 50UL * 1024 * 1024 * 1024,
                vramTotalBytes: 0,
                vramAvailableBytes: 0));
        }

        // Task requires CUDA with 96 GB VRAM
        var task = new TaskPlacementRequest
        {
            RequiresGpu = true,
            RequiredGpu = GpuBackendFlags.Cuda,
            MinVramBytes = 96UL * 1024 * 1024 * 1024
        };

        var decision = scheduler.ScheduleTask(task, nodes);

        Assert.False(decision.IsSuccess);
        Assert.Null(decision.SelectedNode);
        Assert.Equal(100, decision.DisqualifiedCandidates.Count);

        // Check diagnostic reasons:
        // The 30 CUDA nodes possess CUDA but lack sufficient VRAM (96GB required > 40GB available)
        foreach (var cudaId in cudaNodeIds)
        {
            Assert.Equal(FilterDisqualificationReason.InsufficientVram, decision.DisqualifiedCandidates[cudaId]);
        }

        // The 70 non-CUDA nodes are disqualified for missing CUDA
        foreach (var nonCudaId in nonCudaNodeIds)
        {
            Assert.Equal(FilterDisqualificationReason.MissingCudaCapability, decision.DisqualifiedCandidates[nonCudaId]);
        }
    }

    [Fact]
    public void Resource_Exhaustion_All_Nodes_Unhealthy_Asserts_NodeNotHealthy_Diagnostics()
    {
        var detector = new PhiAccrualFailureDetector();
        var scheduler = new TwoPhaseScheduler(failureDetector: detector);

        var nodes = new List<NodeHardwareProfile>(20);
        ulong t0 = 1_000_000_000UL;

        for (int i = 0; i < 20; i++)
        {
            var id = Guid.NewGuid();
            detector.RegisterNode(id, t0);
            nodes.Add(new NodeHardwareProfile(
                id,
                cpuLogicalCores: 8,
                simdFlags: SimdArchitectureFlags.Avx2,
                gpuFlags: GpuBackendFlags.None,
                ramTotalBytes: 32UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 20UL * 1024 * 1024 * 1024,
                vramTotalBytes: 0,
                vramAvailableBytes: 0));
        }

        // Advance time by 60 seconds of complete silence (all nodes become Evicted)
        ulong tDead = t0 + 60_000_000_000UL;

        var task = new TaskPlacementRequest { MinRamBytes = 4UL * 1024 * 1024 * 1024 };
        var decision = scheduler.ScheduleTask(task, nodes, currentTimestampNs: tDead);

        Assert.False(decision.IsSuccess);
        Assert.Equal(20, decision.DisqualifiedCandidates.Count);
        foreach (var (nodeId, reason) in decision.DisqualifiedCandidates)
        {
            Assert.Equal(FilterDisqualificationReason.NodeNotHealthy, reason);
        }
    }

    [Fact]
    public void Resource_Exhaustion_Zero_Logical_Cores_Asserts_ZeroLogicalCores_Diagnostics()
    {
        var scheduler = new TwoPhaseScheduler();
        var brokenNode = new NodeHardwareProfile(
            Guid.NewGuid(),
            cpuLogicalCores: 0, // Uninitialized / broken CPU
            simdFlags: SimdArchitectureFlags.Avx2,
            gpuFlags: GpuBackendFlags.None,
            ramTotalBytes: 64UL * 1024 * 1024 * 1024,
            ramAvailableBytes: 32UL * 1024 * 1024 * 1024,
            vramTotalBytes: 0,
            vramAvailableBytes: 0);

        var task = new TaskPlacementRequest();
        var decision = scheduler.ScheduleTask(task, new[] { brokenNode });

        Assert.False(decision.IsSuccess);
        Assert.Equal(FilterDisqualificationReason.ZeroLogicalCores, decision.DisqualifiedCandidates[brokenNode.NodeId]);
    }

    // =========================================================================
    // SECTION 3: Extreme Tie-Breaking Determinism
    // =========================================================================

    [Fact]
    public void Extreme_Tie_Breaking_Identical_Nodes_Deterministic_Placement_Across_Permutations()
    {
        var scheduler = new TwoPhaseScheduler();

        // 1. Create 32 identical nodes (same cores, SIMD, RAM, VRAM), with distinct Guids
        const int count = 32;
        var nodes = new List<NodeHardwareProfile>(count);
        for (int i = 0; i < count; i++)
        {
            nodes.Add(new NodeHardwareProfile(
                Guid.NewGuid(),
                cpuLogicalCores: 16,
                simdFlags: SimdArchitectureFlags.Avx2 | SimdArchitectureFlags.Sse42,
                gpuFlags: GpuBackendFlags.None,
                ramTotalBytes: 64UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 32UL * 1024 * 1024 * 1024,
                vramTotalBytes: 0,
                vramAvailableBytes: 0));
        }

        // Expected winner is deterministically the node with the lexicographically smallest Guid
        var expectedWinner = nodes.MinBy(n => n.NodeId);
        var task = new TaskPlacementRequest();

        // 2. Test 100 random permutations of the candidate array
        var rng = new Random(42);
        for (int iteration = 0; iteration < 100; iteration++)
        {
            var permuted = nodes.OrderBy(_ => rng.Next()).ToList();
            var decision = scheduler.ScheduleTask(task, permuted);

            Assert.True(decision.IsSuccess);
            Assert.NotNull(decision.SelectedNode);
            Assert.Equal(expectedWinner.NodeId, decision.SelectedNode.Value.NodeId);
        }
    }

    [Fact]
    public void Tie_Breaking_Prefers_Higher_Available_Ram_Before_Guid_Comparison()
    {
        var scheduler = new TwoPhaseScheduler();

        // Node A has Guid 0xFFFF... (lexicographically larger), but 32GB available RAM
        var nodeA = new NodeHardwareProfile(
            Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            cpuLogicalCores: 16,
            simdFlags: SimdArchitectureFlags.Avx2,
            gpuFlags: GpuBackendFlags.None,
            ramTotalBytes: 64UL * 1024 * 1024 * 1024,
            ramAvailableBytes: 32UL * 1024 * 1024 * 1024,
            vramTotalBytes: 0,
            vramAvailableBytes: 0);

        // Node B has Guid 0x0000... (lexicographically smaller), but only 16GB available RAM
        var nodeB = new NodeHardwareProfile(
            Guid.Parse("00000000-0000-0000-0000-000000000000"),
            cpuLogicalCores: 16,
            simdFlags: SimdArchitectureFlags.Avx2,
            gpuFlags: GpuBackendFlags.None,
            ramTotalBytes: 64UL * 1024 * 1024 * 1024,
            ramAvailableBytes: 16UL * 1024 * 1024 * 1024,
            vramTotalBytes: 0,
            vramAvailableBytes: 0);

        var task = new TaskPlacementRequest();

        // Order 1: [Node A, Node B]
        var dec1 = scheduler.ScheduleTask(task, new[] { nodeA, nodeB });
        // Order 2: [Node B, Node A]
        var dec2 = scheduler.ScheduleTask(task, new[] { nodeB, nodeA });

        // Node A has higher RAM headroom score; even if scores were tied, Node A has higher available RAM.
        Assert.Equal(nodeA.NodeId, dec1.SelectedNode!.Value.NodeId);
        Assert.Equal(nodeA.NodeId, dec2.SelectedNode!.Value.NodeId);
    }

    [Fact]
    public void Batch_Scheduling_Virtual_Load_Balancing_Across_Identical_Nodes()
    {
        var scheduler = new TwoPhaseScheduler();

        // 8 identical nodes
        const int nodeCount = 8;
        var nodes = new List<NodeHardwareProfile>(nodeCount);
        for (int i = 0; i < nodeCount; i++)
        {
            nodes.Add(new NodeHardwareProfile(
                Guid.NewGuid(),
                cpuLogicalCores: 8,
                simdFlags: SimdArchitectureFlags.Avx2,
                gpuFlags: GpuBackendFlags.None,
                ramTotalBytes: 32UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 16UL * 1024 * 1024 * 1024,
                vramTotalBytes: 0,
                vramAvailableBytes: 0));
        }

        // 64 identical tasks (exactly matches 8 nodes * 8 cores = 64 total cluster cores)
        const int taskCount = 64;
        var tasks = new ITaskPlacementDescriptor[taskCount];
        for (int i = 0; i < taskCount; i++)
        {
            tasks[i] = new TaskPlacementRequest();
        }

        var decisions = scheduler.ScheduleStageTasks(tasks, nodes);

        Assert.Equal(taskCount, decisions.Count);
        Assert.All(decisions, d => Assert.True(d.IsSuccess));

        // Group decisions by assigned NodeId: each of the 8 nodes must receive exactly 8 tasks (perfect 0 variance)
        var assignmentCounts = decisions
            .GroupBy(d => d.SelectedNode!.Value.NodeId)
            .ToDictionary(g => g.Key, g => g.Count());

        Assert.Equal(nodeCount, assignmentCounts.Count);
        foreach (var (nodeId, count) in assignmentCounts)
        {
            Assert.Equal(8, count);
        }
    }

    [Fact]
    public void Batch_Scheduling_Oversubscription_Reveals_Tie_Breaking_Clumping_Behavior()
    {
        var scheduler = new TwoPhaseScheduler();

        // 8 identical nodes with 8 cores each = 64 total cores
        const int nodeCount = 8;
        var nodes = new List<NodeHardwareProfile>(nodeCount);
        for (int i = 0; i < nodeCount; i++)
        {
            nodes.Add(new NodeHardwareProfile(
                Guid.NewGuid(),
                cpuLogicalCores: 8,
                simdFlags: SimdArchitectureFlags.Avx2,
                gpuFlags: GpuBackendFlags.None,
                ramTotalBytes: 32UL * 1024 * 1024 * 1024,
                ramAvailableBytes: 16UL * 1024 * 1024 * 1024,
                vramTotalBytes: 0,
                vramAvailableBytes: 0));
        }

        var expectedMinNode = nodes.MinBy(n => n.NodeId);

        // Schedule 80 tasks (16 tasks beyond total cluster capacity of 64 cores)
        const int taskCount = 80;
        var tasks = new ITaskPlacementDescriptor[taskCount];
        for (int i = 0; i < taskCount; i++)
        {
            tasks[i] = new TaskPlacementRequest();
        }

        var decisions = scheduler.ScheduleStageTasks(tasks, nodes);

        Assert.Equal(taskCount, decisions.Count);
        var assignmentCounts = decisions
            .GroupBy(d => d.SelectedNode!.Value.NodeId)
            .ToDictionary(g => g.Key, g => g.Count());

        // Empirical Observation:
        // Because idleRatio = Math.Max(0.0, 1.0 - activeTasks / cores) clamps to 0 once activeTasks >= cores,
        // all 8 nodes receive 0 CPU load bonus once activeTasks reaches 8.
        // Therefore, for all 16 overflow tasks, scores are tied and deterministic tie-breaking picks
        // the node with the minimum Guid. Thus, the minimum Guid node gets 8 + 16 = 24 tasks!
        Assert.Equal(24, assignmentCounts[expectedMinNode.NodeId]);
        foreach (var (nodeId, count) in assignmentCounts)
        {
            if (nodeId != expectedMinNode.NodeId)
            {
                Assert.Equal(8, count);
            }
        }
    }

    // =========================================================================
    // SECTION 4: PhiAccrualFailureDetector: 10-Hour Silence Interval (No Overflow / NaN)
    // =========================================================================

    [Fact]
    public void Simulate_10_Hour_Silence_Interval_No_Overflow_Or_NaN()
    {
        var detector = new PhiAccrualFailureDetector(minStdDevMs: 1.0);
        var nodeId = Guid.NewGuid();
        ulong t0 = 1_000_000_000UL;

        detector.RegisterNode(nodeId, t0);

        // Feed 100 regular heartbeats at 500ms
        for (int i = 0; i < 100; i++)
        {
            t0 += 500_000_000UL;
            detector.RecordHeartbeat(nodeId, t0);
        }

        // 1. Advance time by exactly 10 hours: 10 * 3600 * 1,000,000,000 ns = 36,000,000,000,000 ns
        ulong tenHoursNs = 10UL * 3600 * 1_000_000_000UL;
        ulong queryTenHours = t0 + tenHoursNs;

        double phi10h = detector.ComputePhi(nodeId, queryTenHours);
        var health10h = detector.GetHealthState(nodeId, queryTenHours);

        Assert.False(double.IsNaN(phi10h), "Phi must not be NaN after 10-hour silence.");
        Assert.False(double.IsInfinity(phi10h), "Phi must not be Infinity after 10-hour silence.");
        Assert.True(phi10h > 12.0, $"Phi was {phi10h}, expected > 12.0");
        Assert.Equal(NodeHealthState.Evicted, health10h);
        Assert.False(detector.IsHealthy(nodeId, queryTenHours));

        // 2. Advance to 24 hours
        ulong twentyFourHoursNs = 24UL * 3600 * 1_000_000_000UL;
        ulong query24h = t0 + twentyFourHoursNs;
        double phi24h = detector.ComputePhi(nodeId, query24h);

        Assert.False(double.IsNaN(phi24h));
        Assert.False(double.IsInfinity(phi24h));
        Assert.True(phi24h > phi10h, "Phi must monotonically increase with silence duration.");

        // 3. Advance to 30 days
        ulong thirtyDaysNs = 30UL * 24 * 3600 * 1_000_000_000UL;
        ulong query30d = t0 + thirtyDaysNs;
        double phi30d = detector.ComputePhi(nodeId, query30d);

        Assert.False(double.IsNaN(phi30d));
        Assert.False(double.IsInfinity(phi30d));
        Assert.True(phi30d > phi24h);

        // 4. Advance to 365 days (1 year)
        ulong oneYearNs = 365UL * 24 * 3600 * 1_000_000_000UL;
        ulong query1y = t0 + oneYearNs;
        double phi1y = detector.ComputePhi(nodeId, query1y);

        Assert.False(double.IsNaN(phi1y));
        Assert.False(double.IsInfinity(phi1y));
        Assert.True(phi1y > phi30d);
    }

    [Theory]
    [InlineData(36_000_000.0, 500.0, 1.0)]      // 10 hours, min std dev (1ms)
    [InlineData(36_000_000.0, 500.0, 100.0)]    // 10 hours, 100ms std dev
    [InlineData(36_000_000.0, 500.0, 2500.0)]   // 10 hours, 2500ms std dev
    [InlineData(86_400_000.0, 1000.0, 10.0)]    // 24 hours
    [InlineData(604_800_000.0, 500.0, 5.0)]     // 7 days
    [InlineData(31_536_000_000.0, 500.0, 1.0)]  // 365 days
    public void CalculatePhi_Extreme_Input_Boundary_Matrix_Always_Finite(
        double elapsedMs, double meanMs, double stdDevMs)
    {
        double phi = PhiAccrualFailureDetector.CalculatePhi(elapsedMs, meanMs, stdDevMs);

        Assert.False(double.IsNaN(phi));
        Assert.False(double.IsInfinity(phi));
        Assert.True(phi > 12.0);
        Assert.True(phi > 0.0);
    }

    // =========================================================================
    // SECTION 5: Rapid Heartbeat Jitter (Alternating 10ms and 5000ms)
    // =========================================================================

    [Fact]
    public void Rapid_Heartbeat_Jitter_Alternating_10ms_And_5000ms_No_False_Positive_Evictions()
    {
        var detector = new PhiAccrualFailureDetector(
            suspectThreshold: 8.0,
            evictionThreshold: 12.0,
            minStdDevMs: 1.0,
            historyWindowSize: 1000);

        var nodeId = Guid.NewGuid();
        ulong t = 1_000_000_000UL;
        detector.RegisterNode(nodeId, t);

        // 1. Train detector with 200 alternating heartbeat arrivals: 10ms, 5000ms, 10ms, 5000ms...
        // Expected mean ~ 2505 ms, stdDev ~ 2495 ms
        for (int i = 0; i < 200; i++)
        {
            ulong stepNs = (i % 2 == 0) ? 10_000_000UL : 5_000_000_000UL;
            t += stepNs;
            detector.RecordHeartbeat(nodeId, t);
        }

        // 2. We now simulate 10 consecutive full jitter cycles (each cycle is 10ms then 5000ms spike)
        for (int cycle = 0; cycle < 10; cycle++)
        {
            // (a) Heartbeat at 10ms
            t += 10_000_000UL;
            detector.RecordHeartbeat(nodeId, t);

            // Query right after 10ms heartbeat: must be Healthy
            Assert.Equal(NodeHealthState.Healthy, detector.GetHealthState(nodeId, t + 1_000_000UL));

            // (b) Now enter the 5000ms spike window. Query at multiple intervals during the 5000ms spike:
            ulong[] testOffsetsNs = new[]
            {
                10_000_000UL,       // +10 ms
                100_000_000UL,      // +100 ms
                500_000_000UL,      // +500 ms
                1_000_000_000UL,    // +1,000 ms
                2_505_000_000UL,    // +2,505 ms (at exact mean)
                3_500_000_000UL,    // +3,500 ms
                4_500_000_000UL,    // +4,500 ms
                5_000_000_000UL,    // +5,000 ms (exact deadline of the spike)
                5_200_000_000UL     // +5,200 ms (slight network tail latency)
            };

            foreach (var offset in testOffsetsNs)
            {
                ulong queryTime = t + offset;
                double phi = detector.ComputePhi(nodeId, queryTime);
                var health = detector.GetHealthState(nodeId, queryTime);

                // At all of these points during expected jitter, Phi must remain < 8.0 (Healthy threshold)
                // and node MUST NOT be Suspect or Evicted!
                Assert.True(phi < 8.0,
                    $"Cycle {cycle}, offset {offset / 1_000_000}ms: Phi was {phi}, expected < 8.0");
                Assert.Equal(NodeHealthState.Healthy, health);
                Assert.True(detector.IsHealthy(nodeId, queryTime));
            }

            // (c) Heartbeat arrives at 5000ms
            t += 5_000_000_000UL;
            detector.RecordHeartbeat(nodeId, t);
        }

        // 3. Contrast with genuine failure: prolonged silence of 25 seconds (10x mean)
        ulong silenceNs = 25_000_000_000UL;
        ulong failQuery = t + silenceNs;
        double failPhi = detector.ComputePhi(nodeId, failQuery);
        var failHealth = detector.GetHealthState(nodeId, failQuery);

        Assert.True(failPhi >= 12.0, $"Phi was {failPhi}, expected >= 12.0 under true silence");
        Assert.Equal(NodeHealthState.Evicted, failHealth);
        Assert.False(detector.IsHealthy(nodeId, failQuery));
    }

    // =========================================================================
    // SECTION 6: Recursive Partition Lineage Invalidation Under Node Failure
    // =========================================================================

    [Fact]
    public void Recursive_Partition_Lineage_Invalidation_Deep_4Stage_Pipeline()
    {
        var jobId = Guid.NewGuid();

        // 4-Stage Query Pipeline:
        // S0: TableScan (8 partitions, Narrow)
        // S1: FilterPredicate (8 partitions, Narrow, child of S0)
        // S2: GroupByReduce (16 partitions, Wide Shuffle boundary, child of S1)
        // S3: ProjectSort (16 partitions, Narrow, child of S2)
        var s0 = new ExecutionStage(0, jobId, "Scan", false, 8, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "Filter", false, 8, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s2 = new ExecutionStage(2, jobId, "GroupByReduce", true, 16, StageOperatorType.GroupByReduce, Array.Empty<byte>(), parentStageIds: new[] { 1 });
        var s3 = new ExecutionStage(3, jobId, "ProjectSort", false, 16, StageOperatorType.Project, Array.Empty<byte>(), parentStageIds: new[] { 2 });

        var dag = new ExecutionDag(jobId, "DeepPipelineJob", new[] { s0, s1, s2, s3 });

        // Phase A: Simulate full successful execution of all stages
        for (int p = 0; p < 8; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }
        for (int p = 0; p < 8; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }
        for (int p = 0; p < 16; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 2, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }
        for (int p = 0; p < 16; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 3, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }

        Assert.True(dag.IsJobCompleted);

        // Phase B: Simulate physical node crash hosting Partition 5 of Stage 0!
        // Intermediate shuffle files produced by Stage 0 partition 5 were lost.
        dag.InvalidateStagePartition(stageId: 0, partitionId: 5);

        Assert.False(dag.IsJobCompleted);

        // Invariant 1: S0 (Root Narrow) - Only partition 5 is reset; all other partitions remain Completed
        Assert.Equal(TaskState.Pending, s0.Tasks[5].State);
        for (int p = 0; p < 8; p++)
        {
            if (p != 5) Assert.Equal(TaskState.Completed, s0.Tasks[p].State);
        }
        Assert.Equal(StageState.Ready, s0.StageStatus);

        // Invariant 2: S1 (Narrow child of S0) - Only partition 5 is reset
        Assert.Equal(TaskState.Pending, s1.Tasks[5].State);
        for (int p = 0; p < 8; p++)
        {
            if (p != 5) Assert.Equal(TaskState.Completed, s1.Tasks[p].State);
        }
        Assert.Equal(StageState.Ready, s1.StageStatus);

        // Invariant 3: S2 (Wide Shuffle boundary child of S1) - ALL 16 partitions depend on upstream partition 5
        // Every single partition in S2 must be reset to Pending!
        for (int p = 0; p < 16; p++)
        {
            Assert.Equal(TaskState.Pending, s2.Tasks[p].State);
        }
        Assert.Equal(StageState.Ready, s2.StageStatus);

        // Invariant 4: S3 (Narrow child of S2) - Since all partitions of S2 were invalidated, all 16 partitions of S3 must be reset!
        for (int p = 0; p < 16; p++)
        {
            Assert.Equal(TaskState.Pending, s3.Tasks[p].State);
        }
        Assert.Equal(StageState.Ready, s3.StageStatus);

        // Invariant 5: Scheduling Readiness - ONLY Stage 0 is ready; Stage 1, 2, 3 must wait for parent completion
        var readyStages = dag.GetReadyStages().ToList();
        Assert.Single(readyStages);
        Assert.Equal(0, readyStages[0].StageId);

        var schedulableTasks = dag.GetSchedulableTasks().ToList();
        Assert.Single(schedulableTasks);
        Assert.Equal(0, schedulableTasks[0].StageId);
        Assert.Equal(5, schedulableTasks[0].PartitionId);

        // Phase C: Recompute and recover lineage step-by-step
        // 1. Recompute S0 partition 5
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 5, TaskExecutionStatus.Completed, 12, 100, 10));
        Assert.Equal(StageState.Completed, s0.StageStatus);

        // S1 now unlocks for partition 5
        var readyAfterS0 = dag.GetReadyStages().ToList();
        Assert.Single(readyAfterS0);
        Assert.Equal(1, readyAfterS0[0].StageId);

        var schedulableAfterS0 = dag.GetSchedulableTasks().ToList();
        Assert.Single(schedulableAfterS0);
        Assert.Equal(1, schedulableAfterS0[0].StageId);
        Assert.Equal(5, schedulableAfterS0[0].PartitionId);

        // 2. Recompute S1 partition 5
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, 5, TaskExecutionStatus.Completed, 11, 100, 10));
        Assert.Equal(StageState.Completed, s1.StageStatus);

        // S2 (Wide Shuffle) now unlocks for ALL 16 partitions
        var readyAfterS1 = dag.GetReadyStages().ToList();
        Assert.Single(readyAfterS1);
        Assert.Equal(2, readyAfterS1[0].StageId);

        var schedulableAfterS1 = dag.GetSchedulableTasks().ToList();
        Assert.Equal(16, schedulableAfterS1.Count);
        Assert.All(schedulableAfterS1, t => Assert.Equal(2, t.StageId));

        // 3. Recompute all 16 partitions of S2
        for (int p = 0; p < 16; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 2, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }
        Assert.Equal(StageState.Completed, s2.StageStatus);

        // S3 now unlocks for all 16 partitions
        var readyAfterS2 = dag.GetReadyStages().ToList();
        Assert.Single(readyAfterS2);
        Assert.Equal(3, readyAfterS2[0].StageId);

        var schedulableAfterS2 = dag.GetSchedulableTasks().ToList();
        Assert.Equal(16, schedulableAfterS2.Count);
        Assert.All(schedulableAfterS2, t => Assert.Equal(3, t.StageId));

        // 4. Recompute all 16 partitions of S3
        for (int p = 0; p < 16; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 3, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }

        // Entire DAG is once again 100% completed!
        Assert.True(dag.IsJobCompleted);
    }

    [Fact]
    public void Recursive_Partition_Lineage_Invalidation_Diamond_DAG_With_Dual_Parent_Join()
    {
        var jobId = Guid.NewGuid();

        // Diamond DAG Topology:
        //           S0 (Scan, 4 partitions)
        //          /                       \
        // S1A (Narrow, 4 partitions)    S1B (Narrow, 4 partitions)
        //          \                       /
        //           S2 (Wide HashJoin, 8 partitions)
        //                        |
        //           S3 (Narrow Output, 8 partitions)
        var s0 = new ExecutionStage(0, jobId, "RootScan", false, 4, StageOperatorType.Scan, Array.Empty<byte>());
        var s1a = new ExecutionStage(1, jobId, "LeftTransform", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s1b = new ExecutionStage(2, jobId, "RightTransform", false, 4, StageOperatorType.Project, Array.Empty<byte>(), parentStageIds: new[] { 0 });
        var s2 = new ExecutionStage(3, jobId, "WideHashJoin", true, 8, StageOperatorType.HashJoinProbe, Array.Empty<byte>(), parentStageIds: new[] { 1, 2 });
        var s3 = new ExecutionStage(4, jobId, "FinalOutput", false, 8, StageOperatorType.Project, Array.Empty<byte>(), parentStageIds: new[] { 3 });

        var dag = new ExecutionDag(jobId, "DiamondJob", new[] { s0, s1a, s1b, s2, s3 });

        // Complete all tasks in all stages
        for (int p = 0; p < 4; p++) dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, p, TaskExecutionStatus.Completed, 5, 50, 5));
        for (int p = 0; p < 4; p++) dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, p, TaskExecutionStatus.Completed, 5, 50, 5));
        for (int p = 0; p < 4; p++) dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 2, p, TaskExecutionStatus.Completed, 5, 50, 5));
        for (int p = 0; p < 8; p++) dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 3, p, TaskExecutionStatus.Completed, 5, 50, 5));
        for (int p = 0; p < 8; p++) dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 4, p, TaskExecutionStatus.Completed, 5, 50, 5));

        Assert.True(dag.IsJobCompleted);

        // Crash occurs on node holding partition 2 of Root Stage 0:
        dag.InvalidateStagePartition(stageId: 0, partitionId: 2);

        Assert.False(dag.IsJobCompleted);

        // Root S0: partition 2 is pending
        Assert.Equal(TaskState.Pending, s0.Tasks[2].State);
        Assert.Equal(TaskState.Completed, s0.Tasks[0].State);

        // Both diamond branches (S1A and S1B) have partition 2 invalidated
        Assert.Equal(TaskState.Pending, s1a.Tasks[2].State);
        Assert.Equal(TaskState.Completed, s1a.Tasks[0].State);

        Assert.Equal(TaskState.Pending, s1b.Tasks[2].State);
        Assert.Equal(TaskState.Completed, s1b.Tasks[0].State);

        // S2 (Wide Join) and S3 (Narrow Output) have ALL 8 partitions invalidated
        for (int p = 0; p < 8; p++)
        {
            Assert.Equal(TaskState.Pending, s2.Tasks[p].State);
            Assert.Equal(TaskState.Pending, s3.Tasks[p].State);
        }

        // Only S0 is ready initially
        var ready = dag.GetReadyStages().ToList();
        Assert.Single(ready);
        Assert.Equal(0, ready[0].StageId);

        // Complete S0 partition 2
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 2, TaskExecutionStatus.Completed, 5, 50, 5));

        // Now BOTH S1A and S1B unlock concurrently!
        var readyBranches = dag.GetReadyStages().Select(s => s.StageId).ToHashSet();
        Assert.Contains(1, readyBranches);
        Assert.Contains(2, readyBranches);

        var schedulableBranches = dag.GetSchedulableTasks().ToList();
        Assert.Equal(2, schedulableBranches.Count);
        Assert.Contains(schedulableBranches, t => t.StageId == 1 && t.PartitionId == 2);
        Assert.Contains(schedulableBranches, t => t.StageId == 2 && t.PartitionId == 2);

        // Complete S1A partition 2; S2 should NOT be ready yet because S1B is not finished
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, 2, TaskExecutionStatus.Completed, 5, 50, 5));
        var readyHalfBranches = dag.GetReadyStages().Select(s => s.StageId).ToList();
        Assert.DoesNotContain(3, readyHalfBranches); // S2 is NOT ready yet

        // Complete S1B partition 2; Now S2 unlocks!
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 2, 2, TaskExecutionStatus.Completed, 5, 50, 5));
        var readyJoin = dag.GetReadyStages().Select(s => s.StageId).ToList();
        Assert.Contains(3, readyJoin);

        // Complete S2 and S3
        for (int p = 0; p < 8; p++) dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 3, p, TaskExecutionStatus.Completed, 5, 50, 5));
        for (int p = 0; p < 8; p++) dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 4, p, TaskExecutionStatus.Completed, 5, 50, 5));

        Assert.True(dag.IsJobCompleted);
    }

    [Fact]
    public void Concurrent_Multi_Partition_Failure_Lineage_Recovery()
    {
        var jobId = Guid.NewGuid();
        var s0 = new ExecutionStage(0, jobId, "Scan", false, 8, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "Filter", false, 8, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: new[] { 0 });

        var dag = new ExecutionDag(jobId, "MultiFailureJob", new[] { s0, s1 });

        for (int p = 0; p < 8; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, p, TaskExecutionStatus.Completed, 5, 50, 5));
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, p, TaskExecutionStatus.Completed, 5, 50, 5));
        }

        Assert.True(dag.IsJobCompleted);

        // Invalidate multiple partitions simultaneously (partitions 1, 3, and 6)
        dag.InvalidateStagePartition(0, 1);
        dag.InvalidateStagePartition(0, 3);
        dag.InvalidateStagePartition(0, 6);

        var schedulable = dag.GetSchedulableTasks().OrderBy(t => t.PartitionId).ToList();
        Assert.Equal(3, schedulable.Count);
        Assert.Equal(1, schedulable[0].PartitionId);
        Assert.Equal(3, schedulable[1].PartitionId);
        Assert.Equal(6, schedulable[2].PartitionId);

        // Recover all 3 partitions
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 1, TaskExecutionStatus.Completed, 5, 50, 5));
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 3, TaskExecutionStatus.Completed, 5, 50, 5));
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, 6, TaskExecutionStatus.Completed, 5, 50, 5));

        var schedulableNext = dag.GetSchedulableTasks().OrderBy(t => t.PartitionId).ToList();
        Assert.Equal(3, schedulableNext.Count);
        Assert.All(schedulableNext, t => Assert.Equal(1, t.StageId));
        Assert.Equal(1, schedulableNext[0].PartitionId);
        Assert.Equal(3, schedulableNext[1].PartitionId);
        Assert.Equal(6, schedulableNext[2].PartitionId);

        // Recover downstream
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, 1, TaskExecutionStatus.Completed, 5, 50, 5));
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, 3, TaskExecutionStatus.Completed, 5, 50, 5));
        dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, 6, TaskExecutionStatus.Completed, 5, 50, 5));

        Assert.True(dag.IsJobCompleted);
    }

    // =========================================================================
    // SECTION 7: Additional Adversarial Hardening & Numerical Stability
    // =========================================================================

    [Fact]
    public void HeartbeatHistoryWindow_One_Million_Samples_Numerical_Drift_Bound()
    {
        var window = new HeartbeatHistoryWindow(capacity: 1000);
        ulong t = 1_000_000_000UL;
        window.RecordHeartbeat(t);

        // Feed 1,000,000 heartbeats at exactly 500.0ms intervals
        const int iterations = 1_000_000;
        for (int i = 0; i < iterations; i++)
        {
            t += 500_000_000UL; // +500 ms
            window.RecordHeartbeat(t);
        }

        var (meanMs, stdDevMs) = window.ComputeStatistics(minStdDevMs: 1.0);

        // Floating-point accumulation drift over 1M steps must not drift by more than 1e-6 ms
        Assert.Equal(500.0, meanMs, 6);
        Assert.Equal(1.0, stdDevMs, 6);
    }

    [Fact]
    public void HeartbeatHistoryWindow_Concurrent_Record_And_Query_Thread_Safety()
    {
        var window = new HeartbeatHistoryWindow(capacity: 500);
        ulong baseNs = 1_000_000_000UL;
        window.RecordHeartbeat(baseNs);

        const int threads = 8;
        const int opsPerThread = 10_000;

        Parallel.For(0, threads, threadId =>
        {
            for (int i = 0; i < opsPerThread; i++)
            {
                ulong ts = baseNs + (ulong)(threadId * opsPerThread + i + 1) * 10_000_000UL;
                window.RecordHeartbeat(ts);
                var (m, s) = window.ComputeStatistics();
                Assert.False(double.IsNaN(m));
                Assert.False(double.IsNaN(s));
            }
        });

        Assert.Equal(500, window.SampleCount);
    }

    [Fact]
    public void Suspect_Node_Disqualified_From_New_Task_Placement_In_Phase1()
    {
        var detector = new PhiAccrualFailureDetector(minStdDevMs: 200.0);
        var scheduler = new TwoPhaseScheduler(failureDetector: detector);

        var nodeA = new NodeHardwareProfile(
            Guid.NewGuid(), 16, SimdArchitectureFlags.Avx2, GpuBackendFlags.None,
            32UL * 1024 * 1024 * 1024, 24UL * 1024 * 1024 * 1024, 0, 0);

        ulong t0 = 1_000_000_000UL;
        detector.RegisterNode(nodeA.NodeId, t0);

        // Baseline heartbeats
        for (int i = 0; i < 50; i++)
        {
            t0 += 500_000_000UL;
            detector.RecordHeartbeat(nodeA.NodeId, t0);
        }

        // Delay heartbeat by 1.9s into Suspect window (8 <= Phi < 12)
        ulong tSuspect = t0 + 1_900_000_000UL;
        var health = detector.GetHealthState(nodeA.NodeId, tSuspect);
        Assert.Equal(NodeHealthState.Suspect, health);

        // Phase 1 Hard Predicate must disqualify the Suspect node from receiving NEW tasks
        var task = new TaskPlacementRequest();
        var decision = scheduler.ScheduleTask(task, new[] { nodeA }, currentTimestampNs: tSuspect);

        Assert.False(decision.IsSuccess);
        Assert.Equal(FilterDisqualificationReason.NodeNotHealthy, decision.DisqualifiedCandidates[nodeA.NodeId]);
    }

    [Fact]
    public void ScheduleTask_Empty_Candidates_Returns_Failure_Gracefully()
    {
        var scheduler = new TwoPhaseScheduler();
        var task = new TaskPlacementRequest();

        var decision = scheduler.ScheduleTask(task, Array.Empty<NodeHardwareProfile>());

        Assert.False(decision.IsSuccess);
        Assert.Null(decision.SelectedNode);
        Assert.Empty(decision.DisqualifiedCandidates);
    }
}
