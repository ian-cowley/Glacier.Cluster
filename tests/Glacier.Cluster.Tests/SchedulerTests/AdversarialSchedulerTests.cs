namespace Glacier.Cluster.Tests.SchedulerTests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Scheduler.LogicalPlan;
using Xunit;

/// <summary>
/// Empirical adversarial test harness challenging PartitionRouter and StageCompiler/ExecutionDag.
/// Tests pathological hash collisions, prime/power-of-2 partition counts, 1 and 65,536 partitions,
/// Chi-Square uniformity across 1,000,000 keys, zero GC allocations, complex multi-branch diamond &amp; wide-fanout DAGs,
/// cycle detection, reactive stage unlocking, and cascading lineage recovery.
/// </summary>
public sealed class AdversarialSchedulerTests
{
    #region 1. PartitionRouter: Pathological Hashes, Boundaries, & Extremes

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(17)]
    [InlineData(31)]
    [InlineData(64)]
    [InlineData(127)]
    [InlineData(1024)]
    [InlineData(65536)]
    public void PartitionRouter_PathologicalCollisionsAndExtremeHashes_AlwaysWithinRange(int partitionCount)
    {
        // Pathological hashes: bit patterns that could trigger overflow, sign bugs, or clumping
        ulong[] extremeHashes =
        [
            0UL,
            1UL,
            2UL,
            0x7FFFFFFFUL,           // int.MaxValue
            0x80000000UL,           // int.MinValue as uint
            0xFFFFFFFFUL,           // uint.MaxValue
            0x100000000UL,          // 2^32
            0x7FFFFFFFFFFFFFFFUL,   // long.MaxValue
            0x8000000000000000UL,   // long.MinValue as ulong
            0xFFFFFFFFFFFFFFFFUL,   // ulong.MaxValue
            0xAAAAAAAAAAAAAAAAUL,   // alternating 1010
            0x5555555555555555UL,   // alternating 0101
            0x00000000FFFFFFFFUL,   // low 32 bits full
            0xFFFFFFFF00000000UL,   // high 32 bits full
        ];

        foreach (var hash in extremeHashes)
        {
            int partition = PartitionRouter.ReduceRange(hash, partitionCount);
            Assert.InRange(partition, 0, partitionCount - 1);
        }
    }

    [Fact]
    public void PartitionRouter_Boundary_SinglePartition_AlwaysZero()
    {
        // With 1 partition, EVERY input must map to partition 0
        ulong[] testHashes = [0, 1, 42, uint.MaxValue, ulong.MaxValue, 0x12345678];
        foreach (var h in testHashes)
        {
            Assert.Equal(0, PartitionRouter.ReduceRange(h, 1));
        }

        // Test RouteKey over varied types with 1 partition
        Assert.Equal(0, PartitionRouter.RouteKey(0, 1));
        Assert.Equal(0, PartitionRouter.RouteKey(-1, 1));
        Assert.Equal(0, PartitionRouter.RouteKey(int.MaxValue, 1));
        Assert.Equal(0, PartitionRouter.RouteKey(int.MinValue, 1));
        Assert.Equal(0, PartitionRouter.RouteKey(123456789012345L, 1));
        Assert.Equal(0, PartitionRouter.RouteKey(3.14159f, 1));
        Assert.Equal(0, PartitionRouter.RouteKey(3.1415926535d, 1));
        Assert.Equal(0, PartitionRouter.RouteKey(Guid.NewGuid(), 1));
        Assert.Equal(0, PartitionRouter.RouteKey("any_string_key", 1));
        Assert.Equal(0, PartitionRouter.RouteBytes("binary_key"u8, 1));

        // Test RoutePartitions batch fast path with 1 partition
        int[] keys = [10, 20, 30, 40, 50, -100, 9999];
        int[] destinations = new int[keys.Length];
        Array.Fill(destinations, 99); // pollute
        PartitionRouter.RoutePartitions(keys, 1, destinations);
        Assert.All(destinations, d => Assert.Equal(0, d));
    }

    [Fact]
    public void PartitionRouter_Boundary_65536_Partitions_ValidCoverageAndNoOutOfBounds()
    {
        const int partitionCount = 65536;
        const int sampleCount = 500_000;

        int minPartition = int.MaxValue;
        int maxPartition = int.MinValue;

        var rng = new Random(42);
        for (int i = 0; i < sampleCount; i++)
        {
            int key = rng.Next();
            int partition = PartitionRouter.RouteKey(key, partitionCount);

            Assert.InRange(partition, 0, partitionCount - 1);
            if (partition < minPartition) minPartition = partition;
            if (partition > maxPartition) maxPartition = partition;
        }

        // With 500,000 samples over 65,536 buckets, both bottom and top bounds must be reached
        Assert.Equal(0, minPartition);
        Assert.Equal(partitionCount - 1, maxPartition);
    }

    [Theory]
    [InlineData(17)]
    [InlineData(31)]
    [InlineData(127)]
    [InlineData(257)]
    [InlineData(65537)]
    public void PartitionRouter_PrimePartitionCounts_AllOutputsStrictlyBounded(int primePartitions)
    {
        var rng = new Random(777);
        for (int i = 0; i < 50_000; i++)
        {
            int key = rng.Next(int.MinValue, int.MaxValue);
            int partition = PartitionRouter.RouteKey(key, primePartitions);
            Assert.InRange(partition, 0, primePartitions - 1);
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(256)]
    [InlineData(512)]
    [InlineData(1024)]
    [InlineData(2048)]
    [InlineData(4096)]
    [InlineData(8192)]
    [InlineData(16384)]
    [InlineData(32768)]
    [InlineData(65536)]
    public void PartitionRouter_PowerOfTwoPartitionCounts_AllOutputsStrictlyBounded(int powerOfTwoPartitions)
    {
        var rng = new Random(888);
        for (int i = 0; i < 10_000; i++)
        {
            int key = rng.Next(int.MinValue, int.MaxValue);
            int partition = PartitionRouter.RouteKey(key, powerOfTwoPartitions);
            Assert.InRange(partition, 0, powerOfTwoPartitions - 1);
        }
    }

    #endregion

    #region 2. PartitionRouter: Chi-Square Uniformity Over 1,000,000 Keys

    [Theory]
    [InlineData(17, 45.0, 1.05)]   // df = 16, crit 99.9% ~ 39.25 (allow 45.0)
    [InlineData(31, 65.0, 1.05)]   // df = 30, crit 99.9% ~ 59.70 (allow 65.0)
    [InlineData(64, 110.0, 1.05)]  // df = 63, crit 99.9% ~ 103.4 (allow 110.0)
    [InlineData(127, 185.0, 1.05)] // df = 126, crit 99.9% ~ 179.5 (allow 185.0)
    public void PartitionRouter_ChiSquare_1Million_RandomKeys_UniformityOracle(
        int partitionCount,
        double maxChiSquare,
        double maxSkew)
    {
        const int keyCount = 1_000_000;
        int[] buckets = new int[partitionCount];

        var rng = new Random(12345);
        for (int i = 0; i < keyCount; i++)
        {
            int key = rng.Next();
            int partition = PartitionRouter.RouteKey(key, partitionCount);
            buckets[partition]++;
        }

        double expected = (double)keyCount / partitionCount;
        double chiSquare = 0.0;
        int maxBucket = 0;
        int minBucket = int.MaxValue;

        for (int i = 0; i < partitionCount; i++)
        {
            double diff = buckets[i] - expected;
            chiSquare += (diff * diff) / expected;
            if (buckets[i] > maxBucket) maxBucket = buckets[i];
            if (buckets[i] < minBucket) minBucket = buckets[i];
            Assert.True(buckets[i] > 0, $"Bucket {i} was completely empty.");
        }

        double skew = maxBucket / expected;

        Assert.True(chiSquare < maxChiSquare,
            $"Chi-square {chiSquare:F2} exceeded threshold {maxChiSquare} for K={partitionCount}.");
        Assert.True(skew < maxSkew,
            $"Max skew {skew:F4} exceeded threshold {maxSkew} for K={partitionCount}.");
    }

    [Theory]
    [InlineData(17, 45.0, 1.05)]
    [InlineData(31, 65.0, 1.05)]
    [InlineData(64, 110.0, 1.05)]
    [InlineData(127, 185.0, 1.05)]
    public void PartitionRouter_ChiSquare_1Million_SequentialKeys_UniformityOracle(
        int partitionCount,
        double maxChiSquare,
        double maxSkew)
    {
        const int keyCount = 1_000_000;
        int[] buckets = new int[partitionCount];

        // Linear sequence 0, 1, 2, ..., 999,999
        for (int i = 0; i < keyCount; i++)
        {
            int partition = PartitionRouter.RouteKey(i, partitionCount);
            buckets[partition]++;
        }

        double expected = (double)keyCount / partitionCount;
        double chiSquare = 0.0;
        int maxBucket = 0;

        for (int i = 0; i < partitionCount; i++)
        {
            double diff = buckets[i] - expected;
            chiSquare += (diff * diff) / expected;
            if (buckets[i] > maxBucket) maxBucket = buckets[i];
            Assert.True(buckets[i] > 0, $"Bucket {i} was completely empty for sequential keys.");
        }

        double skew = maxBucket / expected;

        Assert.True(chiSquare < maxChiSquare,
            $"Sequential keys Chi-square {chiSquare:F2} exceeded threshold {maxChiSquare} for K={partitionCount}.");
        Assert.True(skew < maxSkew,
            $"Sequential keys Max skew {skew:F4} exceeded threshold {maxSkew} for K={partitionCount}.");
    }

    [Fact]
    public void PartitionRouter_ChiSquare_1Million_SequentialKeys_65536_Partitions()
    {
        const int partitionCount = 65536;
        const int keyCount = 1_000_000;
        int[] buckets = new int[partitionCount];

        for (int i = 0; i < keyCount; i++)
        {
            int partition = PartitionRouter.RouteKey(i, partitionCount);
            buckets[partition]++;
        }

        double expected = (double)keyCount / partitionCount; // ~15.2588
        double chiSquare = 0.0;
        int emptyBuckets = 0;

        for (int i = 0; i < partitionCount; i++)
        {
            double diff = buckets[i] - expected;
            chiSquare += (diff * diff) / expected;
            if (buckets[i] == 0) emptyBuckets++;
        }

        // For df = 65535, standard deviation is sqrt(2*df) ~ 362.
        // Under Poisson/uniform distribution, chiSquare should be within 4 sigma of df: [64087, 66983].
        Assert.InRange(chiSquare, 63000.0, 68000.0);

        // Poisson probability P(0) for lambda = 15.26 is e^-15.26 = ~2.3e-7.
        // With 65,536 buckets, expected empty buckets is ~0.015. So empty buckets should be 0 or at most 2.
        Assert.InRange(emptyBuckets, 0, 5);
    }

    #endregion

    #region 3. PartitionRouter: Zero Heap Allocation Verification

    [Fact]
    public void PartitionRouter_RouteBytes_ZeroHeapAllocations()
    {
        const int iterations = 100_000;
        const int partitionCount = 32;
        byte[] payload = "distributed_dataset_partition_routing_key_payload"u8.ToArray();
        ReadOnlySpan<byte> span = payload;

        // Warm up JIT
        _ = PartitionRouter.RouteBytes(span, partitionCount);

        long memBefore = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < iterations; i++)
        {
            _ = PartitionRouter.RouteBytes(span, partitionCount);
        }

        long memAfter = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, memAfter - memBefore);
    }

    [Fact]
    public void PartitionRouter_RoutePartitions_ZeroHeapAllocations()
    {
        const int size = 1024;
        const int iterations = 50_000;
        const int partitionCount = 64;

        int[] keys = new int[size];
        int[] destinations = new int[size];
        var rng = new Random(101);
        for (int i = 0; i < size; i++) keys[i] = rng.Next();

        ReadOnlySpan<int> keysSpan = keys;
        Span<int> destSpan = destinations;

        // Warm up JIT
        PartitionRouter.RoutePartitions(keysSpan, partitionCount, destSpan);

        long memBefore = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < iterations; i++)
        {
            PartitionRouter.RoutePartitions(keysSpan, partitionCount, destSpan);
        }

        long memAfter = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, memAfter - memBefore);
    }

    [Fact]
    public void PartitionRouter_RouteComposite_SmallKeyStackalloc_ZeroHeapAllocations()
    {
        const int iterations = 50_000;
        const int partitionCount = 16;
        byte[] key1 = "cluster_region_us_east"u8.ToArray();
        byte[] key2 = "customer_account_id_987654321"u8.ToArray();

        ReadOnlySpan<byte> k1 = key1;
        ReadOnlySpan<byte> k2 = key2;

        // Warm up JIT
        _ = PartitionRouter.RouteComposite(k1, k2, partitionCount);

        long memBefore = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < iterations; i++)
        {
            _ = PartitionRouter.RouteComposite(k1, k2, partitionCount);
        }

        long memAfter = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, memAfter - memBefore);
    }

    #endregion

    #region 4. StageCompiler: Multi-Branch Diamond & Wide-Fanout DAGs

    [Fact]
    public void StageCompiler_MultiBranch_Diamond_QueryPlan()
    {
        // Diamond topology:
        // Left branch:  ScanA (8 partitions) -> GroupByA (4 partitions)
        // Right branch: ScanB (8 partitions) -> GroupByB (4 partitions)
        // Rejoin:       HashJoin (8 partitions)
        var compiler = new StageCompiler();
        var jobId = Guid.NewGuid();

        var scanA = new LogicalScanNode("orders.parquet", partitionCount: 8);
        var groupByA = new LogicalGroupByNode(scanA, new[] { "customer_id" }, new[] { "SUM(total)" }, targetPartitions: 4);

        var scanB = new LogicalScanNode("returns.parquet", partitionCount: 8);
        var groupByB = new LogicalGroupByNode(scanB, new[] { "customer_id" }, new[] { "COUNT(return_id)" }, targetPartitions: 4);

        var join = new LogicalHashJoinNode(groupByA, groupByB, "customer_id", targetPartitions: 8);

        var dag = compiler.Compile(jobId, "DiamondQuery", join);

        // Verification of stage compilation:
        // GroupByA compiles ScanA into GroupByMap_0 (fused) + GroupByReduce_1 (shuffle)
        // GroupByB compiles ScanB into GroupByMap_2 (fused) + GroupByReduce_3 (shuffle)
        // HashJoin joins Reduce_1 and Reduce_3 into HashJoin_4 (shuffle)
        Assert.Equal(5, dag.Stages.Count);

        // Check topological sort order
        var topoOrder = dag.TopologicalSortOrder;
        Assert.Equal(5, topoOrder.Count);

        // In topological sort, parents must strictly appear before children
        var stageIndices = topoOrder.Select((s, idx) => (s.StageId, idx)).ToDictionary(x => x.StageId, x => x.idx);

        foreach (var stage in dag.Stages.Values)
        {
            foreach (var parentId in stage.ParentStageIds)
            {
                Assert.True(stageIndices[parentId] < stageIndices[stage.StageId],
                    $"Topological violation: Parent stage {parentId} index {stageIndices[parentId]} is not before child {stage.StageId} index {stageIndices[stage.StageId]}");
            }
        }

        // Roots must be the map stages (in-degree 0)
        Assert.Equal(2, dag.RootStages.Count);
        Assert.All(dag.RootStages, r => Assert.Empty(r.ParentStageIds));

        // Leaf must be the join stage (out-degree 0)
        Assert.Single(dag.LeafStages);
        Assert.Equal(StageOperatorType.HashJoinProbe, dag.LeafStages[0].PrimaryOperator);
        Assert.Equal(8, dag.LeafStages[0].PartitionsCount);
    }

    [Fact]
    public void StageCompiler_WideFanout_BalancedJoinTree()
    {
        // Wide fanout join tree across 8 distinct scan sources:
        // ((S0 JOIN S1) JOIN (S2 JOIN S3)) JOIN ((S4 JOIN S5) JOIN (S6 JOIN S7))
        var compiler = new StageCompiler();
        var jobId = Guid.NewGuid();

        var scans = Enumerable.Range(0, 8)
            .Select(i => new LogicalScanNode($"table_{i}.parquet", partitionCount: 4))
            .ToArray();

        // Level 1 joins (4 joins)
        var j1_0 = new LogicalHashJoinNode(scans[0], scans[1], "key", targetPartitions: 4);
        var j1_1 = new LogicalHashJoinNode(scans[2], scans[3], "key", targetPartitions: 4);
        var j1_2 = new LogicalHashJoinNode(scans[4], scans[5], "key", targetPartitions: 4);
        var j1_3 = new LogicalHashJoinNode(scans[6], scans[7], "key", targetPartitions: 4);

        // Level 2 joins (2 joins)
        var j2_0 = new LogicalHashJoinNode(j1_0, j1_1, "key", targetPartitions: 4);
        var j2_1 = new LogicalHashJoinNode(j1_2, j1_3, "key", targetPartitions: 4);

        // Level 3 root join (1 join)
        var rootJoin = new LogicalHashJoinNode(j2_0, j2_1, "key", targetPartitions: 4);

        var dag = compiler.Compile(jobId, "WideJoinTree", rootJoin);

        // 8 scans + 4 (lvl 1) + 2 (lvl 2) + 1 (lvl 3) = 15 total stages
        Assert.Equal(15, dag.Stages.Count);
        Assert.Equal(8, dag.RootStages.Count);
        Assert.Single(dag.LeafStages);

        // Topological sort order must satisfy all dependencies
        var stageIndices = dag.TopologicalSortOrder
            .Select((s, idx) => (s.StageId, idx))
            .ToDictionary(x => x.StageId, x => x.idx);

        foreach (var stage in dag.Stages.Values)
        {
            foreach (var parentId in stage.ParentStageIds)
            {
                Assert.True(stageIndices[parentId] < stageIndices[stage.StageId]);
            }
        }
    }

    [Fact]
    public void StageCompiler_DeepAlternating_Operators_Compilation()
    {
        // Pipeline: Scan -> Filter -> GroupBy -> Filter -> Project -> GroupBy
        var compiler = new StageCompiler();
        var jobId = Guid.NewGuid();

        var scan = new LogicalScanNode("events.csv", partitionCount: 16);
        var f1 = new LogicalFilterNode(scan, "status == 'ACTIVE'");
        var g1 = new LogicalGroupByNode(f1, new[] { "user_id" }, new[] { "COUNT(*)" }, targetPartitions: 8);
        var f2 = new LogicalFilterNode(g1, "count > 5");
        var p1 = new LogicalProjectNode(f2, new[] { "user_id" });
        var g2 = new LogicalGroupByNode(p1, new[] { "user_id" }, new[] { "SUM(1)" }, targetPartitions: 4);

        var dag = compiler.Compile(jobId, "DeepPipeline", g2);

        // GroupBy 1:
        // f1 fuses into scan -> GroupByMap_0 (16 partitions)
        // GroupByReduce_1 (8 partitions, shuffle)
        // f2 cannot fuse into scan (since source is shuffle reduce) -> Filter_2 (8 partitions, narrow)
        // p1 fuses into Filter_2 (narrow)
        // GroupBy 2:
        // GroupByMap_3 (8 partitions, narrow) cannot fuse into Filter (only into Scan) -> separate map stage
        // GroupByReduce_4 (4 partitions, shuffle) -> parent is GroupByMap_3
        // Total = 5 stages in strict linear topological pipeline: 0 -> 1 -> 2 -> 3 -> 4
        Assert.Equal(5, dag.Stages.Count);

        var stages = dag.Stages;
        Assert.Single(dag.RootStages);
        Assert.Single(dag.LeafStages);

        // Check partition counts propagate correctly
        var map1 = stages.Values.First(s => s.PrimaryOperator == StageOperatorType.GroupByMap && s.PartitionsCount == 16);
        var red1 = stages.Values.First(s => s.PrimaryOperator == StageOperatorType.GroupByReduce && s.PartitionsCount == 8);
        var filt = stages.Values.First(s => s.PrimaryOperator == StageOperatorType.Filter && s.PartitionsCount == 8);
        var map2 = stages.Values.First(s => s.PrimaryOperator == StageOperatorType.GroupByMap && s.PartitionsCount == 8);
        var red2 = stages.Values.First(s => s.PrimaryOperator == StageOperatorType.GroupByReduce && s.PartitionsCount == 4);

        Assert.Contains(map1.StageId, red1.ParentStageIds);
        Assert.Contains(red1.StageId, filt.ParentStageIds);
        Assert.Contains(filt.StageId, map2.ParentStageIds);
        Assert.Contains(map2.StageId, red2.ParentStageIds);
    }

    #endregion

    #region 5. ExecutionDag: Stress Topologies, Cycle Detection, Lineage Recovery

    [Fact]
    public void ExecutionDag_MultiBranch_Diamond_ReactiveUnlocking_And_TopologicalIntegrity()
    {
        // S0 (Root, 4 partitions)
        //    -> S1, S2, S3, S4 (4 parallel branches, 4 partitions each)
        //    -> S5 (Sink, 2 partitions, consumes S1..S4)
        var jobId = Guid.NewGuid();
        var s0 = new ExecutionStage(0, jobId, "RootScan", false, 4, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "Branch1", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [0]);
        var s2 = new ExecutionStage(2, jobId, "Branch2", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [0]);
        var s3 = new ExecutionStage(3, jobId, "Branch3", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [0]);
        var s4 = new ExecutionStage(4, jobId, "Branch4", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [0]);
        var s5 = new ExecutionStage(5, jobId, "SinkJoin", true, 2, StageOperatorType.HashJoinProbe, Array.Empty<byte>(), parentStageIds: [1, 2, 3, 4]);

        var dag = new ExecutionDag(jobId, "DiamondStressJob", [s5, s4, s2, s0, s3, s1]);

        // Verify topological order
        var topo = dag.TopologicalSortOrder;
        Assert.Equal(0, topo[0].StageId);
        Assert.Equal(5, topo[5].StageId);

        // Initially only s0 is ready
        var readyInitial = dag.GetReadyStages().ToList();
        Assert.Single(readyInitial);
        Assert.Equal(0, readyInitial[0].StageId);

        // Complete all tasks in s0
        for (int p = 0; p < 4; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }

        // Now S1, S2, S3, S4 must all unlock and be ready!
        var readyAfterS0 = dag.GetReadyStages().ToList();
        Assert.Equal(4, readyAfterS0.Count);
        Assert.True(readyAfterS0.All(s => s.StageId >= 1 && s.StageId <= 4));
        Assert.DoesNotContain(readyAfterS0, s => s.StageId == 5);

        // Complete S1, S2, S3 completely
        foreach (int stageId in new[] { 1, 2, 3 })
        {
            for (int p = 0; p < 4; p++)
            {
                dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, stageId, p, TaskExecutionStatus.Completed, 10, 100, 10));
            }
        }

        // S5 must STILL NOT be ready because S4 is not complete!
        var readyPartial = dag.GetReadyStages().ToList();
        Assert.DoesNotContain(readyPartial, s => s.StageId == 5);

        // Complete S4
        for (int p = 0; p < 4; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 4, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }

        // Now S5 unlocks!
        var readyFinal = dag.GetReadyStages().ToList();
        Assert.Single(readyFinal);
        Assert.Equal(5, readyFinal[0].StageId);

        // Complete S5
        for (int p = 0; p < 2; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 5, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }

        Assert.True(dag.IsJobCompleted);
    }

    [Fact]
    public void ExecutionDag_WideFanout_256Stages_TopologicalIntegrity()
    {
        // 1 root stage S0 fans out to 256 child stages S1..S256
        var jobId = Guid.NewGuid();
        var s0 = new ExecutionStage(0, jobId, "RootScan", false, 8, StageOperatorType.Scan, Array.Empty<byte>());
        var stages = new List<ExecutionStage> { s0 };

        for (int i = 1; i <= 256; i++)
        {
            stages.Add(new ExecutionStage(i, jobId, $"Child_{i}", false, 8, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [0]));
        }

        var dag = new ExecutionDag(jobId, "WideFanout256", stages);

        Assert.Equal(257, dag.TopologicalSortOrder.Count);
        Assert.Equal(0, dag.TopologicalSortOrder[0].StageId);
        Assert.Single(dag.RootStages);
        Assert.Equal(256, dag.LeafStages.Count);

        // Complete S0
        for (int p = 0; p < 8; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }

        // All 256 children must now be ready
        var ready = dag.GetReadyStages().ToList();
        Assert.Equal(256, ready.Count);
    }

    [Fact]
    public void ExecutionDag_CycleDetection_SelfLoop_Throws()
    {
        var jobId = Guid.NewGuid();
        // S0 references itself as parent
        var s0 = new ExecutionStage(0, jobId, "SelfLoop", false, 4, StageOperatorType.Scan, Array.Empty<byte>(), parentStageIds: [0]);

        var ex = Assert.Throws<InvalidOperationException>(() => new ExecutionDag(jobId, "CyclicJob", [s0]));
        Assert.Contains("Cycle detected", ex.Message);
    }

    [Fact]
    public void ExecutionDag_CycleDetection_ComplexLoop_Throws()
    {
        var jobId = Guid.NewGuid();
        // S0 -> S1 -> S2 -> S3 -> S1 (cycle between 1, 2, 3)
        var s0 = new ExecutionStage(0, jobId, "S0", false, 4, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "S1", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [0, 3]);
        var s2 = new ExecutionStage(2, jobId, "S2", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [1]);
        var s3 = new ExecutionStage(3, jobId, "S3", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [2]);

        var ex = Assert.Throws<InvalidOperationException>(() => new ExecutionDag(jobId, "ComplexLoopJob", [s0, s1, s2, s3]));
        Assert.Contains("Cycle detected", ex.Message);
    }

    [Fact]
    public void ExecutionDag_LineageInvalidation_MultiBranch_Diamond_RecoversAccurately()
    {
        // S0 (Root, 4 partitions)
        //    -> S1 (Narrow, 4 partitions)
        //    -> S2 (Narrow, 4 partitions)
        //    -> S3 (Wide Shuffle, 2 partitions, depends on S1 and S2)
        var jobId = Guid.NewGuid();
        var s0 = new ExecutionStage(0, jobId, "Scan", false, 4, StageOperatorType.Scan, Array.Empty<byte>());
        var s1 = new ExecutionStage(1, jobId, "NarrowLeft", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [0]);
        var s2 = new ExecutionStage(2, jobId, "NarrowRight", false, 4, StageOperatorType.Filter, Array.Empty<byte>(), parentStageIds: [0]);
        var s3 = new ExecutionStage(3, jobId, "WideJoin", true, 2, StageOperatorType.HashJoinProbe, Array.Empty<byte>(), parentStageIds: [1, 2]);

        var dag = new ExecutionDag(jobId, "LineageDiamond", [s0, s1, s2, s3]);

        // Complete entire job
        for (int p = 0; p < 4; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 0, p, TaskExecutionStatus.Completed, 10, 100, 10));
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 1, p, TaskExecutionStatus.Completed, 10, 100, 10));
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 2, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }
        for (int p = 0; p < 2; p++)
        {
            dag.HandleTaskStatusUpdate(new TaskStatusUpdateMessage(jobId, 3, p, TaskExecutionStatus.Completed, 10, 100, 10));
        }

        Assert.True(dag.IsJobCompleted);

        // Invalidate partition 1 of S0
        dag.InvalidateStagePartition(stageId: 0, partitionId: 1);

        // In S0: partition 1 must be Pending; partitions 0, 2, 3 remain Completed
        Assert.Equal(TaskState.Pending, s0.Tasks[1].State);
        Assert.Equal(TaskState.Completed, s0.Tasks[0].State);
        Assert.Equal(TaskState.Completed, s0.Tasks[2].State);
        Assert.Equal(TaskState.Completed, s0.Tasks[3].State);

        // In S1 (Narrow): partition 1 must be Pending; 0, 2, 3 remain Completed
        Assert.Equal(TaskState.Pending, s1.Tasks[1].State);
        Assert.Equal(TaskState.Completed, s1.Tasks[0].State);

        // In S2 (Narrow): partition 1 must be Pending; 0, 2, 3 remain Completed
        Assert.Equal(TaskState.Pending, s2.Tasks[1].State);
        Assert.Equal(TaskState.Completed, s2.Tasks[0].State);

        // In S3 (Wide): ALL partitions (0 and 1) must be reset to Pending
        Assert.Equal(TaskState.Pending, s3.Tasks[0].State);
        Assert.Equal(TaskState.Pending, s3.Tasks[1].State);

        Assert.False(dag.IsJobCompleted);

        // Schedulable task should be exactly S0 partition 1
        var schedulable = dag.GetSchedulableTasks().ToList();
        Assert.Single(schedulable);
        Assert.Equal(0, schedulable[0].StageId);
        Assert.Equal(1, schedulable[0].PartitionId);
    }

    #endregion
}
