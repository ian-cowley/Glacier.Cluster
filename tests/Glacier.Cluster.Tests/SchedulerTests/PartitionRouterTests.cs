namespace Glacier.Cluster.Tests.SchedulerTests;

using System;
using System.Runtime.InteropServices;
using Glacier.Cluster.Scheduler;
using Xunit;

public sealed class PartitionRouterTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(16)]
    [InlineData(64)]
    [InlineData(1024)]
    public void ReduceRange_AlwaysWithinValidBounds(int partitionCount)
    {
        ulong[] testHashes =
        [
            0UL,
            1UL,
            (ulong)uint.MaxValue,
            0x123456789ABCDEF0UL,
            0xDEADBEEFCAFEBABEUL,
            ulong.MaxValue
        ];

        foreach (var hash in testHashes)
        {
            int partition = PartitionRouter.ReduceRange(hash, partitionCount);
            Assert.InRange(partition, 0, partitionCount - 1);
        }
    }

    [Fact]
    public void ReduceRange_SinglePartition_AlwaysReturnsZero()
    {
        for (uint i = 0; i < 100_000; i += 997)
        {
            Assert.Equal(0, PartitionRouter.ReduceRange(i, 1));
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void ReduceRange_InvalidPartitionCount_ThrowsArgumentOutOfRangeException(int invalidCount)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => PartitionRouter.ReduceRange(12345UL, invalidCount));
        Assert.Throws<ArgumentOutOfRangeException>(() => PartitionRouter.RouteKey(42, invalidCount));
    }

    [Fact]
    public void ChiSquareUniformity_RandomKeys_PassesAtHighConfidence()
    {
        const int keyCount = 1_000_000;
        const int partitionCount = 64;
        int[] buckets = new int[partitionCount];

        var rng = new Random(42);
        for (int i = 0; i < keyCount; i++)
        {
            int key = rng.Next();
            int partition = PartitionRouter.RouteKey(key, partitionCount);
            buckets[partition]++;
        }

        // Expected count per partition: E = 1,000,000 / 64 = 15,625
        double expected = (double)keyCount / partitionCount;
        double chiSquare = 0.0;
        int maxCount = 0;

        for (int i = 0; i < partitionCount; i++)
        {
            double diff = buckets[i] - expected;
            chiSquare += (diff * diff) / expected;
            if (buckets[i] > maxCount) maxCount = buckets[i];
        }

        // Critical value for df = 63 at alpha = 0.001 is ~103.4.
        // A uniform distribution easily satisfies chiSquare < 110.0
        Assert.True(chiSquare < 110.0, $"Chi-square value {chiSquare:F2} exceeded critical limit.");

        // Maximum Skew factor must be < 1.05 (less than 5% imbalance)
        double skew = maxCount / expected;
        Assert.True(skew < 1.05, $"Maximum skew factor {skew:F4} exceeded 1.05 threshold.");
    }

    [Fact]
    public void LemireVsModulo_StatisticalParityConfirmed()
    {
        const int keyCount = 500_000;
        const int partitionCount = 32;
        int[] lemireBuckets = new int[partitionCount];
        int[] moduloBuckets = new int[partitionCount];

        var rng = new Random(1337);
        for (int i = 0; i < keyCount; i++)
        {
            int key = rng.Next();
            uint hash = (uint)key ^ 0x5bd1e995;
            hash *= 0x5bd1e995;
            hash ^= hash >> 15;

            int pLemire = PartitionRouter.ReduceRange(hash, partitionCount);
            int pModulo = (int)(hash % (uint)partitionCount);

            lemireBuckets[pLemire]++;
            moduloBuckets[pModulo]++;
        }

        // Compute standard deviations
        double expected = (double)keyCount / partitionCount;
        double varLemire = 0, varModulo = 0;
        for (int i = 0; i < partitionCount; i++)
        {
            varLemire += Math.Pow(lemireBuckets[i] - expected, 2);
            varModulo += Math.Pow(moduloBuckets[i] - expected, 2);
            Assert.True(lemireBuckets[i] > 0, "No partition should be empty.");
            Assert.True(moduloBuckets[i] > 0, "No partition should be empty.");
        }

        double stdLemire = Math.Sqrt(varLemire / partitionCount);
        double stdModulo = Math.Sqrt(varModulo / partitionCount);

        double stdDiff = Math.Abs(stdLemire - stdModulo) / stdModulo;
        Assert.True(stdDiff < 0.10, $"Standard deviation deviation {stdDiff:P2} exceeded 10% tolerance.");
    }

    [Fact]
    public void AdversarialSequences_DoNotClumpOrDegenerate()
    {
        const int count = 65536;
        const int partitionCount = 16;
        int[] buckets = new int[partitionCount];

        // Sequential keys 0, 1, 2, 3...
        for (int i = 0; i < count; i++)
        {
            int partition = PartitionRouter.RouteKey(i, partitionCount);
            buckets[partition]++;
        }

        double expected = (double)count / partitionCount;
        for (int i = 0; i < partitionCount; i++)
        {
            double ratio = buckets[i] / expected;
            Assert.InRange(ratio, 0.90, 1.10);
        }
    }

    [Fact]
    public void FloatingPoint_SignedZeroAndNan_ParityPreserved()
    {
        const int partitions = 16;

        // +0.0f and -0.0f must route to identical partition
        int pPosZero = PartitionRouter.RouteKey(0.0f, partitions);
        int pNegZero = PartitionRouter.RouteKey(-0.0f, partitions);
        Assert.Equal(pPosZero, pNegZero);

        // +0.0d and -0.0d must route to identical partition
        int pPosZeroD = PartitionRouter.RouteKey(0.0d, partitions);
        int pNegZeroD = PartitionRouter.RouteKey(-0.0d, partitions);
        Assert.Equal(pPosZeroD, pNegZeroD);

        // NaNs must route to identical partition
        int pNan1 = PartitionRouter.RouteKey(float.NaN, partitions);
        int pNan2 = PartitionRouter.RouteKey(BitConverter.UInt32BitsToSingle(0x7FC00001), partitions);
        Assert.Equal(pNan1, pNan2);
    }

    [Fact]
    public void CompositeKeys_DeterministicAndDivergent()
    {
        const int partitions = 32;

        byte[] k1 = "dept_eng"u8.ToArray();
        byte[] k2 = "emp_101"u8.ToArray();
        byte[] k3 = "emp_102"u8.ToArray();

        int route1 = PartitionRouter.RouteComposite(k1, k2, partitions);
        int route1Repeat = PartitionRouter.RouteComposite(k1, k2, partitions);
        int route2 = PartitionRouter.RouteComposite(k1, k3, partitions);

        Assert.Equal(route1, route1Repeat);
        Assert.InRange(route1, 0, partitions - 1);
        Assert.InRange(route2, 0, partitions - 1);
    }

    [Fact]
    public void VectorizedRoutePartitions_MatchesScalarRouteKey_BitExact()
    {
        const int count = 10_000;
        const int partitions = 17; // prime partition count tests unaligned boundary

        int[] keys = new int[count];
        var rng = new Random(999);
        for (int i = 0; i < count; i++) keys[i] = rng.Next();

        int[] scalarDestinations = new int[count];
        for (int i = 0; i < count; i++)
        {
            scalarDestinations[i] = PartitionRouter.RouteKey(keys[i], partitions);
        }

        int[] vectorizedDestinations = new int[count];
        PartitionRouter.RoutePartitions(keys, partitions, vectorizedDestinations);

        for (int i = 0; i < count; i++)
        {
            Assert.Equal(scalarDestinations[i], vectorizedDestinations[i]);
        }
    }

    [Fact]
    public void GenericRouteBatch_MatchesSpecializedRouting()
    {
        const int count = 5_000;
        const int partitions = 8;

        int[] keys = new int[count];
        for (int i = 0; i < count; i++) keys[i] = i * 13;

        int[] destinations = new int[count];
        PartitionRouter.RouteBatch<int>(keys, partitions, destinations);

        for (int i = 0; i < count; i++)
        {
            Assert.Equal(PartitionRouter.RouteKey(keys[i], partitions), destinations[i]);
        }
    }

    [Fact]
    public void ZeroAllocations_OnHotRoutingPath()
    {
        const int partitions = 64;
        byte[] sampleBytes = "test_routing_partition_key"u8.ToArray();

        // Warm up JIT
        _ = PartitionRouter.RouteKey(42, partitions);
        _ = PartitionRouter.RouteBytes(sampleBytes, partitions);

        long memBefore = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < 100_000; i++)
        {
            _ = PartitionRouter.RouteKey(i, partitions);
            _ = PartitionRouter.RouteBytes(sampleBytes, partitions);
        }

        long memAfter = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, memAfter - memBefore);
    }
}
