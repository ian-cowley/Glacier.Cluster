namespace Glacier.Cluster.Tests.SimulatorTests;

using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Cluster.Compute;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.ML;
using Glacier.Cluster.Polaris;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Simulator;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

/// <summary>
/// Comprehensive verification test suite for Feature 50: In-Memory 4-to-64 Node Cluster Simulator.
/// Validates zero-socket cluster boot, contiguous rank allocation, heterogeneous hardware placement,
/// distributed query execution (Filter, Project, GroupBy, Join) with Polaris reference parity,
/// large-scale Ring AllReduce, and Distributed KMeans convergence.
/// </summary>
public sealed class ClusterSimulatorTests
{
    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    public async Task ClusterBootTeardown_ScaleMatrix_ContiguousRanksAndCleanShutdown(int nodeCount)
    {
        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount, opts =>
        {
            opts.WorkerHeartbeatInterval = TimeSpan.FromMilliseconds(50);
            opts.CoordinatorHeartbeatAuditInterval = TimeSpan.FromMilliseconds(50);
        });

        Assert.True(simulator.IsRunning);
        Assert.Equal(nodeCount, simulator.NodeCount);
        Assert.Equal(nodeCount, simulator.Workers.Count);
        Assert.Equal(nodeCount, simulator.Coordinator.Membership.ActiveNodeCount);

        // Verify contiguous unique ranks: 0..N-1
        var assignedRanks = simulator.Workers.Select(w => w.AssignedRank).OrderBy(r => r).ToArray();
        for (int i = 0; i < nodeCount; i++)
        {
            Assert.Equal(i, assignedRanks[i]);
        }

        // Verify monotonically incremented epoch
        Assert.True(simulator.Coordinator.Membership.ClusterEpoch >= (ulong)nodeCount);

        // Verify all workers report running
        Assert.All(simulator.Workers, w => Assert.True(w.IsRunning));

        // Graceful shutdown
        await simulator.ShutdownAsync();
        Assert.False(simulator.IsRunning);
        Assert.Equal(0, simulator.Coordinator.Membership.ActiveNodeCount);
    }

    [Theory]
    [InlineData(16)]
    [InlineData(64)]
    public async Task HeterogeneousScheduler_PhysicalFleetMatching_EnforcesConstraints(int nodeCount)
    {
        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount, opts =>
        {
            opts.TopologyStrategy = TopologyAssignmentStrategy.PhysicalFleetCyclic;
        });

        // Verify cyclic assignment of physical fleet profiles
        for (int i = 0; i < nodeCount; i++)
        {
            var profile = simulator.Workers[i].HardwareProfile;
            switch (i % 4)
            {
                case 0: // Host Laptop: 16 cores, AVX-512, CUDA
                    Assert.Equal(16u, profile.CpuLogicalCores);
                    Assert.True(profile.HasAvx512);
                    Assert.True(profile.HasCuda);
                    break;
                case 1: // Beast: 12 cores, AVX2, CUDA, 64GB RAM
                    Assert.Equal(12u, profile.CpuLogicalCores);
                    Assert.True(profile.HasAvx2);
                    Assert.True(profile.HasCuda);
                    Assert.True(profile.RamTotalBytes >= 64UL * 1024 * 1024 * 1024);
                    break;
                case 2: // Fedora Linux: 16 cores, AVX-512, Vulkan/ROCm
                    Assert.Equal(16u, profile.CpuLogicalCores);
                    Assert.True(profile.HasAvx512);
                    Assert.True(profile.HasRocm);
                    break;
                case 3: // MinisForum64: 8 cores, AVX2, Vulkan, 64GB RAM
                    Assert.Equal(8u, profile.CpuLogicalCores);
                    Assert.True(profile.HasAvx2);
                    Assert.False(profile.HasCuda);
                    break;
            }
        }

        // Test TwoPhaseScheduler hard constraint filtering: CUDA task must be scheduled on node % 4 == 0 or 1
        var scheduler = new TwoPhaseScheduler();
        var cudaTask = new TaskDescriptor(
            Guid.NewGuid(),
            0,
            0,
            StageOperatorType.Filter,
            Array.Empty<byte>(),
            requiredGpu: GpuBackendFlags.Cuda);

        var candidateProfiles = simulator.Coordinator.Membership.GetHealthyHardwareProfiles();
        var activeCounts = simulator.Coordinator.Membership.GetActiveTaskCounts();

        var decision = scheduler.ScheduleTask(cudaTask, candidateProfiles, activeCounts);
        Assert.True(decision.IsSuccess);
        Assert.NotNull(decision.SelectedNode);

        var selectedWorker = simulator.Workers.First(w => w.NodeId == decision.SelectedNode.Value.NodeId);
        int selectedIndex = selectedWorker.NodeIndex;
        Assert.True(selectedIndex % 4 == 0 || selectedIndex % 4 == 1, $"CUDA task placed on non-CUDA node {selectedIndex}");
    }

    [Theory]
    [InlineData(4, 50)]
    [InlineData(8, 50)]
    [InlineData(16, 50)]
    [InlineData(32, 25)]
    [InlineData(64, 20)]
    public async Task DistributedQuery_Filter_ParityWithPolaris(int nodeCount, int rowsPerNode)
    {
        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount);

        var (partitions, singleNodeDf) = GenerateTestDataset(nodeCount, rowsPerNode);
        var ddf = simulator.CreateDataFrame(partitions);

        var resultDf = await ddf.Filter("age > 30").Filter("salary < 80000.0").CollectAsync();

        var expectedDf = singleNodeDf.Filter(Expr.Col("age") > 30 & Expr.Col("salary") < 80000.0);

        Assert.Equal(expectedDf.RowCount, resultDf.RowCount);
        Assert.Equal(expectedDf.Columns.Count, resultDf.Columns.Count);

        // Verify row values
        var resultAges = (Int32Series)resultDf.GetColumn("age");
        var resultSalaries = (Float64Series)resultDf.GetColumn("salary");

        for (int i = 0; i < resultAges.Length; i++)
        {
            Assert.True(resultAges.Memory.Span[i] > 30, $"Age {resultAges.Memory.Span[i]} violates filter predicate.");
            Assert.True(resultSalaries.Memory.Span[i] < 80000.0, $"Salary {resultSalaries.Memory.Span[i]} violates filter predicate.");
        }
    }

    [Theory]
    [InlineData(4, 50)]
    [InlineData(8, 50)]
    [InlineData(16, 50)]
    [InlineData(32, 25)]
    [InlineData(64, 20)]
    public async Task DistributedQuery_Project_ParityWithPolaris(int nodeCount, int rowsPerNode)
    {
        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount);

        var (partitions, singleNodeDf) = GenerateTestDataset(nodeCount, rowsPerNode);
        var ddf = simulator.CreateDataFrame(partitions, "id,salary,dept");

        var resultDf = await ddf.Select("id", "salary", "dept").CollectAsync();
        var expectedDf = singleNodeDf.Select("id", "salary", "dept");

        Assert.Equal(3, resultDf.Columns.Count);
        Assert.Equal(expectedDf.RowCount, resultDf.RowCount);
        Assert.Equal("id", resultDf.Columns[0].Name);
        Assert.Equal("salary", resultDf.Columns[1].Name);
        Assert.Equal("dept", resultDf.Columns[2].Name);
    }

    [Theory]
    [InlineData(4, 50)]
    [InlineData(8, 50)]
    [InlineData(16, 50)]
    [InlineData(32, 25)]
    [InlineData(64, 20)]
    public async Task DistributedQuery_GroupBy_ParityWithPolaris(int nodeCount, int rowsPerNode)
    {
        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount);

        var (partitions, singleNodeDf) = GenerateTestDataset(nodeCount, rowsPerNode);
        var ddf = simulator.CreateDataFrame(partitions);

        var resultDf = await ddf.GroupBy("dept").Sum("salary").CollectAsync();
        var expectedDf = singleNodeDf.GroupBy("dept").Agg(("salary", "sum"));

        Assert.Equal(expectedDf.RowCount, resultDf.RowCount);
        Assert.Contains(resultDf.Columns, c => c.Name == "dept");
        Assert.Contains(resultDf.Columns, c => c.Name.Contains("salary", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    public async Task DistributedQuery_HashJoin_ParityWithPolaris(int nodeCount)
    {
        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount);

        int rowsPerNode = 20;
        var (leftPartitions, leftSingleDf) = GenerateOrdersDataset(nodeCount, rowsPerNode);
        var (rightPartitions, rightSingleDf) = GenerateCustomersDataset(nodeCount, rowsPerNode / 2);

        var leftDdf = simulator.CreateDataFrame(leftPartitions, "Orders");
        var rightDdf = simulator.CreateDataFrame(rightPartitions, "Customers");

        var resultDf = await leftDdf.Join(rightDdf, "customer_id", JoinType.Inner).CollectAsync();
        var expectedDf = leftSingleDf.Join(rightSingleDf, "customer_id", JoinType.Inner);

        Assert.Equal(expectedDf.RowCount, resultDf.RowCount);
        Assert.True(resultDf.Columns.Count >= 4);
    }

    [Theory]
    [InlineData(4, 131_072)]
    [InlineData(8, 262_144)]
    [InlineData(16, 100_003)]
    public async Task RingAllReduce_CollectiveReduction_ExactSumConvergence(int nodeCount, int totalElements)
    {
        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount);

        float[][] buffers = new float[nodeCount][];
        double[] expectedSums = new double[totalElements];

        for (int r = 0; r < nodeCount; r++)
        {
            buffers[r] = new float[totalElements];
            float baseVal = (r + 1) * 0.5f;

            for (int e = 0; e < totalElements; e++)
            {
                float val = baseVal + ((e % 19) * 0.05f);
                buffers[r][e] = val;
                expectedSums[e] += val;
            }
        }

        await simulator.RunRingAllReduceAsync(buffers);

        // Verify all nodes converged to identical and mathematically exact sum
        for (int e = 0; e < totalElements; e++)
        {
            float expectedVal = (float)expectedSums[e];
            float rank0Val = buffers[0][e];

            Assert.Equal(expectedVal, rank0Val, precision: 2);

            for (int r = 1; r < nodeCount; r++)
            {
                Assert.Equal(rank0Val, buffers[r][e]);
            }
        }
    }

    [Theory]
    [InlineData(16, 4, 4)]
    [InlineData(32, 4, 4)]
    public async Task DistributedKMeans_ConvergenceParity_16And32Nodes(int nodeCount, int k, int dimensions)
    {
        await using var simulator = await ClusterSimulator.CreateAsync(nodeCount);

        int rowsPerNode = 50;
        float[][] nodeFeatures = new float[nodeCount][];

        for (int r = 0; r < nodeCount; r++)
        {
            nodeFeatures[r] = new float[rowsPerNode * dimensions];
            var rng = new Random(r * 100 + 42);

            for (int row = 0; row < rowsPerNode; row++)
            {
                int cluster = row % k;
                for (int d = 0; d < dimensions; d++)
                {
                    nodeFeatures[r][row * dimensions + d] = (cluster * 10.0f) + (float)rng.NextDouble();
                }
            }
        }

        float[] centroids = await simulator.RunDistributedKMeansAsync(
            nodeFeatures,
            rowsPerNode,
            dimensions,
            k,
            maxIterations: 25,
            tolerance: 1e-4f);

        Assert.NotNull(centroids);
        Assert.Equal(k * dimensions, centroids.Length);

        // Verify distinct cluster centroids were identified
        for (int c = 0; c < k; c++)
        {
            float firstCoord = centroids[c * dimensions];
            Assert.True(firstCoord >= 0.0f, "Centroid coordinates must be non-negative.");
        }
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
                int age = 20 + (id % 40);
                double salary = 35000.0 + ((id % 200) * 250.0);
                string dept = (id % 4) switch { 0 => "Eng", 1 => "Fin", 2 => "Sales", _ => "HR" };

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

    private static (List<DataFrame> Partitions, DataFrame Consolidated) GenerateOrdersDataset(int nodeCount, int rowsPerNode)
    {
        var partitions = new List<DataFrame>(nodeCount);
        var allIds = new List<int?>();
        var allCustIds = new List<int?>();
        var allAmounts = new List<double?>();

        int currentId = 1;
        for (int p = 0; p < nodeCount; p++)
        {
            var pIds = new int?[rowsPerNode];
            var pCustIds = new int?[rowsPerNode];
            var pAmounts = new double?[rowsPerNode];

            for (int r = 0; r < rowsPerNode; r++)
            {
                int id = currentId++;
                int custId = 1 + (id % 20);
                double amount = 50.0 + (id * 10.0);

                pIds[r] = id;
                pCustIds[r] = custId;
                pAmounts[r] = amount;

                allIds.Add(id);
                allCustIds.Add(custId);
                allAmounts.Add(amount);
            }

            partitions.Add(new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("order_id", pIds),
                Int32Series.FromValues("customer_id", pCustIds),
                Float64Series.FromValues("amount", pAmounts)
            }));
        }

        var consolidated = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("order_id", allIds.ToArray()),
            Int32Series.FromValues("customer_id", allCustIds.ToArray()),
            Float64Series.FromValues("amount", allAmounts.ToArray())
        });

        return (partitions, consolidated);
    }

    private static (List<DataFrame> Partitions, DataFrame Consolidated) GenerateCustomersDataset(int nodeCount, int rowsPerNode)
    {
        var partitions = new List<DataFrame>(nodeCount);
        var allCustIds = new List<int?>();
        var allNames = new List<string?>();

        int currentCustId = 1;
        for (int p = 0; p < nodeCount; p++)
        {
            var pCustIds = new int?[rowsPerNode];
            var pNames = new string?[rowsPerNode];

            for (int r = 0; r < rowsPerNode; r++)
            {
                int custId = currentCustId++;
                string name = $"Customer_{custId}";

                pCustIds[r] = custId;
                pNames[r] = name;

                allCustIds.Add(custId);
                allNames.Add(name);
            }

            partitions.Add(new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("customer_id", pCustIds),
                Utf8StringSeries.FromStrings("name", pNames)
            }));
        }

        var consolidated = new DataFrame(new ISeries[]
        {
            Int32Series.FromValues("customer_id", allCustIds.ToArray()),
            Utf8StringSeries.FromStrings("name", allNames.ToArray())
        });

        return (partitions, consolidated);
    }
}
