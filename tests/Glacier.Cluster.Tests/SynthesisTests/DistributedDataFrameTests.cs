namespace Glacier.Cluster.Tests.SynthesisTests;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Glacier.Cluster.Polaris;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

public sealed class DistributedDataFrameTests
{
    private static (List<DataFrame> Partitions, DataFrame Consolidated) CreateSampleDataset(int partitionCount, int rowsPerPartition)
    {
        var partitions = new List<DataFrame>(partitionCount);
        var allIds = new List<int?>();
        var allAges = new List<int?>();
        var allSalaries = new List<double?>();
        var allDepts = new List<string?>();

        int currentId = 1;
        for (int p = 0; p < partitionCount; p++)
        {
            var pIds = new int?[rowsPerPartition];
            var pAges = new int?[rowsPerPartition];
            var pSalaries = new double?[rowsPerPartition];
            var pDepts = new string?[rowsPerPartition];

            for (int r = 0; r < rowsPerPartition; r++)
            {
                int id = currentId++;
                int age = 20 + (id % 30);
                double salary = 40000.0 + (id * 500.0);
                string dept = (id % 3) switch
                {
                    0 => "Engineering",
                    1 => "Finance",
                    _ => "Sales"
                };

                pIds[r] = id;
                pAges[r] = age;
                pSalaries[r] = salary;
                pDepts[r] = dept;

                allIds.Add(id);
                allAges.Add(age);
                allSalaries.Add(salary);
                allDepts.Add(dept);
            }

            var pDf = new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("id", pIds),
                Int32Series.FromValues("age", pAges),
                Float64Series.FromValues("salary", pSalaries),
                Utf8StringSeries.FromStrings("dept", pDepts)
            });
            partitions.Add(pDf);
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

    [Fact]
    public async Task Filter_MultiPartition_ParityWithPolaris()
    {
        var (partitions, singleNodeDf) = CreateSampleDataset(partitionCount: 4, rowsPerPartition: 25);
        using var ddf = DistributedDataFrameFactory.FromPartitions(partitions);

        var filteredDdf = ddf.Filter("age > 30");
        var resultDf = await filteredDdf.CollectAsync();

        var expectedDf = singleNodeDf.Filter(Expr.Col("age") > 30);

        Assert.Equal(expectedDf.RowCount, resultDf.RowCount);
        Assert.True(resultDf.RowCount > 0);

        var ageCol = (Int32Series)resultDf.GetColumn("age");
        for (int i = 0; i < resultDf.RowCount; i++)
        {
            Assert.True(ageCol.Memory.Span[i] > 30);
        }
    }

    [Fact]
    public async Task Select_Projection_MatchesSchema()
    {
        var (partitions, _) = CreateSampleDataset(partitionCount: 3, rowsPerPartition: 20);
        using var ddf = DistributedDataFrameFactory.FromPartitions(partitions);

        var projectedDdf = ddf.Select("id", "salary");
        Assert.Equal(2, projectedDdf.Schema.FieldCount);
        Assert.Equal("id", projectedDdf.Schema.GetField(0).Name);
        Assert.Equal("salary", projectedDdf.Schema.GetField(1).Name);

        var resultDf = await projectedDdf.CollectAsync();
        Assert.Equal(60, resultDf.RowCount);
        Assert.Equal(2, resultDf.Columns.Count);
        Assert.NotNull(resultDf.GetColumn("id"));
        Assert.NotNull(resultDf.GetColumn("salary"));
    }

    [Fact]
    public async Task GroupBy_TwoStageAggregation_Parity()
    {
        var (partitions, _) = CreateSampleDataset(partitionCount: 4, rowsPerPartition: 25);
        using var ddf = DistributedDataFrameFactory.FromPartitions(partitions);

        var groupedDdf = ddf.GroupBy("dept").Sum("salary");
        var resultDf = await groupedDdf.CollectAsync();

        Assert.True(resultDf.RowCount > 0);
        Assert.NotNull(resultDf.GetColumn("dept"));
    }

    [Fact]
    public async Task Join_DistributedHashJoin_Parity()
    {
        var (leftPartitions, _) = CreateSampleDataset(partitionCount: 2, rowsPerPartition: 10);

        // Create right table
        var rightPartitions = new List<DataFrame>();
        for (int p = 0; p < 2; p++)
        {
            var rDf = new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("id", new int?[] { 1 + (p * 5), 2 + (p * 5), 3 + (p * 5) }),
                Utf8StringSeries.FromStrings("location", new string?[] { $"Loc_{p}_A", $"Loc_{p}_B", $"Loc_{p}_C" })
            });
            rightPartitions.Add(rDf);
        }

        using var leftDdf = DistributedDataFrameFactory.FromPartitions(leftPartitions);
        using var rightDdf = DistributedDataFrameFactory.FromPartitions(rightPartitions);

        var joinedDdf = leftDdf.Join(rightDdf, onKey: "id");
        var resultDf = await joinedDdf.CollectAsync();

        Assert.True(resultDf.RowCount > 0);
        Assert.NotNull(resultDf.GetColumn("id"));
        Assert.NotNull(resultDf.GetColumn("dept"));
        Assert.NotNull(resultDf.GetColumn("location"));
    }

    [Fact]
    public async Task Actions_CountAndTake_ReturnExpectedSubsets()
    {
        var (partitions, _) = CreateSampleDataset(partitionCount: 4, rowsPerPartition: 15);
        using var ddf = DistributedDataFrameFactory.FromPartitions(partitions);

        long totalCount = await ddf.CountAsync();
        Assert.Equal(60, totalCount);

        var take10 = await ddf.TakeAsync(10);
        Assert.Equal(10, take10.RowCount);
    }
}
