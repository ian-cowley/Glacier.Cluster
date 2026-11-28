# Glacier.Cluster User Manual

## 1. Coordinator & Worker Nodes

`ClusterCoordinator` accepts worker registrations, runs heartbeat monitoring (`HeartbeatAuditInterval = 500ms`), compiles logical query plans into stages, and tracks partition locations. Always pass shared join secrets via environment variables:

```csharp
using System.Net;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.Worker;

string secret = Environment.GetEnvironmentVariable("GLACIER_CLUSTER_SECRET") ?? string.Empty;

await using var coordinator = new ClusterCoordinator(new ClusterCoordinatorOptions
{
    BindAddress = IPAddress.Any,
    Port = 51000,
    ClusterSecret = secret,
    MaxTaskRetries = 3,
    EnableConsensusReplication = false
});
await coordinator.StartAsync();

await using var worker = new ClusterWorker(new ClusterWorkerOptions
{
    CoordinatorEndPoint = new IPEndPoint(IPAddress.Loopback, 51000),
    ShufflePort = 51002,
    ClusterSecret = secret
});
await worker.StartAsync();
```

Each `ClusterWorker` probes its host hardware (`NodeHardwareProfile.ProbeLocal`), starts an `ArrowShuffleServer` backed by `LocalPartitionStore` and `DiskSpillManager`, and runs scheduled tasks inside `TaskExecutor`.

---

## 2. Distributed DataFrames (`DistributedDataFrame` & `DistributedGroupBy`)

`DistributedDataFrame` builds an immutable logical query plan evaluated lazily across worker partitions (`LogicalFilterNode`, `LogicalProjectNode`, `LogicalHashJoinNode`):

```csharp
using Glacier.Cluster.Polaris;

using var ddf = DistributedDataFrameFactory.FromPartitions(partitions);
var aggregated = await ddf
    .Filter("age > 30")
    .Select("dept", "salary")
    .GroupBy("dept")
    .Sum("salary")
    .CollectAsync();
```

Supported operations include `Filter` (string expression or typed `Glacier.Polaris.Expr`), `Select`, `GroupBy`, `Join` (`JoinType.Inner`, etc.), `Repartition`, `CollectAsync`, `WriteParquetAsync`, and `WriteArrowIpcAsync`.

---

## 3. Ring AllReduce & Distributed K-Means

- **`RingAllReduceCoordinator` / `RingAllReduceWorker`:** Orders participating `NodeHardwareProfile` instances by `/24` IPv4 subnet and `NodeId` (`OrderNodesTopologyAware`), emits `RingAllReduceHandshakeMessage` ring neighbors, and executes scatter-reduce + allgather across the ring.
- **`DistributedKMeans`:** Trains `K` centroids across row-major worker partitions by exchanging only `[K * D | K counts]` gradient buffers through `RingAllReduceWorker.FitAsync` calls without transferring raw feature matrices.

---

## 4. In-Process Cluster Simulator & Fault Injection

`ClusterSimulator` boots `N` virtual worker nodes (`4` to `64+`) wired over in-memory duplex pipes (`InMemoryShuffleBus`) so topologies, DAG retries, and network faults run deterministically inside one process without OS port exhaustion:

```csharp
using Glacier.Cluster.Simulator;

await using var sim = await ClusterSimulator.CreateAsync(nodeCount: 8);
```

Attach `FaultInjector` / `FaultInjectingDuplexPipe` / `FaultInjectingShuffleClient` (`Glacier.Cluster.FaultInjection`) to simulate dropped frames, delayed heartbeats, disk-spill saturation, and worker crashes.

---

## 5. Distributed Model Inference Pipeline

For multi-node and multi-GPU LLM layer partitioning (`DistributedPipelineCoordinator`, `DistributedClusterSession`, `ClusterResourcePlanner`, `HeterogeneousLoadBalancer`), see [docs/manual/distributed-inference.md](manual/distributed-inference.md).
