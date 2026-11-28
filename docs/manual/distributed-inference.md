# Distributed Inference Pipeline Manual

`Glacier.Cluster.Inference.Pipeline` partitions transformer layers across local GPUs, remote LAN workers, and warm CPU expert pools.

## 1. Planning Layer Splits (`ClusterResourcePlanner` & `HeterogeneousLoadBalancer`)

- **`ClusterResourcePlanner`:** Pairs host device capabilities (`NodeResourceProfile`) and remote `StageEndpoint` entries into `StageAllocation` ranges (`--layers auto` style VRAM-weighted distribution).
- **`HeterogeneousLoadBalancer`:** Evaluates `StageDeviceProfile` lists against `HeterogeneousLoadBalancerOptions` using live micro-benchmarked or calibrated stage timings (`StageAllocationPlan`, `LoadBalancerResult`).
- **`DynamicThroughputCalibrator`:** Measures per-layer throughput (`CpuSimdSyntheticRunner` / `ISyntheticLayerRunner`) under `CalibrationOptions` to refine stage layer counts dynamically.

## 2. Executing Multi-Stage Steps (`DistributedPipelineCoordinator` & `DistributedClusterSession`)

- **`DistributedPipelineCoordinator`:** Connects `Stage0` (`AttentionRootStageWorker`), `Stage1` (`IInferenceStageWorker` — such as `GpuFirstExpertStageWorker` or `CpuWarmExpertPoolWorker`), and `Stage2` (`AccumulatorStageWorker`). `ExecuteStep(sequenceId, tokenIndex, layerIndex, inputEmbedding, selectedExperts, expertWeights)` routes activations in process or over `System.IO.Pipelines` via `RefCountedPartitionBuffer` without hot-path managed allocations.
- **`DistributedClusterSession`:** Coordinates end-to-end token generation across multiple `IClusterStageModel` backends (`CpuClusterStageModel`, `D3D12ClusterStageModel`, `VulkanClusterStageModel`, or TCP remote stages built via `ClusterStageModelFactory` and `NetworkChunkWriter`).
- **`SpeculativeOverlapCoordinator`:** Overlaps draft-token verification across stages using `IStageForwardModel` and records acceptance telemetry in `SpeculativePipelineMetrics`.
