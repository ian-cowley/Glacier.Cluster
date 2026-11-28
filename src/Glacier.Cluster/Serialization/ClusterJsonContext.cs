namespace Glacier.Cluster.Serialization;

using System.Collections.Generic;
using System.Text.Json.Serialization;
using Glacier.Cluster.Coordinator;
using Glacier.Cluster.Inference.Pipeline;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Scheduler;
using Glacier.Cluster.Shuffle;
using Glacier.Cluster.Simulator;
using Glacier.Cluster.Transport;
using Glacier.Cluster.Worker;

/// <summary>
/// Source-generated System.Text.Json serialization context for NativeAOT and trim-safe
/// serialization of Glacier.Cluster configuration, planning, consensus, and telemetry DTOs.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true,
    NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    IncludeFields = true,
    WriteIndented = false)]
[JsonSerializable(typeof(ConsensusOptions))]
[JsonSerializable(typeof(ConsensusRole))]
[JsonSerializable(typeof(LeaderLease))]
[JsonSerializable(typeof(FlowControlOptions))]
[JsonSerializable(typeof(CalibrationOptions))]
[JsonSerializable(typeof(HeterogeneousLoadBalancerOptions))]
[JsonSerializable(typeof(SchedulerAffinityWeights))]
[JsonSerializable(typeof(ClusterCoordinatorOptions))]
[JsonSerializable(typeof(ClusterWorkerOptions))]
[JsonSerializable(typeof(ClusterSimulatorOptions))]
[JsonSerializable(typeof(StageTelemetry))]
[JsonSerializable(typeof(List<StageTelemetry>))]
[JsonSerializable(typeof(NodeResourceProfile))]
[JsonSerializable(typeof(List<NodeResourceProfile>))]
[JsonSerializable(typeof(StageEndpoint))]
[JsonSerializable(typeof(List<StageEndpoint>))]
[JsonSerializable(typeof(StageAllocation))]
[JsonSerializable(typeof(List<StageAllocation>))]
[JsonSerializable(typeof(StageDeviceProfile))]
[JsonSerializable(typeof(List<StageDeviceProfile>))]
[JsonSerializable(typeof(StageAllocationPlan))]
[JsonSerializable(typeof(List<StageAllocationPlan>))]
[JsonSerializable(typeof(LoadBalancerResult))]
[JsonSerializable(typeof(NodeCalibrationResult))]
[JsonSerializable(typeof(List<NodeCalibrationResult>))]
[JsonSerializable(typeof(ClusterGenerationResult))]
[JsonSerializable(typeof(SpeculativePipelineMetrics))]
[JsonSerializable(typeof(NodeHardwareProfile))]
[JsonSerializable(typeof(List<NodeHardwareProfile>))]
[JsonSerializable(typeof(ClusterNetworkTransportHarness.BenchmarkResult))]
public partial class ClusterJsonContext : JsonSerializerContext
{
}
