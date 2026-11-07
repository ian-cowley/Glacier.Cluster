namespace Glacier.Cluster.Inference.Pipeline;

using System;
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Transport;
using Glacier.Inference.Engine;
using Glacier.Inference.Gpu;
using Glacier.Inference.Gpu.D3D12;
using Glacier.Inference.Memory;
using Glacier.Inference.Model;

public class StageTelemetry
{
    public string Name { get; set; } = "";
    public string Host { get; set; } = "";
    public int Port { get; set; }
    public int StartLayer { get; set; }
    public int EndLayer { get; set; }
    public int LayerCount => Math.Max(0, EndLayer - StartLayer + 1);
    public double TotalComputeMs { get; set; }
    public double TotalNetworkMs { get; set; }
    public int Invocations { get; set; }
}

public sealed class CpuClusterStageModel : IClusterStageModel
{
    private readonly ICpuModel _model;
    private readonly KVCache _kv;
    public string BackendName { get; }

    public CpuClusterStageModel(ICpuModel model, KVCache kv, string backendName)
    {
        _model = model;
        _kv = kv;
        BackendName = backendName;
    }

    public void ForwardStage(int token, int pos, ReadOnlySpan<float> inputX, Span<float> outputX, Span<float> logits, bool computeLogits)
    {
        _model.ForwardStage(token, pos, inputX, outputX, logits, computeLogits, _kv);
    }

    public void ForwardBatchStage(ReadOnlySpan<int> tokens, int startPos, ReadOnlySpan<float> inputXBatch, Span<float> outputXBatch, Span<float> logits, bool computeLogits)
    {
        _model.ForwardBatchStage(tokens, startPos, inputXBatch, outputXBatch, logits, computeLogits, _kv);
    }

    public void ResetKvCache()
    {
        _kv.Reset();
        _model.ResetState();
    }

    public void Dispose()
    {
        _model.Dispose();
        _kv.Dispose();
    }
}

public sealed class D3D12ClusterStageModel : IClusterStageModel
{
    private readonly ID3D12Model _model;
    private readonly D3D12Context _ctx;
    public string BackendName { get; }

    public D3D12ClusterStageModel(D3D12Context ctx, ID3D12Model model, string backendName)
    {
        _ctx = ctx;
        _model = model;
        BackendName = backendName;
    }

    public void ForwardStage(int token, int pos, ReadOnlySpan<float> inputX, Span<float> outputX, Span<float> logits, bool computeLogits)
    {
        _model.ForwardStage(token, pos, inputX, outputX, logits, computeLogits);
    }

    public void ForwardBatchStage(ReadOnlySpan<int> tokens, int startPos, ReadOnlySpan<float> inputXBatch, Span<float> outputXBatch, Span<float> logits, bool computeLogits)
    {
        _model.ForwardBatchStage(tokens, startPos, inputXBatch, outputXBatch, logits, computeLogits);
    }

    public void ResetKvCache()
    {
        _model.ResetKvCache();
    }

    public void Dispose()
    {
        _model.Dispose();
        _ctx.Dispose();
    }
}

public sealed class NetworkChunkWriter : Stream
{
    private readonly Stream _underlying;
    private readonly ClusterMessageType _msgType;
    private readonly byte[] _buffer = GC.AllocateArray<byte>(4 * 1024 * 1024, pinned: true);
    private readonly byte[] _hdrBytes = new byte[ClusterFrameHeader.HeaderSize];
    private int _buffered;

    public NetworkChunkWriter(Stream underlying, ClusterMessageType msgType)
    {
        _underlying = underlying;
        _msgType = msgType;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        while (count > 0)
        {
            int toCopy = Math.Min(count, _buffer.Length - _buffered);
            Buffer.BlockCopy(buffer, offset, _buffer, _buffered, toCopy);
            _buffered += toCopy;
            offset += toCopy;
            count -= toCopy;

            if (_buffered == _buffer.Length)
            {
                FlushChunk();
            }
        }
    }

    private void FlushChunk()
    {
        if (_buffered == 0) return;

        var hdr = new ClusterFrameHeader(_msgType, 0, (ulong)_buffered);
        hdr.WriteTo(_hdrBytes);

        _underlying.Write(_hdrBytes, 0, ClusterFrameHeader.HeaderSize);
        _underlying.Write(_buffer, 0, _buffered);
        _underlying.Flush();
        _buffered = 0;
    }

    public override void Flush()
    {
        FlushChunk();
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing) FlushChunk();
        base.Dispose(disposing);
    }
}

public static class ClusterStageModelFactory
{
    public static IClusterStageModel CreateStageModel(
        ModelWeights weights,
        int maxSeqLen,
        int startLayer,
        int layerCount,
        bool isLastStage,
        int adapterIndex = -1,
        bool forceCpu = false)
    {
        if (!forceCpu &&
            OperatingSystem.IsWindows() &&
            D3D12Context.IsSupported &&
            (weights.ArchitectureFamily == UniversalArchitecture.Qwen ||
             weights.ArchitectureFamily == UniversalArchitecture.Llama ||
             weights.ArchitectureFamily == UniversalArchitecture.HybridSsm ||
             weights.ArchitectureFamily == UniversalArchitecture.Generic))
        {
            int safeD3DAdapter = ResolveSafeD3D12AdapterIndex(adapterIndex);
            var d3dModel = TryCreateD3D12StageModel(weights, maxSeqLen, startLayer, layerCount, isLastStage, safeD3DAdapter);
            if (d3dModel != null) return d3dModel;
        }

        string cpuSimd = System.Runtime.Intrinsics.Vector512.IsHardwareAccelerated ? "AVX-512" :
                         (System.Runtime.Intrinsics.Vector256.IsHardwareAccelerated ? "AVX2" : "Scalar");
        var cpuModel = CpuModelFactory.Create(weights, maxSeqLen, startLayer, layerCount, isLastStage);
        var kv = new KVCache(layerCount, weights.HeadCountKv, weights.HeadDim, maxSeqLen);
        return new CpuClusterStageModel(cpuModel, kv, $"CPU SIMD ({Environment.ProcessorCount} Cores, {cpuSimd})");
    }

    public static int ResolveSafeD3D12AdapterIndex(int requestedIndex)
    {
        if (!OperatingSystem.IsWindows()) return -1;
        try
        {
            var adapters = D3D12Context.EnumerateAdapters();
            if (adapters.Count == 0) return -1;
            if (requestedIndex < 0) return -1;
            if (requestedIndex >= adapters.Count)
            {
                int clamped = adapters.Count - 1;
                return clamped;
            }
            return requestedIndex;
        }
        catch
        {
            return -1;
        }
    }

    public static int ResolveSafeVulkanDeviceIndex(int requestedIndex)
    {
        if (!VulkanContext.IsSupported) return 0;
        try
        {
            var devices = VulkanContext.EnumerateDevices();
            if (devices.Count == 0) return 0;
            if (requestedIndex < 0) return 0;
            if (requestedIndex >= devices.Count)
            {
                int clamped = devices.Count - 1;
                return clamped;
            }
            return requestedIndex;
        }
        catch
        {
            return 0;
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static IClusterStageModel? TryCreateD3D12StageModel(
        ModelWeights weights,
        int maxSeqLen,
        int startLayer,
        int layerCount,
        bool isLastStage,
        int adapterIndex = -1)
    {
        D3D12Context? ctx = null;
        try
        {
            ctx = new D3D12Context(adapterIndex);
            if (weights.ArchitectureFamily == UniversalArchitecture.HybridSsm)
            {
                var hybridModel = new Qwen3HybridD3D12Model(ctx, weights, maxSeqLen, startLayer, layerCount, isLastStage);
                var wrappedHybrid = new D3D12ClusterStageModel(ctx, hybridModel, $"D3D12 GPU HybridSsm ({ctx.DeviceName})");
                ctx = null;
                return wrappedHybrid;
            }

            var d3dModel = new Qwen2D3D12Model(ctx, weights, maxSeqLen, KvCachePrecision.Auto, startLayer, layerCount, isLastStage);
            var wrapped = new D3D12ClusterStageModel(ctx, d3dModel, $"D3D12 GPU ({ctx.DeviceName})");
            ctx = null;
            return wrapped;
        }
        catch
        {
            ctx?.Dispose();
            return null;
        }
    }
}
