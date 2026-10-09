namespace Glacier.Cluster.Simulator;

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Glacier.Cluster.Shuffle;

/// <summary>
/// High-performance zero-socket P2P shuffle routing bus maintaining in-memory registration of
/// all virtual simulated worker nodes.
/// </summary>
public sealed class InMemoryShuffleBus : IAsyncDisposable, IDisposable
{
    private readonly ConcurrentDictionary<ushort, SimulatedWorkerNode> _nodesByPort = new();
    private readonly ConcurrentDictionary<string, SimulatedWorkerNode> _nodesByHost = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<Guid, SimulatedWorkerNode> _nodesById = new();
    private readonly ConcurrentDictionary<(Guid NodeId, int StageId, int PartitionId), bool> _fetchFailures = new();

    public void InjectFetchFailure(Guid producerNodeId, int stageId, int partitionId)
    {
        _fetchFailures[(producerNodeId, stageId, partitionId)] = true;
    }

    public void ClearFetchFailures()
    {
        _fetchFailures.Clear();
    }

    public bool CheckAndConsumeFetchFailure(Guid producerNodeId, int stageId, int partitionId)
    {
        return _fetchFailures.TryRemove((producerNodeId, stageId, partitionId), out _);
    }

    public void RegisterNode(SimulatedWorkerNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        _nodesByPort[node.VirtualShufflePort] = node;
        _nodesByHost[node.Hostname] = node;
        _nodesById[node.NodeId] = node;
    }

    public void UnregisterNode(SimulatedWorkerNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        _nodesByPort.TryRemove(node.VirtualShufflePort, out _);
        _nodesByHost.TryRemove(node.Hostname, out _);
        _nodesById.TryRemove(node.NodeId, out _);
    }

    public bool TryResolveNode(string hostOrPort, ushort port, out SimulatedWorkerNode? node)
    {
        if (port > 0 && _nodesByPort.TryGetValue(port, out node))
            return true;

        if (!string.IsNullOrEmpty(hostOrPort))
        {
            if (_nodesByHost.TryGetValue(hostOrPort, out node))
                return true;

            // Handle host:port format if passed
            int colonIdx = hostOrPort.IndexOf(':');
            if (colonIdx > 0)
            {
                string hostOnly = hostOrPort.Substring(0, colonIdx);
                if (_nodesByHost.TryGetValue(hostOnly, out node))
                    return true;
                if (ushort.TryParse(hostOrPort.Substring(colonIdx + 1), out ushort parsedPort) &&
                    _nodesByPort.TryGetValue(parsedPort, out node))
                    return true;
            }
        }

        node = null;
        return false;
    }

    public bool TryResolveNode(Guid nodeId, out SimulatedWorkerNode? node)
    {
        return _nodesById.TryGetValue(nodeId, out node);
    }

    public IArrowShuffleClient CreateShuffleClientForNode(SimulatedWorkerNode localNode, SimulatorShuffleMode mode)
    {
        ArgumentNullException.ThrowIfNull(localNode);
        return mode switch
        {
            SimulatorShuffleMode.WirePipes => new PipeShuffleClient(this, localNode),
            SimulatorShuffleMode.DirectZeroCopy => new DirectMemoryShuffleClient(this, localNode),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };
    }

    public ValueTask DisposeAsync()
    {
        _nodesByPort.Clear();
        _nodesByHost.Clear();
        _nodesById.Clear();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        _nodesByPort.Clear();
        _nodesByHost.Clear();
        _nodesById.Clear();
    }
}
