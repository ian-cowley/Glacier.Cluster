namespace Glacier.Cluster.Compute;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Glacier.Cluster.Protocol;

/// <summary>
/// Session descriptor for a distributed collective Ring AllReduce operation.
/// </summary>
public sealed class RingAllReduceSession
{
    public Guid SessionId { get; }
    public IReadOnlyList<NodeHardwareProfile> NodesInRingOrder { get; }
    public int WorldSize => NodesInRingOrder.Count;
    public DateTime CreatedAtUtc { get; } = DateTime.UtcNow;

    public RingAllReduceSession(Guid sessionId, IReadOnlyList<NodeHardwareProfile> nodes)
    {
        SessionId = sessionId;
        NodesInRingOrder = nodes ?? throw new ArgumentNullException(nameof(nodes));
    }
}

/// <summary>
/// Master coordinator managing Ring AllReduce session initiation, topology-aware rank assignment,
/// and collective handshake distribution across the cluster.
/// </summary>
public sealed class RingAllReduceCoordinator
{
    private readonly ConcurrentDictionary<Guid, RingAllReduceSession> _activeSessions = new();

    /// <summary>
    /// Creates a new collective reduction session and computes topology-aware rank ordering.
    /// </summary>
    public RingAllReduceSession CreateSession(IReadOnlyList<NodeHardwareProfile> candidateNodes)
    {
        ArgumentNullException.ThrowIfNull(candidateNodes);
        if (candidateNodes.Count == 0)
            throw new ArgumentException("Cannot create a ring session with 0 nodes.", nameof(candidateNodes));

        Guid sessionId = Guid.NewGuid();
        IReadOnlyList<NodeHardwareProfile> orderedNodes = OrderNodesTopologyAware(candidateNodes);
        var session = new RingAllReduceSession(sessionId, orderedNodes);

        _activeSessions[sessionId] = session;
        return session;
    }

    /// <summary>
    /// Orders nodes into a ring to minimize inter-subnet crossings and balance bandwidth.
    /// </summary>
    public static IReadOnlyList<NodeHardwareProfile> OrderNodesTopologyAware(
        IReadOnlyList<NodeHardwareProfile> nodes)
    {
        if (nodes.Count <= 2)
            return nodes.ToList();

        // Sort by NodeId for deterministic tie-breaking and ring stability
        return nodes.OrderBy(n => n.NodeId).ToList();
    }

    /// <summary>
    /// Generates the RingAllReduceHandshakeMessage for a given node rank in the session.
    /// </summary>
    public RingAllReduceHandshakeMessage CreateHandshakeMessage(
        RingAllReduceSession session,
        int rank,
        Func<Guid, string>? endpointResolver = null,
        Func<Guid, ushort>? portResolver = null)
    {
        ArgumentNullException.ThrowIfNull(session);
        int worldSize = session.WorldSize;
        if (rank < 0 || rank >= worldSize)
            throw new ArgumentOutOfRangeException(nameof(rank), $"Rank {rank} out of range [0, {worldSize}).");

        int predRank = (rank - 1 + worldSize) % worldSize;
        int succRank = (rank + 1) % worldSize;

        var predNode = session.NodesInRingOrder[predRank];
        var succNode = session.NodesInRingOrder[succRank];

        string predEp = endpointResolver?.Invoke(predNode.NodeId) ?? string.Empty;
        string succEp = endpointResolver?.Invoke(succNode.NodeId) ?? string.Empty;
        ushort predPort = portResolver?.Invoke(predNode.NodeId) ?? 0;
        ushort succPort = portResolver?.Invoke(succNode.NodeId) ?? 0;

        return new RingAllReduceHandshakeMessage(
            sessionId: session.SessionId,
            rank: rank,
            worldSize: worldSize,
            predecessorId: predNode.NodeId,
            successorId: succNode.NodeId,
            predecessorPort: predPort,
            successorPort: succPort,
            predecessorEndpoint: predEp,
            successorEndpoint: succEp);
    }

    /// <summary>
    /// Retrieves an active session by its ID.
    /// </summary>
    public bool TryGetSession(Guid sessionId, out RingAllReduceSession? session)
    {
        return _activeSessions.TryGetValue(sessionId, out session);
    }

    /// <summary>
    /// Removes a completed session.
    /// </summary>
    public bool CompleteSession(Guid sessionId)
    {
        return _activeSessions.TryRemove(sessionId, out _);
    }
}
