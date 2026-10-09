namespace Glacier.Cluster.Protocol;

/// <summary>
/// Authoritative binary message opcodes for the Glacier.Cluster wire protocol.
/// Stored as a byte (offset 6) in the 32-byte ClusterFrameHeader.
/// </summary>
public enum ClusterMessageType : byte
{
    /// <summary>0x01: Worker-to-Coordinator periodic liveness ping and load telemetry.</summary>
    HeartbeatPing = 0x01,

    /// <summary>0x02: Coordinator-to-Worker acknowledgment confirming health and epoch.</summary>
    HeartbeatPong = 0x02,

    /// <summary>0x03: Worker node registration request containing hardware profile and hostname.</summary>
    NodeJoinRequest = 0x03,

    /// <summary>0x04: Coordinator membership response with assigned cluster rank and epoch.</summary>
    NodeJoinResponse = 0x04,

    /// <summary>0x05: Graceful leave notification prior to worker shutdown or draining.</summary>
    NodeLeave = 0x05,

    /// <summary>0x10: Coordinator-to-Worker stage task dispatch manifest and dependencies.</summary>
    StageDispatch = 0x10,

    /// <summary>0x11: Worker-to-Coordinator task execution telemetry, metrics, or error.</summary>
    TaskStatusUpdate = 0x11,

    /// <summary>0x20: Worker-to-Worker P2P partition pull request with flow control credit tokens.</summary>
    PartitionFetchRequest = 0x20,

    /// <summary>0x21: Worker-to-Worker P2P partition pull response metadata and data batch.</summary>
    PartitionFetchResponse = 0x21,

    /// <summary>0x22: Worker-to-Worker acknowledgment releasing consumed credit tokens.</summary>
    PartitionAcknowledge = 0x22,

    /// <summary>0x30: Worker-to-Worker collective Ring AllReduce topology handshake.</summary>
    RingAllReduceHandshake = 0x30,

    /// <summary>0x31: Worker-to-Worker collective Ring AllReduce step transfer.</summary>
    RingAllReduceStep = 0x31,

    /// <summary>0x32: Worker-to-Worker collective Ring AllReduce round completion.</summary>
    RingAllReduceCompletion = 0x32,

    /// <summary>0x40: Client-to-Coordinator distributed job DAG submission.</summary>
    JobSubmission = 0x40,

    /// <summary>0x41: Coordinator-to-Client final job completion telemetry and metrics.</summary>
    JobCompletion = 0x41
}
