namespace Glacier.Cluster.Protocol;

using System;

/// <summary>
/// Bitmask flags modifying frame handling and payload characteristics.
/// </summary>
[Flags]
public enum ClusterFrameFlags : byte
{
    None = 0x00,
    Compressed = 0x01,
    Spilled = 0x02,
    ArrowIpc = 0x04,
    Error = 0x08,
    EndOfStream = 0x10
}
