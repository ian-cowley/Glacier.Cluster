namespace Glacier.Cluster.Tests.ProtocolTests;

using System;
using System.Text;
using Glacier.Cluster.Protocol;
using Xunit;

public sealed class Crc32Tests
{
    [Fact]
    public void Compute_EmptySpan_ReturnsZero()
    {
        uint crc = ClusterCrc32.Compute(ReadOnlySpan<byte>.Empty);
        Assert.Equal(0U, crc);
    }

    [Fact]
    public void Compute_StandardCheckVector_MatchesIeee8023()
    {
        // Standard ASCII test vector "123456789"
        // Standard IEEE 802.3 polynomial 0xEDB88320 CRC-32 expected: 0xCBF43926
        byte[] input = Encoding.ASCII.GetBytes("123456789");
        uint crc = ClusterCrc32.Compute(input);
        Assert.Equal(0xCBF43926U, crc);
    }

    [Fact]
    public void Compute_ConsistentAcrossSubSlices()
    {
        byte[] input = new byte[1024];
        for (int i = 0; i < input.Length; i++) input[i] = (byte)(i * 31);

        uint crc1 = ClusterCrc32.Compute(input);
        uint crc2 = ClusterCrc32.Compute(input.AsSpan());

        Assert.Equal(crc1, crc2);
        Assert.NotEqual(0U, crc1);
    }
}
