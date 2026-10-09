namespace Glacier.Cluster.Tests.ShuffleTests;

using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Glacier.Cluster.Shuffle;
using Xunit;

public sealed class DuplexConnectionPoolTests : IDisposable
{
    private readonly Guid _localNodeId = Guid.NewGuid();
    private readonly DuplexConnectionPool _pool;

    public DuplexConnectionPoolTests()
    {
        _pool = new DuplexConnectionPool(_localNodeId, TransportFactory);
    }

    private static ValueTask<System.IO.Pipelines.IDuplexPipe> TransportFactory(
        Guid remoteNodeId,
        EndPoint endPoint,
        CancellationToken ct)
    {
        var (client, _) = DuplexPipeFactory.CreateInProcessPair();
        return ValueTask.FromResult(client);
    }

    public void Dispose()
    {
        _pool.Dispose();
    }

    [Fact]
    public async Task SingleConnectionPerNodePair_ConcurrentGets()
    {
        var remoteNodeId = Guid.NewGuid();
        var dummyEndpoint = new IPEndPoint(IPAddress.Loopback, 12345);

        const int callers = 20;
        var tasks = new Task<DuplexConnection>[callers];

        for (int i = 0; i < callers; i++)
        {
            tasks[i] = _pool.GetOrCreateConnectionAsync(remoteNodeId, dummyEndpoint).AsTask();
        }

        var results = await Task.WhenAll(tasks);

        Assert.Equal(1, _pool.ActiveConnectionCount);
        var first = results[0];
        Assert.NotNull(first);

        for (int i = 1; i < callers; i++)
        {
            Assert.Same(first, results[i]);
        }
    }

    [Fact]
    public async Task DeterministicGuidArbitration_TieBreaking()
    {
        // Case 1: Local GUID is smaller than Remote GUID
        byte[] bLocal = new byte[16];
        byte[] bRemote = new byte[16];
        bLocal[0] = 0x01;
        bRemote[0] = 0x02;
        var smallId = new Guid(bLocal);
        var largeId = new Guid(bRemote);

        await using var poolLocalSmall = new DuplexConnectionPool(smallId, TransportFactory);
        var dummyEndpoint = new IPEndPoint(IPAddress.Loopback, 12345);

        // Pre-create existing local connection
        var existingConn = await poolLocalSmall.GetOrCreateConnectionAsync(largeId, dummyEndpoint);

        // Remote (larger ID) attempts to connect inbound
        var (inboundClient, inboundServer) = DuplexPipeFactory.CreateInProcessPair();
        var arbitratedConn = await poolLocalSmall.RegisterInboundConnectionAsync(largeId, inboundClient);

        // Since local is smaller, existing connection must win and inbound connection is rejected
        Assert.Same(existingConn, arbitratedConn);

        // Case 2: Local GUID is larger than Remote GUID
        await using var poolLocalLarge = new DuplexConnectionPool(largeId, TransportFactory);
        var existingLargeConn = await poolLocalLarge.GetOrCreateConnectionAsync(smallId, dummyEndpoint);

        var (inboundClient2, inboundServer2) = DuplexPipeFactory.CreateInProcessPair();
        var arbitratedConn2 = await poolLocalLarge.RegisterInboundConnectionAsync(smallId, inboundClient2);

        // Since remote is smaller, remote inbound connection must win and replace old local connection
        Assert.NotSame(existingLargeConn, arbitratedConn2);
        Assert.False(existingLargeConn.IsConnected);
        Assert.True(arbitratedConn2.IsConnected);
    }

    [Fact]
    public async Task MultiplexedConcurrentRequests_ViaCorrelationId()
    {
        var remoteNodeId = Guid.NewGuid();
        var (clientPipe, serverPipe) = DuplexPipeFactory.CreateInProcessPair();

        await using var pool = new DuplexConnectionPool(_localNodeId, (id, ep, ct) => ValueTask.FromResult(clientPipe));
        var dummyEndpoint = new IPEndPoint(IPAddress.Loopback, 12345);
        var conn = await pool.GetOrCreateConnectionAsync(remoteNodeId, dummyEndpoint);

        // Server echo loop: reads frames from serverPipe and echoes back with same CorrelationId
        using var cts = new CancellationTokenSource();
        var serverTask = Task.Run(async () =>
        {
            var reader = serverPipe.Input;
            var writer = serverPipe.Output;
            while (!cts.Token.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(cts.Token);
                var buffer = result.Buffer;

                while (ClusterFrameReader.TryReadFrame(ref buffer, out var header, out var payload))
                {
                    // Echo back a HeartbeatPong with matching correlation ID
                    var pong = new HeartbeatPongMessage(coordinatorId: Guid.NewGuid(), timestampNs: 0, clusterEpoch: 1, coordinatorStatusFlags: 0, activeNodeCount: 1);
                    int frameSize = ClusterFrameHeader.HeaderSize + HeartbeatPongMessage.PayloadSize;
                    var span = writer.GetSpan(frameSize);
                    ClusterMessageSerializer.PackFrame(pong, header.CorrelationId, ClusterFrameFlags.None, span);
                    writer.Advance(frameSize);
                    await writer.FlushAsync(cts.Token);
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
                if (result.IsCompleted) break;
            }
        });

        // 32 concurrent multiplexed requests
        const int concurrentRequests = 32;
        var tasks = new Task[concurrentRequests];

        for (int i = 0; i < concurrentRequests; i++)
        {
            ulong corrId = (ulong)(i + 100);
            tasks[i] = Task.Run(async () =>
            {
                var ping = new HeartbeatPingMessage(nodeId: remoteNodeId, timestampNs: 0, activeTaskCount: 0, cpuUsagePercentage: 0f, availablePhysicalMemoryBytes: 1024);
                var response = await conn.SendRequestAsync(ping, corrId, TimeSpan.FromSeconds(15));

                Assert.Equal(corrId, response.Header.CorrelationId);
                Assert.Equal(ClusterMessageType.HeartbeatPong, response.Header.MessageType);
            });
        }

        await Task.WhenAll(tasks);
        cts.Cancel();
        serverPipe.Output.Complete();
        serverPipe.Input.Complete();
    }
}
