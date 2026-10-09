namespace Glacier.Cluster.Tests.ProtocolTests;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Glacier.Cluster.Protocol;
using Xunit;

public sealed class AdversarialProtocolStressTests
{
    // =========================================================================
    // 1. Concurrency Stress Testing
    // =========================================================================

    [Fact]
    public void HighThroughput_Parallel_Serialization_All13Messages()
    {
        const int iterations = 10_000;
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount * 2
        };

        Parallel.For(0, iterations, parallelOptions, i =>
        {
            int messageCase = i % 13;
            byte[] dest = new byte[2048];

            switch (messageCase)
            {
                case 0:
                {
                    var msg = new HeartbeatPingMessage(Guid.NewGuid(), (ulong)i, (uint)(i % 128), 50.0f + (i % 50), 1024UL * 1024 * (ulong)i);
                    int written = msg.Serialize(dest);
                    var deserialized = HeartbeatPingMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 1:
                {
                    var msg = new HeartbeatPongMessage(Guid.NewGuid(), (ulong)i, (ulong)(i / 10), 0x01, (uint)(i % 64));
                    int written = msg.Serialize(dest);
                    var deserialized = HeartbeatPongMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 2:
                {
                    var profile = new NodeHardwareProfile(Guid.NewGuid(), 16, SimdArchitectureFlags.Avx2, GpuBackendFlags.Cuda, 1000, 500, 100, 50);
                    var msg = new NodeJoinRequestMessage(profile, $"worker-{i}.cluster.internal", $"secret-token-{i}");
                    int written = msg.Serialize(dest);
                    var deserialized = NodeJoinRequestMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 3:
                {
                    var msg = new NodeJoinResponseMessage(NodeJoinStatus.Accepted, Guid.NewGuid(), i, i + 1, (ulong)i, $"Reason {i}");
                    int written = msg.Serialize(dest);
                    var deserialized = NodeJoinResponseMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 4:
                {
                    var msg = new NodeLeaveMessage(Guid.NewGuid(), 1, 5000, $"Draining worker {i}");
                    int written = msg.Serialize(dest);
                    var deserialized = NodeLeaveMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 5:
                {
                    byte[] payload = new byte[16];
                    BinaryPrimitives.WriteInt32LittleEndian(payload, i);
                    var upstream = new[] { new UpstreamPartitionLocation(Guid.NewGuid(), 1, 0, 8080, "upstream-host") };
                    var msg = new StageDispatchMessage(Guid.NewGuid(), 2, i % 8, StageOperatorType.Filter, payload, upstream);
                    int written = msg.Serialize(dest);
                    var deserialized = StageDispatchMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 6:
                {
                    var msg = new TaskStatusUpdateMessage(Guid.NewGuid(), 2, i % 8, TaskExecutionStatus.Completed, 123.45, 1024, 50, $"Err {i}");
                    int written = msg.Serialize(dest);
                    var deserialized = TaskStatusUpdateMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 7:
                {
                    var msg = new PartitionFetchRequestMessage(Guid.NewGuid(), 1, i % 4, 10, 0, 65536);
                    int written = msg.Serialize(dest);
                    var deserialized = PartitionFetchRequestMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 8:
                {
                    byte[] chunk = Encoding.UTF8.GetBytes($"partition-data-chunk-{i}");
                    var msg = new PartitionFetchResponseMessage(Guid.NewGuid(), 1, i % 4, 100, PartitionFetchStatus.Success, PartitionDataFlags.FinalChunk, 0, chunk);
                    int written = msg.Serialize(dest);
                    var deserialized = PartitionFetchResponseMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 9:
                {
                    var msg = new PartitionAcknowledgeMessage(Guid.NewGuid(), 1, i % 4, 5, 2048, true);
                    int written = msg.Serialize(dest);
                    var deserialized = PartitionAcknowledgeMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 10:
                {
                    var msg = new RingAllReduceHandshakeMessage(Guid.NewGuid(), i % 4, 4, Guid.NewGuid(), Guid.NewGuid(), 9000, 9001, $"10.0.0.{i % 250}:9000", $"10.0.0.{(i + 1) % 250}:9001");
                    int written = msg.Serialize(dest);
                    var deserialized = RingAllReduceHandshakeMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 11:
                {
                    byte[] dagBytes = Encoding.UTF8.GetBytes($"dag-spec-{i}");
                    var msg = new JobSubmissionMessage(Guid.NewGuid(), $"job-{i}", 3, 16, 60000, dagBytes);
                    int written = msg.Serialize(dest);
                    var deserialized = JobSubmissionMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
                case 12:
                {
                    var msg = new JobCompletionMessage(Guid.NewGuid(), JobCompletionStatus.Succeeded, 543.21, 10000, 500000, $"Telemetry {i}");
                    int written = msg.Serialize(dest);
                    var deserialized = JobCompletionMessage.Deserialize(dest.AsSpan(0, written));
                    Assert.Equal(msg, deserialized);
                    break;
                }
            }
        });
    }

    [Fact]
    public void HighThroughput_PackFrame_And_Deserialize_Concurrent()
    {
        const int iterations = 10_000;
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount * 2
        };

        Parallel.For(0, iterations, parallelOptions, i =>
        {
            byte[] frameDest = new byte[4096];
            IClusterMessage message = (i % 4) switch
            {
                0 => new HeartbeatPingMessage(Guid.NewGuid(), (ulong)i, 4, 12.5f, 1024),
                1 => new NodeLeaveMessage(Guid.NewGuid(), 0, 1000, "shutdown"),
                2 => new TaskStatusUpdateMessage(Guid.NewGuid(), 1, 0, TaskExecutionStatus.Completed, 10.0, 100, 10, string.Empty),
                _ => new PartitionAcknowledgeMessage(Guid.NewGuid(), 1, 0, 1, 100, true)
            };

            int frameSize = ClusterMessageSerializer.PackFrame(message, (ulong)i, ClusterFrameFlags.None, frameDest);
            Assert.True(frameSize >= 32);

            var header = ClusterFrameHeader.ReadFrom(frameDest.AsSpan(0, 32));
            Assert.Equal(ClusterFrameHeader.MagicConstant, header.Magic);
            Assert.Equal(message.MessageType, header.MessageType);
            Assert.Equal((ulong)i, header.CorrelationId);

            var payloadSpan = frameDest.AsSpan(32, (int)header.PayloadLength);
            var deserialized = ClusterMessageSerializer.Deserialize(header.MessageType, payloadSpan);
            Assert.Equal(message.MessageType, deserialized.MessageType);
        });
    }

    // =========================================================================
    // 2. Extreme Inputs: Empty strings, Maximum length strings, Unicode, Emojis
    // =========================================================================

    [Theory]
    [InlineData("")]
    [InlineData("こんにちは世界 🚀🔥")]
    [InlineData("分布式计算集群与机器学习")]
    [InlineData("مرحبا بالعالم - اختبار الترميز")]
    [InlineData("Привет, мир! 12345!")]
    [InlineData("Héllo Wörld üñîçødê €$¥")]
    [InlineData("Complex emoji sequences: 🧑🏽‍💻 🏳️‍🌈 👨‍👩‍👧‍👦 ⚡️✨❄️🧠")]
    public void ExtremeStrings_Roundtrip_Across_All_String_Messages(string testString)
    {
        byte[] buffer = new byte[8192];

        // 1. NodeJoinRequest
        {
            var msg = new NodeJoinRequestMessage(new NodeHardwareProfile(), testString, testString);
            int written = msg.Serialize(buffer);
            var des = NodeJoinRequestMessage.Deserialize(buffer.AsSpan(0, written));
            Assert.Equal(testString, des.Hostname);
            Assert.Equal(testString, des.ClusterSecret);
        }

        // 2. NodeJoinResponse
        {
            var msg = new NodeJoinResponseMessage(NodeJoinStatus.Accepted, Guid.NewGuid(), 0, 1, 1, testString);
            int written = msg.Serialize(buffer);
            var des = NodeJoinResponseMessage.Deserialize(buffer.AsSpan(0, written));
            Assert.Equal(testString, des.RejectionReason);
        }

        // 3. NodeLeave
        {
            var msg = new NodeLeaveMessage(Guid.NewGuid(), 1, 100, testString);
            int written = msg.Serialize(buffer);
            var des = NodeLeaveMessage.Deserialize(buffer.AsSpan(0, written));
            Assert.Equal(testString, des.ReasonDetails);
        }

        // 4. TaskStatusUpdate
        {
            var msg = new TaskStatusUpdateMessage(Guid.NewGuid(), 1, 0, TaskExecutionStatus.Failed, 10.0, 0, 0, testString);
            int written = msg.Serialize(buffer);
            var des = TaskStatusUpdateMessage.Deserialize(buffer.AsSpan(0, written));
            Assert.Equal(testString, des.ErrorMessage);
        }

        // 5. RingAllReduceHandshake
        {
            var msg = new RingAllReduceHandshakeMessage(Guid.NewGuid(), 0, 1, Guid.NewGuid(), Guid.NewGuid(), 80, 81, testString, testString);
            int written = msg.Serialize(buffer);
            var des = RingAllReduceHandshakeMessage.Deserialize(buffer.AsSpan(0, written));
            Assert.Equal(testString, des.PredecessorEndpoint);
            Assert.Equal(testString, des.SuccessorEndpoint);
        }

        // 6. JobSubmission
        {
            var msg = new JobSubmissionMessage(Guid.NewGuid(), testString, 1, 1, 1000, Array.Empty<byte>());
            int written = msg.Serialize(buffer);
            var des = JobSubmissionMessage.Deserialize(buffer.AsSpan(0, written));
            Assert.Equal(testString, des.JobName);
        }

        // 7. JobCompletion
        {
            var msg = new JobCompletionMessage(Guid.NewGuid(), JobCompletionStatus.Failed, 10.0, 0, 0, testString);
            int written = msg.Serialize(buffer);
            var des = JobCompletionMessage.Deserialize(buffer.AsSpan(0, written));
            Assert.Equal(testString, des.ErrorMessage);
        }

        // 8. UpstreamPartitionLocation
        {
            var loc = new UpstreamPartitionLocation(Guid.NewGuid(), 1, 0, 8080, testString);
            int written = loc.Serialize(buffer);
            var des = UpstreamPartitionLocation.Deserialize(buffer.AsSpan(0, written), out int consumed);
            Assert.Equal(written, consumed);
            Assert.Equal(testString, des.Hostname);
        }
    }

    [Fact]
    public void ExtremeStrings_NullValues_HandledGracefullyAsEmpty()
    {
        byte[] buffer = new byte[1024];

        var join = new NodeJoinRequestMessage(new NodeHardwareProfile(), null, null);
        int w1 = join.Serialize(buffer);
        var desJoin = NodeJoinRequestMessage.Deserialize(buffer.AsSpan(0, w1));
        Assert.Equal(string.Empty, desJoin.Hostname);
        Assert.Equal(string.Empty, desJoin.ClusterSecret);

        var leave = new NodeLeaveMessage(Guid.NewGuid(), 0, 0, null);
        int w2 = leave.Serialize(buffer);
        var desLeave = NodeLeaveMessage.Deserialize(buffer.AsSpan(0, w2));
        Assert.Equal(string.Empty, desLeave.ReasonDetails);

        var task = new TaskStatusUpdateMessage(Guid.NewGuid(), 0, 0, TaskExecutionStatus.Pending, 0, 0, 0, null);
        int w3 = task.Serialize(buffer);
        var desTask = TaskStatusUpdateMessage.Deserialize(buffer.AsSpan(0, w3));
        Assert.Equal(string.Empty, desTask.ErrorMessage);
    }

    [Fact]
    public void ExtremeStrings_MaximumLength_65535_Bytes_Roundtrip()
    {
        // 65535 ASCII characters = exactly 65535 bytes in UTF-8
        string maxAsciiString = new string('A', ushort.MaxValue);
        byte[] buffer = new byte[70000];

        var msg = new NodeLeaveMessage(Guid.NewGuid(), 1, 1000, maxAsciiString);
        int written = msg.Serialize(buffer);
        Assert.Equal(28 + 2 + 65535, written);

        var deserialized = NodeLeaveMessage.Deserialize(buffer.AsSpan(0, written));
        Assert.Equal(maxAsciiString.Length, deserialized.ReasonDetails.Length);
        Assert.Equal(maxAsciiString, deserialized.ReasonDetails);
    }

    [Fact]
    public void ExtremeStrings_ExceedsUshortMax_ThrowsArgumentException()
    {
        // 65536 characters -> exceeds ushort.MaxValue (65535)
        string tooLongString = new string('X', 65536);
        byte[] buffer = new byte[70000];

        var msg = new NodeLeaveMessage(Guid.NewGuid(), 1, 1000, tooLongString);
        Assert.Throws<ArgumentException>(() => msg.Serialize(buffer));

        // Multi-byte string that exceeds 65535 bytes
        // 22000 Chinese characters * 3 bytes each = 66000 bytes > 65535
        string tooLongUtf8 = new string('测', 22000);
        var msg2 = new NodeLeaveMessage(Guid.NewGuid(), 1, 1000, tooLongUtf8);
        Assert.Throws<ArgumentException>(() => msg2.Serialize(buffer));
    }

    // =========================================================================
    // 3. Buffer Bounds: Destination Shorter Than Required
    // =========================================================================

    [Fact]
    public void DestinationShorterThanPayload_ThrowsArgumentException_All13Messages()
    {
        var messages = CreateSampleMessages();

        foreach (var msg in messages)
        {
            int required = msg.GetByteCount();
            Assert.True(required > 0, $"{msg.GetType().Name} reported 0 byte count.");

            // Length 0
            Assert.Throws<ArgumentException>(() => msg.Serialize(Span<byte>.Empty));

            // Length 1
            if (required > 1)
            {
                byte[] oneByte = new byte[1];
                Assert.Throws<ArgumentException>(() => msg.Serialize(oneByte));
            }

            // Length required - 1
            byte[] almostEnough = new byte[required - 1];
            Assert.Throws<ArgumentException>(() => msg.Serialize(almostEnough));

            // Length required / 2
            byte[] halfEnough = new byte[required / 2];
            Assert.Throws<ArgumentException>(() => msg.Serialize(halfEnough));
        }
    }

    [Fact]
    public void DestinationShorterThanPayload_NodeHardwareProfile_And_UpstreamLocation()
    {
        // NodeHardwareProfile
        var profile = new NodeHardwareProfile();
        Assert.Throws<ArgumentException>(() => profile.WriteTo(Span<byte>.Empty));
        Assert.Throws<ArgumentException>(() => profile.WriteTo(new byte[63]));

        // UpstreamPartitionLocation
        var loc = new UpstreamPartitionLocation(Guid.NewGuid(), 1, 1, 8080, "host");
        int req = loc.GetByteCount();
        Assert.Throws<ArgumentException>(() => loc.Serialize(Span<byte>.Empty));
        Assert.Throws<ArgumentException>(() => loc.Serialize(new byte[req - 1]));
    }

    [Fact]
    public void DestinationShorterThanPayload_PackFrame_ThrowsArgumentException()
    {
        var msg = new HeartbeatPingMessage(Guid.NewGuid(), 1, 1, 1.0f, 100);
        int fullSize = 32 + msg.GetByteCount();

        Assert.Throws<ArgumentException>(() => ClusterMessageSerializer.PackFrame(msg, 1, ClusterFrameFlags.None, Span<byte>.Empty));
        Assert.Throws<ArgumentException>(() => ClusterMessageSerializer.PackFrame(msg, 1, ClusterFrameFlags.None, new byte[31]));
        Assert.Throws<ArgumentException>(() => ClusterMessageSerializer.PackFrame(msg, 1, ClusterFrameFlags.None, new byte[fullSize - 1]));
    }

    // =========================================================================
    // 4. Malformed Buffers & Deserialization Resilience
    // =========================================================================

    [Fact]
    public void Deserialization_TruncatedBuffers_ThrowsArgumentException()
    {
        var messages = CreateSampleMessages();

        foreach (var msg in messages)
        {
            byte[] validBuffer = new byte[msg.GetByteCount()];
            int written = msg.Serialize(validBuffer);
            Assert.Equal(written, validBuffer.Length);

            // Empty buffer
            Assert.Throws<ArgumentException>(() => ClusterMessageSerializer.Deserialize(msg.MessageType, ReadOnlySpan<byte>.Empty));

            // 1-byte buffer
            Assert.Throws<ArgumentException>(() => ClusterMessageSerializer.Deserialize(msg.MessageType, new byte[1]));

            // Truncate at written - 1 bytes
            Assert.Throws<ArgumentException>(() => ClusterMessageSerializer.Deserialize(msg.MessageType, validBuffer.AsSpan(0, written - 1)));

            // Truncate at half bytes
            Assert.Throws<ArgumentException>(() => ClusterMessageSerializer.Deserialize(msg.MessageType, validBuffer.AsSpan(0, written / 2)));
        }
    }

    [Fact]
    public void Deserialization_RandomNoiseBytes_DoesNotCrashOrCorruptMemory()
    {
        var random = new Random(42);
        var opcodes = Enum.GetValues<ClusterMessageType>();

        for (int iteration = 0; iteration < 1_000; iteration++)
        {
            int size = random.Next(0, 512);
            byte[] noise = new byte[size];
            random.NextBytes(noise);

            foreach (var opcode in opcodes)
            {
                try
                {
                    // Attempt deserialization on random noise
                    _ = ClusterMessageSerializer.Deserialize(opcode, noise);
                    // If it somehow parsed without exception, it must not crash or AV
                }
                catch (ArgumentException)
                {
                    // Expected for invalid lengths / truncated payloads
                }
                catch (OverflowException)
                {
                    // Expected if random noise yields negative array size
                }
                catch (OutOfMemoryException)
                {
                    // Expected if random noise yields absurdly huge array count
                }
                catch (NotSupportedException)
                {
                    // Expected for unknown opcodes
                }
            }
        }
    }

    [Fact]
    public void Deserialization_SpecificPayloadCorruptions()
    {
        // 1. StageDispatch with negative or excessive payload length
        {
            byte[] badDispatch = new byte[64];
            Guid.NewGuid().TryWriteBytes(badDispatch.AsSpan(0, 16));
            BinaryPrimitives.WriteInt32LittleEndian(badDispatch.AsSpan(28, 4), -1); // negative payload length
            Assert.Throws<ArgumentException>(() => StageDispatchMessage.Deserialize(badDispatch));

            BinaryPrimitives.WriteInt32LittleEndian(badDispatch.AsSpan(28, 4), 100_000); // length exceeds buffer
            Assert.Throws<ArgumentException>(() => StageDispatchMessage.Deserialize(badDispatch));
        }

        // 2. PartitionFetchResponse with negative chunk length
        {
            byte[] badFetch = new byte[64];
            BinaryPrimitives.WriteInt32LittleEndian(badFetch.AsSpan(44, 4), -5); // negative chunk length
            Assert.Throws<ArgumentException>(() => PartitionFetchResponseMessage.Deserialize(badFetch));

            BinaryPrimitives.WriteInt32LittleEndian(badFetch.AsSpan(44, 4), 999_999); // exceeds buffer
            Assert.Throws<ArgumentException>(() => PartitionFetchResponseMessage.Deserialize(badFetch));
        }

        // 3. JobSubmission with negative DAG payload length
        {
            byte[] badJob = new byte[64];
            int offset = 16;
            BinaryPrimitives.WriteUInt16LittleEndian(badJob.AsSpan(offset, 2), 0); // 0-length string
            offset += 2;
            BinaryPrimitives.WriteInt32LittleEndian(badJob.AsSpan(offset + 16, 4), -1); // negative dag length
            Assert.Throws<ArgumentException>(() => JobSubmissionMessage.Deserialize(badJob));
        }

        // 4. NodeHardwareProfile with truncated buffer
        {
            Assert.Throws<ArgumentException>(() => NodeHardwareProfile.ReadFrom(ReadOnlySpan<byte>.Empty));
            Assert.Throws<ArgumentException>(() => NodeHardwareProfile.ReadFrom(new byte[63]));
        }

        // 5. BinaryStringHelper with claimed length exceeding buffer
        {
            byte[] badStr = new byte[4];
            BinaryPrimitives.WriteUInt16LittleEndian(badStr.AsSpan(0, 2), 100); // claimed length = 100, buffer has 2
            Assert.Throws<ArgumentException>(() => BinaryStringHelper.ReadUtf8String(badStr, out _));
        }
    }

    // =========================================================================
    // 5. NodeHardwareProfile.ProbeLocal() Across Thread Pools
    // =========================================================================

    [Fact]
    public async Task NodeHardwareProfile_ProbeLocal_ThreadPoolStress()
    {
        const int concurrentTasks = 50;
        var tasks = new Task<NodeHardwareProfile>[concurrentTasks];

        for (int i = 0; i < concurrentTasks; i++)
        {
            tasks[i] = Task.Run(() => NodeHardwareProfile.ProbeLocal());
        }

        var results = await Task.WhenAll(tasks);

        Assert.Equal(concurrentTasks, results.Length);
        var first = results[0];

        foreach (var profile in results)
        {
            Assert.NotEqual(Guid.Empty, profile.NodeId);
            Assert.Equal(first.CpuLogicalCores, profile.CpuLogicalCores);
            Assert.Equal(first.SimdFlags, profile.SimdFlags);
            Assert.Equal(first.GpuFlags, profile.GpuFlags);
            Assert.Equal(first.RamTotalBytes, profile.RamTotalBytes);
            Assert.True(profile.RamAvailableBytes > 0);
        }
    }

    [Fact]
    public async Task NodeHardwareProfile_ProbeLocal_ConcurrentExplicitParameters()
    {
        const int concurrentTasks = 50;
        var tasks = new Task<(Guid expectedId, uint expectedIp, NodeHardwareProfile profile)>[concurrentTasks];

        for (int i = 0; i < concurrentTasks; i++)
        {
            var id = Guid.NewGuid();
            uint ip = (uint)(0x0A000001 + i); // 10.0.0.1 + i

            tasks[i] = Task.Run(() =>
            {
                var profile = NodeHardwareProfile.ProbeLocal(nodeId: id, hostIpv4: ip);
                return (id, ip, profile);
            });
        }

        var results = await Task.WhenAll(tasks);

        foreach (var (expectedId, expectedIp, profile) in results)
        {
            Assert.Equal(expectedId, profile.NodeId);
            Assert.Equal(expectedIp, profile.HostIpv4Address);
        }
    }

    [Fact]
    public void ExtremeStrings_EmbeddedNullCharacters_Roundtrip()
    {
        string embeddedNull = "Glacier\0Cluster\0Protocol\02026";
        byte[] buffer = new byte[2048];

        var msg = new NodeLeaveMessage(Guid.NewGuid(), 1, 1000, embeddedNull);
        int written = msg.Serialize(buffer);
        var des = NodeLeaveMessage.Deserialize(buffer.AsSpan(0, written));
        Assert.Equal(embeddedNull, des.ReasonDetails);

        var task = new TaskStatusUpdateMessage(Guid.NewGuid(), 1, 0, TaskExecutionStatus.Failed, 1.0, 0, 0, embeddedNull);
        int w2 = task.Serialize(buffer);
        var desTask = TaskStatusUpdateMessage.Deserialize(buffer.AsSpan(0, w2));
        Assert.Equal(embeddedNull, desTask.ErrorMessage);
    }

    [Fact]
    public void ExtremeStrings_ExactBoundary_65534_And_65535()
    {
        string str65534 = new string('B', 65534);
        string str65535 = new string('C', 65535);
        byte[] buffer = new byte[70000];

        // 65534 bytes
        var msg1 = new NodeLeaveMessage(Guid.NewGuid(), 1, 1000, str65534);
        int w1 = msg1.Serialize(buffer);
        var des1 = NodeLeaveMessage.Deserialize(buffer.AsSpan(0, w1));
        Assert.Equal(65534, des1.ReasonDetails.Length);

        // 65535 bytes
        var msg2 = new NodeLeaveMessage(Guid.NewGuid(), 1, 1000, str65535);
        int w2 = msg2.Serialize(buffer);
        var des2 = NodeLeaveMessage.Deserialize(buffer.AsSpan(0, w2));
        Assert.Equal(65535, des2.ReasonDetails.Length);
    }

    [Fact]
    public void ClusterCrc32_Concurrency_And_BufferVariations()
    {
        int[] sizes = [0, 1, 3, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 512, 1024, 65536];

        Parallel.ForEach(sizes, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 }, size =>
        {
            byte[] data = new byte[size];
            for (int i = 0; i < size; i++) data[i] = (byte)(i & 0xFF);

            uint crc1 = ClusterCrc32.Compute(data);
            uint crc2 = ClusterCrc32.Compute(data.AsSpan());

            Assert.Equal(crc1, crc2);

            // Recompute 100 times concurrently
            for (int r = 0; r < 100; r++)
            {
                uint crcLoop = ClusterCrc32.Compute(data);
                Assert.Equal(crc1, crcLoop);
            }
        });
    }

    [Fact]
    public void NodeHardwareProfile_RepeatedProbing_NoHandleLeak()
    {
        for (int i = 0; i < 200; i++)
        {
            var profile = NodeHardwareProfile.ProbeLocal();
            Assert.True(profile.CpuLogicalCores >= 1);
            Assert.True(profile.RamTotalBytes > 0);
            Assert.True(profile.RamAvailableBytes > 0);
            Assert.True(profile.RamAvailableBytes <= profile.RamTotalBytes);
        }
    }

    [Fact]
    public void LargePayload_StageDispatch_100Upstreams_And_64KbPayload()
    {
        byte[] bigPayload = new byte[65536];
        new Random(123).NextBytes(bigPayload);

        var upstreams = new UpstreamPartitionLocation[100];
        for (int i = 0; i < 100; i++)
        {
            upstreams[i] = new UpstreamPartitionLocation(Guid.NewGuid(), 1, i, (ushort)(8000 + i), $"worker-{i}.internal");
        }

        var msg = new StageDispatchMessage(Guid.NewGuid(), 10, 5, StageOperatorType.HashJoinProbe, bigPayload, upstreams);
        int byteCount = msg.GetByteCount();

        byte[] buffer = new byte[byteCount];
        int written = msg.Serialize(buffer);
        Assert.Equal(byteCount, written);

        var deserialized = StageDispatchMessage.Deserialize(buffer.AsSpan(0, written));
        Assert.Equal(msg, deserialized);
        Assert.Equal(100, deserialized.UpstreamPartitions.Length);
        Assert.Equal(65536, deserialized.OperatorPayload.Length);
    }

    [Fact]
    public void LargePayload_JobSubmission_128KbDagPayload()
    {
        byte[] bigDag = new byte[131072];
        new Random(456).NextBytes(bigDag);

        var msg = new JobSubmissionMessage(Guid.NewGuid(), "Large-Scale-Analytics", 50, 1024, 300000, bigDag);
        int byteCount = msg.GetByteCount();

        byte[] buffer = new byte[byteCount];
        int written = msg.Serialize(buffer);
        Assert.Equal(byteCount, written);

        var deserialized = JobSubmissionMessage.Deserialize(buffer.AsSpan(0, written));
        Assert.Equal(msg, deserialized);
        Assert.Equal(131072, deserialized.DagPayload.Length);
    }

    [Fact]
    public void LargePayload_PartitionFetchResponse_256KbChunk()
    {
        byte[] bigData = new byte[262144];
        new Random(789).NextBytes(bigData);

        var msg = new PartitionFetchResponseMessage(Guid.NewGuid(), 5, 12, 1048576, PartitionFetchStatus.Success, PartitionDataFlags.ArrowIpcBatch, 262144, bigData);
        int byteCount = msg.GetByteCount();

        byte[] buffer = new byte[byteCount];
        int written = msg.Serialize(buffer);
        Assert.Equal(byteCount, written);

        var deserialized = PartitionFetchResponseMessage.Deserialize(buffer.AsSpan(0, written));
        Assert.Equal(msg, deserialized);
        Assert.Equal(262144, deserialized.Data.Length);
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private static List<IClusterMessage> CreateSampleMessages()
    {
        var profile = new NodeHardwareProfile(Guid.NewGuid(), 8, SimdArchitectureFlags.Avx2, GpuBackendFlags.None, 16000, 8000, 0, 0);

        return new List<IClusterMessage>
        {
            new HeartbeatPingMessage(Guid.NewGuid(), 1000, 2, 45.0f, 1024),
            new HeartbeatPongMessage(Guid.NewGuid(), 1001, 10, 0, 4),
            new NodeJoinRequestMessage(profile, "node-1.glacier", "secret-key"),
            new NodeJoinResponseMessage(NodeJoinStatus.Accepted, Guid.NewGuid(), 1, 4, 10, "Welcome"),
            new NodeLeaveMessage(Guid.NewGuid(), 0, 2000, "Normal termination"),
            new StageDispatchMessage(Guid.NewGuid(), 1, 0, StageOperatorType.Scan, new byte[] { 1, 2, 3, 4 }, new[] { new UpstreamPartitionLocation(Guid.NewGuid(), 0, 0, 7000, "upstream-1") }),
            new TaskStatusUpdateMessage(Guid.NewGuid(), 1, 0, TaskExecutionStatus.Completed, 45.2, 2048, 100, "OK"),
            new PartitionFetchRequestMessage(Guid.NewGuid(), 1, 0, 10, 0, 32768),
            new PartitionFetchResponseMessage(Guid.NewGuid(), 1, 0, 4, PartitionFetchStatus.Success, PartitionDataFlags.None, 0, new byte[] { 9, 8, 7, 6 }),
            new PartitionAcknowledgeMessage(Guid.NewGuid(), 1, 0, 10, 4, true),
            new RingAllReduceHandshakeMessage(Guid.NewGuid(), 0, 2, Guid.NewGuid(), Guid.NewGuid(), 8000, 8001, "node0:8000", "node1:8001"),
            new JobSubmissionMessage(Guid.NewGuid(), "Distributed-Join", 2, 8, 30000, new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }),
            new JobCompletionMessage(Guid.NewGuid(), JobCompletionStatus.Succeeded, 123.45, 1000, 8192, "Success")
        };
    }
}
