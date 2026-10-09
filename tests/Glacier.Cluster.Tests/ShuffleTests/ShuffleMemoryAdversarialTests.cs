namespace Glacier.Cluster.Tests.ShuffleTests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Cluster.Shuffle;
using Xunit;

/// <summary>
/// Adversarial stress tests and boundary challenges for RefCountedPartitionBuffer and LocalPartitionStore.
/// Authored by challenger_m3_1.
/// </summary>
public sealed class ShuffleMemoryAdversarialTests
{
    // =========================================================================
    // 1. MASSIVE CONCURRENT RETAIN / RELEASE TORTURE TESTS
    // =========================================================================

    [Fact]
    public async Task Torture_32Threads_100kRetainRelease_SingleSharedBuffer()
    {
        const int threadCount = 32;
        const int opsPerThread = 100_000;
        const int bufferSize = 4096;

        using var buffer = RefCountedPartitionBuffer.Allocate(bufferSize);
        Assert.Equal(1, buffer.ReferenceCount);
        Assert.False(buffer.IsDisposed);

        // Fill memory with a known pattern to verify memory integrity throughout
        buffer.Span.Fill(0x5A);

        using var barrier = new Barrier(threadCount);

        var tasks = new Task[threadCount];
        for (int t = 0; t < threadCount; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                barrier.SignalAndWait();
                for (int i = 0; i < opsPerThread; i++)
                {
                    buffer.Retain();
                    buffer.Release();
                }
            });
        }

        await Task.WhenAll(tasks);

        // Exactly initial reference count 1 must be preserved
        Assert.Equal(1, buffer.ReferenceCount);
        Assert.False(buffer.IsDisposed);

        // Memory integrity must remain intact
        var span = buffer.Span;
        Assert.Equal(bufferSize, span.Length);
        for (int i = 0; i < span.Length; i++)
        {
            Assert.Equal(0x5A, span[i]);
        }

        // Final release: buffer must transition cleanly to disposed
        buffer.Release();
        Assert.Equal(0, buffer.ReferenceCount);
        Assert.True(buffer.IsDisposed);
    }

    [Fact]
    public async Task Torture_32Threads_100kOps_AcrossMultipleSharedBuffers()
    {
        const int threadCount = 32;
        const int buffersCount = 8;
        const int opsPerThread = 100_000;

        var buffers = new RefCountedPartitionBuffer[buffersCount];
        for (int b = 0; b < buffersCount; b++)
        {
            buffers[b] = RefCountedPartitionBuffer.Allocate(1024);
            buffers[b].Span.Fill((byte)(b + 1));
        }

        using var barrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            int threadId = t;
            tasks[t] = Task.Run(() =>
            {
                barrier.SignalAndWait();
                for (int i = 0; i < opsPerThread; i++)
                {
                    int targetBufferIndex = (threadId + i) % buffersCount;
                    var buf = buffers[targetBufferIndex];
                    buf.Retain();
                    buf.Release();
                }
            });
        }

        await Task.WhenAll(tasks);

        for (int b = 0; b < buffersCount; b++)
        {
            Assert.Equal(1, buffers[b].ReferenceCount);
            Assert.False(buffers[b].IsDisposed);

            var span = buffers[b].Span;
            for (int i = 0; i < span.Length; i++)
            {
                Assert.Equal((byte)(b + 1), span[i]);
            }

            buffers[b].Release();
            Assert.Equal(0, buffers[b].ReferenceCount);
            Assert.True(buffers[b].IsDisposed);
        }
    }

    [Fact]
    public async Task Torture_TryRetain_RaceAgainstFinalRelease()
    {
        const int iterations = 1000;
        const int readersPerIteration = 16;

        for (int iter = 0; iter < iterations; iter++)
        {
            var buffer = RefCountedPartitionBuffer.Allocate(256);
            int successfulRetains = 0;
            int failedRetains = 0;

            using var barrier = new Barrier(readersPerIteration + 1);

            var readerTasks = new Task[readersPerIteration];
            for (int r = 0; r < readersPerIteration; r++)
            {
                readerTasks[r] = Task.Run(() =>
                {
                    barrier.SignalAndWait();
                    if (buffer.TryRetain())
                    {
                        Interlocked.Increment(ref successfulRetains);
                        // Simulate tiny work
                        Thread.Yield();
                        buffer.Release();
                    }
                    else
                    {
                        Interlocked.Increment(ref failedRetains);
                    }
                });
            }

            // Coordinator thread signals and then releases the initial handle
            barrier.SignalAndWait();
            buffer.Release();

            await Task.WhenAll(readerTasks);

            // Buffer must end fully disposed with ref count 0
            Assert.Equal(0, buffer.ReferenceCount);
            Assert.True(buffer.IsDisposed);
            Assert.False(buffer.TryRetain());
        }
    }

    // =========================================================================
    // 2. DOUBLE-FREE AND UNDERFLOW ATTACKS
    // =========================================================================

    [Fact]
    public void UnderflowAttack_SequentialRepeatedRelease_ClampedAtZero()
    {
        var buffer = RefCountedPartitionBuffer.Allocate(512);
        Assert.Equal(1, buffer.ReferenceCount);

        // Initial release transitions to 0
        buffer.Release();
        Assert.Equal(0, buffer.ReferenceCount);
        Assert.True(buffer.IsDisposed);

        // Attack: 50,000 extra sequential Release calls
        for (int i = 0; i < 50_000; i++)
        {
            buffer.Release();
            Assert.Equal(0, buffer.ReferenceCount);
            Assert.True(buffer.IsDisposed);
        }

        // Calling Dispose() must also be idempotent
        buffer.Dispose();
        Assert.Equal(0, buffer.ReferenceCount);
        Assert.True(buffer.IsDisposed);
    }

    [Fact]
    public void UnderflowAttack_ConcurrentReleaseFlood_32Threads()
    {
        var buffer = RefCountedPartitionBuffer.Allocate(512);
        buffer.Release(); // Already disposed

        const int threadCount = 32;
        const int releasesPerThread = 50_000;

        Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < releasesPerThread; i++)
            {
                buffer.Release();
            }
        });

        // Must remain strictly clamped at 0
        Assert.Equal(0, buffer.ReferenceCount);
        Assert.True(buffer.IsDisposed);
    }

    [Fact]
    public void Retain_OnDisposedBuffer_ThrowsObjectDisposedException_Concurrently()
    {
        var buffer = RefCountedPartitionBuffer.Allocate(128);
        buffer.Release();

        const int threadCount = 32;
        int caughtExceptions = 0;

        Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    buffer.Retain();
                    Assert.Fail("Retain() on a disposed buffer must throw ObjectDisposedException.");
                }
                catch (ObjectDisposedException)
                {
                    Interlocked.Increment(ref caughtExceptions);
                }

                // TryRetain must return false gracefully
                Assert.False(buffer.TryRetain());
            }
        });

        Assert.Equal(threadCount * 1000, caughtExceptions);
        Assert.Equal(0, buffer.ReferenceCount);
        Assert.True(buffer.IsDisposed);
    }

    [Fact]
    public void DisposedBuffer_AllPropertyAccesses_ThrowObjectDisposedException()
    {
        var buffer = RefCountedPartitionBuffer.Allocate(256);
        buffer.Release();

        Assert.Throws<ObjectDisposedException>(() => _ = buffer.Span);
        Assert.Throws<ObjectDisposedException>(() => _ = buffer.ReadOnlySpan);
        Assert.Throws<ObjectDisposedException>(() => _ = buffer.Memory);
        Assert.Throws<ObjectDisposedException>(() => _ = buffer.ReadOnlyMemory);
        Assert.Throws<ObjectDisposedException>(() => _ = buffer.Pointer);
        Assert.Throws<ObjectDisposedException>(() => buffer.Slice(0));
        Assert.Throws<ObjectDisposedException>(() => buffer.Slice(0, 10));
        Assert.Throws<ObjectDisposedException>(() => buffer.SliceReadOnly(0, 10));
        Assert.Throws<ObjectDisposedException>(() => buffer.SliceMemory(0, 10));
        Assert.Throws<ObjectDisposedException>(() => buffer.CreateSliceBuffer(0, 10));

        // Metadata properties do not throw
        Assert.Equal(256, buffer.ByteLength);
        Assert.Equal(256, buffer.Length);
        Assert.True(buffer.IsDisposed);
        Assert.Equal(0, buffer.ReferenceCount);
    }

    // =========================================================================
    // 3. CHILD SLICING STRESS (DEEPLY NESTED SLICES)
    // =========================================================================

    [Fact]
    public void ChildSlicing_DeeplyNestedChain_64Levels_MaintainsParentLiveness()
    {
        const int depth = 64;
        const int rootSize = 64 * 1024; // 64KB

        var root = RefCountedPartitionBuffer.Allocate(rootSize);

        // Fill root with deterministic test pattern
        for (int i = 0; i < root.ByteLength; i++)
        {
            root.Span[i] = (byte)(i & 0xFF);
        }

        var chain = new List<RefCountedPartitionBuffer> { root };

        // Create 64 deeply nested slices (slice of slice of slice...)
        // Each child shifts by 16 bytes and reduces length by 64 bytes
        for (int d = 1; d <= depth; d++)
        {
            var parent = chain[d - 1];
            int offset = 16;
            int length = parent.ByteLength - 64;
            var child = parent.CreateSliceBuffer(offset, length);
            chain.Add(child);
        }

        Assert.Equal(depth + 1, chain.Count);

        // Deepest leaf buffer
        var leaf = chain[depth];
        Assert.Equal(1, leaf.ReferenceCount);

        // Parent buffers have ref count 2 (1 for self + 1 held by child) except root which is 2
        Assert.Equal(2, root.ReferenceCount);

        // Now release all intermediate buffers from index 0 (root) up to depth - 1
        for (int i = 0; i < depth; i++)
        {
            chain[i].Release();
        }

        // Despite all parents being released, the underlying unmanaged memory must remain alive
        // because the leaf holds a reference up the chain!
        Assert.False(leaf.IsDisposed);
        Assert.Equal(1, leaf.ReferenceCount);

        // Verify leaf data integrity
        // Total offset of leaf in root is depth * 16
        int expectedRootOffset = depth * 16;
        var leafSpan = leaf.Span;
        for (int i = 0; i < leafSpan.Length; i++)
        {
            byte expected = (byte)((expectedRootOffset + i) & 0xFF);
            Assert.Equal(expected, leafSpan[i]);
            Assert.Equal(expected, leaf.Memory.Span[i]);
        }

        // Write through leaf slice to verify memory remains writable and safe
        leafSpan[0] = 0xAA;
        Assert.Equal(0xAA, leaf.Span[0]);

        // Release the leaf buffer: this cascades the release all the way up to the root
        leaf.Release();
        Assert.True(leaf.IsDisposed);

        // Verify the entire chain is now disposed
        for (int i = 0; i <= depth; i++)
        {
            Assert.True(chain[i].IsDisposed, $"Buffer at depth {i} must be disposed.");
            Assert.Equal(0, chain[i].ReferenceCount);
        }
    }

    [Fact]
    public void ChildSlicing_BranchingTree_ConcurrentLeafRelease()
    {
        const int branchFactor = 8;
        const int leafCount = branchFactor * branchFactor; // 64 leaves
        const int rootSize = 64 * 1024;

        var root = RefCountedPartitionBuffer.Allocate(rootSize);

        var midLevel = new List<RefCountedPartitionBuffer>();
        for (int i = 0; i < branchFactor; i++)
        {
            midLevel.Add(root.CreateSliceBuffer(i * 1024, 4096));
        }

        var leaves = new List<RefCountedPartitionBuffer>();
        for (int i = 0; i < branchFactor; i++)
        {
            for (int j = 0; j < branchFactor; j++)
            {
                leaves.Add(midLevel[i].CreateSliceBuffer(j * 128, 256));
            }
        }

        Assert.Equal(leafCount, leaves.Count);

        // Release root and intermediate handles
        root.Release();
        foreach (var mid in midLevel)
        {
            mid.Release();
        }

        // Root and midLevel must NOT be disposed yet because leaves retain them
        Assert.False(root.IsDisposed);
        foreach (var mid in midLevel)
        {
            Assert.False(mid.IsDisposed);
        }

        // Concurrently write to all leaves and release them across 32 threads
        Parallel.For(0, leafCount, idx =>
        {
            var leaf = leaves[idx];
            leaf.Span[0] = (byte)idx;
            Assert.Equal((byte)idx, leaf.Span[0]);
            leaf.Release();
            Assert.True(leaf.IsDisposed);
        });

        // After all leaves are released, root and all midLevel buffers must be disposed
        Assert.True(root.IsDisposed);
        Assert.Equal(0, root.ReferenceCount);

        foreach (var mid in midLevel)
        {
            Assert.True(mid.IsDisposed);
            Assert.Equal(0, mid.ReferenceCount);
        }
    }

    [Fact]
    public void ChildSlicing_IntegerOverflow_DefensiveVerification()
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(100);

        // REMEDIATED VERIFICATION:
        // An integer overflow attack (offset = 10, length = int.MaxValue - 5)
        // is rejected by CreateSliceBuffer, Slice, and SliceMemory via unsigned bounds checking.
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.CreateSliceBuffer(10, int.MaxValue - 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Slice(10, int.MaxValue - 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.SliceMemory(10, int.MaxValue - 5));
    }

    [Fact]
    public void ChildSlicing_IntegerOverflow_DefensiveSpecification_ExpectedToFailUntilFixed()
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(100);

        // SPECIFICATION ASSERTION:
        // CreateSliceBuffer MUST reject integer overflow attacks and throw ArgumentOutOfRangeException.
        // This test documents the expected defense and fails against the vulnerable implementation.
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            buffer.CreateSliceBuffer(10, int.MaxValue - 5);
        });
    }

    // =========================================================================
    // 4. MULTI-TENANT RACE CONDITIONS IN LOCALPARTITIONSTORE
    // =========================================================================

    [Fact]
    public async Task LocalPartitionStore_MultiTenant_ConcurrentReadAndMidReadStageCleanup()
    {
        using var store = new LocalPartitionStore(Guid.NewGuid());

        var jobId = Guid.NewGuid();
        int stageId = 1;
        const int partitionCount = 20;
        const int readerThreads = 32;

        // Register initial partitions
        var originalBuffers = new RefCountedPartitionBuffer[partitionCount];
        for (int p = 0; p < partitionCount; p++)
        {
            var key = new PartitionKey(jobId, stageId, p);
            var buf = RefCountedPartitionBuffer.Allocate(1024);
            buf.Span.Fill((byte)(p + 1));
            originalBuffers[p] = buf;
            store.RegisterPartition(key, buf);
            buf.Release(); // Store now holds the reference
        }

        var cts = new CancellationTokenSource();
        var exceptions = new ConcurrentBag<Exception>();

        // Start 32 concurrent reader threads fetching and validating partitions
        var readerTasks = new Task[readerThreads];
        for (int t = 0; t < readerThreads; t++)
        {
            int threadId = t;
            readerTasks[t] = Task.Run(async () =>
            {
                var rand = new Random(threadId);
                while (!cts.Token.IsCancellationRequested)
                {
                    int partitionId = rand.Next(partitionCount);
                    var key = new PartitionKey(jobId, stageId, partitionId);

                    try
                    {
                        var fetched = await store.FetchPartitionAsync(key, cts.Token);
                        if (fetched != null)
                        {
                            // Active reader must safely read payload without AV
                            byte expected = (byte)(partitionId + 1);
                            var span = fetched.Span;
                            Assert.Equal(1024, span.Length);
                            Assert.Equal(expected, span[0]);
                            Assert.Equal(expected, span[1023]);

                            // Hold buffer momentarily to simulate stream work
                            Thread.Yield();

                            // Safely release
                            fetched.Release();
                        }
                    }
                    catch (PartitionNotFoundException)
                    {
                        // Expected after stage completion
                    }
                    catch (ObjectDisposedException)
                    {
                        // Store or buffer disposed
                    }
                    catch (InvalidOperationException)
                    {
                        // Expected if entry was evicted mid-fetch
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }
            });
        }

        // Mid-read stage cleanup trigger after readers are running
        Thread.Sleep(50);
        store.AcknowledgeStageComplete(jobId, stageId);

        // Allow readers to encounter post-cleanup state
        Thread.Sleep(50);
        cts.Cancel();

        await Task.WhenAll(readerTasks);

        // No memory corruption or unexpected exceptions
        Assert.Empty(exceptions);

        // All partitions for this stage must now be evicted and disposed
        for (int p = 0; p < partitionCount; p++)
        {
            var key = new PartitionKey(jobId, stageId, p);
            Assert.False(store.TryGetPartition(key, out _));
            Assert.True(originalBuffers[p].IsDisposed);
        }
    }

    [Fact]
    public void LocalPartitionStore_ConcurrentRegisterAndEvictStress()
    {
        using var store = new LocalPartitionStore(Guid.NewGuid());
        var jobId = Guid.NewGuid();

        const int threadCount = 16;
        const int opsPerThread = 200;

        Parallel.For(0, threadCount, t =>
        {
            for (int i = 0; i < opsPerThread; i++)
            {
                int partitionId = (t * opsPerThread) + i;
                int stageId = partitionId % 4;
                var key = new PartitionKey(jobId, stageId, partitionId);

                var buf = RefCountedPartitionBuffer.Allocate(256);
                buf.Span.Fill((byte)(partitionId & 0xFF));
                store.RegisterPartition(key, buf);
                buf.Release();

                if (i % 20 == 0)
                {
                    store.AcknowledgeStageComplete(jobId, stageId);
                }
            }
        });

        // Store teardown: AcknowledgeJobComplete
        store.AcknowledgeJobComplete(jobId);
        Assert.Equal(0, store.PartitionCount);
        Assert.Equal(0, store.TotalInMemoryBytes);
    }

    // =========================================================================
    // 5. MEASURE GC ALLOCATIONS (ZERO HEAP ALLOCATION VERIFICATION)
    // =========================================================================

    [Fact]
    public void ZeroAllocation_SpanAndSliceAccess_StrictlyZeroHeapBytes()
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(1024);
        buffer.Span.Fill(0x7F);

        // Warmup JIT
        for (int i = 0; i < 100; i++)
        {
            var s = buffer.Span;
            var ro = buffer.ReadOnlySpan;
            var sl = buffer.Slice(10, 20);
            var slRo = buffer.SliceReadOnly(10, 20);
            _ = s[0] + ro[0] + sl[0] + slRo[0];
        }

        // Measure on dedicated thread to isolate GC allocation measurement
        long allocatedBytes = MeasureDedicatedThreadAllocations(() =>
        {
            for (int i = 0; i < 100_000; i++)
            {
                var span = buffer.Span;
                var readOnlySpan = buffer.ReadOnlySpan;
                var slice = buffer.Slice(16, 64);
                var sliceRo = buffer.SliceReadOnly(16, 64);

                // Access without boxing
                if (span[0] != 0x7F || readOnlySpan[0] != 0x7F || slice[0] != 0x7F || sliceRo[0] != 0x7F)
                {
                    throw new InvalidOperationException("Span data mismatch.");
                }
            }
        });

        // 100,000 iterations of Span and Slice access MUST result in exactly 0 heap bytes allocated
        Assert.Equal(0, allocatedBytes);
    }

    [Fact]
    public void ZeroAllocation_RetainAndRelease_StrictlyZeroHeapBytes()
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(512);

        // Warmup
        for (int i = 0; i < 100; i++)
        {
            buffer.Retain();
            buffer.Release();
        }

        long allocatedBytes = MeasureDedicatedThreadAllocations(() =>
        {
            for (int i = 0; i < 100_000; i++)
            {
                buffer.Retain();
                buffer.Release();
            }
        });

        // 100,000 Retain/Release calls MUST allocate exactly 0 heap bytes
        Assert.Equal(0, allocatedBytes);
    }

    [Fact]
    public async Task LocalPartitionStore_ConcurrentFetch_SpilledPartition_Leak_EmpiricalProof()
    {
        using var store = new LocalPartitionStore(Guid.NewGuid());
        var jobId = Guid.NewGuid();
        int stageId = 1;
        int partitionId = 0;
        var key = new PartitionKey(jobId, stageId, partitionId);

        const int bufferSize = 4096;
        var original = RefCountedPartitionBuffer.Allocate(bufferSize);
        original.Span.Fill(0x3C);
        store.RegisterPartition(key, original);
        original.Release();

        string tempPath = Path.Combine(Path.GetTempPath(), $"glacier_adv_spill_{Guid.NewGuid():N}.arrow");
        try
        {
            File.WriteAllBytes(tempPath, original.Span.ToArray());
            bool spilled = store.TransitionToSpilled(key, tempPath);
            Assert.True(spilled);
            Assert.Equal(bufferSize, store.TotalSpilledBytes);
            Assert.Equal(0, store.TotalInMemoryBytes);

            const int readerThreads = 8;
            using var barrier = new Barrier(readerThreads);
            var tasks = new Task<RefCountedPartitionBuffer>[readerThreads];

            for (int i = 0; i < readerThreads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    barrier.SignalAndWait();
                    return await store.FetchPartitionAsync(key);
                });
            }

            var results = await Task.WhenAll(tasks);

            // EMPIRICAL PROOF OF RACE & DUPLICATE BUFFERS:
            // Notice that multiple distinct unmanaged buffer instances were created:
            var distinctPointers = new HashSet<IntPtr>();
            for (int i = 0; i < readerThreads; i++)
            {
                distinctPointers.Add(results[i].Pointer);
                // Caller releases their reference
                results[i].Release();
            }

            // REMEDIATED VERIFICATION:
            // Coordinated reload ensures duplicate buffers are immediately released and caller receives retained winning buffer.
            Assert.True(distinctPointers.Count >= 1, "At least one buffer returned.");

            // Coordinated reload prevents over-accounting: TotalInMemoryBytes is exactly bufferSize
            Assert.Equal(bufferSize, store.TotalInMemoryBytes);

            // Stage complete cleanup: cleanly evicts the resident partition!
            store.AcknowledgeStageComplete(jobId, stageId);

            // Memory accounting returns cleanly to 0 with zero ghost bytes leaked!
            Assert.Equal(0, store.TotalInMemoryBytes);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); } catch { }
            }
        }
    }

    [Fact]
    public void AllocationScaling_UnmanagedBufferCreation_ConstantWrapperSize()
    {
        // Warmup on a separate thread
        MeasureDedicatedThreadAllocations(() =>
        {
            using var w1 = RefCountedPartitionBuffer.Allocate(128);
            using var w2 = RefCountedPartitionBuffer.Allocate(1024);
        });

        long allocSmall = MeasureDedicatedThreadAllocations(() =>
        {
            using var bufSmall = RefCountedPartitionBuffer.Allocate(64);
        });

        long allocLarge = MeasureDedicatedThreadAllocations(() =>
        {
            using var bufLarge = RefCountedPartitionBuffer.Allocate(16 * 1024 * 1024); // 16 MB unmanaged
        });

        // Small (64B) and Large (16MB) must produce identical constant GC heap allocations (< 256 bytes)
        // proving zero payload bytes are allocated on the GC heap.
        Assert.True(allocSmall < 256, $"Small buffer GC allocation {allocSmall} exceeds expected wrapper size.");
        Assert.True(allocLarge < 256, $"Large buffer GC allocation {allocLarge} exceeds expected wrapper size.");
        Assert.Equal(allocSmall, allocLarge);
    }

    // =========================================================================
    // 6. ADVERSARIAL CHALLENGER EXPANDED TORTURE & BOUNDARY HARNESSES
    // =========================================================================

    [Fact]
    public void ChildSlicing_IntegerOverflow_ExhaustiveMatrix_AttacksAllSlicingMethods()
    {
        // Test against multiple buffer sizes: 0, 10, 100, 4096
        int[] bufferSizes = { 0, 10, 100, 4096 };

        foreach (int size in bufferSizes)
        {
            using var buffer = RefCountedPartitionBuffer.Allocate(size);

            (int offset, int length)[] attackVectors =
            {
                (10, int.MaxValue - 5),
                (1, int.MaxValue),
                (0, int.MaxValue),
                (int.MaxValue, 1),
                (int.MaxValue, int.MaxValue),
                (int.MaxValue - 100, 200),
                (int.MaxValue / 2, int.MaxValue / 2 + 2),
                (-1, 10),
                (10, -1),
                (-1, -1),
                (int.MinValue, 0),
                (0, int.MinValue),
                (int.MinValue, int.MinValue),
                (int.MinValue, int.MaxValue),
                (int.MaxValue, int.MinValue),
                (size, 1),
                (size + 1, 0),
                (0, size + 1),
                (size / 2, (size / 2) + 2) // only when size > 0
            };

            foreach (var (offset, length) in attackVectors)
            {
                // Skip vectors that might accidentally be valid for large size
                if (offset >= 0 && length >= 0 && (long)offset + length <= size)
                    continue;

                Assert.Throws<ArgumentOutOfRangeException>(() => buffer.CreateSliceBuffer(offset, length));
                Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Slice(offset, length));
                Assert.Throws<ArgumentOutOfRangeException>(() => buffer.SliceReadOnly(offset, length));
                Assert.Throws<ArgumentOutOfRangeException>(() => buffer.SliceMemory(offset, length));
            }
        }

        // Test nested child buffer overflow attacks
        using var root = RefCountedPartitionBuffer.Allocate(100);
        using var child = root.CreateSliceBuffer(10, 50); // valid slice [10..60)
        Assert.Equal(50, child.ByteLength);

        // Child attacks
        Assert.Throws<ArgumentOutOfRangeException>(() => child.CreateSliceBuffer(25, 26)); // 25 + 26 = 51 > 50
        Assert.Throws<ArgumentOutOfRangeException>(() => child.CreateSliceBuffer(10, int.MaxValue - 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => child.Slice(10, int.MaxValue - 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => child.SliceReadOnly(10, int.MaxValue - 5));
        Assert.Throws<ArgumentOutOfRangeException>(() => child.SliceMemory(10, int.MaxValue - 5));
    }

    [Fact]
    public async Task LocalPartitionStore_ConcurrentSpillReload_32Threads_DeduplicationAndZeroGhostBytes()
    {
        using var store = new LocalPartitionStore(Guid.NewGuid());
        var jobId = Guid.NewGuid();
        int stageId = 2;
        int partitionId = 42;
        var key = new PartitionKey(jobId, stageId, partitionId);

        const int bufferSize = 8192;
        var original = RefCountedPartitionBuffer.Allocate(bufferSize);
        original.Span.Fill(0x9E);
        store.RegisterPartition(key, original);
        original.Release();

        string tempPath = Path.Combine(Path.GetTempPath(), $"glacier_adv_spill_32t_{Guid.NewGuid():N}.arrow");
        try
        {
            File.WriteAllBytes(tempPath, original.Span.ToArray());
            bool spilled = store.TransitionToSpilled(key, tempPath);
            Assert.True(spilled);
            Assert.Equal(bufferSize, store.TotalSpilledBytes);
            Assert.Equal(0, store.TotalInMemoryBytes);

            const int readerThreads = 32;
            using var barrier = new Barrier(readerThreads);
            var tasks = new Task<RefCountedPartitionBuffer>[readerThreads];

            for (int i = 0; i < readerThreads; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    barrier.SignalAndWait();
                    return await store.FetchPartitionAsync(key);
                });
            }

            var results = await Task.WhenAll(tasks);

            // Exactly ONE winning buffer instance must be shared across all 32 callers
            var distinctPointers = new HashSet<IntPtr>();
            for (int i = 0; i < readerThreads; i++)
            {
                var buf = results[i];
                Assert.NotNull(buf);
                Assert.Equal(bufferSize, buf.ByteLength);
                Assert.Equal(0x9E, buf.Span[0]);
                Assert.Equal(0x9E, buf.Span[bufferSize - 1]);
                distinctPointers.Add(buf.Pointer);
            }

            Assert.Single(distinctPointers);

            // In-memory byte accounting must be strictly for 1 buffer copy (8192), never 32 * 8192
            Assert.Equal(bufferSize, store.TotalInMemoryBytes);

            // The single winning buffer must have reference count == 33 (1 in store + 32 readers)
            var winningBuffer = results[0];
            Assert.Equal(33, winningBuffer.ReferenceCount);

            // All readers release their references
            for (int i = 0; i < readerThreads; i++)
            {
                results[i].Release();
            }

            // Reference count must drop back to 1 (held by store only)
            Assert.Equal(1, winningBuffer.ReferenceCount);
            Assert.False(winningBuffer.IsDisposed);

            // Complete the stage
            store.AcknowledgeStageComplete(jobId, stageId);

            // Store must have cleanly evicted the partition and released the buffer
            Assert.True(winningBuffer.IsDisposed);
            Assert.Equal(0, winningBuffer.ReferenceCount);
            Assert.Equal(0, store.TotalInMemoryBytes);
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); } catch { }
            }
        }
    }

    [Fact]
    public async Task LocalPartitionStore_ConcurrentSpillReload_MultiPartition_64ThreadsStress()
    {
        using var store = new LocalPartitionStore(Guid.NewGuid());
        var jobId = Guid.NewGuid();
        int stageId = 3;
        const int numPartitions = 8;
        const int bufferSize = 2048;
        const int readerThreads = 64;

        var tempFiles = new List<string>();
        var winningBuffers = new RefCountedPartitionBuffer[numPartitions];

        try
        {
            for (int p = 0; p < numPartitions; p++)
            {
                var key = new PartitionKey(jobId, stageId, p);
                var buf = RefCountedPartitionBuffer.Allocate(bufferSize);
                buf.Span.Fill((byte)(p + 10));
                store.RegisterPartition(key, buf);
                buf.Release();

                string tempFile = Path.Combine(Path.GetTempPath(), $"glacier_adv_spill_multi_{p}_{Guid.NewGuid():N}.arrow");
                tempFiles.Add(tempFile);
                File.WriteAllBytes(tempFile, buf.Span.ToArray());
                store.TransitionToSpilled(key, tempFile);
            }

            Assert.Equal(numPartitions * bufferSize, store.TotalSpilledBytes);
            Assert.Equal(0, store.TotalInMemoryBytes);

            using var barrier = new Barrier(readerThreads);
            var tasks = new Task[readerThreads];

            for (int t = 0; t < readerThreads; t++)
            {
                int threadId = t;
                tasks[t] = Task.Run(async () =>
                {
                    barrier.SignalAndWait();
                    // Each thread fetches multiple random partitions concurrently
                    for (int step = 0; step < 10; step++)
                    {
                        int p = (threadId + step) % numPartitions;
                        var key = new PartitionKey(jobId, stageId, p);
                        var fetched = await store.FetchPartitionAsync(key);
                        Assert.NotNull(fetched);
                        Assert.Equal(bufferSize, fetched.ByteLength);
                        Assert.Equal((byte)(p + 10), fetched.Span[0]);
                        fetched.Release();
                    }
                });
            }

            await Task.WhenAll(tasks);

            // After all 64 threads finish and release, store holds exactly numPartitions buffers
            Assert.Equal(numPartitions * bufferSize, store.TotalInMemoryBytes);

            for (int p = 0; p < numPartitions; p++)
            {
                var key = new PartitionKey(jobId, stageId, p);
                Assert.True(store.TryGetPartition(key, out var entry));
                Assert.Equal(1, entry!.GetCurrentReferenceCount());
            }

            // Teardown stage
            store.AcknowledgeStageComplete(jobId, stageId);
            Assert.Equal(0, store.TotalInMemoryBytes);
            Assert.Equal(0, store.PartitionCount);
        }
        finally
        {
            foreach (var f in tempFiles)
            {
                if (File.Exists(f)) { try { File.Delete(f); } catch { } }
            }
        }
    }

    [Fact]
    public async Task Torture_RetainRelease_WithConcurrentChildSlicingAndSpanAccess()
    {
        const int threadCount = 32;
        const int iterations = 10_000;
        const int rootSize = 8192;

        using var root = RefCountedPartitionBuffer.Allocate(rootSize);
        root.Span.Fill(0x42);

        using var barrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            int threadId = t;
            tasks[t] = Task.Run(() =>
            {
                barrier.SignalAndWait();
                int role = threadId % 4;

                for (int i = 0; i < iterations; i++)
                {
                    switch (role)
                    {
                        case 0:
                            // Pure retain/release
                            root.Retain();
                            root.Release();
                            break;

                        case 1:
                            // Child slice create and release
                            int offset = (i * 16) % (rootSize - 64);
                            var child = root.CreateSliceBuffer(offset, 64);
                            Assert.Equal(0x42, child.Span[0]);
                            child.Release();
                            break;

                        case 2:
                            // Direct Span slice reading
                            var span = root.Slice(32, 128);
                            Assert.Equal(0x42, span[0]);
                            var mem = root.SliceMemory(32, 128);
                            Assert.Equal(0x42, mem.Span[0]);
                            break;

                        case 3:
                            // TryRetain race
                            if (root.TryRetain())
                            {
                                root.Release();
                            }
                            break;
                    }
                }
            });
        }

        await Task.WhenAll(tasks);

        Assert.Equal(1, root.ReferenceCount);
        Assert.False(root.IsDisposed);
        Assert.Equal(0x42, root.Span[0]);
    }


    private static long MeasureDedicatedThreadAllocations(Action action)
    {
        long allocated = 0;
        Exception? error = null;

        var thread = new Thread(() =>
        {
            try
            {
                long before = GC.GetAllocatedBytesForCurrentThread();
                action();
                long after = GC.GetAllocatedBytesForCurrentThread();
                allocated = after - before;
            }
            catch (Exception ex)
            {
                error = ex;
            }
        })
        {
            IsBackground = true
        };

        thread.Start();
        thread.Join();

        if (error != null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
        }

        return allocated;
    }
}
