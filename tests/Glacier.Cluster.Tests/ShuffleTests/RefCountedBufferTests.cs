namespace Glacier.Cluster.Tests.ShuffleTests;

using System;
using System.Threading.Tasks;
using Glacier.Cluster.Shuffle;
using Xunit;

public sealed unsafe class RefCountedBufferTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(4096)]
    [InlineData(1024 * 1024)]
    public void AllocationAndAlignment_64ByteBoundary(int size)
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(size);

        Assert.NotNull(buffer);
        Assert.Equal(size, buffer.ByteLength);
        Assert.Equal(size, buffer.Length);
        Assert.False(buffer.IsDisposed);
        Assert.Equal(1, buffer.ReferenceCount);

        // Verify 64-byte natural cache-line alignment
        ulong address = (ulong)buffer.Pointer;
        Assert.True(address % 64 == 0, $"Pointer 0x{address:X} must be aligned to 64-byte boundary.");

        // Verify memory is zero-initialized
        var span = buffer.Span;
        Assert.Equal(size, span.Length);
        for (int i = 0; i < Math.Min(size, 256); i++)
        {
            Assert.Equal(0, span[i]);
        }
    }

    [Fact]
    public void InitialState_RefCountIsOne()
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(256);
        Assert.Equal(1, buffer.ReferenceCount);
        Assert.False(buffer.IsDisposed);
    }

    [Fact]
    public void RetainAndRelease_UpdatesReferenceCount()
    {
        var buffer = RefCountedPartitionBuffer.Allocate(128);

        buffer.Retain();
        Assert.Equal(2, buffer.ReferenceCount);

        buffer.Retain();
        Assert.Equal(3, buffer.ReferenceCount);

        buffer.Release();
        Assert.Equal(2, buffer.ReferenceCount);

        buffer.Release();
        Assert.Equal(1, buffer.ReferenceCount);

        buffer.Release();
        Assert.Equal(0, buffer.ReferenceCount);
        Assert.True(buffer.IsDisposed);
    }

    [Fact]
    public void DisposedBuffer_ThrowsObjectDisposedException()
    {
        var buffer = RefCountedPartitionBuffer.Allocate(64);
        buffer.Release();

        Assert.True(buffer.IsDisposed);
        Assert.Equal(0, buffer.ReferenceCount);

        Assert.Throws<ObjectDisposedException>(() => buffer.Retain());
        Assert.Throws<ObjectDisposedException>(() => _ = buffer.Span);
        Assert.Throws<ObjectDisposedException>(() => _ = buffer.Memory);
        Assert.Throws<ObjectDisposedException>(() => _ = buffer.Pointer);
        Assert.Throws<ObjectDisposedException>(() => _ = buffer.UnmanagedPointer);
        Assert.Throws<ObjectDisposedException>(() => buffer.Slice(0, 10));

        // TryRetain returns false gracefully without throwing
        Assert.False(buffer.TryRetain());
    }

    [Fact]
    public void DoubleFree_SafeAndIdempotent()
    {
        var buffer = RefCountedPartitionBuffer.Allocate(128);
        buffer.Release();

        // Repeated release calls must not crash or trigger double-free faults
        for (int i = 0; i < 50; i++)
        {
            buffer.Release();
            buffer.Dispose();
        }

        Assert.Equal(0, buffer.ReferenceCount);
        Assert.True(buffer.IsDisposed);
    }

    [Fact]
    public void ChildSliceBuffer_MaintainsParentLiveness()
    {
        var parent = RefCountedPartitionBuffer.Allocate(1024);

        // Fill parent with known deterministic pattern
        for (int i = 0; i < parent.ByteLength; i++)
        {
            parent.Span[i] = (byte)(i % 251);
        }

        var child = parent.CreateSliceBuffer(100, 200);

        Assert.Equal(200, child.ByteLength);
        Assert.Equal(1, child.ReferenceCount);
        Assert.Equal(2, parent.ReferenceCount); // Parent retained by child

        // Verify child slice content
        for (int i = 0; i < child.ByteLength; i++)
        {
            Assert.Equal((byte)((100 + i) % 251), child.Span[i]);
            Assert.Equal((byte)((100 + i) % 251), child.Memory.Span[i]);
        }

        // Release parent handle; underlying memory must remain alive because child holds reference
        parent.Release();
        Assert.Equal(1, parent.ReferenceCount);
        Assert.False(parent.IsDisposed);

        // Child can still read and write memory safely
        child.Span[0] = 0xFF;
        Assert.Equal(0xFF, child.Span[0]);

        // Release child handle; this cascades to parent release and frees unmanaged memory
        child.Release();
        Assert.True(child.IsDisposed);
        Assert.True(parent.IsDisposed);
    }

    [Fact]
    public void Slicing_BoundsChecking()
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(100);

        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Slice(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Slice(101));
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Slice(0, 101));
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Slice(50, 51));
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.CreateSliceBuffer(-1, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.CreateSliceBuffer(50, 60));

        var validSlice = buffer.Slice(10, 20);
        Assert.Equal(20, validSlice.Length);

        var validMemory = buffer.SliceMemory(10, 20);
        Assert.Equal(20, validMemory.Length);
    }

    [Fact]
    public void SpanAndMemory_ReadWriteIntegrity()
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(256);

        var span = buffer.Span;
        for (int i = 0; i < span.Length; i++)
        {
            span[i] = (byte)i;
        }

        var memory = buffer.Memory;
        for (int i = 0; i < memory.Length; i++)
        {
            Assert.Equal((byte)i, memory.Span[i]);
        }

        var copy = RefCountedPartitionBuffer.CreateCopy(span);
        Assert.Equal(buffer.ByteLength, copy.ByteLength);
        Assert.True(copy.Span.SequenceEqual(span));
        copy.Release();
    }

    [Fact]
    public void EmptyBuffer_ZeroLengthHandledGracefully()
    {
        using var empty = RefCountedPartitionBuffer.Allocate(0);
        Assert.Equal(0, empty.ByteLength);
        Assert.True(empty.Span.IsEmpty);
        Assert.True(empty.Memory.IsEmpty);
        Assert.False(empty.IsDisposed);
    }

    [Fact]
    public void ConcurrentRetainRelease_StressTest()
    {
        using var buffer = RefCountedPartitionBuffer.Allocate(512);

        const int threadCount = 16;
        const int iterationsPerThread = 2000;

        Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < iterationsPerThread; i++)
            {
                buffer.Retain();
                buffer.Release();
            }
        });

        Assert.Equal(1, buffer.ReferenceCount);
        Assert.False(buffer.IsDisposed);
    }
}
