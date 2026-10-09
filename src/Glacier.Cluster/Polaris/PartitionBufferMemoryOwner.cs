namespace Glacier.Cluster.Polaris;

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Glacier.Cluster.Shuffle;
using Glacier.Polaris.Memory;

/// <summary>
/// Zero-allocation <see cref="IMemoryOwner{T}"/> wrapping a slice of a <see cref="RefCountedPartitionBuffer"/>.
/// Holds an active reference on the parent partition buffer, ensuring underlying unmanaged memory remains
/// valid until all referencing columns are disposed.
/// </summary>
public sealed unsafe class PartitionBufferMemoryOwner<T> : IMemoryOwner<T> where T : unmanaged
{
    private readonly RefCountedPartitionBuffer _buffer;
    private readonly void* _slicePointer;
    private readonly int _elementCount;
    private int _disposed;

    public PartitionBufferMemoryOwner(RefCountedPartitionBuffer buffer, ReadOnlyMemory<byte> sliceMemory)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        _buffer = buffer;
        _buffer.Retain(); // Increment atomic reference count

        int elemSize = sizeof(T);
        _elementCount = sliceMemory.Length / elemSize;

        if (sliceMemory.IsEmpty || buffer.ByteLength == 0)
        {
            _slicePointer = null;
        }
        else
        {
            _slicePointer = Unsafe.AsPointer(ref MemoryMarshal.GetReference(sliceMemory.Span));
        }
    }

    public Memory<T> Memory
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_elementCount == 0 || _slicePointer == null)
                return Memory<T>.Empty;

            return new UnmanagedMemoryManager<T>((T*)_slicePointer, _elementCount).Memory;
        }
    }

    public Span<T> Span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_elementCount == 0 || _slicePointer == null)
                return Span<T>.Empty;

            return new Span<T>(_slicePointer, _elementCount);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _buffer.Release(); // Decrement atomic reference count
        }
    }
}
