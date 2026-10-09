namespace Glacier.Cluster.Shuffle;

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Glacier.Polaris.Memory;

/// <summary>
/// High-performance unmanaged partition memory buffer with 64-byte cache alignment
/// and atomic, thread-safe reference counting. Implements <see cref="IMemoryOwner{T}"/>
/// for zero-allocation System.IO.Pipelines and Arrow IPC integration.
/// </summary>
public sealed unsafe class RefCountedPartitionBuffer : IMemoryOwner<byte>, IDisposable
{
    private const int CacheLineAlignment = 64;

    private readonly NativeMemoryOwner<byte>? _underlyingOwner;
    private void* _rawPointer;
    private readonly int _byteLength;
    private readonly bool _ownsMemory;
    private readonly RefCountedPartitionBuffer? _parentBuffer;
    private readonly int _sliceOffset;

    private int _referenceCount;
    private int _disposed;

    /// <summary>
    /// Allocates unmanaged memory aligned to 64 bytes and cleared to zero.
    /// </summary>
    public RefCountedPartitionBuffer(int byteLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(byteLength);
        _byteLength = byteLength;
        _ownsMemory = true;
        _parentBuffer = null;
        _sliceOffset = 0;
        _referenceCount = 1;
        _disposed = 0;

        if (byteLength == 0)
        {
            _rawPointer = null;
            _underlyingOwner = null;
        }
        else
        {
            _underlyingOwner = new NativeMemoryOwner<byte>(byteLength);
            _rawPointer = _underlyingOwner.UnmanagedPointer;
        }
    }

    /// <summary>
    /// Wraps an existing <see cref="NativeMemoryOwner{T}"/> instance.
    /// </summary>
    public RefCountedPartitionBuffer(NativeMemoryOwner<byte> owner, int byteLength = -1)
    {
        ArgumentNullException.ThrowIfNull(owner);
        _underlyingOwner = owner;
        _byteLength = byteLength < 0 ? owner.Length : byteLength;
        ArgumentOutOfRangeException.ThrowIfGreaterThan(_byteLength, owner.Length, nameof(byteLength));

        _rawPointer = owner.UnmanagedPointer;
        _ownsMemory = true;
        _parentBuffer = null;
        _sliceOffset = 0;
        _referenceCount = 1;
        _disposed = 0;
    }

    /// <summary>
    /// Wraps an existing raw unmanaged pointer.
    /// </summary>
    public RefCountedPartitionBuffer(void* pointer, int byteLength, bool ownsMemory = true)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(byteLength);
        if (byteLength > 0 && pointer == null)
            throw new ArgumentNullException(nameof(pointer));

        _rawPointer = pointer;
        _byteLength = byteLength;
        _ownsMemory = ownsMemory;
        _underlyingOwner = null;
        _parentBuffer = null;
        _sliceOffset = 0;
        _referenceCount = 1;
        _disposed = 0;
    }

    /// <summary>
    /// Internal constructor for child slice buffers that retain a parent buffer.
    /// </summary>
    private RefCountedPartitionBuffer(RefCountedPartitionBuffer parent, void* slicePointer, int byteLength, int sliceOffset)
    {
        _parentBuffer = parent;
        _sliceOffset = sliceOffset;
        _rawPointer = slicePointer;
        _byteLength = byteLength;
        _ownsMemory = false;
        _underlyingOwner = null;
        _referenceCount = 1;
        _disposed = 0;

        // Atomically retain parent buffer
        _parentBuffer.Retain();
    }

    /// <summary>
    /// Allocates unmanaged memory aligned to 64 bytes and cleared to zero.
    /// </summary>
    public static RefCountedPartitionBuffer Allocate(int byteLength) => new(byteLength);

    /// <summary>
    /// Allocates a new 64-byte aligned buffer and copies source span data into it.
    /// </summary>
    public static RefCountedPartitionBuffer CreateCopy(ReadOnlySpan<byte> source)
    {
        var buffer = Allocate(source.Length);
        if (source.Length > 0)
        {
            source.CopyTo(buffer.Span);
        }
        return buffer;
    }

    /// <summary>
    /// Allocates a new 64-byte aligned buffer and copies a ReadOnlySequence&lt;byte&gt; into it.
    /// </summary>
    public static RefCountedPartitionBuffer CreateCopy(ReadOnlySequence<byte> sequence)
    {
        int length = checked((int)sequence.Length);
        var buffer = Allocate(length);
        if (length > 0)
        {
            sequence.CopyTo(buffer.Span);
        }
        return buffer;
    }

    /// <summary>Wraps an existing NativeMemoryOwner.</summary>
    public static RefCountedPartitionBuffer Wrap(NativeMemoryOwner<byte> owner, int byteLength = -1) =>
        new(owner, byteLength);

    /// <summary>Wraps an existing unmanaged pointer.</summary>
    public static RefCountedPartitionBuffer WrapUnmanaged(void* pointer, int byteLength, bool ownsMemory = true) =>
        new(pointer, byteLength, ownsMemory);

    /// <summary>Total byte capacity of the partition buffer.</summary>
    public int ByteLength => _byteLength;

    /// <summary>Total byte capacity of the partition buffer (alias for ByteLength).</summary>
    public int Length => _byteLength;

    /// <summary>Raw unmanaged pointer as IntPtr.</summary>
    public IntPtr Pointer
    {
        get
        {
            ThrowIfDisposed();
            return (IntPtr)_rawPointer;
        }
    }

    /// <summary>Raw unmanaged void* pointer.</summary>
    public void* UnmanagedPointer
    {
        get
        {
            ThrowIfDisposed();
            return _rawPointer;
        }
    }

    /// <summary>Current active reference count.</summary>
    public int ReferenceCount => Volatile.Read(ref _referenceCount);

    /// <summary>True if the buffer has reached a reference count of 0 and been freed.</summary>
    public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    /// <summary>Zero-allocation Span view over the unmanaged buffer.</summary>
    public Span<byte> Span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ThrowIfDisposed();
            if (_byteLength == 0 || _rawPointer == null)
                return Span<byte>.Empty;
            return new Span<byte>(_rawPointer, _byteLength);
        }
    }

    /// <summary>Zero-allocation ReadOnlySpan view over the unmanaged buffer.</summary>
    public ReadOnlySpan<byte> ReadOnlySpan => Span;

    /// <summary>
    /// Zero-copy Memory&lt;byte&gt; representation backed by unmanaged memory.
    /// </summary>
    public Memory<byte> Memory
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ThrowIfDisposed();
            if (_byteLength == 0 || _rawPointer == null)
                return Memory<byte>.Empty;

            if (_parentBuffer != null)
            {
                return _parentBuffer.Memory.Slice(_sliceOffset, _byteLength);
            }

            if (_underlyingOwner != null)
            {
                return _underlyingOwner.Memory.Slice(0, _byteLength);
            }

            return new UnmanagedMemoryManager<byte>((byte*)_rawPointer, _byteLength).Memory;
        }
    }

    /// <summary>Zero-copy ReadOnlyMemory&lt;byte&gt; view.</summary>
    public ReadOnlyMemory<byte> ReadOnlyMemory => Memory;

    /// <summary>
    /// Atomically increments the reference count.
    /// Throws ObjectDisposedException if the buffer has already been freed or is disposing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Retain()
    {
        if (!TryRetain())
        {
            throw new ObjectDisposedException(nameof(RefCountedPartitionBuffer), "Cannot retain an already-disposed partition buffer.");
        }
    }

    /// <summary>
    /// Atomically attempts to increment reference count via CAS loop.
    /// Returns false without throwing if the buffer has already reached ref-count 0.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRetain()
    {
        while (true)
        {
            int current = Volatile.Read(ref _referenceCount);
            if (current <= 0 || Volatile.Read(ref _disposed) != 0)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _referenceCount, current + 1, current) == current)
            {
                return true;
            }
        }
    }

    /// <summary>
    /// Atomically decrements the reference count.
    /// When reference count reaches 0, unmanaged memory is immediately freed.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Release()
    {
        int newCount = Interlocked.Decrement(ref _referenceCount);
        if (newCount == 0)
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                FreeMemory();
            }
        }
        else if (newCount < 0)
        {
            // Clamp underflow to prevent negative runaway and double-free
            Interlocked.Exchange(ref _referenceCount, 0);
        }
    }

    /// <summary>Equivalent to Release().</summary>
    public void Dispose() => Release();

    /// <summary>Slices the buffer starting at offset.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> Slice(int offset)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, _byteLength);
        return Span.Slice(offset);
    }

    /// <summary>Slices the buffer with specific offset and length.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> Slice(int offset, int length)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if ((uint)offset + (uint)length > (uint)_byteLength)
        {
            throw new ArgumentOutOfRangeException(nameof(length), "Offset and length exceed buffer capacity or overflow.");
        }
        return Span.Slice(offset, length);
    }

    /// <summary>Returns a ReadOnlySpan slice.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> SliceReadOnly(int offset, int length) => Slice(offset, length);

    /// <summary>Returns a Memory slice.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Memory<byte> SliceMemory(int offset, int length)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if ((uint)offset + (uint)length > (uint)_byteLength)
        {
            throw new ArgumentOutOfRangeException(nameof(length), "Offset and length exceed buffer capacity or overflow.");
        }
        return Memory.Slice(offset, length);
    }

    /// <summary>
    /// Creates a child RefCountedPartitionBuffer slice that retains this parent buffer,
    /// ensuring underlying memory remains valid until the child buffer is released.
    /// </summary>
    public RefCountedPartitionBuffer CreateSliceBuffer(int offset, int length)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if ((uint)offset + (uint)length > (uint)_byteLength)
        {
            throw new ArgumentOutOfRangeException(nameof(length), "Offset and length exceed buffer capacity or overflow.");
        }

        byte* slicePtr = _rawPointer != null ? (byte*)_rawPointer + offset : null;
        return new RefCountedPartitionBuffer(this, slicePtr, length, offset);
    }

    private void FreeMemory()
    {
        if (_parentBuffer != null)
        {
            _parentBuffer.Release();
        }
        else if (_underlyingOwner != null)
        {
            _underlyingOwner.Dispose();
            _rawPointer = null;
        }
        else if (_ownsMemory && _rawPointer != null)
        {
            NativeMemory.AlignedFree(_rawPointer);
            _rawPointer = null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
    }
}
