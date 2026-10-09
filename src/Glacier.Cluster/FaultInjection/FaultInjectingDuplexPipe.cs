namespace Glacier.Cluster.FaultInjection;

using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Transport proxy wrapping IDuplexPipe with deterministic fault injection capabilities
/// including abrupt connection severing, heartbeat silencing, and transport delays.
/// </summary>
public sealed class FaultInjectingDuplexPipe : IDuplexPipe
{
    private readonly IDuplexPipe _innerPipe;
    private readonly Guid _targetNodeId;
    private volatile bool _isSevered;
    private volatile bool _silenceHeartbeats;
    private TimeSpan _heartbeatDelay = TimeSpan.Zero;
    private readonly FaultInjectingPipeReader _reader;
    private readonly FaultInjectingPipeWriter _writer;

    public PipeReader Input => _reader;
    public PipeWriter Output => _writer;
    public Guid TargetNodeId => _targetNodeId;
    public bool IsSevered => _isSevered;
    public bool SilenceHeartbeats => _silenceHeartbeats;
    public TimeSpan HeartbeatDelay => _heartbeatDelay;

    public FaultInjectingDuplexPipe(IDuplexPipe innerPipe, Guid targetNodeId)
    {
        _innerPipe = innerPipe ?? throw new ArgumentNullException(nameof(innerPipe));
        _targetNodeId = targetNodeId;
        _reader = new FaultInjectingPipeReader(innerPipe.Input, this);
        _writer = new FaultInjectingPipeWriter(innerPipe.Output, this);
    }

    public void Sever()
    {
        _isSevered = true;
        _innerPipe.Input.CancelPendingRead();
        _innerPipe.Output.CancelPendingFlush();
    }

    public void SetSilenceHeartbeats(bool silence) => _silenceHeartbeats = silence;
    public void SetHeartbeatDelay(TimeSpan delay) => _heartbeatDelay = delay;

    private sealed class FaultInjectingPipeReader : PipeReader
    {
        private readonly PipeReader _inner;
        private readonly FaultInjectingDuplexPipe _parent;

        public FaultInjectingPipeReader(PipeReader inner, FaultInjectingDuplexPipe parent)
        {
            _inner = inner;
            _parent = parent;
        }

        public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            if (_parent._isSevered)
                throw new IOException($"Transport severed for node {_parent._targetNodeId}.");

            return await _inner.ReadAsync(cancellationToken).ConfigureAwait(false);
        }

        public override bool TryRead(out ReadResult result)
        {
            if (_parent._isSevered)
            {
                result = default;
                return false;
            }
            return _inner.TryRead(out result);
        }

        public override void AdvanceTo(SequencePosition consumed) => _inner.AdvanceTo(consumed);
        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => _inner.AdvanceTo(consumed, examined);
        public override void CancelPendingRead() => _inner.CancelPendingRead();
        public override void Complete(Exception? exception = null) => _inner.Complete(exception);
    }

    private sealed class FaultInjectingPipeWriter : PipeWriter
    {
        private readonly PipeWriter _inner;
        private readonly FaultInjectingDuplexPipe _parent;

        public FaultInjectingPipeWriter(PipeWriter inner, FaultInjectingDuplexPipe parent)
        {
            _inner = inner;
            _parent = parent;
        }

        public override Memory<byte> GetMemory(int sizeHint = 0) => _inner.GetMemory(sizeHint);
        public override Span<byte> GetSpan(int sizeHint = 0) => _inner.GetSpan(sizeHint);
        public override void Advance(int bytes) => _inner.Advance(bytes);

        public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        {
            if (_parent._isSevered)
                throw new IOException($"Transport severed for node {_parent._targetNodeId}.");

            if (_parent._heartbeatDelay > TimeSpan.Zero)
            {
                await Task.Delay(_parent._heartbeatDelay, cancellationToken).ConfigureAwait(false);
            }

            return await _inner.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        public override void CancelPendingFlush() => _inner.CancelPendingFlush();
        public override void Complete(Exception? exception = null) => _inner.Complete(exception);
    }
}
