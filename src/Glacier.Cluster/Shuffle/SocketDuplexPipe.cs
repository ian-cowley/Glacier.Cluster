namespace Glacier.Cluster.Shuffle;

using System;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Tasks;

/// <summary>
/// Adapts a physical TCP socket into an <see cref="IDuplexPipe"/> over <see cref="PipeReader"/> and <see cref="PipeWriter"/>.
/// </summary>
public sealed class SocketDuplexPipe : IDuplexPipe, IAsyncDisposable, IDisposable
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private int _disposed;

    public PipeReader Input { get; }
    public PipeWriter Output { get; }
    public Socket Socket => _socket;

    public SocketDuplexPipe(Socket socket)
    {
        _socket = socket ?? throw new ArgumentNullException(nameof(socket));
        _stream = new NetworkStream(socket, ownsSocket: true);
        Input = PipeReader.Create(_stream);
        Output = PipeWriter.Create(_stream);
    }

    public async ValueTask DisposeAsync()
    {
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try
        {
            await Input.CompleteAsync().ConfigureAwait(false);
            await Output.CompleteAsync().ConfigureAwait(false);
            await _stream.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // Best-effort cleanup
        }
        finally
        {
            _socket.Dispose();
        }
    }

    public void Dispose()
    {
        if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try
        {
            Input.Complete();
            Output.Complete();
            _stream.Dispose();
        }
        catch
        {
            // Best-effort cleanup
        }
        finally
        {
            _socket.Dispose();
        }
    }
}
