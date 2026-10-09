namespace Glacier.Cluster.Shuffle;

using System;
using System.IO.Pipelines;

/// <summary>
/// An in-process implementation of <see cref="IDuplexPipe"/> wrapping two pipe endpoints.
/// </summary>
public sealed class InProcessDuplexPipe : IDuplexPipe
{
    public PipeReader Input { get; }
    public PipeWriter Output { get; }

    public InProcessDuplexPipe(PipeReader input, PipeWriter output)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        Output = output ?? throw new ArgumentNullException(nameof(output));
    }
}

/// <summary>
/// Factory for creating in-process memory pipes (<see cref="IDuplexPipe"/> pairs), enabling
/// deterministic loopback tests and virtual node simulations without requiring real network sockets.
/// </summary>
public static class DuplexPipeFactory
{
    /// <summary>
    /// Creates a matched pair of in-process duplex pipes where output from the client pipe
    /// flows into the input of the server pipe, and vice versa.
    /// </summary>
    public static (IDuplexPipe ClientSide, IDuplexPipe ServerSide) CreateInProcessPair(
        PipeOptions? clientOptions = null,
        PipeOptions? serverOptions = null)
    {
        var clientToServer = new Pipe(clientOptions ?? PipeOptions.Default);
        var serverToClient = new Pipe(serverOptions ?? PipeOptions.Default);

        var client = new InProcessDuplexPipe(serverToClient.Reader, clientToServer.Writer);
        var server = new InProcessDuplexPipe(clientToServer.Reader, serverToClient.Writer);

        return (client, server);
    }
}
