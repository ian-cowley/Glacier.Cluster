namespace Glacier.Cluster.Scheduler;

using System;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// High-throughput, thread-safe circular ring buffer tracking arrival intervals in nanoseconds.
/// Pre-allocated; zero heap allocations on hot heartbeat paths.
/// </summary>
public sealed class HeartbeatHistoryWindow
{
    private readonly double[] _intervalsMs;
    private readonly int _capacity;
    private int _head;
    private int _count;
    private double _sumMs;
    private ulong _lastArrivalNs;
    private readonly Lock _gate = new();

    public int Capacity => _capacity;
    public int SampleCount { get { lock (_gate) return _count; } }
    public ulong LastArrivalNs { get { lock (_gate) return _lastArrivalNs; } }

    public HeartbeatHistoryWindow(int capacity = 1000)
    {
        if (capacity < 2) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be >= 2.");
        _capacity = capacity;
        _intervalsMs = new double[capacity];
    }

    /// <summary>
    /// Records a new heartbeat timestamp and appends the inter-arrival interval.
    /// </summary>
    public void RecordHeartbeat(ulong timestampNs)
    {
        lock (_gate)
        {
            if (_lastArrivalNs == 0)
            {
                _lastArrivalNs = timestampNs;
                return;
            }

            if (timestampNs <= _lastArrivalNs)
            {
                // Discard clock jitter / out-of-order timestamps
                return;
            }

            ulong diffNs = timestampNs - _lastArrivalNs;
            _lastArrivalNs = timestampNs;
            double intervalMs = diffNs / 1_000_000.0;

            if (_count < _capacity)
            {
                _intervalsMs[_head] = intervalMs;
                _sumMs += intervalMs;
                _head = (_head + 1) % _capacity;
                _count++;
            }
            else
            {
                double evicted = _intervalsMs[_head];
                _intervalsMs[_head] = intervalMs;
                _sumMs += (intervalMs - evicted);
                _head = (_head + 1) % _capacity;
            }
        }
    }

    /// <summary>
    /// Computes sample mean and sample standard deviation in milliseconds.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public (double MeanMs, double StdDevMs) ComputeStatistics(double minStdDevMs = 1.0)
    {
        lock (_gate)
        {
            if (_count == 0)
            {
                return (500.0, minStdDevMs); // Default 500ms heartbeat expectation
            }

            double mean = _sumMs / _count;

            if (_count == 1)
            {
                return (mean, minStdDevMs);
            }

            // Two-pass variance computation for numerical stability
            double sumSquares = 0.0;
            for (int i = 0; i < _count; i++)
            {
                double delta = _intervalsMs[i] - mean;
                sumSquares += delta * delta;
            }

            double variance = sumSquares / _count;
            double stdDev = Math.Sqrt(variance);
            stdDev = Math.Max(stdDev, minStdDevMs);

            return (mean, stdDev);
        }
    }
}
