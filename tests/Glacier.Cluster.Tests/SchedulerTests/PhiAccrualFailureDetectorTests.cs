namespace Glacier.Cluster.Tests.SchedulerTests;

using System;
using System.Collections.Generic;
using Glacier.Cluster.Scheduler;
using Xunit;

public sealed class PhiAccrualFailureDetectorTests
{
    [Fact]
    public void Regular_Heartbeats_Maintain_Healthy_State()
    {
        var detector = new PhiAccrualFailureDetector();
        var nodeId = Guid.NewGuid();
        ulong t = 1_000_000_000UL; // 1.0 s in ns

        detector.RegisterNode(nodeId, t);

        // Feed 100 heartbeats arriving at 500ms intervals
        for (int i = 0; i < 100; i++)
        {
            t += 500_000_000UL; // +500 ms
            detector.RecordHeartbeat(nodeId, t);
        }

        // Query immediately after last heartbeat (50 ms elapsed, well under 500ms mean)
        ulong queryTime = t + 50_000_000UL;
        double phi = detector.ComputePhi(nodeId, queryTime);
        var health = detector.GetHealthState(nodeId, queryTime);

        Assert.Equal(0.0, phi);
        Assert.Equal(NodeHealthState.Healthy, health);
        Assert.True(detector.IsHealthy(nodeId, queryTime));
    }

    [Fact]
    public void Transient_GC_Pause_Enters_Suspect_Without_Eviction_And_Recovers()
    {
        var detector = new PhiAccrualFailureDetector(
            suspectThreshold: 8.0,
            evictionThreshold: 12.0,
            minStdDevMs: 200.0); // 200ms std dev simulates realistic network/GC jitter variance

        var nodeId = Guid.NewGuid();
        ulong t = 1_000_000_000UL;
        detector.RegisterNode(nodeId, t);

        // Feed baseline heartbeats with slight variance around 500ms
        for (int i = 0; i < 50; i++)
        {
            t += 500_000_000UL;
            detector.RecordHeartbeat(nodeId, t);
        }

        // Simulate a 2.0-second GC pause:
        // elapsed = 2000ms, mean = 500ms, stdDev = 200ms -> z = (2000-500)/200 = 7.5
        // y = 7.5 / sqrt(2) = 5.30 -> y >= 4.0 log-domain -> Phi ~ (y^2 + ln(y) + ...)/ln(10)
        // y^2 = 28.1 -> Phi ~ 28.1 / 2.302 ~ 12.2 or with 220ms stdDev ~ 10.0
        ulong gcPauseNs = 1_900_000_000UL; // 1.9s
        ulong duringPause = t + gcPauseNs;

        double phiDuringPause = detector.ComputePhi(nodeId, duringPause);
        var healthDuringPause = detector.GetHealthState(nodeId, duringPause);

        // At 1.9s delay with 200ms std dev, phi is in the Suspect window [8.0, 12.0)
        Assert.InRange(phiDuringPause, 8.0, 12.0);
        Assert.Equal(NodeHealthState.Suspect, healthDuringPause);
        Assert.False(detector.IsHealthy(nodeId, duringPause));

        // Now the GC pause completes and worker immediately dispatches queued heartbeat
        ulong recoveryTime = duringPause + 10_000_000UL; // 10ms later
        detector.RecordHeartbeat(nodeId, recoveryTime);

        // Check health immediately after recovery
        double phiRecovered = detector.ComputePhi(nodeId, recoveryTime + 20_000_000UL);
        var healthRecovered = detector.GetHealthState(nodeId, recoveryTime + 20_000_000UL);

        Assert.Equal(0.0, phiRecovered);
        Assert.Equal(NodeHealthState.Healthy, healthRecovered);
        Assert.True(detector.IsHealthy(nodeId, recoveryTime + 20_000_000UL));
    }

    [Fact]
    public void Prolonged_Silence_Triggers_Eviction()
    {
        var detector = new PhiAccrualFailureDetector(suspectThreshold: 8.0, evictionThreshold: 12.0);
        var nodeId = Guid.NewGuid();
        ulong t = 1_000_000_000UL;
        detector.RegisterNode(nodeId, t);

        for (int i = 0; i < 30; i++)
        {
            t += 500_000_000UL;
            detector.RecordHeartbeat(nodeId, t);
        }

        // Advance time by 30 seconds of total silence
        ulong deadTime = t + 30_000_000_000UL;

        double phi = detector.ComputePhi(nodeId, deadTime);
        var health = detector.GetHealthState(nodeId, deadTime);

        Assert.True(phi >= 12.0, $"Phi was {phi}, expected >= 12.0");
        Assert.Equal(NodeHealthState.Evicted, health);
        Assert.False(detector.IsHealthy(nodeId, deadTime));
    }

    [Fact]
    public void Log_Domain_Asymptotic_Expansion_No_Underflow_Or_NaN()
    {
        // Test extreme delays where standard double erfc(y) would underflow to 0.0
        // and produce -log10(0) = Infinity or NaN.
        double meanMs = 500.0;
        double stdDevMs = 10.0;

        // 1. Moderate delay (z ~ 50, y ~ 35)
        double elapsed1 = 1000.0;
        double phi1 = PhiAccrualFailureDetector.CalculatePhi(elapsed1, meanMs, stdDevMs);
        Assert.False(double.IsNaN(phi1));
        Assert.False(double.IsInfinity(phi1));
        Assert.True(phi1 > 12.0);

        // 2. Extreme delay: 1 hour (z = 360,000, y = 254,558)
        double elapsed2 = 3_600_000.0;
        double phi2 = PhiAccrualFailureDetector.CalculatePhi(elapsed2, meanMs, stdDevMs);
        Assert.False(double.IsNaN(phi2));
        Assert.False(double.IsInfinity(phi2));
        Assert.True(phi2 > phi1);

        // 3. Ultra-extreme delay: 1 year (z = 3.15e9)
        double elapsed3 = 31_536_000_000.0;
        double phi3 = PhiAccrualFailureDetector.CalculatePhi(elapsed3, meanMs, stdDevMs);
        Assert.False(double.IsNaN(phi3));
        Assert.False(double.IsInfinity(phi3));
        Assert.True(phi3 > phi2);
    }

    [Fact]
    public void Adaptive_Variance_Tolerates_Jittery_Networks()
    {
        var detector = new PhiAccrualFailureDetector(minStdDevMs: 1.0);
        var nodePredictable = Guid.NewGuid();
        var nodeJittery = Guid.NewGuid();

        ulong tPred = 1_000_000_000UL;
        ulong tJitt = 1_000_000_000UL;

        detector.RegisterNode(nodePredictable, tPred);
        detector.RegisterNode(nodeJittery, tJitt);

        // Feed predictable intervals: exactly 500ms
        for (int i = 0; i < 50; i++)
        {
            tPred += 500_000_000UL;
            detector.RecordHeartbeat(nodePredictable, tPred);
        }

        // Feed jittery intervals: alternating 300ms and 700ms (mean = 500ms, stdDev ~ 200ms)
        for (int i = 0; i < 50; i++)
        {
            ulong step = (i % 2 == 0) ? 300_000_000UL : 700_000_000UL;
            tJitt += step;
            detector.RecordHeartbeat(nodeJittery, tJitt);
        }

        // Query both at 800ms after last heartbeat:
        // Predictable node: 800ms is 300ms late relative to 1ms std dev -> huge z -> high Phi!
        // Jittery node: 800ms is normal within its +/- 200ms variance -> low Phi!
        ulong queryPred = tPred + 800_000_000UL;
        ulong queryJitt = tJitt + 800_000_000UL;

        double phiPred = detector.ComputePhi(nodePredictable, queryPred);
        double phiJitt = detector.ComputePhi(nodeJittery, queryJitt);

        Assert.True(phiPred > phiJitt, $"Predictable phi {phiPred} should be higher than jittery phi {phiJitt}");
        Assert.True(phiPred >= 12.0, "Predictable node should be evicted given near-zero variance.");
        Assert.True(phiJitt < 8.0, "Jittery node should remain healthy due to learned high variance.");
    }

    [Fact]
    public void Minimum_StdDev_Guard_Prevents_Zero_Division()
    {
        var detector = new PhiAccrualFailureDetector(minStdDevMs: 1.0);
        var nodeId = Guid.NewGuid();
        ulong t = 1_000_000_000UL;

        detector.RegisterNode(nodeId, t);

        // 100 identical intervals with 0.0 empirical variance
        for (int i = 0; i < 100; i++)
        {
            t += 500_000_000UL;
            detector.RecordHeartbeat(nodeId, t);
        }

        // Advance by 505 ms (5 ms late)
        double phi = detector.ComputePhi(nodeId, t + 505_000_000UL);

        Assert.False(double.IsNaN(phi));
        Assert.False(double.IsInfinity(phi));
    }

    [Fact]
    public void Cold_Start_New_Node_Registration_Defaults_To_Healthy()
    {
        var detector = new PhiAccrualFailureDetector();
        var nodeId = Guid.NewGuid();
        ulong t = 1_000_000_000UL;

        detector.RegisterNode(nodeId, t);

        // Query 100ms later before second heartbeat
        var health = detector.GetHealthState(nodeId, t + 100_000_000UL);
        Assert.Equal(NodeHealthState.Healthy, health);
        Assert.True(detector.IsHealthy(nodeId, t + 100_000_000UL));
    }

    [Fact]
    public void NodeHealthChanged_Event_Fires_On_Transitions()
    {
        var detector = new PhiAccrualFailureDetector(suspectThreshold: 8.0, evictionThreshold: 12.0);
        var nodeId = Guid.NewGuid();
        ulong t = 1_000_000_000UL;

        var transitions = new List<(NodeHealthState Old, NodeHealthState New)>();
        detector.NodeHealthChanged += (id, oldState, newState) =>
        {
            if (id == nodeId) transitions.Add((oldState, newState));
        };

        detector.RegisterNode(nodeId, t);
        for (int i = 0; i < 30; i++)
        {
            t += 500_000_000UL;
            detector.RecordHeartbeat(nodeId, t);
        }

        // Trigger eviction by waiting 30 seconds
        detector.GetHealthState(nodeId, t + 30_000_000_000UL);

        Assert.NotEmpty(transitions);
        Assert.Equal(NodeHealthState.Evicted, transitions[^1].New);

        // Now send heartbeat to recover
        ulong recoverTime = t + 30_000_000_000UL + 10_000_000UL;
        detector.RecordHeartbeat(nodeId, recoverTime);

        Assert.Equal(NodeHealthState.Healthy, transitions[^1].New);
    }

    [Fact]
    public void ComputeErfc_Accurate_Across_Domain()
    {
        // erfc(0) = 1.0
        Assert.Equal(1.0, PhiAccrualFailureDetector.ComputeErfc(0.0), 4);

        // erfc(1.0) ~ 0.157299
        Assert.Equal(0.1573, PhiAccrualFailureDetector.ComputeErfc(1.0), 3);

        // erfc(2.0) ~ 0.0046777
        Assert.Equal(0.00468, PhiAccrualFailureDetector.ComputeErfc(2.0), 4);

        // erfc(-1.0) = 2.0 - erfc(1.0) ~ 1.8427
        Assert.Equal(1.8427, PhiAccrualFailureDetector.ComputeErfc(-1.0), 3);
    }
}
