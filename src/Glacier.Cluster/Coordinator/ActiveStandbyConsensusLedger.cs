namespace Glacier.Cluster.Coordinator;

using System;
using System.Threading;
using System.Threading.Tasks;

public enum ConsensusRole : byte
{
    Standalone = 0,
    Active = 1,
    Standby = 2
}

public readonly struct LeaderLease
{
    public readonly Guid LeaderId;
    public readonly ulong Epoch;
    public readonly DateTimeOffset ExpiryUtc;

    public LeaderLease(Guid leaderId, ulong epoch, DateTimeOffset expiryUtc)
    {
        LeaderId = leaderId;
        Epoch = epoch;
        ExpiryUtc = expiryUtc;
    }

    public bool IsExpired => DateTimeOffset.UtcNow >= ExpiryUtc;
}

public sealed class ConsensusOptions
{
    public ConsensusRole InitialRole { get; init; } = ConsensusRole.Standalone;
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMilliseconds(3000);
    public TimeSpan RenewInterval { get; init; } = TimeSpan.FromMilliseconds(1000);
    public TimeSpan GracePeriod { get; init; } = TimeSpan.FromMilliseconds(500);
}

/// <summary>
/// Lightweight leader lease election and cluster metadata state replication between active and standby coordinators.
/// Zero external dependencies (no ZooKeeper/etcd/Raft JVM services required).
/// </summary>
public sealed class ActiveStandbyConsensusLedger : IAsyncDisposable, IDisposable
{
    private readonly Guid _localCoordinatorId;
    private readonly ConsensusOptions _options;
    private volatile ConsensusRole _currentRole;
    private LeaderLease _currentLease;
    private readonly object _leaseLock = new();
    private readonly CancellationTokenSource _cts = new();
    private Task? _leaseLoopTask;
    private int _isDisposed;

    public Guid LocalCoordinatorId => _localCoordinatorId;
    public ConsensusRole CurrentRole => _currentRole;
    public LeaderLease CurrentLease { get { lock (_leaseLock) return _currentLease; } }

    public event Action? PromotedToActive;
    public event Action? DemotedToStandby;

    public ActiveStandbyConsensusLedger(
        Guid localCoordinatorId,
        ConsensusOptions? options = null)
    {
        _localCoordinatorId = localCoordinatorId;
        _options = options ?? new ConsensusOptions();
        _currentRole = _options.InitialRole;

        if (_currentRole == ConsensusRole.Standalone || _currentRole == ConsensusRole.Active)
        {
            _currentLease = new LeaderLease(_localCoordinatorId, 1, DateTimeOffset.UtcNow.AddYears(10));
        }
        else
        {
            _currentLease = new LeaderLease(Guid.Empty, 0, DateTimeOffset.MinValue);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_currentRole == ConsensusRole.Standalone)
            return Task.CompletedTask;

        _leaseLoopTask = Task.Run(() => LeaseLoopAsync(_cts.Token));
        return Task.CompletedTask;
    }

    public bool TryRenewLease()
    {
        if (_currentRole != ConsensusRole.Active) return false;

        lock (_leaseLock)
        {
            _currentLease = new LeaderLease(
                _localCoordinatorId,
                _currentLease.Epoch,
                DateTimeOffset.UtcNow.Add(_options.LeaseDuration));
            return true;
        }
    }

    public void UpdateLeaderLeaseFromRemote(Guid leaderId, ulong epoch, DateTimeOffset expiryUtc)
    {
        lock (_leaseLock)
        {
            if (epoch >= _currentLease.Epoch)
            {
                _currentLease = new LeaderLease(leaderId, epoch, expiryUtc);
                if (_currentRole == ConsensusRole.Active && leaderId != _localCoordinatorId)
                {
                    _currentRole = ConsensusRole.Standby;
                    DemotedToStandby?.Invoke();
                }
            }
        }
    }

    private async Task LeaseLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_options.RenewInterval);
        while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
        {
            if (_currentRole == ConsensusRole.Active)
            {
                TryRenewLease();
            }
            else if (_currentRole == ConsensusRole.Standby)
            {
                bool shouldPromote = false;
                lock (_leaseLock)
                {
                    if (DateTimeOffset.UtcNow >= _currentLease.ExpiryUtc.Add(_options.GracePeriod))
                    {
                        // Lease expired: assume leadership
                        ulong newEpoch = _currentLease.Epoch + 1;
                        _currentLease = new LeaderLease(_localCoordinatorId, newEpoch, DateTimeOffset.UtcNow.Add(_options.LeaseDuration));
                        _currentRole = ConsensusRole.Active;
                        shouldPromote = true;
                    }
                }

                if (shouldPromote)
                {
                    PromotedToActive?.Invoke();
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0) return;
        _cts.Cancel();
        if (_leaseLoopTask != null)
        {
            try { await _leaseLoopTask.ConfigureAwait(false); } catch { }
        }
        _cts.Dispose();
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
