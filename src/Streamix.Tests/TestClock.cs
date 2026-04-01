using Streamix.Abstractions;

namespace Streamix.Tests;

public sealed class TestClock : IClock
{
    private DateTimeOffset _now;
    private readonly List<(DateTimeOffset Time, TaskCompletionSource Tcs)> _delays = new();
    private readonly List<TaskCompletionSource> _waiters = new();

    public TestClock(DateTimeOffset? startTime = null)
    {
        _now = startTime ?? new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
    }

    public DateTimeOffset Now => _now;

    public Task Delay(TimeSpan delay, CancellationToken cancellationToken = default)
    {
        if (delay <= TimeSpan.Zero) return Task.CompletedTask;

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (cancellationToken.CanBeCanceled)
        {
            cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
        }

        lock (_delays)
        {
            _delays.Add((_now + delay, tcs));
            var waiters = _waiters.ToList();
            _waiters.Clear();
            foreach (var waiter in waiters) waiter.TrySetResult();
        }

        return tcs.Task;
    }

    public async Task WaitForDelay(int count, TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (DateTime.UtcNow - start < timeout)
        {
            Task waiterTask;
            lock (_delays)
            {
                if (_delays.Count >= count) return;
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add(tcs);
                waiterTask = tcs.Task;
            }
            await Task.WhenAny(waiterTask, Task.Delay(100));
        }
        lock (_delays)
        {
            if (_delays.Count < count) throw new TimeoutException($"Timed out waiting for {count} delays. Current count: {_delays.Count}");
        }
    }

    public void AdvanceBy(TimeSpan interval)
    {
        lock (_delays)
        {
            _now += interval;
            var toTrigger = _delays.Where(d => d.Time <= _now).ToList();
            foreach (var delay in toTrigger)
            {
                _delays.Remove(delay);
                delay.Tcs.TrySetResult();
            }
        }
    }
}
