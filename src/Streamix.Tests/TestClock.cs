using Streamix.Abstractions;
using System.Diagnostics;

namespace Streamix.Tests;

public sealed class TestClock : IClock
{
    DateTimeOffset now;
    readonly List<(DateTimeOffset Time, TaskCompletionSource Tcs)> delays = new();
    readonly List<TaskCompletionSource> waiters = new();

    public TestClock(DateTimeOffset? startTime = null)
    {
        now = startTime ?? new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
    }

    public DateTimeOffset Now => now;

    public Task Delay(TimeSpan delay, CancellationToken cancellationToken = default)
    {
        if (delay <= TimeSpan.Zero) return Task.CompletedTask;

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (cancellationToken.CanBeCanceled)
        {
            cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
        }

        lock (delays)
        {
            delays.Add((now + delay, tcs));
            var waiters = this.waiters.ToList();
            this.waiters.Clear();
            foreach (var waiter in waiters) waiter.TrySetResult();
        }

        return tcs.Task;
    }

    public async Task WaitForDelay(int count, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            Task waiterTask;
            lock (delays)
            {
                if (delays.Count >= count) return;
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                waiters.Add(tcs);
                waiterTask = tcs.Task;
            }

            var remainingTimeout = timeout - sw.Elapsed;
            var delayMs = Math.Min(100, (int)remainingTimeout.TotalMilliseconds + 1);
            if (delayMs <= 0) break;

            await Task.WhenAny(waiterTask, Task.Delay(delayMs));

            // Check again after awaiting, in case condition is now met
            lock (delays)
            {
                if (delays.Count >= count) return;
            }
        }
        lock (delays)
        {
            if (delays.Count < count) throw new TimeoutException($"Timed out waiting for {count} delays. Current count: {delays.Count}");
        }
    }

    public void AdvanceBy(TimeSpan interval)
    {
        lock (delays)
        {
            now += interval;
            var toTrigger = delays.Where(d => d.Time <= now).ToList();
            foreach (var delay in toTrigger)
            {
                delays.Remove(delay);
                delay.Tcs.TrySetResult();
            }
        }
    }
}
