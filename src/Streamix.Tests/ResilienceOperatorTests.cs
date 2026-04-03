using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

[TestFixture]
public class ResilienceOperatorTests
{
    class SlowAsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        class Enumerator : IAsyncEnumerator<T>
        {
            private readonly IClock clock;
            private readonly TimeSpan delay;
            private readonly CancellationToken cancellationToken;

            public Enumerator(IClock clock, TimeSpan delay, CancellationToken cancellationToken)
            {
                this.clock = clock;
                this.delay = delay;
                this.cancellationToken = cancellationToken;
            }

            public T Current => default!;

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            public async ValueTask<bool> MoveNextAsync()
            {
                await clock.Delay(delay, cancellationToken);
                return true;
            }
        }

        readonly IClock clock;
        readonly TimeSpan delay;

        public SlowAsyncEnumerable(IClock clock, TimeSpan delay)
        {
            this.clock = clock;
            this.delay = delay;
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new Enumerator(clock, delay, cancellationToken);
        }
    }

    TestClock clock = new TestClock();

    [Test]
    public async Task Single_Retry_ShouldReenumerateOnFailure()
    {
        var callCount = 0;
        async IAsyncEnumerable<int> Source()
        {
            callCount++;
            if (callCount == 1)
            {
                throw new InvalidOperationException("First try failed");
            }
            yield return 1;
        }
        var source = Single.From<int>(Source());

        var retried = ((Single<int>)source).Retry(1);
        var result = await retried.ToTask();

        Assert.That(result, Is.EqualTo(1));
        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public void Single_Timeout_ShouldThrowWhenSlow()
    {
        var source = new SlowAsyncEnumerable<int>(clock, TimeSpan.FromSeconds(2));
        var timeoutSingle = ((Single<int>)Single.From<int>(source, clock)).Timeout(TimeSpan.FromSeconds(1));

        var task = timeoutSingle.ToTask();

        clock.AdvanceBy(TimeSpan.FromSeconds(1.1));
        Assert.ThrowsAsync<TimeoutException>(async () => await task);
    }

    [Test]
    public async Task Retry_ShouldReenumerateOnFailure()
    {
        var callCount = 0;
        async IAsyncEnumerable<int> Source()
        {
            callCount++;
            if (callCount == 1)
            {
                throw new InvalidOperationException("First try failed");
            }
            yield return 1;
            yield return 2;
        }
        var source = Stream.From(Source());

        var retried = ((Stream<int>)source).Retry(1);
        var results = new List<int>();

        await foreach (var item in retried)
        {
            results.Add(item);
        }

        Assert.That(results, Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Retry_ShouldFailWhenRetriesExhausted()
    {
        var callCount = 0;
        async IAsyncEnumerable<int> Source()
        {
            callCount++;
            throw new InvalidOperationException("Persistent failure");
            yield return 1;
        }
        var source = Stream.From(Source());

        var retried = ((Stream<int>)source).Retry(2);

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in retried) { }
        });

        Assert.That(callCount, Is.EqualTo(3)); // 1 try + 2 retries
    }

    [Test]
    public void Timeout_ShouldThrowWhenEmissionIsSlow()
    {
        var source = new SlowAsyncEnumerable<int>(clock, TimeSpan.FromSeconds(2));
        var timeoutStream = ((Stream<int>)Stream.From<int>(source, clock)).Timeout(TimeSpan.FromSeconds(1));

        var task = timeoutStream.ForEachAsync(_ => { });

        clock.AdvanceBy(TimeSpan.FromSeconds(1.1));
        Assert.ThrowsAsync<TimeoutException>(async () => await task);
    }

    [Test]
    public async Task Timeout_ShouldSucceedWhenEmissionIsFast()
    {
        var source = new SlowAsyncEnumerable<int>(clock, TimeSpan.FromSeconds(0.5));
        var timeoutStream = ((Stream<int>)Stream.From<int>(source, clock)).Timeout(TimeSpan.FromSeconds(1));
        var results = new List<int>();

        var task = Task.Run(async () =>
        {
            await using var e = timeoutStream.Take(2).GetAsyncEnumerator();
            while (await e.MoveNextAsync())
            {
                results.Add(e.Current);
            }
        });

        await clock.WaitForDelay(2, TimeSpan.FromSeconds(2)); // MoveNext + Timeout Delay
        clock.AdvanceBy(TimeSpan.FromSeconds(0.6));
        for (int i = 0; i < 100 && results.Count < 1; i++) await Task.Delay(10);
        Assert.That(results, Has.Count.EqualTo(1));

        await clock.WaitForDelay(2, TimeSpan.FromSeconds(2)); // Next MoveNext + Timeout Delay
        clock.AdvanceBy(TimeSpan.FromSeconds(0.6));
        for (int i = 0; i < 100 && results.Count < 2; i++) await Task.Delay(10);
        await task;
        Assert.That(results, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Retry_WithBackoff_ShouldWaitBetweenAttempts()
    {
        var callCount = 0;
        async IAsyncEnumerable<int> Source()
        {
            callCount++;
            if (callCount < 3)
            {
                throw new InvalidOperationException($"Attempt {callCount} failed");
            }
            yield return 42;
        }

        var source = Stream.From(Source(), clock);
        var backoffStrategyCalls = new List<(int Attempt, Exception Ex)>();
        var retried = source.Retry(2, (attempt, ex) =>
        {
            backoffStrategyCalls.Add((attempt, ex));
            return TimeSpan.FromSeconds(attempt);
        });

        var results = new List<int>();
        var task = Task.Run(async () =>
        {
            await foreach (var item in retried)
            {
                results.Add(item);
            }
        });

        // First attempt fails immediately.
        // Then it schedules the first backoff delay (1s)
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        Assert.That(backoffStrategyCalls, Has.Count.EqualTo(1));
        Assert.That(backoffStrategyCalls[0].Attempt, Is.EqualTo(1));

        clock.AdvanceBy(TimeSpan.FromSeconds(1.1));

        // Second attempt fails.
        // We wait for the second backoff delay (2s)
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        Assert.That(backoffStrategyCalls, Has.Count.EqualTo(2));
        Assert.That(backoffStrategyCalls[1].Attempt, Is.EqualTo(2));

        clock.AdvanceBy(TimeSpan.FromSeconds(2.1));

        // Third attempt succeeds
        await task;
        Assert.That(callCount, Is.EqualTo(3));
        Assert.That(results, Is.EquivalentTo(new[] { 42 }));
    }

    [Test]
    public async Task Single_Retry_WithBackoff_ShouldWaitBetweenAttempts()
    {
        var callCount = 0;
        async IAsyncEnumerable<int> Source()
        {
            callCount++;
            if (callCount < 3)
            {
                throw new InvalidOperationException($"Attempt {callCount} failed");
            }
            yield return 42;
        }

        var source = Single.From(Source(), clock);
        var retried = source.Retry(2, (attempt, ex) => TimeSpan.FromSeconds(attempt));

        var task = retried.ToTask();

        // First attempt fails, waiting for 1s
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));

        clock.AdvanceBy(TimeSpan.FromSeconds(1.1));

        // Second attempt fails, waiting for 2s
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));

        clock.AdvanceBy(TimeSpan.FromSeconds(2.1));

        // Third attempt succeeds
        var result = await task;
        Assert.That(callCount, Is.EqualTo(3));
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task Retry_WithBackoff_ShouldPropagateFinalException()
    {
        var callCount = 0;
        async IAsyncEnumerable<int> Source()
        {
            callCount++;
            throw new InvalidOperationException($"Persistent failure {callCount}");
            yield break;
        }

        var source = Stream.From(Source());
        var retried = source.Retry(2, (attempt, ex) => TimeSpan.FromMilliseconds(10));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in retried) { }
        });

        Assert.That(ex.Message, Is.EqualTo("Persistent failure 3"));
        Assert.That(callCount, Is.EqualTo(3));
    }

    [Test]
    public void Retry_WithBackoff_ShouldBeCancellableDuringDelay()
    {
        async IAsyncEnumerable<int> Source()
        {
            throw new InvalidOperationException("Failed");
            yield break;
        }

        var cts = new CancellationTokenSource();
        var source = Stream.From(Source(), clock);
        var retried = source.Retry(5, (attempt, ex) => TimeSpan.FromSeconds(10));

        var task = Task.Run(async () =>
        {
            await foreach (var item in retried.WithCancellation(cts.Token)) { }
        });

        // Wait for the first delay to be scheduled
        var waitTask = clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        if (Task.WhenAny(waitTask, Task.Delay(2000)).Result != waitTask)
        {
            Assert.Fail("Timed out waiting for delay to be scheduled");
        }

        cts.Cancel();

        Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
    }
}
