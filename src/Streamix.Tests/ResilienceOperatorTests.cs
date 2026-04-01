using NUnit.Framework;
using Streamix.Abstractions;
using System.Runtime.CompilerServices;

namespace Streamix.Tests;

[TestFixture]
public class ResilienceOperatorTests
{
    private TestClock _clock;

    [SetUp]
    public void SetUp()
    {
        _clock = new TestClock();
    }

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
        var source = new SlowAsyncEnumerable<int>(_clock, TimeSpan.FromSeconds(2));
        var timeoutSingle = ((Single<int>)Single.From<int>(source, _clock)).Timeout(TimeSpan.FromSeconds(1));

        var task = timeoutSingle.ToTask();

        _clock.AdvanceBy(TimeSpan.FromSeconds(1.1));
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
        var source = new SlowAsyncEnumerable<int>(_clock, TimeSpan.FromSeconds(2));
        var timeoutStream = ((Stream<int>)Stream.From<int>(source, _clock)).Timeout(TimeSpan.FromSeconds(1));

        var task = timeoutStream.ForEachAsync(_ => { });

        _clock.AdvanceBy(TimeSpan.FromSeconds(1.1));
        Assert.ThrowsAsync<TimeoutException>(async () => await task);
    }

    [Test]
    public async Task Timeout_ShouldSucceedWhenEmissionIsFast()
    {
        var source = new SlowAsyncEnumerable<int>(_clock, TimeSpan.FromSeconds(0.5));
        var timeoutStream = ((Stream<int>)Stream.From<int>(source, _clock)).Timeout(TimeSpan.FromSeconds(1));
        var results = new List<int>();

        var task = Task.Run(async () =>
        {
            await using var e = timeoutStream.Take(2).GetAsyncEnumerator();
            while (await e.MoveNextAsync())
            {
                results.Add(e.Current);
            }
        });

        await _clock.WaitForDelay(2, TimeSpan.FromSeconds(2)); // MoveNext + Timeout Delay
        _clock.AdvanceBy(TimeSpan.FromSeconds(0.6));
        for (int i = 0; i < 100 && results.Count < 1; i++) await Task.Delay(10);
        Assert.That(results, Has.Count.EqualTo(1));

        await _clock.WaitForDelay(2, TimeSpan.FromSeconds(2)); // Next MoveNext + Timeout Delay
        _clock.AdvanceBy(TimeSpan.FromSeconds(0.6));
        for (int i = 0; i < 100 && results.Count < 2; i++) await Task.Delay(10);
        await task;
        Assert.That(results, Has.Count.EqualTo(2));
    }

    private class SlowAsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        private readonly IClock _clock;
        private readonly TimeSpan _delay;

        public SlowAsyncEnumerable(IClock clock, TimeSpan delay)
        {
            _clock = clock;
            _delay = delay;
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new Enumerator(_clock, _delay, cancellationToken);
        }

        private class Enumerator : IAsyncEnumerator<T>
        {
            private readonly IClock _clock;
            private readonly TimeSpan _delay;
            private readonly CancellationToken _cancellationToken;

            public Enumerator(IClock clock, TimeSpan delay, CancellationToken cancellationToken)
            {
                _clock = clock;
                _delay = delay;
                _cancellationToken = cancellationToken;
            }

            public T Current => default!;

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            public async ValueTask<bool> MoveNextAsync()
            {
                await _clock.Delay(_delay, _cancellationToken);
                return true;
            }
        }
    }
}
