using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

[TestFixture]
public class TimeBasedOperatorTests
{
    private TestClock _clock;

    [SetUp]
    public void SetUp()
    {
        _clock = new TestClock();
    }

    [Test]
    public async Task Delay_ShouldRespectCancellation()
    {
        var source = Stream.Range(1, 10);
        var delayed = Stream.From<int>(source, _clock).Delay(TimeSpan.FromSeconds(1));
        using var cts = new CancellationTokenSource();

        var task = Task.Run(async () =>
        {
            await foreach (var item in delayed.WithCancellation(cts.Token))
            {
            }
        });

        await _clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        await cts.CancelAsync();

        Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
    }

    [Test]
    public async Task Delay_ShouldPostponeEmission()
    {
        var source = Stream.Range(1, 3);
        var delayed = Stream.From<int>(source, _clock).Delay(TimeSpan.FromSeconds(1));
        var results = new List<int>();

        var task = Task.Run(async () =>
        {
            await foreach (var item in delayed)
            {
                results.Add(item);
            }
        });

        // Initially no items
        Assert.That(results, Is.Empty);

        // Advance 1s -> first item
        await _clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        _clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && results.Count < 1; i++) await Task.Delay(10);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0], Is.EqualTo(1));

        // Advance another 1s -> second item
        await _clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        _clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && results.Count < 2; i++) await Task.Delay(10);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[1], Is.EqualTo(2));

        // Advance another 1s -> third item
        await _clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        _clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await task;
        Assert.That(results, Has.Count.EqualTo(3));
        Assert.That(results[2], Is.EqualTo(3));
    }

    [Test]
    public async Task Throttle_ShouldEmitOnlyFirstItemInInterval()
    {
        var source = new ManualAsyncEnumerable<int>(_clock);
        var throttled = ((Stream<int>)Stream.From<int>(source, _clock)).Throttle(TimeSpan.FromSeconds(1));
        var results = new List<int>();

        var task = Task.Run(async () =>
        {
            await foreach (var item in throttled)
            {
                results.Add(item);
            }
        });

        // 1. Emit first item
        source.Push(1);
        await Task.Delay(10);
        Assert.That(results, Is.EquivalentTo(new[] { 1 }));

        // 2. Emit second item immediately (should be throttled)
        source.Push(2);
        await Task.Delay(10);
        Assert.That(results, Is.EquivalentTo(new[] { 1 }));

        // 3. Advance clock by 0.5s and emit third item (still throttled)
        _clock.AdvanceBy(TimeSpan.FromMilliseconds(500));
        source.Push(3);
        await Task.Delay(10);
        Assert.That(results, Is.EquivalentTo(new[] { 1 }));

        // 4. Advance clock by another 0.6s (total 1.1s) and emit fourth item (should pass)
        _clock.AdvanceBy(TimeSpan.FromMilliseconds(600));
        source.Push(4);
        await Task.Delay(10);
        Assert.That(results, Is.EquivalentTo(new[] { 1, 4 }));

        source.Complete();
        await task;
    }

    private class ManualAsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        private readonly TaskCompletionSource<bool> _nextTcs = new();
        private readonly Queue<T> _queue = new();
        private bool _completed;
        private readonly IClock _clock;

        public ManualAsyncEnumerable(IClock clock) => _clock = clock;

        public void Push(T item)
        {
            lock (_queue)
            {
                _queue.Enqueue(item);
            }
            _nextTcs.TrySetResult(true);
        }

        public void Complete()
        {
            _completed = true;
            _nextTcs.TrySetResult(false);
        }

        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                await _nextTcs.Task;
                T item = default!;
                bool hasItem = false;

                lock (_queue)
                {
                    if (_queue.Count > 0)
                    {
                        item = _queue.Dequeue();
                        hasItem = true;
                    }
                }

                if (hasItem)
                {
                    yield return item;
                }

                if (_completed && _queue.Count == 0) yield break;

                // Note: This manual implementation is simplified.
                await Task.Delay(1, cancellationToken); // yield to allow next push
            }
        }
    }
}
