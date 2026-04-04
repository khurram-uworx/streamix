using NUnit.Framework;
using Streamix.Implementations;

namespace Streamix.Tests;

[TestFixture]
public class TimeBasedOperatorTests
{
    class ManualAsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        TaskCompletionSource<bool> nextTcs = new();
        readonly Queue<T> queue = new();
        bool completed;
        readonly IClock clock;

        public ManualAsyncEnumerable(IClock clock) => this.clock = clock;

        public void Push(T item)
        {
            lock (queue)
            {
                queue.Enqueue(item);
            }
            nextTcs.TrySetResult(true);
        }

        public void Complete()
        {
            completed = true;
            nextTcs.TrySetResult(false);
        }

        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                await nextTcs.Task;
                T item = default!;
                bool hasItem = false;

                lock (queue)
                {
                    if (queue.Count > 0)
                    {
                        item = queue.Dequeue();
                        hasItem = true;
                    }
                }

                if (hasItem)
                {
                    yield return item;
                }

                if (completed && queue.Count == 0) yield break;

                // Reset the TCS for the next iteration
                nextTcs = new TaskCompletionSource<bool>();

                // Note: This manual implementation is simplified.
                await Task.Delay(1, cancellationToken); // yield to allow next push
            }
        }
    }

    [Test]
    public async Task Delay_ShouldRespectCancellation()
    {
        var clock = new TestClock();
        var source = Stream.Range(1, 10);
        var delayed = Stream.From<int>(source, clock).Delay(TimeSpan.FromSeconds(1));
        using var cts = new CancellationTokenSource();

        var subscribeTask = TestSubscriber<int>.SubscribeAsync(delayed, cts.Token);

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        await cts.CancelAsync();

        var subscriber = await subscribeTask;
        subscriber.AssertValueCount(0);
        subscriber.AssertNotComplete();
    }

    [Test]
    public async Task Delay_ShouldPostponeEmission()
    {
        var clock = new TestClock();
        var source = Stream.Range(1, 3);
        var delayed = Stream.From<int>(source, clock).Delay(TimeSpan.FromSeconds(1));

        var subscriber = new TestSubscriber<int>();
        var task = Task.Run(() => subscriber.RunAsync(delayed, default));

        // Initially no items
        Assert.That(subscriber.Items, Is.Empty);

        // Advance 1s -> first item
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && subscriber.Items.Count < 1; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Has.Count.EqualTo(1));
        Assert.That(subscriber.Items[0], Is.EqualTo(1));

        // Advance another 1s -> second item
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && subscriber.Items.Count < 2; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Has.Count.EqualTo(2));
        Assert.That(subscriber.Items[1], Is.EqualTo(2));

        // Advance another 1s -> third item
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await task;
        Assert.That(subscriber.Items, Has.Count.EqualTo(3));
        Assert.That(subscriber.Items[2], Is.EqualTo(3));
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Throttle_ShouldEmitOnlyFirstItemInInterval()
    {
        var clock = new TestClock();
        var source = new ManualAsyncEnumerable<int>(clock);
        var throttled = ((Stream<int>)Stream.From<int>(source, clock)).Throttle(TimeSpan.FromSeconds(1));

        var subscriber = new TestSubscriber<int>();
        var task = Task.Run(() => subscriber.RunAsync(throttled, default));

        // 1. Emit first item
        source.Push(1);
        for (int i = 0; i < 100 && subscriber.Items.Count < 1; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 1 }));

        // 2. Emit second item immediately (should be throttled)
        source.Push(2);
        await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 1 }));

        // 3. Advance clock by 0.5s and emit third item (still throttled)
        clock.AdvanceBy(TimeSpan.FromMilliseconds(500));
        source.Push(3);
        await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 1 }));

        // 4. Advance clock by another 0.6s (total 1.1s) and emit fourth item (should pass)
        clock.AdvanceBy(TimeSpan.FromMilliseconds(600));
        source.Push(4);
        for (int i = 0; i < 100 && subscriber.Items.Count < 2; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 1, 4 }));

        source.Complete();
        await task;
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Interval_ShouldEmitSequentialLongs()
    {
        var clock = new TestClock();
        var interval = Stream.Interval(TimeSpan.Zero, TimeSpan.FromSeconds(1), clock);

        var subscriber = new TestSubscriber<long>();
        var task = Task.Run(() => subscriber.RunAsync(interval.Take(3), default));

        // First item emitted immediately due to TimeSpan.Zero dueTime
        for (int i = 0; i < 100 && subscriber.Items.Count < 1; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 0L }));

        // Advance 1s -> second item
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && subscriber.Items.Count < 2; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 0L, 1L }));

        // Advance another 1s -> third item
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await task;
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 0L, 1L, 2L }));
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Interval_WithDueTime_ShouldRespectInitialDelay()
    {
        var clock = new TestClock();
        var interval = Stream.Interval(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), clock);

        var subscriber = new TestSubscriber<long>();
        var task = Task.Run(() => subscriber.RunAsync(interval.Take(1), default));

        // Wait for first delay (dueTime)
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        Assert.That(subscriber.Items, Is.Empty);

        // Advance 2s -> first item
        clock.AdvanceBy(TimeSpan.FromSeconds(2));
        await task;
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 0L }));
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Interval_ShouldNotAccumulateTicks()
    {
        var clock = new TestClock();
        var interval = Stream.Interval(TimeSpan.Zero, TimeSpan.FromSeconds(1), clock);
        var semaphore = new SemaphoreSlim(0);
        var results = new List<long>();

        var task = Task.Run(async () =>
        {
            await foreach (var item in interval.Take(2))
            {
                results.Add(item);
                await semaphore.WaitAsync(); // Simulate slow consumer
            }
        });

        // 1. First item emitted immediately
        for (int i = 0; i < 100 && results.Count < 1; i++) await Task.Delay(10);
        Assert.That(results, Is.EquivalentTo(new[] { 0L }));

        // 2. Advance time by 5 seconds.
        // Even though 5 seconds passed, the second item should not have been emitted yet
        // because the consumer is still processing the first item and hasn't called MoveNextAsync yet.
        clock.AdvanceBy(TimeSpan.FromSeconds(5));
        await Task.Delay(100);
        Assert.That(results, Has.Count.EqualTo(1));

        // 3. Release consumer.
        semaphore.Release();

        // Now the consumer finishes processing first item and calls MoveNextAsync() for the second.
        // The iterator will then reach its first 'await clock.Delay(period)'.
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));

        // 4. Advance clock again to trigger the second emission
        clock.AdvanceBy(TimeSpan.FromSeconds(1));

        for (int i = 0; i < 100 && results.Count < 2; i++) await Task.Delay(10);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[1], Is.EqualTo(1L));

        semaphore.Release(); // allow Take(2) to complete
        await task;
    }

    [Test]
    public async Task Interval_ShouldRespectCancellation()
    {
        var clock = new TestClock();
        var interval = Stream.Interval(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), clock);
        using var cts = new CancellationTokenSource();

        var subscribeTask = TestSubscriber<long>.SubscribeAsync(interval, cts.Token);

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        await cts.CancelAsync();

        var subscriber = await subscribeTask;
        subscriber.AssertValueCount(0);
        subscriber.AssertNotComplete();
    }
}
