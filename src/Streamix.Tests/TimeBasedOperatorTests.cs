using NUnit.Framework;
using Streamix.Tests.Implementations;

namespace Streamix.Tests;

[TestFixture]
public class TimeBasedOperatorTests
{
    class ManualAsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        private readonly System.Threading.Channels.Channel<T> _channel = System.Threading.Channels.Channel.CreateUnbounded<T>();

        public ManualAsyncEnumerable(IClock clock) { }

        public void Push(T item) => _channel.Writer.TryWrite(item);
        public void Complete() => _channel.Writer.TryComplete();

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => _channel.Reader.ReadAllAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);
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
        var throttled = Stream.From<int>(source, clock).Throttle(TimeSpan.FromSeconds(1));

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

    [Test]
    public async Task Timer_ShouldEmit_Single_Zero_After_DueTime()
    {
        var clock = new TestClock();
        var timer = Stream.FromTimer(TimeSpan.FromSeconds(2), clock);

        var subscriber = new TestSubscriber<long>();
        var task = Task.Run(() => subscriber.RunAsync(timer, default));

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        Assert.That(subscriber.Items, Is.Empty);

        clock.AdvanceBy(TimeSpan.FromSeconds(2));
        await task;

        Assert.That(subscriber.Items, Is.EqualTo(new[] { 0L }));
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Timer_With_Zero_DueTime_ShouldEmit_Immediately()
    {
        var clock = new TestClock();
        var timer = Stream.FromTimer(TimeSpan.Zero, clock);

        var subscriber = await TestSubscriber<long>.SubscribeAsync(timer);

        subscriber.AssertValues(0L);
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Timer_ShouldRespectCancellation()
    {
        var clock = new TestClock();
        var timer = Stream.FromTimer(TimeSpan.FromSeconds(1), clock);
        using var cts = new CancellationTokenSource();

        var subscribeTask = TestSubscriber<long>.SubscribeAsync(timer, cts.Token);

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        await cts.CancelAsync();

        var subscriber = await subscribeTask;
        subscriber.AssertValueCount(0);
        subscriber.AssertNotComplete();
    }

    [Test]
    public void Timer_ShouldThrowForNegativeDueTime()
    {
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            var timer = Stream.FromTimer(TimeSpan.FromSeconds(-1));
            await foreach (var item in timer) { }
        });
    }

    [Test]
    public async Task Timer_ShouldBe_Cold_Per_Subscription()
    {
        var clock = new TestClock();
        var timer = Stream.FromTimer(TimeSpan.FromSeconds(1), clock);

        var first = new TestSubscriber<long>();
        var firstTask = Task.Run(() => first.RunAsync(timer, default));
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await firstTask;

        var second = new TestSubscriber<long>();
        var secondTask = Task.Run(() => second.RunAsync(timer, default));
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        Assert.That(second.Items, Is.Empty);
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await secondTask;

        first.AssertValues(0L);
        first.AssertComplete();
        second.AssertValues(0L);
        second.AssertComplete();
    }

    [Test]
    public async Task Poll_ShouldEmit_Results_On_Each_Interval()
    {
        var clock = new TestClock();
        var calls = 0;
        var poll = Stream.Poll<int>(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), ct => ValueTask.FromResult(++calls), clock);

        var subscriber = new TestSubscriber<int>();
        var task = Task.Run(() => subscriber.RunAsync(poll.Take(3), default));

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        Assert.That(subscriber.Items, Is.Empty);

        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && subscriber.Items.Count < 1; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EqualTo(new[] { 1 }));

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && subscriber.Items.Count < 2; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EqualTo(new[] { 1, 2 }));

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await task;

        Assert.That(subscriber.Items, Is.EqualTo(new[] { 1, 2, 3 }));
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Poll_ShouldPropagate_CancellationToken()
    {
        var clock = new TestClock();
        CancellationToken observedToken = default;
        var poll = Stream.Poll<int>(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), ct =>
        {
            observedToken = ct;
            return ValueTask.FromResult(42);
        }, clock);

        using var cts = new CancellationTokenSource();
        var subscriberTask = TestSubscriber<int>.SubscribeAsync(poll.Take(1), cts.Token);

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));

        var subscriber = await subscriberTask;
        subscriber.AssertValues(42);
        subscriber.AssertComplete();
        Assert.That(observedToken, Is.EqualTo(cts.Token));
    }

    [Test]
    public async Task Poll_ShouldRespect_Cancellation_While_Waiting()
    {
        var clock = new TestClock();
        var poll = Stream.Poll<int>(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), ct => ValueTask.FromResult(1), clock);
        using var cts = new CancellationTokenSource();

        var subscribeTask = TestSubscriber<int>.SubscribeAsync(poll, cts.Token);

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        await cts.CancelAsync();

        var subscriber = await subscribeTask;
        subscriber.AssertValueCount(0);
        subscriber.AssertNotComplete();
    }

    [Test]
    public void Poll_ShouldThrowFor_NonPositive_Interval()
    {
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            var poll = Stream.Poll<int>(TimeSpan.Zero, ct => ValueTask.FromResult(1));
            await foreach (var item in poll) { }
        });
    }

    [Test]
    public void Poll_ShouldPropagate_Poll_Exception()
    {
        var clock = new TestClock();
        var poll = Stream.Poll<int>(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), ct => ValueTask.FromException<int>(new InvalidOperationException("poll failed")), clock);

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var enumerator = poll.GetAsyncEnumerator();
            var moveNextTask = enumerator.MoveNextAsync().AsTask();
            await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
            clock.AdvanceBy(TimeSpan.FromSeconds(1));
            await moveNextTask;
        });
    }

    [Test]
    public async Task Poll_ShouldBe_Cold_Per_Subscription()
    {
        var clock = new TestClock();
        var calls = 0;
        var poll = Stream.Poll<int>(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), ct => ValueTask.FromResult(++calls), clock);

        var first = new TestSubscriber<int>();
        var firstTask = Task.Run(() => first.RunAsync(poll.Take(1), default));
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await firstTask;

        var second = new TestSubscriber<int>();
        var secondTask = Task.Run(() => second.RunAsync(poll.Take(1), default));
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        Assert.That(second.Items, Is.Empty);
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await secondTask;

        first.AssertValues(1);
        first.AssertComplete();
        second.AssertValues(2);
        second.AssertComplete();
    }

    [Test]
    public async Task Poll_ShouldNotAccumulate_When_Consumer_Is_Slow()
    {
        var clock = new TestClock();
        var calls = 0;
        var poll = Stream.Poll<int>(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), ct => ValueTask.FromResult(++calls), clock);
        var semaphore = new SemaphoreSlim(0);
        var results = new List<int>();

        var task = Task.Run(async () =>
        {
            await foreach (var item in poll.Take(2))
            {
                results.Add(item);
                await semaphore.WaitAsync();
            }
        });

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && results.Count < 1; i++) await Task.Delay(10);
        Assert.That(results, Is.EqualTo(new[] { 1 }));
        Assert.That(calls, Is.EqualTo(1));

        clock.AdvanceBy(TimeSpan.FromSeconds(5));
        await Task.Delay(100);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(calls, Is.EqualTo(1));

        semaphore.Release();

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && results.Count < 2; i++) await Task.Delay(10);
        Assert.That(results, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(calls, Is.EqualTo(2));

        semaphore.Release();
        await task;
    }

    [Test]
    public void Interval_ShouldThrowForNegativeDueTime()
    {
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            var interval = Stream.Interval(TimeSpan.FromSeconds(-1), TimeSpan.FromSeconds(1));
            await foreach (var item in interval) { }
        });
    }

    [Test]
    public void Interval_ShouldThrowForZeroPeriod()
    {
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            var interval = Stream.Interval(TimeSpan.Zero, TimeSpan.Zero);
            await foreach (var item in interval) { }
        });
    }

    [Test]
    public void Interval_ShouldThrowForNegativePeriod()
    {
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            var interval = Stream.Interval(TimeSpan.Zero, TimeSpan.FromSeconds(-1));
            await foreach (var item in interval) { }
        });
    }

    [Test]
    public async Task Never_ShouldNeverEmitOrComplete()
    {
        var never = Stream.Never<int>();
        using var cts = new CancellationTokenSource();

        var subscriber = new TestSubscriber<int>();
        var task = Task.Run(() => subscriber.RunAsync(never, cts.Token));

        await Task.Delay(100);
        Assert.That(subscriber.Items, Is.Empty);
        subscriber.AssertNotComplete();

        await cts.CancelAsync();
        await task;

        // TestSubscriber catches OperationCanceledException and sets completed = false by default
        subscriber.AssertNotComplete();
    }

    [Test]
    public async Task Timer_ShouldEmitAfterDelayAndComplete()
    {
        var clock = new TestClock();
        var timer = Stream.FromTimer(TimeSpan.FromSeconds(1), clock);

        var subscriber = new TestSubscriber<long>();
        var task = Task.Run(() => subscriber.RunAsync(timer, default));

        // Initially no items
        Assert.That(subscriber.Items, Is.Empty);

        // Wait for delay
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));

        await task;
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 0L }));
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Poll_ShouldEmitPeriodically()
    {
        var clock = new TestClock();
        int counter = 0;
        var poll = Stream.Poll(TimeSpan.Zero, TimeSpan.FromSeconds(1), ct => ValueTask.FromResult(counter++), clock);

        var subscriber = new TestSubscriber<int>();
        var task = Task.Run(() => subscriber.RunAsync(poll.Take(3), default));

        // First item emitted immediately (due to zero dueTime)
        for (int i = 0; i < 100 && subscriber.Items.Count < 1; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 0 }));

        // Advance 1s -> second item
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 100 && subscriber.Items.Count < 2; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 0, 1 }));

        // Advance another 1s -> third item
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await task;
        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 0, 1, 2 }));
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Poll_ShouldRespectCancellation()
    {
        var clock = new TestClock();
        var poll = Stream.Poll<int>(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), ct => ValueTask.FromResult(1), clock);
        using var cts = new CancellationTokenSource();

        var subscribeTask = TestSubscriber<int>.SubscribeAsync(poll, cts.Token);

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        await cts.CancelAsync();

        var subscriber = await subscribeTask;
        subscriber.AssertValueCount(0);
        subscriber.AssertNotComplete();
    }

    [Test]
    public async Task BufferByTime_ShouldGroupItemsByInterval()
    {
        var clock = new TestClock();
        var source = new ManualAsyncEnumerable<int>(clock);
        var buffered = Stream.From<int>(source, clock).BufferByTime(TimeSpan.FromSeconds(1));

        var subscriber = new TestSubscriber<IList<int>>();
        var task = Task.Run(() => subscriber.RunAsync(buffered, default));

        // 1. Push items
        source.Push(1);
        source.Push(2);
        await Task.Delay(100); // Allow items to propagate to the operator

        // 2. Advance clock to trigger buffer emission
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 500 && subscriber.Items.Count < 1; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Has.Count.AtLeast(1));
        Assert.That(subscriber.Items[0], Is.EquivalentTo(new[] { 1, 2 }));

        // 3. Push more items and advance again
        source.Push(3);
        await Task.Delay(100); // Allow item to propagate
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 500 && subscriber.Items.Count < 2; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Has.Count.AtLeast(2));
        Assert.That(subscriber.Items[1], Is.EquivalentTo(new[] { 3 }));

        source.Complete();
        await task;
        subscriber.AssertComplete();
    }

    [Test]
    public async Task BufferByTime_ShouldRespectMaxCount()
    {
        var clock = new TestClock();
        var source = new ManualAsyncEnumerable<int>(clock);
        var buffered = Stream.From<int>(source, clock).BufferByTime(TimeSpan.FromSeconds(1), maxCount: 2);

        var subscriber = new TestSubscriber<IList<int>>();
        var task = Task.Run(() => subscriber.RunAsync(buffered, default));

        // 1. Push 2 items (maxCount reached)
        source.Push(1);
        source.Push(2);
        for (int i = 0; i < 500 && subscriber.Items.Count < 1; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Has.Count.AtLeast(1));
        Assert.That(subscriber.Items[0], Is.EquivalentTo(new[] { 1, 2 }));

        // 2. Push 1 item and advance clock
        source.Push(3);
        await Task.Delay(100); // Allow item to propagate
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 500 && subscriber.Items.Count < 2; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Has.Count.AtLeast(2));
        Assert.That(subscriber.Items[1], Is.EquivalentTo(new[] { 3 }));

        source.Complete();
        await task;
    }

    [Test]
    public async Task BufferByTime_ShouldFlushOnCompletion()
    {
        var clock = new TestClock();
        var source = new ManualAsyncEnumerable<int>(clock);
        var buffered = Stream.From<int>(source, clock).BufferByTime(TimeSpan.FromSeconds(1));

        var subscriber = new TestSubscriber<IList<int>>();
        var task = Task.Run(() => subscriber.RunAsync(buffered, default));

        source.Push(1);
        source.Complete();
        await task;

        Assert.That(subscriber.Items, Has.Count.EqualTo(1));
        Assert.That(subscriber.Items[0], Is.EquivalentTo(new[] { 1 }));
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Sample_ShouldEmitLatestItemInInterval()
    {
        var clock = new TestClock();
        var source = new ManualAsyncEnumerable<int>(clock);
        var sampled = Stream.From<int>(source, clock).Sample(TimeSpan.FromSeconds(1));

        var subscriber = new TestSubscriber<int>();
        var task = Task.Run(() => subscriber.RunAsync(sampled, default));

        // 1. Push multiple items in one interval
        source.Push(1);
        source.Push(2);
        source.Push(3);
        await Task.Delay(100); // Allow items to propagate

        // 2. Advance clock
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 500 && subscriber.Items.Count < 1; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Has.Count.AtLeast(1));
        Assert.That(subscriber.Items[0], Is.EqualTo(3));

        // 3. Interval with no items
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await Task.Delay(100);
        Assert.That(subscriber.Items, Has.Count.EqualTo(1));

        // 4. Another interval
        source.Push(4);
        await Task.Delay(100); // Allow item to propagate
        await clock.WaitForDelay(1, TimeSpan.FromSeconds(2));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        for (int i = 0; i < 500 && subscriber.Items.Count < 2; i++) await Task.Delay(10);
        Assert.That(subscriber.Items, Has.Count.AtLeast(2));
        Assert.That(subscriber.Items[1], Is.EqualTo(4));

        source.Complete();
        await task;
        subscriber.AssertComplete();
    }

    [Test]
    public async Task Sample_ShouldEmitFinalItemOnCompletion()
    {
        var clock = new TestClock();
        var source = new ManualAsyncEnumerable<int>(clock);
        var sampled = Stream.From<int>(source, clock).Sample(TimeSpan.FromSeconds(1));

        var subscriber = new TestSubscriber<int>();
        var task = Task.Run(() => subscriber.RunAsync(sampled, default));

        source.Push(1);
        source.Complete();
        await task;

        Assert.That(subscriber.Items, Is.EquivalentTo(new[] { 1 }));
        subscriber.AssertComplete();
    }
}
