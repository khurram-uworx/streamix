using NUnit.Framework;
using Streamix;

namespace Streamix.Tests.Extensions;

[TestFixture]
public class TimeseriesTests
{
    [Test]
    public void Timestamped_ShouldHoldValueAndTimestamp()
    {
        var now = DateTimeOffset.UtcNow;
        var ts = new Timestamped<int>(42, now);

        Assert.That(ts.Value, Is.EqualTo(42));
        Assert.That(ts.Timestamp, Is.EqualTo(now));
    }

    [Test]
    public void Timestamped_Create_ShouldCreateInstance()
    {
        var now = DateTimeOffset.UtcNow;
        var ts = Timestamped.Create(42, now);

        Assert.That(ts.Value, Is.EqualTo(42));
        Assert.That(ts.Timestamp, Is.EqualTo(now));
    }

    [Test]
    public void Timestamped_Equality_ShouldWork()
    {
        var now = DateTimeOffset.UtcNow;
        var ts1 = new Timestamped<int>(42, now);
        var ts2 = new Timestamped<int>(42, now);
        var ts3 = new Timestamped<int>(43, now);

        Assert.That(ts1, Is.EqualTo(ts2));
        Assert.That(ts1, Is.Not.EqualTo(ts3));
    }

    [Test]
    public async Task MapWithTimestamp_ShouldWork()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var source = Stream.From(1, 2, 3);
        var result = await source.MapWithTimestamp(x => start.AddMinutes(x)).ToListAsync();

        Assert.That(result, Has.Count.EqualTo(3));
        Assert.That(result[0], Is.EqualTo(Timestamped.Create(1, start.AddMinutes(1))));
        Assert.That(result[1], Is.EqualTo(Timestamped.Create(2, start.AddMinutes(2))));
        Assert.That(result[2], Is.EqualTo(Timestamped.Create(3, start.AddMinutes(3))));
    }

    [Test]
    public async Task WindowByTime_Tumbling_ShouldGroupItemsCorrectly()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(5)),
            Timestamped.Create(3, start.AddMinutes(11)),
            Timestamped.Create(4, start.AddMinutes(15)),
            Timestamped.Create(5, start.AddMinutes(21)),
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<Timestamped<int>>>();
        foreach (var window in windowStreams)
        {
            windowList.Add(await window.ToListAsync());
        }

        Assert.That(windowList, Has.Count.EqualTo(3));
        Assert.That(windowList[0].Select(x => x.Value), Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(windowList[1].Select(x => x.Value), Is.EquivalentTo(new[] { 3, 4 }));
        Assert.That(windowList[2].Select(x => x.Value), Is.EquivalentTo(new[] { 5 }));
    }

    [Test]
    public async Task WindowByTime_Tumbling_ShouldHandleExactBoundaries()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        var items = new[]
        {
            Timestamped.Create(1, start), // Inclusive
            Timestamped.Create(2, start.AddMinutes(10)), // Exclusive for first, inclusive for second
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<Timestamped<int>>>();
        foreach (var window in windowStreams)
        {
            windowList.Add(await window.ToListAsync());
        }

        Assert.That(windowList, Has.Count.EqualTo(2));
        Assert.That(windowList[0].Select(x => x.Value), Is.EquivalentTo(new[] { 1 }));
        Assert.That(windowList[1].Select(x => x.Value), Is.EquivalentTo(new[] { 2 }));
    }

    [Test]
    public async Task WindowByTime_Tumbling_ShouldHandleEmptyStream()
    {
        var source = Stream.Empty<Timestamped<int>>();
        var windows = source.WindowByTime(TimeSpan.FromMinutes(10));

        var count = 0;
        await foreach (var window in windows)
        {
            count++;
        }

        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task WindowByTime_Tumbling_ShouldHandleSingleItem()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var items = new[] { Timestamped.Create(1, start) };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration: TimeSpan.FromMinutes(10));

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<Timestamped<int>>>();
        foreach (var window in windowStreams)
        {
            windowList.Add(await window.ToListAsync());
        }

        Assert.That(windowList, Has.Count.EqualTo(1));
        Assert.That(windowList[0].Select(x => x.Value), Is.EquivalentTo(new[] { 1 }));
    }

    [Test]
    public async Task WindowByTime_Sliding_ShouldGroupItemsCorrectly()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var slide = TimeSpan.FromMinutes(5);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(6)),
            Timestamped.Create(3, start.AddMinutes(11)),
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, slide);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<int>>();
        var tasks = new List<Task>();
        foreach (var w in windowStreams)
        {
            var inner = w;
            tasks.Add(Task.Run(async () =>
            {
                var list = await inner.Map(x => x.Value).ToListAsync();
                lock (windowList) windowList.Add(list);
            }));
        }
        await Task.WhenAll(tasks);

        Assert.That(windowList, Has.Count.EqualTo(4));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1, 2 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 2, 3 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 3 }));
    }

    [Test]
    public async Task WindowByTime_Sliding_SparseSliding_ShouldHaveGaps()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(5);
        var slide = TimeSpan.FromMinutes(10);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(6)), // In gap [5, 10)
            Timestamped.Create(3, start.AddMinutes(11)),
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, slide);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<int>>();
        foreach (var w in windowStreams)
        {
            windowList.Add(await w.Map(x => x.Value).ToListAsync());
        }

        Assert.That(windowList, Has.Count.EqualTo(2));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 3 }));
    }

    [Test]
    public async Task WindowByTime_Sliding_ExactBoundaries_ShouldHandleInclusionCorrectly()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var slide = TimeSpan.FromMinutes(5);

        var items = new[]
        {
            Timestamped.Create(1, start), // Start of Window starting at 0
            Timestamped.Create(2, start.AddMinutes(5)), // Start of Window starting at 5
            Timestamped.Create(3, start.AddMinutes(10)), // Start of Window starting at 10
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, slide);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<int>>();
        var tasks = new List<Task>();
        foreach (var w in windowStreams)
        {
            var inner = w;
            tasks.Add(Task.Run(async () =>
            {
                var list = await inner.Map(x => x.Value).ToListAsync();
                lock (windowList) windowList.Add(list);
            }));
        }
        await Task.WhenAll(tasks);

        Assert.That(windowList, Has.Count.EqualTo(4));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1, 2 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 2, 3 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 3 }));
    }

    [Test]
    public async Task WindowByTime_Backpressure_Fail_ShouldThrow()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        // Produce 5 items for the same window, with capacity 2 and Fail mode
        var items = Enumerable.Range(1, 5).Select(i => Timestamped.Create(i, start.AddMinutes(1)));
        var source = Stream.From<Timestamped<int>>(items);
        var windows = source.WindowByTime(duration, capacity: 2, mode: ChannelBackpressureMode.Fail);

        var ex = Assert.ThrowsAsync<BackpressureException>(async () =>
        {
            await foreach (var window in windows)
            {
                // We don't consume the window items, so it should fill up and fail upstream
                await Task.Delay(100);
            }
        });

        Assert.That(ex.Message, Does.Contain("Channel boundary is full"));
    }

    [Test]
    public async Task WindowByTime_Backpressure_DropOldest_ShouldDrop()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        // Produce 5 items for the same window, with capacity 2 and DropOldest mode
        var items = Enumerable.Range(1, 5).Select(i => Timestamped.Create(i, start.AddMinutes(1)));
        var source = Stream.From<Timestamped<int>>(items);
        var windows = source.WindowByTime(duration, capacity: 2, mode: ChannelBackpressureMode.DropOldest);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<Timestamped<int>>>();
        foreach (var window in windowStreams)
        {
            // Wait to ensure producer can fill the window
            await Task.Delay(200);
            var list = await window.ToListAsync();
            windowList.Add(list);
        }

        Assert.That(windowList, Has.Count.EqualTo(1));
        // Capacity 2, DropOldest. 1, 2, 3(drops 1), 4(drops 2), 5(drops 3) -> should have 4, 5
        Assert.That(windowList[0].Select(x => x.Value), Is.EqualTo(new[] { 4, 5 }));
    }

    [Test]
    public async Task WindowByTime_Backpressure_DropNewest_ShouldDrop()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        // Produce 5 items for the same window, with capacity 2 and DropNewest mode
        var items = Enumerable.Range(1, 5).Select(i => Timestamped.Create(i, start.AddMinutes(1)));
        var source = Stream.From<Timestamped<int>>(items);
        var windows = source.WindowByTime(duration, capacity: 2, mode: ChannelBackpressureMode.DropNewest);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<Timestamped<int>>>();
        foreach (var window in windowStreams)
        {
            // Wait to ensure producer can fill the window
            await Task.Delay(200);
            var list = await window.ToListAsync();
            windowList.Add(list);
        }

        Assert.That(windowList, Has.Count.EqualTo(1));
        // Capacity 2, DropNewest. [1, 2] -> 3 comes, drops 2 -> [1, 3] -> 4 comes, drops 3 -> [1, 4] -> 5 comes, drops 4 -> [1, 5]
        Assert.That(windowList[0].Select(x => x.Value), Is.EqualTo(new[] { 1, 5 }));
    }

    [Test]
    public async Task WindowByTime_Backpressure_LatestOnly_ShouldDrop()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        // Produce 5 items for the same window, with LatestOnly mode (capacity ignored, effective capacity 1)
        var items = Enumerable.Range(1, 5).Select(i => Timestamped.Create(i, start.AddMinutes(1)));
        var source = Stream.From<Timestamped<int>>(items);
        var windows = source.WindowByTime(duration, mode: ChannelBackpressureMode.LatestOnly);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<Timestamped<int>>>();
        foreach (var window in windowStreams)
        {
            // Wait to ensure producer can fill the window
            await Task.Delay(200);
            var list = await window.ToListAsync();
            windowList.Add(list);
        }

        Assert.That(windowList, Has.Count.EqualTo(1));
        // Effective capacity 1, DropOldest behavior.
        // 1, 2(drops 1), 3(drops 2), 4(drops 3), 5(drops 4) -> should have 5
        Assert.That(windowList[0].Select(x => x.Value), Is.EqualTo(new[] { 5 }));
    }

    [Test]
    public async Task WindowByTime_Backpressure_Wait_ShouldPropagate()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        var itemsEmitted = 0;
        var source = Stream.Create<Timestamped<int>>(async emitter =>
        {
            for (int i = 1; i <= 5; i++)
            {
                await emitter.EmitAsync(Timestamped.Create(i, start.AddMinutes(1)));
                itemsEmitted++;
            }
        });

        // Capacity 2, mode Wait
        var windows = source.WindowByTime(duration, capacity: 2, mode: ChannelBackpressureMode.Wait);

        var windowEnumerator = windows.GetAsyncEnumerator();
        Assert.That(await windowEnumerator.MoveNextAsync(), Is.True);
        var window = windowEnumerator.Current;

        // Do NOT consume the window yet.
        // Producer should be blocked after emitting 3 or 4 items (capacity + current + internal)
        await Task.Delay(100);
        Assert.That(itemsEmitted, Is.LessThanOrEqualTo(4));

        var list = await window.ToListAsync();
        Assert.That(list.Select(x => x.Value), Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
        Assert.That(itemsEmitted, Is.EqualTo(5));
    }

    [Test]
    public async Task WindowByTime_Sliding_StressTest_SlowConsumer()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromSeconds(5);
        var slide = TimeSpan.FromSeconds(1);

        // 20 items, 1 per second
        var items = Enumerable.Range(0, 20).Select(i => Timestamped.Create(i, start.AddSeconds(i))).ToList();
        var source = Stream.From<Timestamped<int>>(items);

        // Many overlapping windows, small capacity
        var windows = source.WindowByTime(duration, slide, capacity: 2);

        var windowCount = 0;
        var totalItemsAcrossWindows = 0;

        // Use FlatMap to consume concurrently (important to avoid deadlocks with overlapping windows)
        await windows.FlatMap(async window =>
        {
            Interlocked.Increment(ref windowCount);
            var list = await window.ToListAsync();
            Interlocked.Add(ref totalItemsAcrossWindows, list.Count);
            // Simulate slow consumption
            await Task.Delay(20);
            return 0;
        }, maxConcurrency: 8).DrainAsync();

        Assert.That(windowCount, Is.GreaterThan(10));
        // Each item belongs to up to 5 windows.
        // Total items across all windows should be 100 with the new earliest window logic.
        Assert.That(totalItemsAcrossWindows, Is.EqualTo(100));
    }

    [Test]
    public async Task WindowByTime_Completion_ShouldCompleteAllWindows()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        var source = Stream.From(
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(5))
        );

        var windows = source.WindowByTime(duration);
        var windowList = await windows.ToListAsync();

        Assert.That(windowList, Has.Count.EqualTo(1));
        var items = await windowList[0].ToListAsync();
        Assert.That(items, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task WindowByTime_Cancellation_ShouldPropagateToWindows()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);

        var tcs = new TaskCompletionSource<bool>();
        var source = Stream.Create<Timestamped<int>>(async (emitter, ct) =>
        {
            await emitter.EmitAsync(Timestamped.Create(1, start.AddMinutes(1)));
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                tcs.SetResult(true);
                throw;
            }
        });

        using var cts = new CancellationTokenSource();
        var windows = source.WindowByTime(duration);

        var windowEnumerator = windows.GetAsyncEnumerator(cts.Token);
        Assert.That(await windowEnumerator.MoveNextAsync(), Is.True);
        var firstWindow = windowEnumerator.Current;

        var itemsEnumerator = firstWindow.GetAsyncEnumerator(cts.Token);
        Assert.That(await itemsEnumerator.MoveNextAsync(), Is.True);

        // Cancel outer
        await cts.CancelAsync();

        // Should catch OperationCanceledException (or subclass like TaskCanceledException)
        Assert.CatchAsync<OperationCanceledException>(async () => await itemsEnumerator.MoveNextAsync());
        Assert.That(await tcs.Task, Is.True);
    }

    [Test]
    public async Task WindowBySession_Ordered_ShouldGroupAndExtend()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var gap = TimeSpan.FromMinutes(10);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(5)),   // Extends session 1
            Timestamped.Create(3, start.AddMinutes(20)),  // New session 2
            Timestamped.Create(4, start.AddMinutes(25)),  // Extends session 2
            Timestamped.Create(5, start.AddMinutes(40)),  // New session 3
        };

        var source = Stream.From(items);
        var windows = source.WindowBySession(gap);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<int>>();
        foreach (var window in windowStreams)
        {
            windowList.Add(await window.Map(x => x.Value).ToListAsync());
        }

        Assert.That(windowList, Has.Count.EqualTo(3));
        Assert.That(windowList[0], Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(windowList[1], Is.EquivalentTo(new[] { 3, 4 }));
        Assert.That(windowList[2], Is.EquivalentTo(new[] { 5 }));
    }

    [Test]
    public async Task WindowBySession_Ordered_ShouldHandleBackwardItemsWithinGap()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var gap = TimeSpan.FromMinutes(10);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(10)),
            Timestamped.Create(2, start.AddMinutes(5)),   // Backward but within gap
            Timestamped.Create(3, start.AddMinutes(1)),   // Backward but within gap
            Timestamped.Create(4, start.AddMinutes(20)),  // Forward but within gap of original max
        };

        var source = Stream.From(items);
        var windows = source.WindowBySession(gap);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<int>>();
        foreach (var window in windowStreams)
        {
            windowList.Add(await window.Map(x => x.Value).ToListAsync());
        }

        Assert.That(windowList, Has.Count.EqualTo(1));
        Assert.That(windowList[0], Is.EquivalentTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public async Task WindowBySession_Watermark_ShouldMergeAndEmitFinalized()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var gap = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(5);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(25)), // max=25, wm=20. S1 finalized (20 >= 1+10). S2=[25,25]
            Timestamped.Create(3, start.AddMinutes(50)), // max=50, wm=45. S2 finalized (45 >= 25+10). S3=[50,50]
        };

        var source = Stream.From(items);
        var windows = source.WindowBySession(gap, outOfOrderness: outOfOrderness);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<int>>();
        foreach (var window in windowStreams)
        {
            windowList.Add(await window.Map(x => x.Value).ToListAsync());
        }

        Assert.That(windowList, Has.Count.EqualTo(3));
        Assert.That(windowList[0], Is.EquivalentTo(new[] { 1 }));
        Assert.That(windowList[1], Is.EquivalentTo(new[] { 2 }));
        Assert.That(windowList[2], Is.EquivalentTo(new[] { 3 }));
    }

    [Test]
    public async Task WindowBySession_Watermark_ShouldMergeSessions()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var gap = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(20); // Large out-of-orderness to keep sessions active

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),  // S1: [1, 1]
            Timestamped.Create(2, start.AddMinutes(20)), // S2: [20, 20], Watermark: 0
            Timestamped.Create(3, start.AddMinutes(10)), // Bridges S1 and S2 -> S3: [1, 20]
        };

        var source = Stream.From(items);
        var windows = source.WindowBySession(gap, outOfOrderness: outOfOrderness);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<int>>();
        foreach (var window in windowStreams)
        {
            windowList.Add(await window.Map(x => x.Value).ToListAsync());
        }

        // All merged into one because of item 3
        Assert.That(windowList, Has.Count.EqualTo(1));
        Assert.That(windowList[0], Is.EqualTo(new[] { 1, 3, 2 })); // Ordered by timestamp
    }

    [Test]
    public async Task WindowBySession_Watermark_ShouldDropLateEvents()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var gap = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(5);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(20)), // Watermark: 15
            Timestamped.Create(2, start.AddMinutes(10)), // Late! (10 <= 15)
            Timestamped.Create(3, start.AddMinutes(40)), // Watermark: 35. New session
        };

        var source = Stream.From(items);
        var windows = source.WindowBySession(gap, outOfOrderness: outOfOrderness);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<int>>();
        foreach (var window in windowStreams)
        {
            windowList.Add(await window.Map(x => x.Value).ToListAsync());
        }

        Assert.That(windowList, Has.Count.EqualTo(2));
        Assert.That(windowList[0], Is.EquivalentTo(new[] { 1 }));
        Assert.That(windowList[1], Is.EquivalentTo(new[] { 3 }));
        // Item 2 is nowhere
    }
}
