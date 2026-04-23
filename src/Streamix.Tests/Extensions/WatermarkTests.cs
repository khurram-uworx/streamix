using NUnit.Framework;
using Streamix;

namespace Streamix.Tests.Extensions;

[TestFixture]
public class WatermarkTests
{
    [Test]
    public async Task WindowByTime_Watermark_Tumbling_ShouldDropLateEvents()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(5);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),  // Watermark: none -> -4
            Timestamped.Create(2, start.AddMinutes(12)), // Watermark: 1 -> 7. Admitted to [10, 20)
            Timestamped.Create(3, start.AddMinutes(6)),  // Watermark: 7. 6 <= 7, so LATE. Dropped.
            Timestamped.Create(4, start.AddMinutes(15)), // Watermark: 7 -> 10. Admitted to [10, 20)
            Timestamped.Create(5, start.AddMinutes(7)),  // Watermark: 10. 7 <= 10, so LATE. Dropped.
            Timestamped.Create(6, start.AddMinutes(21)), // Watermark: 10 -> 16. [10, 20) completes. Admitted to [20, 30)
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, outOfOrderness: outOfOrderness);

        var windowList = new List<List<int>>();
        await windows.FlatMap(async w =>
        {
            var list = await w.Map(x => x.Value).ToListAsync();
            lock (windowList) windowList.Add(list);
            return 0;
        }).DrainAsync();

        Assert.That(windowList, Has.Count.EqualTo(3));

        // Windows:
        // [0, 10): contains {1}
        // [10, 20): contains {2, 4}
        // [20, 30): contains {6}

        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 2, 4 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 6 }));
    }

    [Test]
    public async Task WindowByTime_Watermark_Sliding_ShouldDropLateEvents()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var slide = TimeSpan.FromMinutes(5);
        var outOfOrderness = TimeSpan.FromMinutes(2);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),  // Watermark: none -> -1. Windows: [0,10)
            Timestamped.Create(2, start.AddMinutes(6)),  // Watermark: -1 -> 4. Windows: [0,10), [5,15)
            Timestamped.Create(3, start.AddMinutes(3)),  // Watermark: 4. 3 <= 4, so LATE. Dropped.
            Timestamped.Create(4, start.AddMinutes(12)), // Watermark: 4 -> 10. [0,10) completes. Windows: [5,15), [10,20)
            Timestamped.Create(5, start.AddMinutes(9)),  // Watermark: 10. 9 <= 10, so LATE. Dropped.
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, slide, outOfOrderness: outOfOrderness);

        var windowList = new List<List<int>>();
        await windows.FlatMap(async w =>
        {
            var list = await w.Map(x => x.Value).ToListAsync();
            lock (windowList) windowList.Add(list);
            return 0;
        }).DrainAsync();

        // Expected Windows and their items:
        // [-5, 5): {1}     (3 was late)
        // [0, 10): {1, 2}  (3 was late)
        // [5, 15): {2, 4}  (3 was late, 9 was late)
        // [10, 20): {4}    (9 was late)

        Assert.That(windowList, Has.Count.EqualTo(4));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1, 2 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 2, 4 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 4 }));
    }

    [Test]
    public async Task WindowByTime_Watermark_ShouldCompleteAtExactBoundary()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.Zero;

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(10)), // Watermark: 10. Window [0, 10) should complete.
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, outOfOrderness: outOfOrderness);

        var windowList = new List<List<int>>();
        await windows.FlatMap(async w =>
        {
            var list = await w.Map(x => x.Value).ToListAsync();
            lock (windowList) windowList.Add(list);
            return 0;
        }).DrainAsync();

        Assert.That(windowList, Has.Count.EqualTo(2));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 1 }));
        Assert.That(windowList, Has.Some.EquivalentTo(new[] { 2 }));
    }

    [Test]
    public async Task WindowByTime_Watermark_ShouldDropIfEqualToWatermark()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(5);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(10)), // Watermark: 5
            Timestamped.Create(2, start.AddMinutes(5)),  // Watermark: 5. 5 <= 5 is LATE.
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, outOfOrderness: outOfOrderness);

        var windowList = new List<List<int>>();
        await windows.FlatMap(async w =>
        {
            var list = await w.Map(x => x.Value).ToListAsync();
            lock (windowList) windowList.Add(list);
            return 0;
        }).DrainAsync();

        Assert.That(windowList, Has.Count.EqualTo(1));
        Assert.That(windowList[0], Is.EquivalentTo(new[] { 1 }));
    }

    [Test]
    public async Task WindowByTime_Watermark_UpstreamCompletion_ShouldFlushAll()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(100); // Watermark will always be far in the past

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(5)),
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, outOfOrderness: outOfOrderness);

        var windowList = new List<List<int>>();
        await windows.FlatMap(async w =>
        {
            var list = await w.Map(x => x.Value).ToListAsync();
            lock (windowList) windowList.Add(list);
            return 0;
        }).DrainAsync();

        // Even though watermark never reached 10, upstream completion should close the window.
        Assert.That(windowList, Has.Count.EqualTo(1));
        Assert.That(windowList[0], Is.EquivalentTo(new[] { 1, 2 }));
    }
}
