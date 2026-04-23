using NUnit.Framework;
using Streamix;

namespace Streamix.Tests.Extensions;

[TestFixture]
public class WatermarkTests
{
    [Test]
    public async Task WindowByTime_WithWatermark_InOrder_ShouldWork()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.Zero;

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),
            Timestamped.Create(2, start.AddMinutes(5)),
            Timestamped.Create(3, start.AddMinutes(11)), // This should close the first window [0, 10)
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, outOfOrderness: outOfOrderness);

        var windowStreams = await windows.ToListAsync();
        Assert.That(windowStreams, Has.Count.EqualTo(2));

        var w1 = await windowStreams[0].ToListAsync();
        var w2 = await windowStreams[1].ToListAsync();

        Assert.That(w1.Select(x => x.Value), Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(w2.Select(x => x.Value), Is.EquivalentTo(new[] { 3 }));
    }

    [Test]
    public async Task WindowByTime_WithWatermark_OutOfOrder_WithinBound_ShouldBeAdmitted()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(5);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(5)),  // Watermark = 5 - 5 = 0
            Timestamped.Create(2, start.AddMinutes(2)),  // 2 > 0, admitted to [0, 10)
            Timestamped.Create(3, start.AddMinutes(12)), // Watermark = 12 - 5 = 7. [0, 10) is still open because 10 > 7
            Timestamped.Create(4, start.AddMinutes(8)),  // 8 > 7, admitted to [0, 10)
            Timestamped.Create(5, start.AddMinutes(16)), // Watermark = 16 - 5 = 11. [0, 10) closed because 10 <= 11
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, outOfOrderness: outOfOrderness);

        var windowStreams = await windows.ToListAsync();
        Assert.That(windowStreams, Has.Count.EqualTo(2));

        var w1 = await windowStreams[0].ToListAsync();
        Assert.That(w1.Select(x => x.Value), Is.EquivalentTo(new[] { 1, 2, 4 }));

        var w2 = await windowStreams[1].ToListAsync();
        Assert.That(w2.Select(x => x.Value), Is.EquivalentTo(new[] { 3, 5 }));
    }

    [Test]
    public async Task WindowByTime_WithWatermark_LateData_ShouldBeDropped()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(2);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(5)),  // Watermark = 5 - 2 = 3
            Timestamped.Create(2, start.AddMinutes(2)),  // 2 <= 3, LATE, dropped
            Timestamped.Create(3, start.AddMinutes(10)), // Watermark = 10 - 2 = 8
            Timestamped.Create(4, start.AddMinutes(7)),  // 7 <= 8, LATE, dropped
            Timestamped.Create(5, start.AddMinutes(15)), // Watermark = 15 - 2 = 13. [0, 10) closed.
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, outOfOrderness: outOfOrderness);

        var windowStreams = await windows.ToListAsync();
        var windowList = new List<List<Timestamped<int>>>();
        foreach (var w in windowStreams) windowList.Add(await w.ToListAsync());

        Assert.That(windowList, Has.Count.EqualTo(2));
        Assert.That(windowList[0].Select(x => x.Value), Is.EquivalentTo(new[] { 1 }));
        Assert.That(windowList[1].Select(x => x.Value), Is.EquivalentTo(new[] { 3, 5 }));
    }

    [Test]
    public async Task WindowByTime_WithWatermark_UpstreamCompletion_ShouldFlush()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var outOfOrderness = TimeSpan.FromMinutes(5);

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)), // Watermark = 1 - 5 = -4
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, outOfOrderness: outOfOrderness);

        var windowStreams = await windows.ToListAsync();
        Assert.That(windowStreams, Has.Count.EqualTo(1));
        var w1 = await windowStreams[0].ToListAsync();
        Assert.That(w1.Select(x => x.Value), Is.EquivalentTo(new[] { 1 }));
    }

    [Test]
    public async Task WindowByTime_WithWatermark_Sliding_ShouldWork()
    {
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var duration = TimeSpan.FromMinutes(10);
        var slide = TimeSpan.FromMinutes(5);
        var outOfOrderness = TimeSpan.FromMinutes(2);

        // Windows:
        // W-5: [-5, 5)
        // W0: [0, 10)
        // W5: [5, 15)
        // W10: [10, 20)

        var items = new[]
        {
            Timestamped.Create(1, start.AddMinutes(1)),  // Max=1, WM=-1. W-5, W0 created. 1 added to W-5, W0.
            Timestamped.Create(2, start.AddMinutes(6)),  // Max=6, WM=4. W5 created. 2 added to W0, W5.
            Timestamped.Create(3, start.AddMinutes(4)),  // 4 <= 4, LATE, dropped.
            Timestamped.Create(4, start.AddMinutes(13)), // Max=13, WM=11. W10 created. W-5, W0 close. 4 added to W5, W10.
            Timestamped.Create(5, start.AddMinutes(8)),  // 8 <= 11, LATE, dropped.
        };

        var source = Stream.From(items);
        var windows = source.WindowByTime(duration, slide, outOfOrderness: outOfOrderness);

        var windowStreams = await windows.ToListAsync();
        Assert.That(windowStreams, Has.Count.EqualTo(4));

        var wm5 = await windowStreams[0].ToListAsync();
        var w0 = await windowStreams[1].ToListAsync();
        var w5 = await windowStreams[2].ToListAsync();
        var w10 = await windowStreams[3].ToListAsync();

        Assert.That(wm5.Select(x => x.Value), Is.EquivalentTo(new[] { 1 }));
        Assert.That(w0.Select(x => x.Value), Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(w5.Select(x => x.Value), Is.EquivalentTo(new[] { 2, 4 }));
        Assert.That(w10.Select(x => x.Value), Is.EquivalentTo(new[] { 4 }));
    }
}
