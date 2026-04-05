using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class TimeAwareTerminalTests
{
    [Test]
    public async Task FirstAsync_With_Timeout_Returns_Correct_Element()
    {
        var clock = new TestClock();
        var stream = Stream.From(new[] { 1, 2, 3 }.ToAsyncEnumerable(), clock);

        var result = await stream.FirstAsync(TimeSpan.FromSeconds(5));

        Assert.That(result, Is.EqualTo(1));
    }

    [Test]
    public async Task FirstAsync_With_Timeout_Throws_On_Timeout()
    {
        var clock = new TestClock();
        // A stream that never emits
        var stream = Stream.Create<int>(async emitter =>
        {
            await clock.Delay(TimeSpan.FromSeconds(10), emitter.CancellationToken);
        });
        // We need to use Stream.From(..., clock) to ensure it uses our TestClock
        var streamWithClock = Stream.From(stream, clock);

        var task = streamWithClock.FirstAsync(TimeSpan.FromSeconds(5));

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(1));
        clock.AdvanceBy(TimeSpan.FromSeconds(6));

        Assert.ThrowsAsync<TimeoutException>(async () => await task);
    }

    [Test]
    public async Task FirstOrDefaultAsync_With_Timeout_Returns_Default_On_Timeout()
    {
        var clock = new TestClock();
        var stream = Stream.Create<int>(async emitter =>
        {
            await clock.Delay(TimeSpan.FromSeconds(10), emitter.CancellationToken);
        });
        var streamWithClock = Stream.From(stream, clock);

        var task = streamWithClock.FirstOrDefaultAsync(TimeSpan.FromSeconds(5));

        await clock.WaitForDelay(1, TimeSpan.FromSeconds(1));
        clock.AdvanceBy(TimeSpan.FromSeconds(6));

        var result = await task;
        Assert.That(result, Is.EqualTo(0));
    }

    [Test]
    public async Task CollectAsync_With_Duration_Works()
    {
        var clock = new TestClock();

        var stream = Stream.Create<long>(async emitter =>
        {
            await clock.Delay(TimeSpan.FromSeconds(1), emitter.CancellationToken);
            await emitter.EmitAsync(0);
            await clock.Delay(TimeSpan.FromSeconds(1), emitter.CancellationToken);
            await emitter.EmitAsync(1);
            await clock.Delay(TimeSpan.FromSeconds(1), emitter.CancellationToken);
            await emitter.EmitAsync(2);
            await clock.Delay(TimeSpan.FromSeconds(1), emitter.CancellationToken);
            await emitter.EmitAsync(3);
        });
        var streamWithClock = Stream.From(stream, clock);

        var task = streamWithClock.CollectAsync(TimeSpan.FromSeconds(3.5));

        // Let it run.
        // Item 0 at 1s.
        await clock.WaitForDelay(2, TimeSpan.FromSeconds(1)); // Delay for item and delay for timeout
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await Task.Delay(50); // Real time to allow async processing

        // Item 1 at 2s.
        await clock.WaitForDelay(2, TimeSpan.FromSeconds(1));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await Task.Delay(50);

        // Item 2 at 3s.
        await clock.WaitForDelay(2, TimeSpan.FromSeconds(1));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        await Task.Delay(50);

        // Timeout at 3.5s.
        await clock.WaitForDelay(2, TimeSpan.FromSeconds(1));
        clock.AdvanceBy(TimeSpan.FromSeconds(0.5));
        await Task.Delay(50);

        var result = await task;
        Assert.That(result, Is.EquivalentTo(new[] { 0L, 1L, 2L }));
    }
}
