using NUnit.Framework;
using Streamix;

namespace Streamix.Tests;

[TestFixture]
public class DevxBehavioralTests
{
    [Test]
    public void Stream_Named_PropagatesToDerivedStream()
    {
        var stream = Stream.Range(1, 5).Named("Original");
        var derived = stream.Map(x => x * 2);

        Assert.That(derived.Name, Is.EqualTo("Original"));
    }

    [Test]
    public void Single_Named_PropagatesToDerivedSingle()
    {
        var single = Single.Just(1).Named("Original");
        var derived = single.Map(x => x * 2);

        Assert.That(derived.Name, Is.EqualTo("Original"));
    }

    [Test]
    public void Stream_Named_PropagatesThroughChain()
    {
        var name = "ChainStream";
        var stream = Stream.Range(1, 5)
            .Named(name)
            .Filter(x => x % 2 == 0)
            .Map(x => x * 2)
            .Take(1);

        Assert.That(stream.Name, Is.EqualTo(name));
    }

    [Test]
    public async Task Stream_Log_DoesNotAlterStreamContent()
    {
        var source = new[] { 1, 2, 3, 4, 5 };
        var result = await Stream.From(source)
            .Log()
            .ToListAsync();

        Assert.That(result, Is.EqualTo(source));
    }

    [Test]
    public async Task Stream_Trace_DoesNotAlterStreamContent()
    {
        var source = new[] { 1, 2, 3, 4, 5 };
        var result = await Stream.From(source)
            .Trace()
            .ToListAsync();

        Assert.That(result, Is.EqualTo(source));
    }

    [Test]
    public async Task Stream_Checkpoint_DoesNotAlterStreamContent()
    {
        var source = new[] { 1, 2, 3, 4, 5 };
        var result = await Stream.From(source)
            .Checkpoint("Test")
            .ToListAsync();

        Assert.That(result, Is.EqualTo(source));
    }

    [Test]
    public async Task Stream_Debug_DoesNotAlterStreamContent()
    {
        var source = new[] { 1, 2, 3, 4, 5 };
        var result = await Stream.From(source)
            .Debug()
            .ToListAsync();

        Assert.That(result, Is.EqualTo(source));
    }

    [Test]
    public void Stream_Log_PropagatesError()
    {
        var stream = Stream.Error<int>(new Exception("Fail")).Log();
        Assert.ThrowsAsync<Exception>(async () => await stream.ToListAsync());
    }

    [Test]
    public void Stream_Trace_PropagatesError()
    {
        var stream = Stream.Error<int>(new Exception("Fail")).Trace();
        Assert.ThrowsAsync<Exception>(async () => await stream.ToListAsync());
    }

    [Test]
    public void Stream_Checkpoint_PropagatesError()
    {
        var stream = Stream.Error<int>(new Exception("Fail")).Checkpoint("Test");
        Assert.ThrowsAsync<Exception>(async () => await stream.ToListAsync());
    }

    [Test]
    public async Task Stream_DiagnosticOperators_PropagateCancellation()
    {
        using var cts = new CancellationTokenSource();
        var stream = Stream.Interval(TimeSpan.FromMilliseconds(10))
            .Log()
            .Trace()
            .Checkpoint("Test")
            .Debug();

        var task = Task.Run(async () => {
            await foreach (var item in stream.WithCancellation(cts.Token))
            {
                if (item == 2) cts.Cancel();
            }
        });

        Assert.CatchAsync<OperationCanceledException>(async () => await task);
    }

    [Test]
    public async Task Single_Log_DoesNotAlterSingleContent()
    {
        var result = await Single.Just(42)
            .Log()
            .ToTask();

        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task Single_Trace_DoesNotAlterSingleContent()
    {
        var result = await Single.Just(42)
            .Trace()
            .ToTask();

        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task Single_Checkpoint_DoesNotAlterSingleContent()
    {
        var result = await Single.Just(42)
            .Checkpoint("Test")
            .ToTask();

        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task Single_Debug_DoesNotAlterSingleContent()
    {
        var result = await Single.Just(42)
            .Debug()
            .ToTask();

        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public void Stream_Named_CanBeOverridden()
    {
        var stream = Stream.Range(1, 5)
            .Named("First")
            .Named("Second");

        Assert.That(stream.Name, Is.EqualTo("Second"));
    }

    [Test]
    public void Single_Named_CanBeOverridden()
    {
        var single = Single.Just(1)
            .Named("First")
            .Named("Second");

        Assert.That(single.Name, Is.EqualTo("Second"));
    }
}
