using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class DiagnosticOperatorTests
{
    [Test]
    public async Task Stream_DoOnNext_ExecutesForEveryItem()
    {
        var items = new List<int>();
        var result = await Stream.Range(1, 5)
            .DoOnNext(x => items.Add(x))
            .Select(x => x)
            .ToListAsync();

        Assert.That(items, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task Stream_Do_ExecutesForEveryItem()
    {
        var items = new List<int>();
        var result = await Stream.Range(1, 5)
            .Do(x => items.Add(x))
            .ToListAsync();

        Assert.That(items, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task Stream_Tap_ExecutesForEveryItem()
    {
        var items = new List<int>();
        var result = await Stream.Range(1, 5)
            .Tap(x => items.Add(x))
            .ToListAsync();

        Assert.That(items, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void Stream_DoOnError_ExecutesUponStreamFailure()
    {
        var exception = new Exception("Test error");
        Exception? caught = null;

        var stream = Stream.Error<int>(exception)
            .DoOnError(ex => caught = ex);

        Assert.ThrowsAsync<Exception>(async () => await stream.ToListAsync());
        Assert.That(caught, Is.SameAs(exception));
    }

    [Test]
    public async Task Stream_DoOnTerminate_ExecutesUponSuccessfulCompletion()
    {
        bool terminated = false;
        await Stream.Range(1, 3)
            .DoOnTerminate(() => terminated = true)
            .ToListAsync();

        Assert.That(terminated, Is.True);
    }

    [Test]
    public async Task Stream_DoOnComplete_ExecutesUponSuccessfulCompletion()
    {
        bool completed = false;
        await Stream.Range(1, 3)
            .DoOnComplete(() => completed = true)
            .ToListAsync();

        Assert.That(completed, Is.True);
    }

    [Test]
    public void Stream_DoOnComplete_DoesNotExecuteUponError()
    {
        bool completed = false;
        var stream = Stream.Error<int>(new Exception())
            .DoOnComplete(() => completed = true);

        Assert.ThrowsAsync<Exception>(async () => await stream.ToListAsync());
        Assert.That(completed, Is.False);
    }

    [Test]
    public async Task Stream_DoOnComplete_DoesNotExecuteUponCancellation()
    {
        bool completed = false;
        using var cts = new CancellationTokenSource();

        var stream = Stream.Range(1, 10)
            .DoOnComplete(() => completed = true);

        try
        {
            await foreach (var item in stream.WithCancellation(cts.Token))
            {
                if (item == 5) cts.Cancel();
            }
        }
        catch (OperationCanceledException)
        {
        }

        Assert.That(completed, Is.False);
    }

    [Test]
    public void Stream_DoOnTerminate_ExecutesUponError()
    {
        bool terminated = false;
        var stream = Stream.Error<int>(new Exception())
            .DoOnTerminate(() => terminated = true);

        Assert.ThrowsAsync<Exception>(async () => await stream.ToListAsync());
        Assert.That(terminated, Is.True);
    }

    [Test]
    public async Task Single_DoOnNext_ExecutesForItem()
    {
        int value = 0;
        var result = await Single.From(42)
            .DoOnNext(x => value = x)
            .ToTask();

        Assert.That(value, Is.EqualTo(42));
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task Single_Do_ExecutesForItem()
    {
        int value = 0;
        var result = await Single.From(42)
            .Do(x => value = x)
            .ToTask();

        Assert.That(value, Is.EqualTo(42));
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task Single_Tap_ExecutesForItem()
    {
        int value = 0;
        var result = await Single.From(42)
            .Tap(x => value = x)
            .ToTask();

        Assert.That(value, Is.EqualTo(42));
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public void Single_DoOnError_ExecutesUponFailure()
    {
        var exception = new Exception("Test error");
        Exception? caught = null;

        var single = Single.Error<int>(exception)
            .DoOnError(ex => caught = ex);

        Assert.ThrowsAsync<Exception>(async () => await single.ToTask());
        Assert.That(caught, Is.SameAs(exception));
    }

    [Test]
    public async Task Single_DoOnTerminate_ExecutesUponSuccessfulCompletion()
    {
        bool terminated = false;
        await Single.From(42)
            .DoOnTerminate(() => terminated = true)
            .ToTask();

        Assert.That(terminated, Is.True);
    }

    [Test]
    public async Task Single_DoOnComplete_ExecutesUponSuccessfulCompletion()
    {
        bool completed = false;
        await Single.From(42)
            .DoOnComplete(() => completed = true)
            .ToTask();

        Assert.That(completed, Is.True);
    }

    [Test]
    public void Single_DoOnComplete_DoesNotExecuteUponError()
    {
        bool completed = false;
        var single = Single.Error<int>(new Exception())
            .DoOnComplete(() => completed = true);

        Assert.ThrowsAsync<Exception>(async () => await single.ToTask());
        Assert.That(completed, Is.False);
    }

    [Test]
    public async Task Single_DoOnComplete_DoesNotExecuteUponCancellation()
    {
        bool completed = false;
        using var cts = new CancellationTokenSource();

        // Use a task that respects cancellation
        var single = Single.From(Task.Delay(1000, cts.Token).ContinueWith(_ => 42, cts.Token))
            .DoOnComplete(() => completed = true);

        var task = single.ToTask(cts.Token);
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await task);
        Assert.That(completed, Is.False);
    }

    [Test]
    public void Single_DoOnTerminate_ExecutesUponError()
    {
        bool terminated = false;
        var single = Single.Error<int>(new Exception())
            .DoOnTerminate(() => terminated = true);

        Assert.ThrowsAsync<Exception>(async () => await single.ToTask());
        Assert.That(terminated, Is.True);
    }

    [Test]
    public async Task Stream_Log_Action_LogsAllSignals()
    {
        var logs = new List<string>();
        await Stream.Range(1, 2)
            .Named("TestStream")
            .LogAction(s => logs.Add(s))
            .DrainAsync();

        Assert.That(logs, Contains.Item("[TestStream] Next(1)"));
        Assert.That(logs, Contains.Item("[TestStream] Next(2)"));
        Assert.That(logs, Contains.Item("[TestStream] Completed"));
    }

    [Test]
    public async Task Stream_Log_Action_Prefix_LogsAllSignals()
    {
        var logs = new List<string>();
        await Stream.Range(1, 2)
            .LogAction(s => logs.Add(s))
            .DrainAsync();

        Assert.That(logs, Contains.Item("Next(1)"));
        Assert.That(logs, Contains.Item("Next(2)"));
        Assert.That(logs, Contains.Item("Completed"));
    }

    [Test]
    public void Stream_Log_Action_LogsError()
    {
        var logs = new List<string>();
        var stream = Stream.Error<int>(new Exception("Fail"))
            .Named("ErrorStream")
            .LogAction(s => logs.Add(s));

        Assert.ThrowsAsync<Exception>(async () => await stream.DrainAsync());
        Assert.That(logs, Contains.Item("[ErrorStream] Error(Fail)"));
    }

    [Test]
    public async Task Stream_Log_ILogger_LogsAllSignals()
    {
        var mockLogger = new Mock<ILogger>();
        await Stream.Range(1, 1)
            .Named("LoggerStream")
            .Log(mockLogger.Object)
            .DrainAsync();

        mockLogger.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("[LoggerStream] Next(1)")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);

        mockLogger.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("[LoggerStream] Completed")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Test]
    public async Task Single_Log_Action_LogsAllSignals()
    {
        var logs = new List<string>();
        await Single.From(42)
            .Named("TestSingle")
            .LogAction(s => logs.Add(s))
            .ToTask();

        Assert.That(logs, Contains.Item("[TestSingle] Next(42)"));
        Assert.That(logs, Contains.Item("[TestSingle] Completed"));
    }
}
