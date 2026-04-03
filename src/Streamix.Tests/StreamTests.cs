using NUnit.Framework;
using Streamix.Abstractions;
using System.Threading.Channels;

namespace Streamix.Tests;

[TestFixture]
public class StreamTests
{
    [Test]
    public async Task Empty_Stream_Is_Empty()
    {
        IStream<int> stream = Stream.Empty<int>();

        int count = 0;
        await foreach (var _ in stream)
            count++;

        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task Range_Stream_Emits_Correct_Values()
    {
        IStream<int> stream = Stream.Range(1, 5);
        var result = new List<int>();
        await foreach (var item in stream)
        {
            result.Add(item);
        }
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task From_AsyncEnumerable_Emits_Correct_Values()
    {
        async IAsyncEnumerable<int> Source()
        {
            yield return 10;
            yield return 20;
            await Task.Yield();
            yield return 30;
        }

        IStream<int> stream = Stream.From(Source());
        var result = new List<int>();
        await foreach (var item in stream)
        {
            result.Add(item);
        }
        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task Stream_Is_Cold_And_Can_Be_Enumerated_Multiple_Times()
    {
        int sideEffect = 0;
        async IAsyncEnumerable<int> Source()
        {
            sideEffect++;
            yield return sideEffect;
        }

        IStream<int> stream = Stream.From(Source());

        var result1 = new List<int>();
        await foreach (var item in stream) result1.Add(item);

        var result2 = new List<int>();
        await foreach (var item in stream) result2.Add(item);

        Assert.That(result1, Is.EqualTo(new[] { 1 }));
        Assert.That(result2, Is.EqualTo(new[] { 2 }));
        Assert.That(sideEffect, Is.EqualTo(2));
    }

    [Test]
    public void Range_Throws_When_Cancelled()
    {
        var cts = new CancellationTokenSource();
        IStream<int> stream = Stream.Range(1, 100);

        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var item in stream.WithCancellation(cts.Token))
            {
            }
        });
    }

    [Test]
    public async Task ForEachAsync_Action_Executes_For_All_Items()
    {
        IStream<int> stream = Stream.Range(1, 5);
        var result = new List<int>();
        await stream.ForEachAsync(item => result.Add(item));
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task ForEachAsync_Func_Executes_For_All_Items()
    {
        IStream<int> stream = Stream.Range(1, 5);
        var result = new List<int>();
        await stream.ForEachAsync(async item =>
        {
            await Task.Yield();
            result.Add(item);
        });
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ForEachAsync_Propagates_Exceptions()
    {
        async IAsyncEnumerable<int> Source()
        {
            yield return 1;
            throw new InvalidOperationException("Test exception");
        }

        IStream<int> stream = Stream.From(Source());

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await stream.ForEachAsync(item => { });
        });
    }

    [Test]
    public void ForEachAsync_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        IStream<int> stream = Stream.Range(1, 10);

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await stream.ForEachAsync(item =>
            {
                if (item == 5) cts.Cancel();
            }, cts.Token);
        });
    }

    [Test]
    public async Task FromChannel_Reads_All_Items()
    {
        var channel = Channel.CreateUnbounded<int>();
        await channel.Writer.WriteAsync(1);
        await channel.Writer.WriteAsync(2);
        channel.Writer.Complete();

        var stream = Stream.FromChannel(channel);
        var result = await stream.ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public void FromChannel_Propagates_Error()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryComplete(new InvalidOperationException("Channel Error"));

        var stream = Stream.FromChannel(channel);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public async Task ToChannel_Writes_All_Items()
    {
        var channel = Channel.CreateUnbounded<int>();
        var stream = Stream.Range(1, 3);

        await stream.ToChannel(channel.Writer);

        var result = new List<int>();
        await foreach (var item in channel.Reader.ReadAllAsync())
        {
            result.Add(item);
        }

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task ToChannel_Supports_Backpressure()
    {
        var channel = Channel.CreateBounded<int>(1);
        var stream = Stream.Range(1, 3);

        var toChannelTask = stream.ToChannel(channel.Writer);

        // It should be waiting for space
        await Task.Delay(100);
        Assert.That(toChannelTask.IsCompleted, Is.False);

        Assert.That(await channel.Reader.ReadAsync(), Is.EqualTo(1));
        Assert.That(await channel.Reader.ReadAsync(), Is.EqualTo(2));
        Assert.That(await channel.Reader.ReadAsync(), Is.EqualTo(3));

        await toChannelTask;
        Assert.That(channel.Reader.Completion.IsCompleted, Is.True);
    }

    [Test]
    public async Task ToChannel_Propagates_Error()
    {
        async IAsyncEnumerable<int> Source()
        {
            yield return 1;
            throw new InvalidOperationException("Stream Error");
        }

        var channel = Channel.CreateUnbounded<int>();
        var stream = Stream.From(Source());

        try
        {
            await stream.ToChannel(channel.Writer);
            Assert.Fail("ToChannel should have thrown.");
        }
        catch (InvalidOperationException ex)
        {
            Assert.That(ex.Message, Is.EqualTo("Stream Error"));
        }
    }

    [Test]
    public async Task ToChannel_Does_Not_Complete_Writer_If_Requested()
    {
        var channel = Channel.CreateUnbounded<int>();
        var stream = Stream.Range(1, 3);

        await stream.ToChannel(channel.Writer, completeWriter: false);

        Assert.That(channel.Reader.Completion.IsCompleted, Is.False);

        channel.Writer.Complete();
        await channel.Reader.Completion;
    }
}
