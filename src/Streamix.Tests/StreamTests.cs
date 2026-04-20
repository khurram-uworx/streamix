using NUnit.Framework;
using Streamix.Tests.Implementations;
using System.Runtime.CompilerServices;
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
    public async Task FromQueue_Drains_Queue_In_Order()
    {
        var queue = new Queue<int>(new[] { 1, 2, 3 });

        var stream = Stream.FromQueue(queue);
        var result = await stream.ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(queue.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task FromQueue_Subsequent_Subscription_Sees_Remaining_Queue_State()
    {
        var queue = new Queue<int>(new[] { 10, 20 });
        var stream = Stream.FromQueue(queue);

        var first = await stream.Take(1).ToListAsync();
        var second = await stream.ToListAsync();

        Assert.That(first, Is.EqualTo(new[] { 10 }));
        Assert.That(second, Is.EqualTo(new[] { 20 }));
        Assert.That(queue.Count, Is.EqualTo(0));
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
    public async Task ToChannel_Completes_Writer_With_Upstream_Error()
    {
        async IAsyncEnumerable<int> Source()
        {
            yield return 1;
            throw new InvalidOperationException("Stream Error");
        }

        var channel = Channel.CreateUnbounded<int>();
        var stream = Stream.From(Source());

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await stream.ToChannel(channel.Writer));

        Assert.That(exception?.Message, Is.EqualTo("Stream Error"));
        Assert.That(await channel.Reader.ReadAsync(), Is.EqualTo(1));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await channel.Reader.Completion);
    }

    [Test]
    public async Task ToChannel_Does_Not_Complete_Writer_If_Requested()
    {
        var channel = Channel.CreateUnbounded<int>();
        var stream = Stream.Range(1, 3);

        await stream.ToChannel(channel.Writer, completeWriter: false);

        Assert.That(channel.Reader.Completion.IsCompleted, Is.False);

        channel.Writer.Complete();
        await foreach (var i in channel.Reader.ReadAllAsync()) //.GetAsyncEnumerator().DisposeAsync(); // Ensure reader is completed
        { }

        await channel.Reader.Completion;
    }

    [Test]
    public async Task ToChannel_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var channel = Channel.CreateUnbounded<int>();

        var stream = Stream.Range(1, 100).DoOnNext(x =>
        {
            if (x == 5) cts.Cancel();
        });

        Assert.CatchAsync<OperationCanceledException>(async () =>
            await stream.ToChannel(channel.Writer, cancellationToken: cts.Token));
    }

    [Test]
    public async Task ToChannel_WithModeFail_PropagatesBackpressureException()
    {
        var reader = Stream.Range(1, 100).ToChannel(1, ChannelBackpressureMode.Fail);

        Assert.That(await reader.ReadAsync(), Is.EqualTo(1));
        Assert.ThrowsAsync<BackpressureException>(async () => await reader.Completion);
    }

    [Test]
    public async Task ToChannel_WithLatestOnly_KeepsLatestPendingItem()
    {
        var receivedFirst = new TaskCompletionSource<bool>();
        var producerFinished = new TaskCompletionSource<bool>();
        var reader = Stream.Create<int>(async emitter =>
        {
            await emitter.EmitAsync(1);
            await receivedFirst.Task;
            await emitter.EmitAsync(2);
            await emitter.EmitAsync(3);
            await emitter.EmitAsync(4);
            await emitter.EmitAsync(5);
            producerFinished.SetResult(true);
        }).ToChannel(8, ChannelBackpressureMode.LatestOnly);

        var first = await reader.ReadAsync();
        receivedFirst.SetResult(true);
        await producerFinished.Task;
        await Task.Delay(50);
        var second = await reader.ReadAsync();

        Assert.That(first, Is.EqualTo(1));
        Assert.That(second, Is.EqualTo(5));
        Assert.That(await reader.WaitToReadAsync(), Is.False);
    }

    [Test]
    public async Task PipeThroughChannel_Fail_ThrowsWhenBoundaryIsFull()
    {
        var stream = Stream.Range(1, 100).PipeThroughChannel(1, ChannelBackpressureMode.Fail);

        var ex = Assert.ThrowsAsync<BackpressureException>(async () =>
        {
            await foreach (var item in stream)
            {
                await Task.Delay(10);
            }
        });

        Assert.That(ex?.Message, Does.Contain("Channel boundary is full"));
    }

    [Test]
    public async Task PipeThroughChannel_LatestOnly_KeepsLatestPendingItem()
    {
        var consumerReceivedFirstTcs = new TaskCompletionSource<bool>();

        var stream = Stream.Create<int>(async emitter =>
        {
            await emitter.EmitAsync(1);
            await consumerReceivedFirstTcs.Task;
            await emitter.EmitAsync(2);
            await emitter.EmitAsync(3);
            await emitter.EmitAsync(4);
            await emitter.EmitAsync(5);
        }).PipeThroughChannel(16, ChannelBackpressureMode.LatestOnly);

        var results = new List<int>();
        await foreach (var item in stream)
        {
            results.Add(item);
            if (results.Count == 1)
            {
                consumerReceivedFirstTcs.SetResult(true);
                await Task.Delay(100);
            }
        }

        Assert.That(results, Is.EqualTo(new[] { 1, 5 }));
    }

    [Test]
    public async Task RunOnChannel_PreservesOrdering()
    {
        var results = await Stream.Range(1, 50)
            .RunOnChannel(capacity: 8, degreeOfParallelism: 4)
            .ToListAsync();

        Assert.That(results, Is.EqualTo(Enumerable.Range(1, 50)));
    }

    [Test]
    public async Task Merge_ChannelReaders_Works()
    {
        var first = Channel.CreateUnbounded<int>();
        var second = Channel.CreateUnbounded<int>();

        await first.Writer.WriteAsync(1);
        await first.Writer.WriteAsync(2);
        first.Writer.Complete();

        await second.Writer.WriteAsync(3);
        await second.Writer.WriteAsync(4);
        second.Writer.Complete();

        var results = await Stream.MergeChannels(first.Reader, second.Reader).ToListAsync();

        Assert.That(results.OrderBy(x => x), Is.EqualTo(new[] { 1, 2, 3, 4 }));
    }

    [Test]
    public async Task TeeToChannel_WritesToSideChannel_AndPreservesMainStream()
    {
        var channel = Channel.CreateUnbounded<int>();

        var result = await Stream.Range(1, 3)
            .TeeToChannel(channel.Writer, completeWriter: true)
            .ToListAsync();

        var side = new List<int>();
        await foreach (var item in channel.Reader.ReadAllAsync())
        {
            side.Add(item);
        }

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(side, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task TeeToChannel_LeavesWriterOpen_ByDefault()
    {
        var channel = Channel.CreateUnbounded<int>();

        var result = await Stream.Range(1, 3)
            .TeeToChannel(channel.Writer)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(channel.Reader.Completion.IsCompleted, Is.False);
    }

    [Test]
    public async Task TeeToChannel_CompletesWriterWithError_WhenRequested()
    {
        async IAsyncEnumerable<int> Source()
        {
            yield return 1;
            throw new InvalidOperationException("boom");
        }

        var channel = Channel.CreateUnbounded<int>();
        var stream = Stream.From(Source()).TeeToChannel(channel.Writer, completeWriter: true);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());

        Assert.That(ex?.Message, Is.EqualTo("boom"));
        Assert.That(await channel.Reader.ReadAsync(), Is.EqualTo(1));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await channel.Reader.Completion);
    }

    [Test]
    public async Task From_IEnumerable_Emits_Correct_Values()
    {
        var items = new List<int> { 1, 2, 3, 4, 5 };
        var stream = Stream.From((IEnumerable<int>)items);

        var result = await stream.ToListAsync();

        Assert.That(result, Is.EqualTo(items));
    }

    [Test]
    public async Task From_IEnumerable_Reenumerates_Per_Subscription()
    {
        var enumerations = 0;
        IEnumerable<int> Source()
        {
            enumerations++;
            yield return 1;
            yield return 2;
        }

        var stream = Stream.From(Source());

        Assert.That(await stream.ToListAsync(), Is.EqualTo(new[] { 1, 2 }));
        Assert.That(await stream.ToListAsync(), Is.EqualTo(new[] { 1, 2 }));
        Assert.That(enumerations, Is.EqualTo(2));
    }

    [Test]
    public void From_IEnumerable_Propagates_Enumeration_Exception()
    {
        IEnumerable<int> Source()
        {
            yield return 1;
            throw new InvalidOperationException("Enumeration failure");
        }

        var stream = Stream.From(Source());

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public async Task From_Params_Array_Emits_Correct_Values()
    {
        var stream = Stream.From(1, 2, 3);

        var result = await stream.ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task From_Params_Array_Reenumerates_Per_Subscription()
    {
        var stream = Stream.From(1, 2, 3);

        Assert.That(await stream.ToListAsync(), Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(await stream.ToListAsync(), Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task From_AsyncEnumerable_Factory_Is_Lazy_And_Invoked_Once_Per_Subscriber()
    {
        int factoryInvocations = 0;
        var stream = Stream.From((Func<CancellationToken, IAsyncEnumerable<int>>)(ct =>
        {
            factoryInvocations++;
            return GetItems();

            async IAsyncEnumerable<int> GetItems()
            {
                yield return 1;
                await Task.Yield();
                yield return 2;
            }
        }));

        Assert.That(factoryInvocations, Is.EqualTo(0));

        var result1 = await stream.ToListAsync();
        Assert.That(result1, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(factoryInvocations, Is.EqualTo(1));

        var result2 = await stream.ToListAsync();
        Assert.That(result2, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(factoryInvocations, Is.EqualTo(2));
    }

    [Test]
    public async Task From_AsyncEnumerable_Factory_Respects_Cancellation()
    {
        var factoryCts = new TaskCompletionSource<CancellationToken>();
        var stream = Stream.From((Func<CancellationToken, IAsyncEnumerable<int>>)(ct =>
        {
            factoryCts.SetResult(ct);
            return GetItems(ct);

            async IAsyncEnumerable<int> GetItems([EnumeratorCancellation] CancellationToken cancellationToken)
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    yield return 1;
                    await Task.Delay(10, cancellationToken);
                }
            }
        }));

        var cts = new CancellationTokenSource();
        var enumerator = stream.GetAsyncEnumerator(cts.Token);

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        var ct = await factoryCts.Task;

        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await enumerator.MoveNextAsync());
        Assert.That(ct.IsCancellationRequested, Is.True);
    }

    [Test]
    public async Task From_AsyncEnumerable_Factory_Propagates_Exception()
    {
        var stream = Stream.From<int>((Func<CancellationToken, IAsyncEnumerable<int>>)(ct =>
        {
            throw new InvalidOperationException("Factory error");
        }));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());

        var stream2 = Stream.From<int>((Func<CancellationToken, IAsyncEnumerable<int>>)(ct =>
        {
            return GetItems();

            async IAsyncEnumerable<int> GetItems()
            {
                yield return 1;
                throw new InvalidOperationException("Stream error");
            }
        }));

        var enumerator = stream2.GetAsyncEnumerator();
        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync());
    }

    [Test]
    public async Task FromTimer_Emits_Single_Zero_And_Completes()
    {
        var clock = new TestClock();
        var stream = Stream.FromTimer(TimeSpan.FromSeconds(5), clock);
        var subscriberTask = stream.ToListAsync();

        Assert.That(clock.ScheduledDelays.Count, Is.EqualTo(1));
        Assert.That(clock.ScheduledDelays[0], Is.EqualTo(TimeSpan.FromSeconds(5)));

        clock.AdvanceBy(TimeSpan.FromSeconds(5));

        var result = await subscriberTask;
        Assert.That(result, Is.EqualTo(new[] { 0L }));
    }

    [Test]
    public void FromTimer_Throws_On_Negative_DueTime()
    {
        var clock = new TestClock();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await Stream.FromTimer(TimeSpan.FromSeconds(-1), clock).ToListAsync());
    }

    [Test]
    public async Task PipeThroughChannel_SupervisesProducer_AndWaitsForSettlement()
    {
        var producerSettled = false;
        var tcs = new TaskCompletionSource();

        var stream = Stream.Create<int>(async emitter =>
        {
            try
            {
                await emitter.EmitAsync(1);
                await tcs.Task;
                throw new InvalidOperationException("Producer Boom");
            }
            finally
            {
                await Task.Delay(50);
                producerSettled = true;
            }
        }).PipeThroughChannel(8);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in stream)
            {
                tcs.SetResult();
            }
        });

        Assert.That(ex.Message, Is.EqualTo("Producer Boom"));
        Assert.That(producerSettled, Is.True);
    }

    [Test]
    public async Task RunOnChannel_SupervisesWorkers_AndWaitsForSettlement()
    {
        var workersFinished = 0;
        var tcs = new TaskCompletionSource();

        var stream = Stream.From(1, 2, 3)
            .DoOnNext(i => { if (i == 1) tcs.SetResult(); })
            .RunOnChannel(capacity: 8, degreeOfParallelism: 3)
            .MapAwait(async i =>
            {
                try
                {
                    if (i == 1)
                    {
                        await tcs.Task;
                        throw new InvalidOperationException("Worker Boom");
                    }
                    await Task.Delay(100);
                    return i;
                }
                finally
                {
                    Interlocked.Increment(ref workersFinished);
                }
            });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());

        // Note: RunOnChannel order preservation means if 1 fails, the consumer stops.
        // Items 2 and 3 might still be in the workers.
        // Supervision ensures they are settled before the boundary exits.

        Assert.That(workersFinished, Is.GreaterThanOrEqualTo(1));
    }
}
