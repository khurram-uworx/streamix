using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class CreateTests
{
    sealed class AsyncEventSource<T>
    {
        sealed class Subscription : IDisposable
        {
            readonly AsyncEventSource<T> owner;
            readonly Func<T, ValueTask> handler;
            int disposed;

            public Subscription(AsyncEventSource<T> owner, Func<T, ValueTask> handler)
            {
                this.owner = owner;
                this.handler = handler;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref disposed, 1) == 0)
                {
                    owner.Unsubscribe(handler);
                }
            }
        }

        readonly object gate = new();
        readonly List<Func<T, ValueTask>> handlers = new();

        public int SubscribeCount { get; private set; }

        public int UnsubscribeCount { get; private set; }

        public IDisposable Subscribe(Func<T, ValueTask> handler)
        {
            lock (gate)
            {
                handlers.Add(handler);
                SubscribeCount++;
            }

            return new Subscription(this, handler);
        }

        public async ValueTask PublishAsync(T item)
        {
            Func<T, ValueTask>[] snapshot;
            lock (gate)
            {
                snapshot = handlers.ToArray();
            }

            foreach (var handler in snapshot)
            {
                await handler(item);
            }
        }

        void Unsubscribe(Func<T, ValueTask> handler)
        {
            lock (gate)
            {
                handlers.Remove(handler);
                UnsubscribeCount++;
            }
        }
    }

    static async Task WaitUntilAsync(Func<bool> condition)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(10);
        }

        Assert.Fail("Condition was not met in time.");
    }

    [Test]
    public async Task Create_Emits_Items_And_Completes()
    {
        var stream = Stream.Create<int>(async emitter =>
        {
            await emitter.EmitAsync(1);
            await emitter.EmitAsync(2);
            await emitter.EmitAsync(3);
            emitter.Complete();
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertValues(1, 2, 3)
            .AssertComplete();
    }

    [Test]
    public async Task Create_Producer_Throws_After_Emit_And_Complete_Should_Not_Affect_Stream()
    {
        var stream = Stream.Create<int>(async emitter =>
        {
            await emitter.EmitAsync(1);
            emitter.Complete();
            throw new InvalidOperationException("Should be ignored");
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertValues(1)
            .AssertComplete();
    }

    [Test]
    public async Task Create_Supports_Backpressure()
    {
        var secondEmitStarted = new TaskCompletionSource<bool>();
        var firstItemConsumed = new TaskCompletionSource<bool>();

        var stream = Stream.Create<int>(async emitter =>
        {
            await emitter.EmitAsync(1);

            // The second EmitAsync should block until the first item is consumed
            // because the internal channel has capacity 1.
            var emitTask = emitter.EmitAsync(2).AsTask();
            secondEmitStarted.SetResult(true);

            await firstItemConsumed.Task;
            await emitTask;

            emitter.Complete();
        });

        await using var enumerator = stream.GetAsyncEnumerator();

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(1));

        await secondEmitStarted.Task;

        // At this point, the producer should be blocked on the second EmitAsync
        // Let's give it a short delay to be sure
        await Task.Delay(50);

        firstItemConsumed.SetResult(true);

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(2));

        Assert.That(await enumerator.MoveNextAsync(), Is.False);
    }

    [Test]
    public async Task Create_Propagates_Error_Via_Fail()
    {
        var stream = Stream.Create<int>(emitter =>
        {
            emitter.Fail(new InvalidOperationException("Test Error"));
            return Task.CompletedTask;
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertError<InvalidOperationException>(ex => Assert.That(ex.Message, Is.EqualTo("Test Error")));
    }

    [Test]
    public async Task Create_Propagates_Producer_Exception()
    {
        var stream = Stream.Create<int>(async emitter =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Producer Exception");
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertError<InvalidOperationException>(ex => Assert.That(ex.Message, Is.EqualTo("Producer Exception")));
    }

    [Test]
    public async Task Create_Respects_Cancellation()
    {
        var producerCancelled = new TaskCompletionSource<bool>();
        var stream = Stream.Create<int>(async emitter =>
        {
            try
            {
                await Task.Delay(10000, emitter.CancellationToken);
            }
            catch (OperationCanceledException)
            {
                producerCancelled.SetResult(true);
                throw;
            }
        });

        var cts = new CancellationTokenSource();
        var subscribeTask = TestSubscriber<int>.SubscribeAsync(stream, cts.Token);

        await Task.Delay(100);
        await cts.CancelAsync();

        var subscriber = await subscribeTask;
        subscriber.AssertValueCount(0);
        subscriber.AssertNotComplete();
        Assert.That(await producerCancelled.Task, Is.True);
    }

    [Test]
    public async Task Create_Terminal_State_Is_Idempotent()
    {
        var stream = Stream.Create<int>(async emitter =>
        {
            emitter.Complete();
            emitter.Fail(new Exception("Should be ignored"));

            // This should throw because the stream is terminal
            try
            {
                await emitter.EmitAsync(1);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }

            emitter.Complete();
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertValueCount(0)
            .AssertComplete();
    }

    [Test]
    public async Task Create_ValueTask_Overload_Works()
    {
        var stream = Stream.Create<int>(async (emitter, ct) =>
        {
            await emitter.EmitAsync(1);
            await emitter.EmitAsync(2);
            emitter.Complete();
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertValues(1, 2)
            .AssertComplete();
    }

    [Test]
    public async Task Create_EmitAsync_Throws_After_Terminal_State()
    {
        Exception? caughtAtEmit = null;
        var stream = Stream.Create<int>(async emitter =>
        {
            emitter.Complete();
            try
            {
                await emitter.EmitAsync(1);
            }
            catch (Exception ex)
            {
                caughtAtEmit = ex;
            }
        });

        await TestSubscriber<int>.SubscribeAsync(stream);
        Assert.That(caughtAtEmit, Is.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Create_Producer_Throws_After_Complete_Should_Not_Affect_Stream()
    {
        var stream = Stream.Create<int>(async emitter =>
        {
            emitter.Complete();
            throw new InvalidOperationException("Should be ignored");
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertComplete()
            .AssertValueCount(0);
    }

    [Test]
    public async Task Create_Producer_Throws_After_Fail_Should_Not_Affect_Stream()
    {
        var stream = Stream.Create<int>(async emitter =>
        {
            emitter.Fail(new InvalidOperationException("Original Error"));
            throw new Exception("Secondary error - should be ignored");
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertError<InvalidOperationException>(ex => Assert.That(ex.Message, Is.EqualTo("Original Error")));
    }

    [Test]
    public async Task Create_Producer_Exits_On_Consumer_Disposal()
    {
        var producerLoopCount = 0;
        var producerFinished = new TaskCompletionSource<bool>();

        var stream = Stream.Create<int>(async emitter =>
        {
            try
            {
                while (!emitter.CancellationToken.IsCancellationRequested)
                {
                    await emitter.EmitAsync(producerLoopCount++);
                    await Task.Delay(10, emitter.CancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                producerFinished.SetResult(true);
            }
        });

        await using (var enumerator = stream.GetAsyncEnumerator())
        {
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
        } // Dispose here

        await producerFinished.Task;
        Assert.That(producerLoopCount, Is.LessThan(10)); // Should have stopped early
    }

    [Test]
    public async Task FromEvent_Emits_Items_And_Tears_Down_After_Take_Completes()
    {
        var source = new AsyncEventSource<int>();
        var stream = Stream.FromEvent<int>(source.Subscribe);

        var subscriberTask = TestSubscriber<int>.SubscribeAsync(stream.Take(2));
        await WaitUntilAsync(() => source.SubscribeCount == 1);

        await source.PublishAsync(1);
        await source.PublishAsync(2);

        (await subscriberTask)
            .AssertValues(1, 2)
            .AssertComplete();

        await WaitUntilAsync(() => source.UnsubscribeCount == 1);
    }

    [Test]
    public async Task FromEvent_Unsubscribes_On_Cancellation()
    {
        var source = new AsyncEventSource<int>();
        var stream = Stream.FromEvent<int>(source.Subscribe);
        using var cts = new CancellationTokenSource();

        var subscriberTask = TestSubscriber<int>.SubscribeAsync(stream, cts.Token);
        await WaitUntilAsync(() => source.SubscribeCount == 1);

        await cts.CancelAsync();

        (await subscriberTask)
            .AssertValueCount(0)
            .AssertNotComplete();

        await WaitUntilAsync(() => source.UnsubscribeCount == 1);
    }

    [Test]
    public async Task FromEvent_Creates_Fresh_Subscription_Per_Subscriber()
    {
        var source = new AsyncEventSource<int>();
        var stream = Stream.FromEvent<int>(source.Subscribe);

        var firstTask = TestSubscriber<int>.SubscribeAsync(stream.Take(1));
        await WaitUntilAsync(() => source.SubscribeCount == 1);
        await source.PublishAsync(1);
        (await firstTask)
            .AssertValues(1)
            .AssertComplete();
        await WaitUntilAsync(() => source.UnsubscribeCount == 1);

        var secondTask = TestSubscriber<int>.SubscribeAsync(stream.Take(1));
        await WaitUntilAsync(() => source.SubscribeCount == 2);
        await source.PublishAsync(2);
        (await secondTask)
            .AssertValues(2)
            .AssertComplete();
        await WaitUntilAsync(() => source.UnsubscribeCount == 2);
    }

    [Test]
    public async Task FromEvent_Preserves_Backpressure_When_Source_Awaits_Handler()
    {
        var source = new AsyncEventSource<int>();
        var stream = Stream.FromEvent<int>(source.Subscribe);

        await using var enumerator = stream.GetAsyncEnumerator();

        var firstMoveNext = enumerator.MoveNextAsync().AsTask();
        await WaitUntilAsync(() => source.SubscribeCount == 1);

        await source.PublishAsync(1);
        Assert.That(await firstMoveNext, Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(1));

        await source.PublishAsync(2);

        var blockedPublish = source.PublishAsync(3).AsTask();
        await Task.Delay(50);
        Assert.That(blockedPublish.IsCompleted, Is.False);

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(2));

        await blockedPublish;

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(3));
    }
}
