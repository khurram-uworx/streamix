using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class CreateTests
{
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
}
