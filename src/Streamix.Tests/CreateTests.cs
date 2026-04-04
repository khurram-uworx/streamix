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
    public async Task Create_Supports_Backpressure()
    {
        var stream = Stream.Create<int>(async emitter =>
        {
            await emitter.EmitAsync(1);
            await emitter.EmitAsync(2);
            emitter.Complete();
        });

        await using var enumerator = stream.GetAsyncEnumerator();

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(1));

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
            await emitter.EmitAsync(1);
            emitter.Complete();
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertValueCount(0)
            .AssertComplete();
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
