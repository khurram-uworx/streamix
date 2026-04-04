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

        var result = await stream.ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
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
    public void Create_Propagates_Error_Via_Fail()
    {
        var stream = Stream.Create<int>(emitter =>
        {
            emitter.Fail(new InvalidOperationException("Test Error"));
            return Task.CompletedTask;
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public void Create_Propagates_Producer_Exception()
    {
        var stream = Stream.Create<int>(async emitter =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Producer Exception");
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
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
        var consumeTask = stream.ToListAsync(cts.Token);

        await Task.Delay(100);
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await consumeTask);
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

        var result = await stream.ToListAsync();
        Assert.That(result, Is.Empty);
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
