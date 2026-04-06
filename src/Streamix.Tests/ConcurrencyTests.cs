using NUnit.Framework;
using System.Runtime.CompilerServices;

namespace Streamix.Tests;

[TestFixture]
public class ConcurrencyTests
{
    async IAsyncEnumerable<int> generateWithLogging(List<int> log, [EnumeratorCancellation] CancellationToken ct = default)
    {
        for (int i = 1; i <= 10; i++)
        {
            log.Add(i);
            yield return i;
            await Task.Yield();
        }
    }

    [Test]
    public async Task FlatMap_RespectsMaxConcurrency()
    {
        const int maxConcurrency = 3;
        int activeTasks = 0;
        int maxObservedConcurrency = 0;
        var lockObj = new object();

        var result = await Stream.Range(1, 10)
            .FlatMap(async x =>
            {
                int currentActive = Interlocked.Increment(ref activeTasks);
                lock (lockObj)
                {
                    maxObservedConcurrency = Math.Max(maxObservedConcurrency, currentActive);
                }

                await Task.Delay(50);

                Interlocked.Decrement(ref activeTasks);
                return x;
            }, maxConcurrency: maxConcurrency)
            .ToListAsync();

        Assert.That(maxObservedConcurrency, Is.LessThanOrEqualTo(maxConcurrency));
        Assert.That(result, Is.EquivalentTo(Enumerable.Range(1, 10)));
    }

    [Test]
    public async Task FlatMapOrdered_RespectsMaxConcurrency()
    {
        const int maxConcurrency = 2;
        int activeStreams = 0;
        int maxObservedStreams = 0;
        var lockObj = new object();

        var result = await Stream.Range(1, 4)
            .FlatMapOrdered(x => Stream.From(GenerateWithTracking(x)), maxConcurrency: maxConcurrency)
            .ToListAsync();

        async IAsyncEnumerable<int> GenerateWithTracking(int x)
        {
            int currentActive = Interlocked.Increment(ref activeStreams);
            lock (lockObj)
            {
                maxObservedStreams = Math.Max(maxObservedStreams, currentActive);
            }

            yield return x * 10;
            await Task.Delay(50);
            yield return x * 10 + 1;

            Interlocked.Decrement(ref activeStreams);
        }

        Assert.That(maxObservedStreams, Is.LessThanOrEqualTo(maxConcurrency));
        Assert.That(result, Is.EquivalentTo(new[] { 10, 11, 20, 21, 30, 31, 40, 41 }));
    }

    [Test]
    public void FlatMapOrdered_RejectsNonPositiveLimits()
    {
        Assert.That(
            () => Stream.Range(1, 3).FlatMapOrdered(x => Stream.From(x), maxConcurrency: 0),
            Throws.TypeOf<ArgumentOutOfRangeException>().With.Property("ParamName").EqualTo("maxConcurrency"));

        Assert.That(
            () => Stream.Range(1, 3).FlatMapOrdered(x => Stream.From(x), maxConcurrency: 2, maxBufferedItemsPerInner: 0),
            Throws.TypeOf<ArgumentOutOfRangeException>().With.Property("ParamName").EqualTo("maxBufferedItemsPerInner"));
    }

    [Test]
    public async Task FlatMapOrdered_BoundsBufferedItemsPerInner()
    {
        var firstInnerCanContinue = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondInnerAttemptedSecondWrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdInnerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var stream = Stream.Range(1, 3)
            .FlatMapOrdered(CreateInner, maxConcurrency: 2, maxBufferedItemsPerInner: 1);

        await using var enumerator = stream.GetAsyncEnumerator();

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(10));

        await secondInnerAttemptedSecondWrite.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(100);
        Assert.That(thirdInnerStarted.Task.IsCompleted, Is.False);

        firstInnerCanContinue.SetResult();

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(11));
        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(20));
        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(21));
        await thirdInnerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(30));
        Assert.That(await enumerator.MoveNextAsync(), Is.False);

        IStream<int> CreateInner(int value)
        {
            return Stream.Create<int>(async emitter =>
            {
                if (value == 1)
                {
                    await emitter.EmitAsync(10);
                    await firstInnerCanContinue.Task.WaitAsync(emitter.CancellationToken);
                    await emitter.EmitAsync(11);
                    emitter.Complete();
                    return;
                }

                if (value == 2)
                {
                    await emitter.EmitAsync(20);
                    secondInnerAttemptedSecondWrite.TrySetResult();
                    await emitter.EmitAsync(21);
                    emitter.Complete();
                    return;
                }

                thirdInnerStarted.TrySetResult();
                await emitter.EmitAsync(30);
                emitter.Complete();
            });
        }
    }

    [Test]
    public async Task FlatMap_BackpressureBlocksProducer()
    {
        const int maxConcurrency = 2;
        var pulledItems = new List<int>();
        var source = Stream.From(generateWithLogging(pulledItems));

        var enumerator = source
            .FlatMap(async x =>
            {
                await Task.Delay(10);
                return x;
            }, maxConcurrency: maxConcurrency)
            .GetAsyncEnumerator();

        // Initially nothing pulled
        Assert.That(pulledItems, Is.Empty);

        // Pull first item
        Assert.That(await enumerator.MoveNextAsync(), Is.True);

        // Due to maxConcurrency=2 and BoundedChannel(2), the producer can pull:
        // 1. One item being processed (semaphore wait)
        // 2. Another item being processed (semaphore wait)
        // 3. One item waiting to be written to bounded channel (WriteAsync blocks)
        // Actually, the loop is:
        // await semaphore.WaitAsync()
        // Task.Run(...)
        //   await channel.Writer.WriteAsync(...)
        //   semaphore.Release()

        // If maxConcurrency is 2:
        // Iteration 1: semaphore.Wait (success), Task 1 starts.
        // Iteration 2: semaphore.Wait (success), Task 2 starts.
        // Iteration 3: semaphore.Wait (blocks)

        // So at most 2 items are pulled from source before semaphore blocks.
        // If we use Channel.CreateBounded(maxConcurrency), it's another buffer.

        // Let's check how many were pulled. It should be small and bounded.
        // maxConcurrency (2) are being processed, and 1 might be waiting at semaphore.WaitAsync.
        // Another one might be pulled by the foreach before it hits the semaphore wait.
        Assert.That(pulledItems.Count, Is.LessThanOrEqualTo(maxConcurrency + 3));

        int count = 1;
        while (await enumerator.MoveNextAsync())
        {
            count++;
            // At any point, the number of items pulled from source should not be
            // significantly larger than what we've consumed + buffer
            Assert.That(pulledItems.Count, Is.LessThanOrEqualTo(count + maxConcurrency + 1));
        }

        Assert.That(count, Is.EqualTo(10));
    }

    [Test]
    public void FlatMap_PropagatesErrorsCorrectly()
    {
        var stream = Stream.Range(1, 10)
            .FlatMap(async x =>
            {
                if (x == 5) throw new InvalidOperationException("Boom");
                await Task.Yield();
                return x;
            }, maxConcurrency: 2);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public async Task FlatMap_OrderingIsNonDeterministic()
    {
        // We want to prove that with concurrency, results can arrive out of order
        // if they take different amounts of time.
        var result = await Stream.Range(1, 5)
            .FlatMap(async x =>
            {
                // Task for 1 takes 100ms, others take 1ms
                await Task.Delay(x == 1 ? 100 : 1);
                return x;
            }, maxConcurrency: 5)
            .ToListAsync();

        // 1 should NOT be first because it was delayed
        Assert.That(result[0], Is.Not.EqualTo(1));
        Assert.That(result, Is.EquivalentTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task Map_RespectsMaxConcurrency()
    {
        const int maxConcurrency = 3;
        int activeTasks = 0;
        int maxObservedConcurrency = 0;
        var lockObj = new object();

        var result = await Stream.Range(1, 10)
            .Map(async x =>
            {
                int currentActive = Interlocked.Increment(ref activeTasks);
                lock (lockObj)
                {
                    maxObservedConcurrency = Math.Max(maxObservedConcurrency, currentActive);
                }

                await Task.Delay(50);

                Interlocked.Decrement(ref activeTasks);
                return x;
            }, maxConcurrency: maxConcurrency)
            .ToListAsync();

        Assert.That(maxObservedConcurrency, Is.LessThanOrEqualTo(maxConcurrency));
        Assert.That(result, Is.EquivalentTo(Enumerable.Range(1, 10)));
    }

    [Test]
    public async Task MapOrdered_RespectsMaxConcurrency()
    {
        const int maxConcurrency = 3;
        int activeTasks = 0;
        int maxObservedConcurrency = 0;
        var lockObj = new object();

        var result = await Stream.Range(1, 10)
            .MapOrdered(async x =>
            {
                int currentActive = Interlocked.Increment(ref activeTasks);
                lock (lockObj)
                {
                    maxObservedConcurrency = Math.Max(maxObservedConcurrency, currentActive);
                }

                await Task.Delay(50);

                Interlocked.Decrement(ref activeTasks);
                return x;
            }, maxConcurrency: maxConcurrency)
            .ToListAsync();

        Assert.That(maxObservedConcurrency, Is.LessThanOrEqualTo(maxConcurrency));
        Assert.That(result, Is.EqualTo(Enumerable.Range(1, 10)));
    }

    [Test]
    public async Task MapOrdered_PreservesOrder()
    {
        var result = await Stream.Range(1, 5)
            .MapOrdered(async x =>
            {
                // Task for 1 takes 100ms, others take 1ms
                await Task.Delay(x == 1 ? 100 : 1);
                return x;
            }, maxConcurrency: 5)
            .ToListAsync();

        // 1 SHOULD be first because it's ordered
        Assert.That(result[0], Is.EqualTo(1));
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task MapOrdered_DefersLaterFailureUntilEarlierWorkCanDrain()
    {
        var firstCanComplete = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondFailed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var stream = Stream.Range(1, 2)
            .MapOrdered(async x =>
            {
                if (x == 1)
                {
                    await firstCanComplete.Task;
                    return 10;
                }

                secondFailed.TrySetResult();
                throw new InvalidOperationException("Boom");
            }, maxConcurrency: 2);

        await using var enumerator = stream.GetAsyncEnumerator();

        var firstMoveNextTask = enumerator.MoveNextAsync().AsTask();
        await secondFailed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(100);
        Assert.That(firstMoveNextTask.IsCompleted, Is.False);

        firstCanComplete.SetResult();

        Assert.That(await firstMoveNextTask, Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(10));

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync().AsTask());
        Assert.That(exception?.Message, Is.EqualTo("Boom"));
    }

    [Test]
    public async Task FlatMapOrdered_DefersLaterInnerFailureUntilEarlierInnerDrains()
    {
        var firstInnerCanContinue = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondInnerFailed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var stream = Stream.Range(1, 2)
            .FlatMapOrdered(CreateInner, maxConcurrency: 2, maxBufferedItemsPerInner: 1);

        await using var enumerator = stream.GetAsyncEnumerator();

        Assert.That(await enumerator.MoveNextAsync(), Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(10));

        await secondInnerFailed.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var nextTask = enumerator.MoveNextAsync().AsTask();
        await Task.Delay(100);
        Assert.That(nextTask.IsCompleted, Is.False);

        firstInnerCanContinue.SetResult();

        Assert.That(await nextTask, Is.True);
        Assert.That(enumerator.Current, Is.EqualTo(11));

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync().AsTask());
        Assert.That(exception?.Message, Is.EqualTo("Boom"));
        return;

        IStream<int> CreateInner(int value)
        {
            return Stream.Create<int>(async emitter =>
            {
                if (value == 1)
                {
                    await emitter.EmitAsync(10);
                    await firstInnerCanContinue.Task.WaitAsync(emitter.CancellationToken);
                    await emitter.EmitAsync(11);
                    emitter.Complete();
                    return;
                }

                secondInnerFailed.TrySetResult();
                throw new InvalidOperationException("Boom");
            });
        }
    }

    [Test]
    public void FlatMapOrdered_StopsPromptlyWhenConsumerCancels()
    {
        var innerCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellation = new CancellationTokenSource();

        var stream = Stream.Range(1, 2)
            .FlatMapOrdered(_ => Stream.Create<int>(async emitter =>
            {
                try
                {
                    await emitter.EmitAsync(1);
                    await Task.Delay(Timeout.InfiniteTimeSpan, emitter.CancellationToken);
                }
                catch (TaskCanceledException) when (emitter.CancellationToken.IsCancellationRequested)
                {
                    innerCancelled.TrySetResult();
                }
                catch (OperationCanceledException) when (emitter.CancellationToken.IsCancellationRequested)
                {
                    innerCancelled.TrySetResult();
                }
            }), maxConcurrency: 2, maxBufferedItemsPerInner: 1);

        Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await foreach (var item in stream.WithCancellation(cancellation.Token))
            {
                cancellation.Cancel();
            }
        });

        Assert.That(async () => await innerCancelled.Task.WaitAsync(TimeSpan.FromSeconds(2)), Throws.Nothing);
    }

    [Test]
    public void Map_PropagatesErrorsCorrectly()
    {
        var stream = Stream.Range(1, 10)
            .Map(async x =>
            {
                if (x == 5) throw new InvalidOperationException("Boom");
                await Task.Yield();
                return x;
            }, maxConcurrency: 2);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public void MapOrdered_PropagatesErrorsCorrectly()
    {
        var stream = Stream.Range(1, 10)
            .MapOrdered(async x =>
            {
                if (x == 5) throw new InvalidOperationException("Boom");
                await Task.Yield();
                return x;
            }, maxConcurrency: 2);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }
}
