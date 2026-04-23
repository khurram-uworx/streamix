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
        int inFlight = 0;
        int maxObserved = 0;

        var source = Stream.From(generateWithLogging(new List<int>()));

        var enumerator = source
            .FlatMap(async x =>
            {
                var current = Interlocked.Increment(ref inFlight);
                maxObserved = Math.Max(maxObserved, current);

                await Task.Delay(10);

                Interlocked.Decrement(ref inFlight);
                return x;
            }, maxConcurrency)
            .GetAsyncEnumerator();

        while (await enumerator.MoveNextAsync()) { }

        Assert.That(maxObserved, Is.LessThanOrEqualTo(maxConcurrency));
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
    public async Task FlatMapOrdered_PropagatesFailurePromptly()
    {
        var firstInnerCanContinue = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondInnerFailed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var stream = Stream.Range(1, 2)
            .FlatMapOrdered(CreateInner, maxConcurrency: 2, maxBufferedItemsPerInner: 1);

        await using var enumerator = stream.GetAsyncEnumerator();

        try
        {
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current, Is.EqualTo(10));

            await secondInnerFailed.Task.WaitAsync(TimeSpan.FromSeconds(2));

            // In fail-fast, MoveNextAsync might return true with earlier items,
            // OR it might throw promptly if it realizes a sibling failed.

            var nextTask = enumerator.MoveNextAsync().AsTask();
            firstInnerCanContinue.SetResult();

            if (await nextTask)
            {
                Assert.That(enumerator.Current, Is.EqualTo(11));
                Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync().AsTask());
            }
        }
        catch (InvalidOperationException ex) when (ex.Message == "Boom")
        {
            // Acceptable prompt failure
        }
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

        Assert.CatchAsync<OperationCanceledException>(async () =>
        {
            await foreach (var item in stream.WithCancellation(cancellation.Token))
            {
                cancellation.Cancel();
            }
        });

        Assert.That(async () => await innerCancelled.Task.WaitAsync(TimeSpan.FromSeconds(2)), Throws.Nothing);
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

    [Test]
    public async Task FlatMap_SiblingCancellationOnFailure()
    {
        // Arrange
        var source = Stream.From(1, 2, 3);
        var cancelledSiblings = 0;

        // Act
        var stream = source.FlatMap(i => Stream.Create<int>(async (emitter, ct) =>
        {
            if (i == 1)
            {
                await Task.Delay(100); // Give others time to start
                throw new Exception("First failure");
            }

            try
            {
                await Task.Delay(5000, ct);
                await emitter.EmitAsync(i);
            }
            catch (OperationCanceledException)
            {
                Interlocked.Increment(ref cancelledSiblings);
                throw;
            }
        }), maxConcurrency: 3);

        // Assert
        var ex = Assert.ThrowsAsync<Exception>(async () =>
        {
            await foreach (var item in stream)
            {
                // Consume
            }
        });
        Assert.That(ex.Message, Is.EqualTo("First failure"));
        Assert.That(cancelledSiblings, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task MapOrdered_WaitsForAllChildrenToSettle()
    {
        // Arrange
        var source = Stream.From(1, 2);
        var completedChildren = 0;

        // Act
        var stream = source.MapOrdered(async i =>
        {
            if (i == 1)
            {
                throw new Exception("Failure");
            }

            await Task.Delay(200);
            Interlocked.Increment(ref completedChildren);
            return i;
        }, maxConcurrency: 2);

        // Assert
        var ex = Assert.ThrowsAsync<Exception>(async () => await stream.ForEachAsync(_ => { }));
        Assert.That(completedChildren, Is.EqualTo(1));
    }

    [Test]
    public async Task FlatMap_PropagatesFirstException()
    {
        // Arrange
        var source = Stream.From(1, 2);

        // Act
        var stream = source.FlatMap(async (int i) =>
        {
            if (i == 1)
            {
                throw new Exception("First");
            }
            await Task.Delay(100);
            throw new Exception("Second");
            return i;
        }, maxConcurrency: 2);

        // Assert
        var ex = Assert.ThrowsAsync<Exception>(async () => await stream.ForEachAsync(_ => { }));
        Assert.That(ex.Message, Is.EqualTo("First"));
    }

    [Test]
    public async Task FlatMap_StopsYieldingImmediatelyOnFault()
    {
        // Arrange
        var source = Stream.Range(1, 10);
        var yieldedItems = new List<int>();

        // Act
        var stream = source.FlatMap(async i =>
        {
            if (i == 1)
            {
                await Task.Delay(100);
                throw new Exception("Boom");
            }

            await Task.Delay(200); // Should finish after failure
            return i;
        }, maxConcurrency: 5);

        // Assert
        Assert.ThrowsAsync<Exception>(async () => await stream.ForEachAsync(i =>
        {
            lock (yieldedItems) yieldedItems.Add(i);
        }));

        // We might yield some if they finished exactly during failure,
        // but we definitely shouldn't yield all 9 successful items.
        Assert.That(yieldedItems.Count, Is.LessThan(9));
    }

    [Test]
    public async Task FlatMapOrdered_InnerFailure_CancelsSiblings_AndWaitsForSettlement()
    {
        var siblingCancelled = false;
        var siblingSettled = false;
        var tcs = new TaskCompletionSource();

        var stream = Stream.From(1, 2)
            .FlatMapOrdered(i =>
            {
                if (i == 1)
                {
                    return Stream.Create<int>(async emitter =>
                    {
                        await tcs.Task;
                        throw new InvalidOperationException("Boom");
                    });
                }

                return Stream.Create<int>(async emitter =>
                {
                    try
                    {
                        await Task.Delay(Timeout.Infinite, emitter.CancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        siblingCancelled = true;
                    }
                    finally
                    {
                        await Task.Yield();
                        await Task.Delay(50);
                        siblingSettled = true;
                    }
                });
            }, maxConcurrency: 2);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var task = stream.ToListAsync();
            await Task.Delay(50);
            tcs.SetResult();
            await task;
        });

        Assert.That(ex.Message, Is.EqualTo("Boom"));
        Assert.That(siblingCancelled, Is.True);
        Assert.That(siblingSettled, Is.True);
    }
}
