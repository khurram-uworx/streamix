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
    public async Task FlatMapMany_RespectsMaxConcurrency()
    {
        const int maxConcurrency = 2;
        int activeStreams = 0;
        int maxObservedStreams = 0;
        var lockObj = new object();

        var result = await Stream.Range(1, 4)
            .FlatMapMany(x => Stream.From(GenerateWithTracking(x)), maxConcurrency: maxConcurrency)
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
    public async Task ParallelMap_RespectsMaxConcurrency()
    {
        const int maxConcurrency = 3;
        int activeTasks = 0;
        int maxObservedConcurrency = 0;
        var lockObj = new object();

        var result = await Stream.Range(1, 10)
            .ParallelMap(async x =>
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
    public async Task ParallelMapOrdered_RespectsMaxConcurrency()
    {
        const int maxConcurrency = 3;
        int activeTasks = 0;
        int maxObservedConcurrency = 0;
        var lockObj = new object();

        var result = await Stream.Range(1, 10)
            .ParallelMapOrdered(async x =>
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
    public async Task ParallelMapOrdered_PreservesOrder()
    {
        var result = await Stream.Range(1, 5)
            .ParallelMapOrdered(async x =>
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
    public void ParallelMap_PropagatesErrorsCorrectly()
    {
        var stream = Stream.Range(1, 10)
            .ParallelMap(async x =>
            {
                if (x == 5) throw new InvalidOperationException("Boom");
                await Task.Yield();
                return x;
            }, maxConcurrency: 2);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public void ParallelMapOrdered_PropagatesErrorsCorrectly()
    {
        var stream = Stream.Range(1, 10)
            .ParallelMapOrdered(async x =>
            {
                if (x == 5) throw new InvalidOperationException("Boom");
                await Task.Yield();
                return x;
            }, maxConcurrency: 2);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public async Task FlatMap_Concurrent_CancelOn_Stops_Production()
    {
        var cts = new CancellationTokenSource();
        var pulledItems = new List<int>();

        async IAsyncEnumerable<int> Source([EnumeratorCancellation] CancellationToken ct = default)
        {
            for (int i = 1; i <= 100; i++)
            {
                pulledItems.Add(i);
                yield return i;
                await Task.Delay(10, ct);
            }
        }

        var stream = Stream.From(Source())
            .FlatMap(async x =>
            {
                await Task.Delay(50);
                return x;
            }, maxConcurrency: 2)
            .CancelOn(cts.Token);

        var enumerator = stream.GetAsyncEnumerator();
        await enumerator.MoveNextAsync();

        await cts.CancelAsync();

        try { await enumerator.MoveNextAsync(); } catch (OperationCanceledException) { }

        var countAfterCancel = pulledItems.Count;
        await Task.Delay(200);

        Assert.That(pulledItems.Count, Is.LessThanOrEqualTo(countAfterCancel + 2), "Production should have stopped promptly");
    }
}
