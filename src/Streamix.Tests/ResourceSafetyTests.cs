using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class ResourceSafetyTests
{
    class DisposableSource : IAsyncEnumerable<int>, IAsyncDisposable
    {
        public int DisposeCount { get; private set; }
        public int MoveNextCount { get; private set; }
        private readonly int count;
        private readonly bool throwOnMoveNext;

        public DisposableSource(int count = 10, bool throwOnMoveNext = false)
        {
            this.count = count;
            this.throwOnMoveNext = throwOnMoveNext;
        }

        public async IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            try
            {
                for (int i = 0; i < count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    MoveNextCount++;
                    if (throwOnMoveNext && i == 1) throw new InvalidOperationException("Source failure");
                    yield return i;
                    await Task.Yield();
                }
            }
            finally
            {
                DisposeCount++;
            }
        }

        public ValueTask DisposeAsync()
        {
            // The async iterator's finally block handles "disposal" logic in this mock
            return ValueTask.CompletedTask;
        }
    }

    [Test]
    public async Task Merge_DisposesAllSources_OnCancellation()
    {
        var s1 = new DisposableSource(100);
        var s2 = new DisposableSource(100);

        var cts = new CancellationTokenSource();
        var merged = Stream.Merge(Stream.From((IAsyncEnumerable<int>)s1), Stream.From((IAsyncEnumerable<int>)s2));

        int count = 0;
        try
        {
            await foreach (var item in merged.WithCancellation(cts.Token))
            {
                count++;
                if (count == 5) await cts.CancelAsync();
            }
        }
        catch (OperationCanceledException) { }

        // Wait for background tasks to settle
        await Task.Delay(500);

        Assert.That(s1.DisposeCount, Is.EqualTo(1), "Source 1 should be disposed");
        Assert.That(s2.DisposeCount, Is.EqualTo(1), "Source 2 should be disposed");
    }

    [Test]
    public async Task Merge_DisposesAllSources_OnFailure()
    {
        var s1 = new DisposableSource(100, throwOnMoveNext: true);
        var s2 = new DisposableSource(100);

        var merged = Stream.Merge(Stream.From((IAsyncEnumerable<int>)s1), Stream.From((IAsyncEnumerable<int>)s2));

        try
        {
            await foreach (var item in merged)
            {
                // Just consume
            }
        }
        catch (InvalidOperationException) { }

        // We need to wait a bit because the other source might be disposed asynchronously
        await Task.Delay(500);

        Assert.That(s1.DisposeCount, Is.EqualTo(1), "Source 1 should be disposed");
        Assert.That(s2.DisposeCount, Is.EqualTo(1), "Source 2 should be disposed");
    }

    [Test]
    public async Task Zip_DisposesBothSources_WhenOneFinishes()
    {
        var s1 = new DisposableSource(5);
        var s2 = new DisposableSource(100);

        var zipped = Stream.Zip(Stream.From((IAsyncEnumerable<int>)s1), Stream.From((IAsyncEnumerable<int>)s2), (a, b) => a + b);

        await foreach (var item in zipped) { }

        Assert.That(s1.DisposeCount, Is.EqualTo(1), "Source 1 should be disposed");
        Assert.That(s2.DisposeCount, Is.EqualTo(1), "Source 2 should be disposed");
    }

    [Test]
    public async Task FlatMap_DisposesInnerSource_OnCancellation()
    {
        var innerDisposed = 0;
        async IAsyncEnumerable<int> GetInner()
        {
            try
            {
                yield return 1;
                await Task.Delay(1000);
                yield return 2;
            }
            finally
            {
                innerDisposed++;
            }
        }

        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 1).FlatMapMany<int>(x => Stream.From((IAsyncEnumerable<int>)GetInner()));

        var enumerator = stream.GetAsyncEnumerator(cts.Token);
        Assert.That(await enumerator.MoveNextAsync(), Is.True);

        await cts.CancelAsync();
        try { await enumerator.MoveNextAsync(); } catch (OperationCanceledException) { }
        await enumerator.DisposeAsync();

        Assert.That(innerDisposed, Is.EqualTo(1));
    }

    [Test]
    public async Task Buffer_DisposesSource_OnCancellation()
    {
        var source = new DisposableSource(100);
        var cts = new CancellationTokenSource();

        var buffered = Stream.From((IAsyncEnumerable<int>)source).Buffer(10);

        var enumerator = buffered.GetAsyncEnumerator(cts.Token);
        Assert.That(await enumerator.MoveNextAsync(), Is.True);

        await cts.CancelAsync();
        // MoveNextAsync might or might not throw depending on where it was,
        // but DisposeAsync MUST dispose the source.
        try { await enumerator.MoveNextAsync(); } catch (OperationCanceledException) { }
        await enumerator.DisposeAsync();

        Assert.That(source.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task RefCount_DisposesSource_WhenLastSubscriberLeaves()
    {
        var source = new DisposableSource(100);
        var shared = Stream.From((IAsyncEnumerable<int>)source).Publish().RefCount();

        async Task ConsumeOne()
        {
            var enumerator = shared.GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
            await Task.Delay(50);
            await enumerator.DisposeAsync();
        }

        var t1 = ConsumeOne();
        var t2 = ConsumeOne();

        await Task.WhenAll(t1, t2);

        // Wait for background tasks to settle
        await Task.Delay(500);

        Assert.That(source.DisposeCount, Is.EqualTo(1), "Source should be disposed now");
    }

    [Test]
    public async Task ParallelMap_CancelsOutstandingTasks_OnCancellation()
    {
        var taskStarted = 0;
        var taskCancelled = 0;

        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 10)
            .ParallelMap(async x =>
            {
                Interlocked.Increment(ref taskStarted);
                try
                {
                    await Task.Delay(1000, cts.Token);
                    return x;
                }
                catch (OperationCanceledException)
                {
                    Interlocked.Increment(ref taskCancelled);
                    throw;
                }
            }, maxConcurrency: 5);

        var enumerator = stream.GetAsyncEnumerator(cts.Token);
        var moveNextTask = enumerator.MoveNextAsync();

        // Give some time for producer to start tasks
        await Task.Delay(200);

        await cts.CancelAsync();
        try { await moveNextTask; } catch (OperationCanceledException) { }
        await enumerator.DisposeAsync();

        // Wait for background tasks to settle
        await Task.Delay(500);

        Assert.That(taskStarted, Is.GreaterThan(0));
        Assert.That(taskCancelled, Is.EqualTo(taskStarted));
    }

    [Test]
    public async Task ParallelMapOrdered_CancelsOutstandingTasks_OnCancellation()
    {
        var taskStarted = 0;
        var taskCancelled = 0;

        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 10)
            .ParallelMapOrdered(async x =>
            {
                Interlocked.Increment(ref taskStarted);
                try
                {
                    await Task.Delay(1000, cts.Token);
                    return x;
                }
                catch (OperationCanceledException)
                {
                    Interlocked.Increment(ref taskCancelled);
                    throw;
                }
            }, maxConcurrency: 5);

        var enumerator = stream.GetAsyncEnumerator(cts.Token);
        var moveNextTask = enumerator.MoveNextAsync();

        // Give some time for producer to start tasks
        await Task.Delay(200);

        await cts.CancelAsync();
        try { await moveNextTask; } catch (OperationCanceledException) { }
        await enumerator.DisposeAsync();

        // Wait for background tasks to settle
        await Task.Delay(500);

        Assert.That(taskStarted, Is.GreaterThan(0));
        Assert.That(taskCancelled, Is.EqualTo(taskStarted));
    }

    class MockResource : IDisposable, IAsyncDisposable
    {
        public int DisposeCount { get; private set; }
        public bool ThrowOnDispose { get; set; }

        public void Dispose()
        {
            DisposeCount++;
            if (ThrowOnDispose) throw new InvalidOperationException("Dispose failure");
        }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            if (ThrowOnDispose) throw new InvalidOperationException("DisposeAsync failure");
            return ValueTask.CompletedTask;
        }
    }

    [Test]
    public async Task Using_IDisposable_DisposesOnCompletion()
    {
        var resource = new MockResource();
        var stream = Stream.Using(() => resource, r => Stream.Range(1, 5));

        var result = await stream.ToListAsync();

        Assert.That(result, Has.Count.EqualTo(5));
        Assert.That(resource.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Using_IDisposable_DisposesOnFailure()
    {
        var resource = new MockResource();
        var stream = Stream.Using(() => resource, r => Stream.Error<int>(new Exception("Upstream failure")));

        Assert.ThrowsAsync<Exception>(async () => await stream.ToListAsync());
        Assert.That(resource.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Using_IDisposable_DisposesOnCancellation()
    {
        var resource = new MockResource();
        var cts = new CancellationTokenSource();
        var stream = Stream.Using(() => resource, r => Stream.Range(1, 100));

        var count = 0;
        try
        {
            await foreach (var item in stream.WithCancellation(cts.Token))
            {
                if (++count == 5) await cts.CancelAsync();
            }
        }
        catch (OperationCanceledException) { }

        Assert.That(resource.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Using_IAsyncDisposable_DisposesOnCompletion()
    {
        var resource = new MockResource();
        var stream = Stream.Using(ct => ValueTask.FromResult(resource), r => Stream.Range(1, 5));

        var result = await stream.ToListAsync();

        Assert.That(result, Has.Count.EqualTo(5));
        Assert.That(resource.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Using_IAsyncDisposable_DisposesOnFailure()
    {
        var resource = new MockResource();
        var stream = Stream.Using(ct => ValueTask.FromResult(resource), r => Stream.Error<int>(new Exception("Upstream failure")));

        Assert.ThrowsAsync<Exception>(async () => await stream.ToListAsync());
        Assert.That(resource.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Using_IAsyncDisposable_DisposesOnCancellation()
    {
        var resource = new MockResource();
        var cts = new CancellationTokenSource();
        var stream = Stream.Using(ct => ValueTask.FromResult(resource), r => Stream.Range(1, 100));

        var count = 0;
        try
        {
            await foreach (var item in stream.WithCancellation(cts.Token))
            {
                if (++count == 5) await cts.CancelAsync();
            }
        }
        catch (OperationCanceledException) { }

        Assert.That(resource.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Using_PropagatesDisposalException()
    {
        var resource = new MockResource { ThrowOnDispose = true };
        var stream = Stream.Using(() => resource, r => Stream.Range(1, 5));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public void Using_DisposalException_ReplacesUpstreamException()
    {
        var resource = new MockResource { ThrowOnDispose = true };
        var stream = Stream.Using(() => resource, r => Stream.Error<int>(new Exception("Upstream failure")));

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());

        Assert.That(exception!.Message, Is.EqualTo("Dispose failure"));
    }

    [Test]
    public void Using_DisposesResource_WhenStreamFactoryThrows()
    {
        var resource = new MockResource();
        var stream = Stream.Using<MockResource, int>(() => resource, _ => throw new InvalidOperationException("Factory failure"));

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());

        Assert.That(exception!.Message, Is.EqualTo("Factory failure"));
        Assert.That(resource.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public void Using_IAsyncDisposable_DisposesResource_WhenStreamFactoryThrows()
    {
        var resource = new MockResource();
        var stream = Stream.Using<MockResource, int>(ct => ValueTask.FromResult(resource), _ => throw new InvalidOperationException("Factory failure"));

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());

        Assert.That(exception!.Message, Is.EqualTo("Factory failure"));
        Assert.That(resource.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Using_CreatesFreshResourcePerSubscription()
    {
        var createCount = 0;
        var stream = Stream.Using(() =>
        {
            createCount++;
            return new MockResource();
        }, r => Stream.Range(1, 3));

        await stream.ToListAsync();
        await stream.ToListAsync();

        Assert.That(createCount, Is.EqualTo(2));
    }
}
