using Streamix.Abstractions;
using Streamix.Concurrency;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Default implementation of <see cref="IStream{T}"/>.
/// This class is sealed to provide a stable API surface and ensure consistent behavior across operator chains.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public sealed class Stream<T> : IStream<T>
{
    static async IAsyncEnumerable<T> merge(IStream<T>[] streams, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (streams == null || streams.Length == 0) yield break;

        var channel = Channel.CreateUnbounded<T>();
        var tasks = new List<Task>();

        foreach (var stream in streams)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in stream.WithCancellation(cancellationToken))
                        await channel.Writer.WriteAsync(item, cancellationToken);
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    throw;
                }
            }, cancellationToken));
        }

        _ = Task.WhenAll(tasks).ContinueWith(t =>
        {
            if (t.IsFaulted)
                channel.Writer.TryComplete(t.Exception?.InnerException);
            else
                channel.Writer.TryComplete();
        }, cancellationToken);

        while (await channel.Reader.WaitToReadAsync(cancellationToken))
            while (channel.Reader.TryRead(out var item))
                yield return item;

        // Ensure any exception that completed the channel is rethrown
        await channel.Reader.Completion;
    }

    static async IAsyncEnumerable<TResult> zip<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var e1 = first.GetAsyncEnumerator(cancellationToken);
        await using var e2 = second.GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            var t1 = e1.MoveNextAsync();
            var t2 = e2.MoveNextAsync();

            if (!await t1 || !await t2)
                yield break;

            yield return resultSelector(e1.Current, e2.Current);
        }
    }

    readonly IAsyncEnumerable<T> source;
    readonly IClock clock;

    internal Stream(IAsyncEnumerable<T> source, IClock? clock = null)
    {
        this.source = source;
        this.clock = clock ?? SystemClock.Instance;
    }

    async IAsyncEnumerable<TResult> map<TResult>(Func<T, TResult> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            yield return selector(item);
    }

    async IAsyncEnumerable<TResult> mapAwait<TResult>(Func<T, ValueTask<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            yield return await selector(item);
    }

    async IAsyncEnumerable<T> filter(Func<T, bool> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            if (predicate(item))
                yield return item;
    }

    async IAsyncEnumerable<T> filterAwait(Func<T, ValueTask<bool>> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            if (await predicate(item))
                yield return item;
    }

    async IAsyncEnumerable<TResult> flatMap<TResult>(Func<T, ISingle<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                yield return innerItem;
    }

    async IAsyncEnumerable<TResult> flatMapConcurrent<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
                yield return await selector(item);
            yield break;
        }

        var channel = Channel.CreateBounded<TResult>(maxConcurrency);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task>();
            try
            {
                var enumerator = this.WithCancellation(cts.Token).GetAsyncEnumerator();
                try
                {
                    while (true)
                    {
                        await semaphore.WaitAsync(cts.Token);

                        if (!await enumerator.MoveNextAsync())
                        {
                            semaphore.Release();
                            break;
                        }

                        var item = enumerator.Current;

                        var task = Task.Run(async () =>
                        {
                            try
                            {
                                var result = await selector(item);
                                await channel.Writer.WriteAsync(result, cts.Token);
                            }
                            catch (Exception ex)
                            {
                                channel.Writer.TryComplete(ex);
                                throw;
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }, cts.Token);

                        tasks.Add(task);
                        tasks.RemoveAll(t => t.IsCompleted);
                    }
                }
                finally
                {
                    await enumerator.DisposeAsync();
                }

                await Task.WhenAll(tasks);
                channel.Writer.Complete();
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
            }
            finally
            {
                semaphore.Dispose();
            }
        }, cts.Token);

        try
        {
            while (true)
            {
                bool hasMore;
                try
                {
                    hasMore = await channel.Reader.WaitToReadAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                if (!hasMore) break;

                while (channel.Reader.TryRead(out var result))
                    yield return result;
            }

            // Wait for producer to complete and check for exceptions
            await producerTask;
            await channel.Reader.Completion;
        }
        finally
        {
            await cts.CancelAsync();
            try { await producerTask; } catch { }
        }
    }

    async IAsyncEnumerable<TResult> flatMapMany<TResult>(Func<T, IStream<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                yield return innerItem;
    }

    async IAsyncEnumerable<TResult> flatMapManyAwait<TResult>(Func<T, ValueTask<IStream<TResult>>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            var innerStream = await selector(item);
            await foreach (var innerItem in innerStream.WithCancellation(cancellationToken))
                yield return innerItem;
        }
    }

    async IAsyncEnumerable<TResult> flatMapAwaitConcurrent<TResult>(Func<T, ValueTask<ISingle<TResult>>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
            {
                var innerSingle = await selector(item);
                await foreach (var innerItem in innerSingle.WithCancellation(cancellationToken))
                    yield return innerItem;
            }
            yield break;
        }

        var channel = Channel.CreateBounded<TResult>(maxConcurrency);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task>();
            try
            {
                await foreach (var item in this.WithCancellation(cts.Token))
                {
                    await semaphore.WaitAsync(cts.Token);

                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            var innerSingle = await selector(item);
                            await foreach (var result in innerSingle.WithCancellation(cts.Token))
                                await channel.Writer.WriteAsync(result, cts.Token);
                        }
                        catch (Exception ex)
                        {
                            channel.Writer.TryComplete(ex);
                            throw;
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, cts.Token);

                    tasks.Add(task);
                    tasks.RemoveAll(t => t.IsCompleted);
                }

                await Task.WhenAll(tasks);
                channel.Writer.Complete();
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
            }
            finally
            {
                semaphore.Dispose();
            }
        }, cts.Token);

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
                while (channel.Reader.TryRead(out var result))
                    yield return result;

            await producerTask;
            await channel.Reader.Completion;
        }
        finally
        {
            await cts.CancelAsync();
            try { await producerTask; } catch { }
        }
    }

    async IAsyncEnumerable<TResult> flatMapManyAwaitConcurrent<TResult>(Func<T, ValueTask<IStream<TResult>>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in flatMapManyAwait(selector, cancellationToken))
                yield return item;
            yield break;
        }

        var channel = Channel.CreateBounded<TResult>(maxConcurrency);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task>();
            try
            {
                await foreach (var item in this.WithCancellation(cts.Token))
                {
                    await semaphore.WaitAsync(cts.Token);

                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            var innerStream = await selector(item);
                            await foreach (var result in innerStream.WithCancellation(cts.Token))
                                await channel.Writer.WriteAsync(result, cts.Token);
                        }
                        catch (Exception ex)
                        {
                            channel.Writer.TryComplete(ex);
                            throw;
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, cts.Token);

                    tasks.Add(task);
                    tasks.RemoveAll(t => t.IsCompleted);
                }

                await Task.WhenAll(tasks);
                channel.Writer.Complete();
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
            }
            finally
            {
                semaphore.Dispose();
            }
        }, cts.Token);

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
                while (channel.Reader.TryRead(out var result))
                    yield return result;

            await producerTask;
            await channel.Reader.Completion;
        }
        finally
        {
            await cts.CancelAsync();
            try { await producerTask; } catch { }
        }
    }

    async IAsyncEnumerable<TResult> flatMapManyConcurrent<TResult>(Func<T, IAsyncEnumerable<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<TResult>(maxConcurrency);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task>();
            try
            {
                await foreach (var item in this.WithCancellation(cts.Token))
                {
                    await semaphore.WaitAsync(cts.Token);

                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            await foreach (var result in selector(item).WithCancellation(cts.Token))
                                await channel.Writer.WriteAsync(result, cts.Token);
                        }
                        catch (Exception ex)
                        {
                            channel.Writer.TryComplete(ex);
                            throw;
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, cts.Token);

                    tasks.Add(task);
                    tasks.RemoveAll(t => t.IsCompleted);
                }

                await Task.WhenAll(tasks);
                channel.Writer.Complete();
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
            }
            finally
            {
                semaphore.Dispose();
            }
        }, cts.Token);

        try
        {
            while (true)
            {
                bool hasMore;
                try
                {
                    hasMore = await channel.Reader.WaitToReadAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                if (!hasMore) break;

                while (channel.Reader.TryRead(out var result))
                    yield return result;
            }

            // Wait for producer to complete and check for exceptions
            await producerTask;
            await channel.Reader.Completion;
        }
        finally
        {
            await cts.CancelAsync();
            try { await producerTask; } catch { }
        }
    }

    async IAsyncEnumerable<T> take(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (count <= 0) yield break;

        int remaining = count;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            yield return item;
            if (--remaining == 0) break;
        }
    }

    async IAsyncEnumerable<IList<T>> buffer(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var buffer = new List<T>(count);
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            buffer.Add(item);
            if (buffer.Count == count)
            {
                yield return buffer;
                buffer = new List<T>(count);
            }
        }

        if (buffer.Count > 0)
            yield return buffer;
    }

    async IAsyncEnumerable<IStream<T>> window(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var items = new List<T>(count);
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            items.Add(item);
            if (items.Count == count)
            {
                yield return Stream.From(toAsyncEnumerable(items));
                items = new List<T>(count);
            }
        }

        if (items.Count > 0)
        {
            yield return Stream.From(toAsyncEnumerable(items));
        }
    }

    async IAsyncEnumerable<T> toAsyncEnumerable(IEnumerable<T> items)
    {
        foreach (var item in items)
            yield return item;

        await Task.Yield();
    }

    async IAsyncEnumerable<T> throttle(TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        DateTimeOffset? nextAllowedEmission = null;

        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            var now = clock.Now;
            if (nextAllowedEmission == null || now >= nextAllowedEmission.Value)
            {
                yield return item;
                nextAllowedEmission = now + interval;
            }
        }
    }

    async IAsyncEnumerable<T> delay(TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            await clock.Delay(interval, cancellationToken);
            yield return item;
        }
    }

    async IAsyncEnumerable<T> timeout(TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var enumerator = this.GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            var moveNextTask = enumerator.MoveNextAsync().AsTask();
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var timeoutTask = clock.Delay(interval, timeoutCts.Token);

            var completedTask = await Task.WhenAny(moveNextTask, timeoutTask);
            await timeoutCts.CancelAsync();

            if (completedTask == timeoutTask)
                throw new TimeoutException($"The operation has timed out after {interval}.");

            if (await moveNextTask)
                yield return enumerator.Current;
            else
                break;
        }
    }

    async IAsyncEnumerable<T> onErrorResume(Func<Exception, IStream<T>> errorHandler, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        IStream<T>? resumeSource = null;
        try
        {
            try
            {
                enumerator = source.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception ex)
            {
                resumeSource = errorHandler(ex);
            }

            if (enumerator != null)
            {
                while (true)
                {
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync();
                    }
                    catch (Exception ex)
                    {
                        resumeSource = errorHandler(ex);
                        break;
                    }

                    if (hasNext)
                        yield return enumerator.Current;
                    else
                        break;
                }
            }
        }
        finally
        {
            if (enumerator != null)
                await enumerator.DisposeAsync();
        }

        if (resumeSource != null)
        {
            await foreach (var item in resumeSource.WithCancellation(cancellationToken))
                yield return item;
        }
    }

    async IAsyncEnumerable<T> skip(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int remaining = count;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (remaining > 0)
            {
                remaining--;
                continue;
            }

            yield return item;
        }
    }

    async IAsyncEnumerable<T> retry(int retryCount, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int attempts = 0;
        while (true)
        {
            bool failed = false;
            IAsyncEnumerator<T>? enumerator = null;
            try
            {
                enumerator = source.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception)
            {
                if (attempts >= retryCount) throw;
                attempts++;
                continue;
            }

            await using (enumerator)
            {
                while (true)
                {
                    T current = default!;
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync();
                        if (hasNext) current = enumerator.Current;
                    }
                    catch (Exception)
                    {
                        if (attempts >= retryCount) throw;
                        attempts++;
                        failed = true;
                        break;
                    }

                    if (hasNext)
                        yield return current;
                    else
                        break;
                }
            }

            if (!failed) yield break;
        }
    }

    async IAsyncEnumerable<T> runOn(TaskScheduler scheduler, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var enumerator = await Task.Factory.StartNew(() => source.GetAsyncEnumerator(cancellationToken), cancellationToken, TaskCreationOptions.None, scheduler);
        try
        {
            while (true)
            {
                var hasNext = await Task.Factory.StartNew(() => enumerator.MoveNextAsync().AsTask(), cancellationToken, TaskCreationOptions.None, scheduler).Unwrap();
                if (hasNext)
                    yield return enumerator.Current;
                else
                    break;
            }
        }
        finally
        {
            await Task.Factory.StartNew(() => enumerator.DisposeAsync().AsTask(), cancellationToken, TaskCreationOptions.None, scheduler).Unwrap();
        }
    }

    async IAsyncEnumerable<T> doOnNext(Action<T> onNext, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            onNext(item);
            yield return item;
        }
    }

    async IAsyncEnumerable<T> doOnError(Action<Exception> onError, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        try
        {
            try
            {
                enumerator = source.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception ex)
            {
                onError(ex);
                throw;
            }

            while (true)
            {
                bool hasNext;
                try
                {
                    hasNext = await enumerator.MoveNextAsync();
                }
                catch (Exception ex)
                {
                    onError(ex);
                    throw;
                }

                if (hasNext)
                    yield return enumerator.Current;
                else
                    break;
            }
        }
        finally
        {
            if (enumerator != null)
                await enumerator.DisposeAsync();
        }
    }

    async IAsyncEnumerable<T> doOnTerminate(Action onTerminate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        try
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
                yield return item;
        }
        finally
        {
            onTerminate();
        }
    }

    internal IClock Clock => clock;

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return source.GetAsyncEnumerator(cancellationToken);
    }

    /// <inheritdoc />
    public IStream<TResult> MapAwait<TResult>(Func<T, ValueTask<TResult>> selector)
    {
        return Stream.From(mapAwait(selector));
    }

    /// <inheritdoc />
    public IStream<TResult> Map<TResult>(Func<T, TResult> selector)
    {
        return Stream.From(map(selector));
    }

    /// <inheritdoc />
    public IStream<T> FilterAwait(Func<T, ValueTask<bool>> predicate)
    {
        return Stream.From(filterAwait(predicate));
    }

    /// <inheritdoc />
    public IStream<TResult> Select<TResult>(Func<T, TResult> selector) => Map(selector);

    /// <inheritdoc />
    public IStream<T> Filter(Func<T, bool> predicate)
    {
        return Stream.From(filter(predicate));
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMapAwait<TResult>(Func<T, ValueTask<ISingle<TResult>>> selector, int maxConcurrency = 1)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(flatMapAwaitConcurrent(selector, maxConcurrency));
    }

    /// <inheritdoc />
    public IStream<T> Where(Func<T, bool> predicate) => Filter(predicate);

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = 1)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(flatMap(selector))
            : Stream.From(flatMapManyConcurrent(selector, maxConcurrency));
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency = 1)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(flatMapConcurrent(selector, maxConcurrency));
    }

    /// <inheritdoc />
    public IStream<TResult> SelectMany<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = 1) => FlatMap(selector, maxConcurrency);

    /// <inheritdoc />
    public IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency = 1)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(flatMapMany(selector))
            : Stream.From(flatMapManyConcurrent(selector, maxConcurrency));
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMapManyAwait<TResult>(Func<T, ValueTask<IStream<TResult>>> selector, int maxConcurrency = 1)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(flatMapManyAwaitConcurrent(selector, maxConcurrency));
    }

    /// <inheritdoc />
    public IStream<T> Take(int count)
    {
        return Stream.From(take(count));
    }

    /// <inheritdoc />
    public IStream<T> Skip(int count)
    {
        return Stream.From(skip(count));
    }

    /// <summary>
    /// Merges multiple streams into one by combining their elements.
    /// </summary>
    /// <param name="streams">The streams to merge.</param>
    /// <returns>A merged stream.</returns>
    public static IStream<T> Merge(params IStream<T>[] streams)
    {
        return Stream.From(merge(streams));
    }

    /// <summary>
    /// Combines elements from multiple streams using a specified function.
    /// </summary>
    /// <typeparam name="T1">The type of items in the first stream.</typeparam>
    /// <typeparam name="T2">The type of items in the second stream.</typeparam>
    /// <typeparam name="TResult">The type of items in the resulting stream.</typeparam>
    /// <param name="first">The first stream.</param>
    /// <param name="second">The second stream.</param>
    /// <param name="resultSelector">The result selector function.</param>
    /// <returns>A zipped stream.</returns>
    public static IStream<TResult> Zip<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector)
    {
        return Stream.From(zip(first, second, resultSelector));
    }

    /// <inheritdoc />
    public IStream<T> MergeWith(params IStream<T>[] others)
    {
        var all = new IStream<T>[others.Length + 1];
        all[0] = this;
        others.CopyTo(all, 1);
        return Merge(all);
    }

    /// <inheritdoc />
    public IStream<TResult> ZipWith<TOther, TResult>(IStream<TOther> other, Func<T, TOther, TResult> resultSelector)
    {
        return Zip(this, other, resultSelector);
    }

    /// <inheritdoc />
    public IStream<IList<T>> Buffer(int count)
    {
        if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than 0.");
        return Stream.From(buffer(count));
    }

    /// <inheritdoc />
    public IStream<IStream<T>> Window(int count)
    {
        if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than 0.");
        return Stream.From(window(count));
    }

    /// <inheritdoc />
    public IStream<T> Throttle(TimeSpan interval)
    {
        return Stream.From(throttle(interval), clock);
    }

    /// <inheritdoc />
    public IStream<T> Delay(TimeSpan interval)
    {
        return Stream.From<T>(delay(interval), clock);
    }

    /// <inheritdoc />
    public IStream<T> Retry(int retryCount = 1)
    {
        return Stream.From(retry(retryCount), clock);
    }

    /// <inheritdoc />
    public IStream<T> Timeout(TimeSpan interval)
    {
        return Stream.From(timeout(interval), clock);
    }

    /// <inheritdoc />
    public IStream<T> OnErrorResume(Func<Exception, IStream<T>> errorHandler)
    {
        return Stream.From(onErrorResume(errorHandler));
    }

    /// <inheritdoc />
    public IStream<T> OnErrorReturn(T value)
    {
        return OnErrorResume(_ => Stream.From(value));
    }

    /// <inheritdoc />
    public IStream<T> OnErrorMap(Func<Exception, Exception> mapper)
    {
        return OnErrorResume(ex => Stream.Error<T>(mapper(ex)));
    }

    /// <inheritdoc />
    public IConnectableStream<T> Publish() => new Streamix.Operators.ConnectableStream<T>(this);

    /// <inheritdoc />
    public IStream<T> RunOn(TaskScheduler scheduler)
    {
        return Stream.From(runOn(scheduler), clock);
    }

    /// <inheritdoc />
    public async Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            action(item);
    }

    /// <inheritdoc />
    public async Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            await action(item);
    }

    /// <inheritdoc />
    public IStream<T> DoOnNext(Action<T> onNext)
    {
        return Stream.From(doOnNext(onNext), clock);
    }

    /// <inheritdoc />
    public IStream<T> DoOnError(Action<Exception> onError)
    {
        return Stream.From(doOnError(onError), clock);
    }

    /// <inheritdoc />
    public IStream<T> DoOnTerminate(Action onTerminate)
    {
        return Stream.From(doOnTerminate(onTerminate), clock);
    }
}

/// <summary>
/// Provides static methods for creating streams.
/// </summary>
public static class Stream
{
    internal static IStream<T> From<T>(IAsyncEnumerable<T> source, IClock clock) => new Stream<T>(source, clock);

    /// <summary>
    /// Creates a stream from an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="source">The source asynchronous enumerable.</param>
    /// <returns>A stream wrapping the source.</returns>
    public static IStream<T> From<T>(IAsyncEnumerable<T> source)
    {
        if (source is IStream<T> stream) return stream;
        return new Stream<T>(source);
    }

    /// <summary>
    /// Creates a stream from a <see cref="ISingle{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="source">The source single-item stream.</param>
    /// <returns>A stream wrapping the source.</returns>
    public static IStream<T> From<T>(ISingle<T> source) => new Stream<T>(source);

    /// <summary>
    /// Creates a stream from a single value.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>A stream that emits the specified value and then completes.</returns>
    public static IStream<T> From<T>(T value) => From(AsyncEnumerable.Just(value));

    /// <summary>
    /// Creates an empty stream.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <returns>An empty stream.</returns>
    public static IStream<T> Empty<T>() => From(AsyncEnumerable.Empty<T>());

    /// <summary>
    /// Creates a stream that fails with the specified exception.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="exception">The exception to fail with.</param>
    /// <returns>A failing stream.</returns>
    public static IStream<T> Error<T>(Exception exception) => From(AsyncEnumerable.Error<T>(exception));

    /// <summary>
    /// Creates a stream that emits a range of sequential integers.
    /// </summary>
    /// <param name="start">The value of the first integer in the sequence.</param>
    /// <param name="count">The number of sequential integers to generate.</param>
    /// <returns>A stream that contains a range of sequential integers.</returns>
    public static IStream<int> Range(int start, int count) => From(AsyncEnumerable.Range(start, count));

    /// <summary>
    /// Merges multiple streams into one by combining their elements.
    /// </summary>
    /// <typeparam name="T">The type of items in the streams.</typeparam>
    /// <param name="streams">The streams to merge.</param>
    /// <returns>A merged stream.</returns>
    public static IStream<T> Merge<T>(params IStream<T>[] streams) => Stream<T>.Merge(streams);

    /// <summary>
    /// Combines elements from multiple streams using a specified function.
    /// </summary>
    /// <typeparam name="T1">The type of items in the first stream.</typeparam>
    /// <typeparam name="T2">The type of items in the second stream.</typeparam>
    /// <typeparam name="TResult">The type of items in the resulting stream.</typeparam>
    /// <param name="first">The first stream.</param>
    /// <param name="second">The second stream.</param>
    /// <param name="resultSelector">The result selector function.</param>
    /// <returns>A zipped stream.</returns>
    public static IStream<TResult> Zip<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector) => Stream<TResult>.Zip(first, second, resultSelector);
}

internal static class AsyncEnumerable
{
    public static async IAsyncEnumerable<T> Empty<T>([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield break;
    }

    public static async IAsyncEnumerable<T> Just<T>(T value, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return value;
    }

    public static async IAsyncEnumerable<T> Error<T>(Exception exception, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        throw exception;
        yield break;
    }

    public static async IAsyncEnumerable<int> Range(int start, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return start + i;
        }
    }
}
