using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Streamix.Abstractions;

namespace Streamix;

/// <summary>
/// Default implementation of <see cref="IStream{T}"/>.
/// This class is sealed to provide a stable API surface and ensure consistent behavior across operator chains.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public sealed class Stream<T> : IStream<T>
{
    private readonly IAsyncEnumerable<T> _source;

    internal Stream(IAsyncEnumerable<T> source)
    {
        _source = source;
    }

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return _source.GetAsyncEnumerator(cancellationToken);
    }

    /// <inheritdoc />
    public IStream<TResult> Map<TResult>(Func<T, TResult> selector)
    {
        return Stream.From(MapInternal(selector));
    }

    private async IAsyncEnumerable<TResult> MapInternal<TResult>(Func<T, TResult> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            yield return selector(item);
        }
    }

    /// <inheritdoc />
    public IStream<TResult> Select<TResult>(Func<T, TResult> selector) => Map(selector);

    /// <inheritdoc />
    public IStream<T> Filter(Func<T, bool> predicate)
    {
        return Stream.From(FilterInternal(predicate));
    }

    private async IAsyncEnumerable<T> FilterInternal(Func<T, bool> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (predicate(item))
            {
                yield return item;
            }
        }
    }

    /// <inheritdoc />
    public IStream<T> Where(Func<T, bool> predicate) => Filter(predicate);

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = 1)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(FlatMapInternal(selector))
            : Stream.From(FlatMapManyConcurrentInternal(selector, maxConcurrency));
    }

    private async IAsyncEnumerable<TResult> FlatMapInternal<TResult>(Func<T, ISingle<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
            {
                yield return innerItem;
            }
        }
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency = 1)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(FlatMapConcurrentInternal(selector, maxConcurrency));
    }

    private async IAsyncEnumerable<TResult> FlatMapConcurrentInternal<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
            {
                yield return await selector(item);
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
                {
                    yield return result;
                }
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

    /// <inheritdoc />
    public IStream<TResult> SelectMany<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = 1) => FlatMap(selector, maxConcurrency);

    /// <inheritdoc />
    public IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency = 1)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(FlatMapManyInternal(selector))
            : Stream.From(FlatMapManyConcurrentInternal(selector, maxConcurrency));
    }

    private async IAsyncEnumerable<TResult> FlatMapManyInternal<TResult>(Func<T, IStream<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
            {
                yield return innerItem;
            }
        }
    }

    private async IAsyncEnumerable<TResult> FlatMapManyConcurrentInternal<TResult>(Func<T, IAsyncEnumerable<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
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
                            {
                                await channel.Writer.WriteAsync(result, cts.Token);
                            }
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
                {
                    yield return result;
                }
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

    /// <inheritdoc />
    public IStream<T> Take(int count)
    {
        return Stream.From(TakeInternal(count));
    }

    private async IAsyncEnumerable<T> TakeInternal(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (count <= 0) yield break;

        int remaining = count;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            yield return item;
            if (--remaining == 0) break;
        }
    }

    /// <inheritdoc />
    public IStream<T> Skip(int count)
    {
        return Stream.From(SkipInternal(count));
    }

    private async IAsyncEnumerable<T> SkipInternal(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
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

    /// <summary>
    /// Merges multiple streams into one by combining their elements.
    /// </summary>
    public static IStream<T> Merge(params IStream<T>[] streams)
    {
        return Stream.From(MergeInternal(streams));
    }

    private static async IAsyncEnumerable<T> MergeInternal(IStream<T>[] streams, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (streams == null || streams.Length == 0) yield break;

        var channel = System.Threading.Channels.Channel.CreateUnbounded<T>();
        var tasks = new List<Task>();

        foreach (var stream in streams)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in stream.WithCancellation(cancellationToken))
                    {
                        await channel.Writer.WriteAsync(item, cancellationToken);
                    }
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
            {
                channel.Writer.TryComplete(t.Exception?.InnerException);
            }
            else
            {
                channel.Writer.TryComplete();
            }
        }, cancellationToken);

        while (await channel.Reader.WaitToReadAsync(cancellationToken))
        {
            while (channel.Reader.TryRead(out var item))
            {
                yield return item;
            }
        }

        // Ensure any exception that completed the channel is rethrown
        await channel.Reader.Completion;
    }

    /// <summary>
    /// Combines elements from multiple streams using a specified function.
    /// </summary>
    public static IStream<TResult> Zip<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector)
    {
        return Stream.From(ZipInternal(first, second, resultSelector));
    }

    private static async IAsyncEnumerable<TResult> ZipInternal<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var e1 = first.GetAsyncEnumerator(cancellationToken);
        await using var e2 = second.GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            var t1 = e1.MoveNextAsync();
            var t2 = e2.MoveNextAsync();

            if (!await t1 || !await t2)
            {
                yield break;
            }

            yield return resultSelector(e1.Current, e2.Current);
        }
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
        return Stream.From(BufferInternal(count));
    }

    private async IAsyncEnumerable<IList<T>> BufferInternal(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
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
        {
            yield return buffer;
        }
    }

    /// <inheritdoc />
    public IStream<IStream<T>> Window(int count)
    {
        if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than 0.");
        return Stream.From(WindowInternal(count));
    }

    private async IAsyncEnumerable<IStream<T>> WindowInternal(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var items = new List<T>(count);
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            items.Add(item);
            if (items.Count == count)
            {
                yield return Stream.From(ToAsyncEnumerable(items));
                items = new List<T>(count);
            }
        }

        if (items.Count > 0)
        {
            yield return Stream.From(ToAsyncEnumerable(items));
        }
    }

    private async IAsyncEnumerable<T> ToAsyncEnumerable(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
        await Task.Yield();
    }


    /// <inheritdoc />
    public IStream<T> Throttle(TimeSpan interval) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> Delay(TimeSpan interval) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> Retry(int retryCount = 1) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> Timeout(TimeSpan interval) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> OnErrorResume(Func<Exception, IStream<T>> errorHandler)
    {
        return Stream.From(OnErrorResumeInternal(errorHandler));
    }

    private async IAsyncEnumerable<T> OnErrorResumeInternal(Func<Exception, IStream<T>> errorHandler, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        IStream<T>? resumeSource = null;
        try
        {
            try
            {
                enumerator = _source.GetAsyncEnumerator(cancellationToken);
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
                    {
                        yield return enumerator.Current;
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            if (enumerator != null)
            {
                await enumerator.DisposeAsync();
            }
        }

        if (resumeSource != null)
        {
            await foreach (var item in resumeSource.WithCancellation(cancellationToken))
            {
                yield return item;
            }
        }
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
    public IConnectableStream<T> Publish() => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> RunOn(TaskScheduler scheduler) => throw new NotImplementedException();

    /// <inheritdoc />
    public async Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            action(item);
        }
    }

    /// <inheritdoc />
    public async Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            await action(item);
        }
    }
}

/// <summary>
/// Provides static methods for creating streams.
/// </summary>
public static class Stream
{
    /// <summary>
    /// Creates a stream from an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    public static IStream<T> From<T>(IAsyncEnumerable<T> source)
    {
        if (source is IStream<T> stream) return stream;
        return new Stream<T>(source);
    }

    /// <summary>
    /// Creates a stream from a <see cref="ISingle{T}"/>.
    /// </summary>
    public static IStream<T> From<T>(ISingle<T> source) => new Stream<T>(source);

    /// <summary>
    /// Creates a stream from a single value.
    /// </summary>
    public static IStream<T> From<T>(T value) => From(AsyncEnumerable.Just(value));

    /// <summary>
    /// Creates an empty stream.
    /// </summary>
    public static IStream<T> Empty<T>() => From(AsyncEnumerable.Empty<T>());

    /// <summary>
    /// Creates a stream that fails with the specified exception.
    /// </summary>
    public static IStream<T> Error<T>(Exception exception) => From(AsyncEnumerable.Error<T>(exception));

    /// <summary>
    /// Creates a stream that emits a range of sequential integers.
    /// </summary>
    public static IStream<int> Range(int start, int count) => From(AsyncEnumerable.Range(start, count));

    /// <summary>
    /// Merges multiple streams into one by combining their elements.
    /// </summary>
    public static IStream<T> Merge<T>(params IStream<T>[] streams) => Stream<T>.Merge(streams);

    /// <summary>
    /// Combines elements from multiple streams using a specified function.
    /// </summary>
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
