using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Provides LINQ-style extension methods for <see cref="IStream{T}"/>.
/// These extensions make it easy to work with streams using familiar LINQ patterns.
/// For now they are a convenience layer and do not expose the full fluent concurrency-control surface.
/// </summary>
public static class LinqExtensions
{
    static async IAsyncEnumerable<TResult> selectManyAwaitConcurrent<T, TResult>(
        IStream<T> source,
        Func<T, ValueTask<IStream<TResult>>> selector,
        int maxConcurrency,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");

        if (maxConcurrency == 1)
        {
            await foreach (var item in source.WithCancellation(cancellationToken))
            {
                var innerStream = await selector(item);
                await foreach (var innerItem in innerStream.WithCancellation(cancellationToken))
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
                var enumerator = source.WithCancellation(cts.Token).GetAsyncEnumerator();

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
                try { await Task.WhenAll(tasks); } catch { }
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

    /// <summary>
    /// Filters a stream of values based on a predicate.
    /// LINQ-style extension for <see cref="IStream{T}.Filter(Func{T, bool})"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="predicate">A function to test each element for a condition.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains elements from the input stream that satisfy the condition.</returns>
    public static IStream<T> Where<T>(this IStream<T> source, Func<T, bool> predicate)
        => source.Filter(predicate);

    /// <summary>
    /// Filters a stream of values based on an asynchronous predicate.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="predicate">An asynchronous function to test each element for a condition.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains elements from the input stream that satisfy the condition.</returns>
    public static IStream<T> WhereAsync<T>(this IStream<T> source, Func<T, ValueTask<bool>> predicate)
    {
        return source.FilterAwait(predicate);
    }

    /// <summary>
    /// Projects each element of a stream into a new form using a synchronous selector function.
    /// LINQ-style extension for <see cref="IStream{T}.Map{TResult}(Func{T, TResult})"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to transform.</param>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the transform function on each element of source.</returns>
    public static IStream<TResult> Select<T, TResult>(this IStream<T> source, Func<T, TResult> selector)
        => source.Map(selector);

    /// <summary>
    /// Projects each element of a stream into a new form using an asynchronous selector function.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to transform.</param>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async transform function on each element of source.</returns>
    public static IStream<TResult> SelectAsync<T, TResult>(this IStream<T> source, Func<T, ValueTask<TResult>> selector)
    {
        return source.MapAwait(selector);
    }

    /// <summary>
    /// Projects each element of a stream to an <see cref="IStream{TResult}"/> and flattens the resulting streams into one stream.
    /// Results are emitted concurrently as they complete (unordered).
    /// Use fluent operators such as <see cref="IStream{T}.ConcatMap{TResult}(Func{T, IStream{TResult}})"/> or <see cref="IStream{T}.FlatMapOrdered{TResult}(Func{T, IStream{TResult}}, int, int)"/> when ordered or sequential flattening is required.
    /// </summary>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, IStream<TResult>> selector)
        => source.FlatMap(selector, maxConcurrency: int.MaxValue);

    /// <summary>
    /// Projects each element of a stream to an <see cref="IStream{TResult}"/> and flattens the resulting streams into one stream with concurrency support (unordered).
    /// Use fluent operators such as <see cref="IStream{T}.ConcatMap{TResult}(Func{T, IStream{TResult}})"/> or <see cref="IStream{T}.FlatMapOrdered{TResult}(Func{T, IStream{TResult}}, int, int)"/> when ordered or sequential flattening is required.
    /// </summary>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, IStream<TResult>> selector, int maxConcurrency)
        => source.FlatMap(selector, maxConcurrency);

    /// <summary>
    /// Projects each element of a stream using an asynchronous selector that returns an <see cref="IStream{TResult}"/>, and flattens the result concurrently (unordered).
    /// Use fluent operators such as <see cref="IStream{T}.ConcatMap{TResult}(Func{T, IStream{TResult}})"/> or <see cref="IStream{T}.FlatMapOrdered{TResult}(Func{T, IStream{TResult}}, int, int)"/> when ordered or sequential flattening is required.
    /// </summary>
    public static IStream<TResult> SelectManyAsync<T, TResult>(this IStream<T> source, Func<T, ValueTask<IStream<TResult>>> selector)
    {
        return Streamix.Stream.From(selectManyAwaitConcurrent(source, selector, int.MaxValue));
    }

    /// <summary>
    /// Projects each element of a stream using an asynchronous selector that returns an <see cref="IStream{TResult}"/>, and flattens the result with concurrency support (unordered).
    /// Use fluent operators such as <see cref="IStream{T}.ConcatMap{TResult}(Func{T, IStream{TResult}})"/> or <see cref="IStream{T}.FlatMapOrdered{TResult}(Func{T, IStream{TResult}}, int, int)"/> when ordered or sequential flattening is required.
    /// </summary>
    public static IStream<TResult> SelectManyAsync<T, TResult>(this IStream<T> source, Func<T, ValueTask<IStream<TResult>>> selector, int maxConcurrency)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Streamix.Stream.From(selectManyAwaitConcurrent(source, selector, maxConcurrency));
    }

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream (unordered concurrent).
    /// </summary>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, ISingle<TResult>> selector)
        => source.FlatMap(selector, maxConcurrency: int.MaxValue);

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream with concurrency support (unordered).
    /// </summary>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, ISingle<TResult>> selector, int maxConcurrency)
        => source.FlatMap(selector, maxConcurrency);

    /// <summary>
    /// Projects each element of a stream using an asynchronous selector function and flattens the result (unordered concurrent).
    /// </summary>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, Task<TResult>> selector)
        => source.FlatMap(selector, maxConcurrency: int.MaxValue);

    /// <summary>
    /// Projects each element of a stream using an asynchronous selector function and flattens the result with concurrency support (unordered).
    /// </summary>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, Task<TResult>> selector, int maxConcurrency)
        => source.FlatMap(selector, maxConcurrency);
}
