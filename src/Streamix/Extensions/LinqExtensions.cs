namespace Streamix;

using Streamix.Abstractions;
using System.Runtime.CompilerServices;

/// <summary>
/// Provides LINQ-style extension methods for <see cref="IStream{T}"/>.
/// These extensions make it easy to work with streams using familiar LINQ patterns.
/// </summary>
public static class LinqExtensions
{
    static async IAsyncEnumerable<T> filterAsync<T>(IStream<T> source, Func<T, ValueTask<bool>> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            if (await predicate(item))
                yield return item;
        }
    }

    static async IAsyncEnumerable<TResult> selectAsync<T, TResult>(IStream<T> source, Func<T, ValueTask<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken))
            yield return await selector(item);
    }

    static async IAsyncEnumerable<TResult> selectManyAsync<T, TResult>(IStream<T> source, Func<T, ValueTask<IStream<TResult>>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            var innerStream = await selector(item);
            await foreach (var innerItem in innerStream.WithCancellation(cancellationToken))
                yield return innerItem;
        }
    }

    static async IAsyncEnumerable<TResult> selectManyAsyncConcurrent<T, TResult>(IStream<T> source, Func<T, ValueTask<IStream<TResult>>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency <= 1)
        {
            await foreach (var item in selectManyAsync(source, selector, cancellationToken))
                yield return item;
            yield break;
        }

        var channel = System.Threading.Channels.Channel.CreateBounded<TResult>(maxConcurrency);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task>();
            try
            {
                await foreach (var item in source.WithCancellation(cts.Token))
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
        return Stream.From(filterAsync(source, predicate));
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
        return Stream.From(selectAsync(source, selector));
    }

    /// <summary>
    /// Projects each element of a stream to an <see cref="IStream{TResult}"/> and flattens the resulting streams into one stream.
    /// LINQ-style extension for <see cref="IStream{T}.FlatMapMany{TResult}(Func{T, IStream{TResult}}, int)"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to flatten.</param>
    /// <param name="selector">A transform function to apply to each element that returns a stream.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the one-to-many transform function on each element of the input stream.</returns>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, IStream<TResult>> selector)
        => source.FlatMapMany(selector);

    /// <summary>
    /// Projects each element of a stream to an <see cref="IStream{TResult}"/> and flattens the resulting streams into one stream.
    /// LINQ-style extension for <see cref="IStream{T}.FlatMapMany{TResult}(Func{T, IStream{TResult}}, int)"/> with concurrency support.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to flatten.</param>
    /// <param name="selector">A transform function to apply to each element that returns a stream.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the one-to-many transform function on each element of the input stream.</returns>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, IStream<TResult>> selector, int maxConcurrency)
        => source.FlatMapMany(selector, maxConcurrency);

    /// <summary>
    /// Projects each element of a stream using an asynchronous selector that returns an <see cref="IStream{TResult}"/>, and flattens the result.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to flatten.</param>
    /// <param name="selector">An asynchronous transform function that returns a stream.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async one-to-many transform function on each element of the input stream.</returns>
    public static IStream<TResult> SelectManyAsync<T, TResult>(this IStream<T> source, Func<T, ValueTask<IStream<TResult>>> selector)
    {
        return Stream.From(selectManyAsync(source, selector));
    }

    /// <summary>
    /// Projects each element of a stream using an asynchronous selector that returns an <see cref="IStream{TResult}"/>, and flattens the result with concurrency support.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to flatten.</param>
    /// <param name="selector">An asynchronous transform function that returns a stream.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async one-to-many transform function on each element of the input stream.</returns>
    public static IStream<TResult> SelectManyAsync<T, TResult>(this IStream<T> source, Func<T, ValueTask<IStream<TResult>>> selector, int maxConcurrency)
    {
        return Stream.From(selectManyAsyncConcurrent(source, selector, maxConcurrency));
    }

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream.
    /// LINQ-style extension for <see cref="IStream{T}.FlatMap{TResult}(Func{T, ISingle{TResult}}, int)"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to flatten.</param>
    /// <param name="selector">A transform function to apply to each element that returns a single value.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the one-to-one transform function on each element of the input stream.</returns>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, ISingle<TResult>> selector)
        => source.FlatMap(selector);

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream with concurrency support.
    /// LINQ-style extension for <see cref="IStream{T}.FlatMap{TResult}(Func{T, ISingle{TResult}}, int)"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to flatten.</param>
    /// <param name="selector">A transform function to apply to each element that returns a single value.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the one-to-one transform function on each element of the input stream.</returns>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, ISingle<TResult>> selector, int maxConcurrency)
        => source.FlatMap(selector, maxConcurrency);

    /// <summary>
    /// Projects each element of a stream using an asynchronous selector function and flattens the result.
    /// LINQ-style extension for <see cref="IStream{T}.FlatMap{TResult}(Func{T, Task{TResult}}, int)"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to transform.</param>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async transform function on each element of the input stream.</returns>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, Task<TResult>> selector)
        => source.FlatMap(selector);

    /// <summary>
    /// Projects each element of a stream using an asynchronous selector function and flattens the result with concurrency support.
    /// LINQ-style extension for <see cref="IStream{T}.FlatMap{TResult}(Func{T, Task{TResult}}, int)"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to transform.</param>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async transform function on each element of the input stream.</returns>
    public static IStream<TResult> SelectMany<T, TResult>(this IStream<T> source, Func<T, Task<TResult>> selector, int maxConcurrency)
        => source.FlatMap(selector, maxConcurrency);
}
