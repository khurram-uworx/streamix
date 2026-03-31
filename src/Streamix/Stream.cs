using System.Runtime.CompilerServices;
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
    public IStream<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency = 1) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<TResult> SelectMany<TResult>(Func<T, ISingle<TResult>> selector) => FlatMap(selector);

    /// <inheritdoc />
    public IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector) => throw new NotImplementedException();

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
    public static IStream<T> Merge(params IStream<T>[] streams) => throw new NotImplementedException();

    /// <summary>
    /// Combines elements from multiple streams using a specified function.
    /// </summary>
    public static IStream<TResult> Zip<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<IList<T>> Buffer(int count) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<IStream<T>> Window(int count) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> Throttle(TimeSpan interval) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> Delay(TimeSpan interval) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> Retry(int retryCount = 1) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> Timeout(TimeSpan interval) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<T> OnErrorResume(Func<Exception, IStream<T>> errorHandler) => throw new NotImplementedException();

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
    public static IStream<T> From<T>(IAsyncEnumerable<T> source) => new Stream<T>(source);

    /// <summary>
    /// Creates an empty stream.
    /// </summary>
    public static IStream<T> Empty<T>() => From(AsyncEnumerable.Empty<T>());

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

    public static async IAsyncEnumerable<int> Range(int start, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return start + i;
        }
    }
}
