using System.Runtime.CompilerServices;
using Streamix.Abstractions;
using Streamix.Concurrency;

namespace Streamix;

/// <summary>
/// Default implementation of <see cref="ISingle{T}"/>.
/// This class is sealed to provide a stable API surface and ensure consistent behavior across operator chains.
/// </summary>
/// <typeparam name="T">The type of item in the stream.</typeparam>
public sealed class Single<T> : ISingle<T>
{
    private readonly IAsyncEnumerable<T> _source;
    private readonly IClock _clock;

    internal Single(IAsyncEnumerable<T> source, IClock? clock = null)
    {
        _source = source;
        _clock = clock ?? SystemClock.Instance;
    }

    internal IClock Clock => _clock;

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return _source.GetAsyncEnumerator(cancellationToken);
    }

    /// <inheritdoc />
    public ISingle<TResult> Map<TResult>(Func<T, TResult> selector)
    {
        return new Single<TResult>(MapInternal(selector));
    }

    private async IAsyncEnumerable<TResult> MapInternal<TResult>(Func<T, TResult> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in _source.WithCancellation(ct))
        {
            yield return selector(item);
        }
    }

    /// <inheritdoc />
    public ISingle<TResult> Select<TResult>(Func<T, TResult> selector) => Map(selector);

    /// <inheritdoc />
    public ISingle<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector)
    {
        return new Single<TResult>(FlatMapInternal(selector));
    }

    private async IAsyncEnumerable<TResult> FlatMapInternal<TResult>(Func<T, ISingle<TResult>> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in _source.WithCancellation(ct))
        {
            await foreach (var innerItem in selector(item).WithCancellation(ct))
            {
                yield return innerItem;
            }
        }
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector)
    {
        return new Stream<TResult>(FlatMapManyInternal(selector));
    }

    private async IAsyncEnumerable<TResult> FlatMapManyInternal<TResult>(Func<T, IStream<TResult>> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in _source.WithCancellation(ct))
        {
            await foreach (var innerItem in selector(item).WithCancellation(ct))
            {
                yield return innerItem;
            }
        }
    }

    /// <inheritdoc />
    public ISingle<T> OnErrorResume(Func<Exception, ISingle<T>> errorHandler)
    {
        return new Single<T>(OnErrorResumeInternal(errorHandler));
    }

    /// <inheritdoc />
    public ISingle<T> OnErrorReturn(T value)
    {
        return OnErrorResume(_ => Single.From(value));
    }

    /// <inheritdoc />
    public ISingle<T> OnErrorMap(Func<Exception, Exception> mapper)
    {
        return OnErrorResume(ex => Single.Error<T>(mapper(ex)));
    }

    private async IAsyncEnumerable<T> OnErrorResumeInternal(Func<Exception, ISingle<T>> errorHandler, [EnumeratorCancellation] CancellationToken ct = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        ISingle<T>? resumeSource = null;
        try
        {
            try
            {
                enumerator = _source.GetAsyncEnumerator(ct);
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
            await foreach (var item in resumeSource.WithCancellation(ct))
            {
                yield return item;
            }
        }
    }

    /// <inheritdoc />
    public ISingle<T> RunOn(TaskScheduler scheduler)
    {
        return new Single<T>(RunOnInternal(scheduler, ct => _source.GetAsyncEnumerator(ct)));
    }

    private async IAsyncEnumerable<T> RunOnInternal(TaskScheduler scheduler, Func<CancellationToken, IAsyncEnumerator<T>> enumeratorFactory, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var enumerator = await Task.Factory.StartNew(() => enumeratorFactory(ct), ct, TaskCreationOptions.None, scheduler);
        try
        {
            while (true)
            {
                var hasNext = await Task.Factory.StartNew(() => enumerator.MoveNextAsync().AsTask(), ct, TaskCreationOptions.None, scheduler).Unwrap();
                if (hasNext)
                {
                    yield return enumerator.Current;
                }
                else
                {
                    yield break;
                }
            }
        }
        finally
        {
            await Task.Factory.StartNew(() => enumerator.DisposeAsync().AsTask(), ct, TaskCreationOptions.None, scheduler).Unwrap();
        }
    }

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

    /// <inheritdoc />
    public async Task<T> ToTask(CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            return item;
        }
        return default!;
    }

    /// <inheritdoc />
    public ISingle<T> Retry(int retryCount = 1)
    {
        return Single.From(RetryInternal(retryCount), _clock);
    }

    private async IAsyncEnumerable<T> RetryInternal(int retryCount, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int attempts = 0;
        while (true)
        {
            bool failed = false;
            IAsyncEnumerator<T>? enumerator = null;
            try
            {
                enumerator = _source.GetAsyncEnumerator(cancellationToken);
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
                    {
                        yield return current;
                        yield break; // Single should only emit one item
                    }
                    else
                    {
                        yield break;
                    }
                }
            }

            if (!failed) yield break;
        }
    }

    /// <inheritdoc />
    public ISingle<T> Timeout(TimeSpan interval)
    {
        return Single.From(TimeoutInternal(interval), _clock);
    }

    private async IAsyncEnumerable<T> TimeoutInternal(TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var enumerator = this.GetAsyncEnumerator(cancellationToken);

        var moveNextTask = enumerator.MoveNextAsync().AsTask();
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var timeoutTask = _clock.Delay(interval, timeoutCts.Token);

        var completedTask = await Task.WhenAny(moveNextTask, timeoutTask);
        await timeoutCts.CancelAsync();

        if (completedTask == timeoutTask)
        {
            throw new TimeoutException($"The operation has timed out after {interval}.");
        }

        if (await moveNextTask)
        {
            yield return enumerator.Current;
        }
    }
}

/// <summary>
/// Provides static methods for creating single-item streams.
/// </summary>
public static class Single
{
    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    public static ISingle<T> From<T>(IAsyncEnumerable<T> source) => new Single<T>(source);

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from an <see cref="IAsyncEnumerable{T}"/> with a specific clock.
    /// </summary>
    internal static ISingle<T> From<T>(IAsyncEnumerable<T> source, IClock clock) => new Single<T>(source, clock);

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a single value.
    /// </summary>
    public static ISingle<T> From<T>(T value) => From(ToAsyncEnumerable(value));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a <see cref="Task{T}"/>.
    /// </summary>
    public static ISingle<T> From<T>(Task<T> task) => From(ToAsyncEnumerable(task));

    /// <summary>
    /// Creates an empty <see cref="ISingle{T}"/>.
    /// </summary>
    public static ISingle<T> Empty<T>() => From(AsyncEnumerableInternal.Empty<T>());

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> that fails with the specified exception.
    /// </summary>
    public static ISingle<T> Error<T>(Exception exception) => From(AsyncEnumerableInternal.Error<T>(exception));

    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(T value)
    {
        yield return value;
    }

    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(Task<T> task)
    {
        yield return await task;
    }
}

internal static class AsyncEnumerableInternal
{
    public static async IAsyncEnumerable<T> Empty<T>([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield break;
    }

    public static async IAsyncEnumerable<T> Error<T>(Exception exception, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        throw exception;
        yield break;
    }
}
