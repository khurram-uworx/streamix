using Streamix.Abstractions;
using Streamix.Concurrency;
using System.Runtime.CompilerServices;

namespace Streamix;

/// <summary>
/// Default implementation of <see cref="ISingle{T}"/>.
/// This class is sealed to provide a stable API surface and ensure consistent behavior across operator chains.
/// </summary>
/// <typeparam name="T">The type of item in the stream.</typeparam>
public sealed class Single<T> : ISingle<T>
{
    readonly IAsyncEnumerable<T> source;
    readonly IClock clock;

    internal Single(IAsyncEnumerable<T> source, IClock? clock = null)
    {
        this.source = source;
        this.clock = clock ?? SystemClock.Instance;
    }

    async IAsyncEnumerable<TResult> map<TResult>(Func<T, TResult> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
            yield return selector(item);
    }

    async IAsyncEnumerable<TResult> mapAwait<TResult>(Func<T, ValueTask<TResult>> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
            yield return await selector(item);
    }

    async IAsyncEnumerable<TResult> flatMap<TResult>(Func<T, ISingle<TResult>> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
            await foreach (var innerItem in selector(item).WithCancellation(ct))
                yield return innerItem;
    }

    async IAsyncEnumerable<TResult> flatMapAwait<TResult>(Func<T, ValueTask<ISingle<TResult>>> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
        {
            var innerSingle = await selector(item);
            await foreach (var innerItem in innerSingle.WithCancellation(ct))
                yield return innerItem;
        }
    }

    async IAsyncEnumerable<TResult> flatMapMany<TResult>(Func<T, IStream<TResult>> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
            await foreach (var innerItem in selector(item).WithCancellation(ct))
                yield return innerItem;
    }

    async IAsyncEnumerable<TResult> flatMapManyAwait<TResult>(Func<T, ValueTask<IStream<TResult>>> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
        {
            var innerStream = await selector(item);
            await foreach (var innerItem in innerStream.WithCancellation(ct))
                yield return innerItem;
        }
    }

    async IAsyncEnumerable<T> onErrorResume(Func<Exception, ISingle<T>> errorHandler, [EnumeratorCancellation] CancellationToken ct = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        ISingle<T>? resumeSource = null;
        try
        {
            try
            {
                enumerator = source.GetAsyncEnumerator(ct);
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
            await foreach (var item in resumeSource.WithCancellation(ct))
                yield return item;
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
                {
                    yield return enumerator.Current;
                }
                else
                {
                    break;
                }
            }
        }
        finally
        {
            await Task.Factory.StartNew(() => enumerator.DisposeAsync().AsTask(), cancellationToken, TaskCreationOptions.None, scheduler).Unwrap();
        }
    }

    async IAsyncEnumerable<T> retry(int retryCount, Func<int, Exception, TimeSpan>? backoffStrategy = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int attempts = 0;
        while (true)
        {
            bool failed = false;
            IAsyncEnumerator<T>? enumerator = null;
            Exception? lastException = null;
            try
            {
                enumerator = source.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception ex)
            {
                lastException = ex;
            }

            if (enumerator != null)
            {
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
                        catch (Exception ex)
                        {
                            lastException = ex;
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
            }
            else
            {
                failed = true;
            }

            if (failed)
            {
                attempts++;
                if (attempts > retryCount)
                {
                    if (lastException != null) throw lastException;
                    yield break;
                }

                if (backoffStrategy != null && lastException != null)
                {
                    var delay = backoffStrategy(attempts, lastException);
                    if (delay > TimeSpan.Zero)
                    {
                        await clock.Delay(delay, cancellationToken);
                    }
                }
                continue;
            }

            yield break;
        }
    }

    async IAsyncEnumerable<T> timeout(TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var enumerator = this.GetAsyncEnumerator(cancellationToken);

        var moveNextTask = enumerator.MoveNextAsync().AsTask();
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var timeoutTask = clock.Delay(interval, timeoutCts.Token);

        var completedTask = await Task.WhenAny(moveNextTask, timeoutTask);
        await timeoutCts.CancelAsync();

        if (completedTask == timeoutTask)
            throw new TimeoutException($"The operation has timed out after {interval}.");

        if (await moveNextTask)
            yield return enumerator.Current;
    }

    async IAsyncEnumerable<T> doOnNext(Action<T> onNext, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
        {
            onNext(item);
            yield return item;
        }
    }

    async IAsyncEnumerable<T> doOnError(Action<Exception> onError, [EnumeratorCancellation] CancellationToken ct = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        try
        {
            try
            {
                enumerator = source.GetAsyncEnumerator(ct);
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

    async IAsyncEnumerable<T> doOnTerminate(Action onTerminate, [EnumeratorCancellation] CancellationToken ct = default)
    {
        try
        {
            await foreach (var item in source.WithCancellation(ct))
            {
                yield return item;
            }
        }
        finally
        {
            onTerminate();
        }
    }

    async IAsyncEnumerable<T> doOnComplete(Action onComplete, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
            yield return item;

        onComplete();
    }

    internal IClock Clock => clock;

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return source.GetAsyncEnumerator(cancellationToken);
    }

    /// <inheritdoc />
    public ISingle<T> Retry(int retryCount, Func<int, Exception, TimeSpan> backoffStrategy)
    {
        return Single.From(retry(retryCount, backoffStrategy), clock);
    }

    /// <inheritdoc />
    public ISingle<TResult> MapAwait<TResult>(Func<T, ValueTask<TResult>> selector)
    {
        return new Single<TResult>(mapAwait(selector));
    }

    /// <inheritdoc />
    public ISingle<TResult> Map<TResult>(Func<T, TResult> selector)
    {
        return new Single<TResult>(map(selector));
    }

    /// <inheritdoc />
    public ISingle<TResult> FlatMapAwait<TResult>(Func<T, ValueTask<ISingle<TResult>>> selector)
    {
        return new Single<TResult>(flatMapAwait(selector));
    }

    /// <inheritdoc />
    public ISingle<TResult> Select<TResult>(Func<T, TResult> selector) => Map(selector);

    /// <inheritdoc />
    public ISingle<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector)
    {
        return new Single<TResult>(flatMap(selector));
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector)
    {
        return new Stream<TResult>(flatMapMany(selector));
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMapManyAwait<TResult>(Func<T, ValueTask<IStream<TResult>>> selector)
    {
        return new Stream<TResult>(flatMapManyAwait(selector));
    }

    /// <inheritdoc />
    public ISingle<T> OnErrorResume(Func<Exception, ISingle<T>> errorHandler)
    {
        return new Single<T>(onErrorResume(errorHandler));
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

    /// <inheritdoc />
    public ISingle<T> RunOn(TaskScheduler scheduler)
    {
        return new Single<T>(runOn(scheduler), clock);
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
        T result = default!;
        bool hasValue = false;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (!hasValue)
            {
                result = item;
                hasValue = true;
            }
        }
        return result;
    }

    /// <inheritdoc />
    public ISingle<T> Retry(int retryCount = 1)
    {
        return Single.From(retry(retryCount), clock);
    }

    /// <inheritdoc />
    public ISingle<T> Timeout(TimeSpan interval)
    {
        return Single.From(timeout(interval), clock);
    }

    /// <inheritdoc />
    public ISingle<T> DoOnNext(Action<T> onNext)
    {
        return new Single<T>(doOnNext(onNext), clock);
    }

    /// <inheritdoc />
    public ISingle<T> Do(Action<T> onNext) => DoOnNext(onNext);

    /// <inheritdoc />
    public ISingle<T> Tap(Action<T> onNext) => DoOnNext(onNext);

    /// <inheritdoc />
    public ISingle<T> DoOnError(Action<Exception> onError)
    {
        return new Single<T>(doOnError(onError), clock);
    }

    /// <inheritdoc />
    public ISingle<T> DoOnComplete(Action onComplete)
    {
        return new Single<T>(doOnComplete(onComplete), clock);
    }

    /// <inheritdoc />
    public ISingle<T> DoOnTerminate(Action onTerminate)
    {
        return new Single<T>(doOnTerminate(onTerminate), clock);
    }
}

/// <summary>
/// Provides static methods for creating single-item streams.
/// </summary>
public static class Single
{
    static async IAsyncEnumerable<TValue> toAsyncEnumerableFromTask<TValue>(Task<TValue> task, [EnumeratorCancellation] CancellationToken ct = default)
    {
        yield return await task.WaitAsync(ct);
    }

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="source">The source asynchronous enumerable.</param>
    /// <returns>A single-item stream wrapping the source.</returns>
    public static ISingle<T> From<T>(IAsyncEnumerable<T> source) => new Single<T>(source);

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from an <see cref="IAsyncEnumerable{T}"/> with a specific clock.
    /// </summary>
    internal static ISingle<T> From<T>(IAsyncEnumerable<T> source, IClock clock) => new Single<T>(source, clock);

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a single value.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>A single-item stream that emits the specified value and then completes.</returns>
    public static ISingle<T> From<T>(T value) => From(AsyncEnumerableInternal.Just(value));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a <see cref="Task{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="task">The task to wrap.</param>
    /// <returns>A single-item stream that emits the result of the task and then completes.</returns>
    public static ISingle<T> From<T>(Task<T> task) => From(toAsyncEnumerableFromTask(task));

    /// <summary>
    /// Creates an empty <see cref="ISingle{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <returns>An empty single-item stream.</returns>
    public static ISingle<T> Empty<T>() => From(AsyncEnumerableInternal.Empty<T>());

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> that fails with the specified exception.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="exception">The exception to fail with.</param>
    /// <returns>A failing single-item stream.</returns>
    public static ISingle<T> Error<T>(Exception exception) => From(AsyncEnumerableInternal.Error<T>(exception));
}

static class AsyncEnumerableInternal
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
}
