using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace Streamix.Implementations;

/// <summary>
/// Default implementation of <see cref="ISingle{T}"/>.
/// This class is sealed to provide a stable API surface and ensure consistent behavior across operator chains.
/// </summary>
/// <typeparam name="T">The type of item in the stream.</typeparam>
class Single<T> : ISingle<T>
{
    readonly IAsyncEnumerable<T> source;
    readonly IClock clock;
    readonly string? name;

    internal Single(IAsyncEnumerable<T> source, IClock? clock = null, string? name = null)
    {
        this.source = source;
        this.clock = clock ?? SystemClock.Instance;
        this.name = name;
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

    async IAsyncEnumerable<TResult> concatMapInternal<TResult>(Func<T, IStream<TResult>> selector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct))
            await foreach (var innerItem in selector(item).WithCancellation(ct))
                yield return innerItem;
    }

    async IAsyncEnumerable<TResult> concatMapAwaitInternal<TResult>(Func<T, ValueTask<IStream<TResult>>> selector, [EnumeratorCancellation] CancellationToken ct = default)
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
                    bool hasValue = false;
                    T current = default!;

                    while (true)
                    {
                        bool hasNext;
                        try
                        {
                            hasNext = await enumerator.MoveNextAsync();
                        }
                        catch (Exception ex)
                        {
                            lastException = ex;
                            failed = true;
                            break;
                        }

                        if (hasNext)
                        {
                            if (hasValue)
                                throw new InvalidOperationException("Sequence contains more than one element.");

                            current = enumerator.Current;
                            hasValue = true;
                        }
                        else
                        {
                            if (hasValue)
                            {
                                yield return current;
                                yield break;
                            }
                            else
                            {
                                yield break;
                            }
                        }
                    }

                    if (failed)
                    {
                        // continue to retry logic below
                    }
                    else if (hasValue)
                    {
                        yield break;
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
        await using var enumerator = source.GetAsyncEnumerator(cancellationToken);

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
    public string? Name => name;

    /// <inheritdoc />
    public ISingle<T> Named(string name)
    {
        return new Single<T>(source, clock, name);
    }

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return Single.EnforceAtMostOne(source, cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    /// <inheritdoc />
    public ISingle<T> Retry(int retryCount, Func<int, Exception, TimeSpan> backoffStrategy)
    {
        return Single.From(retry(retryCount, backoffStrategy), clock, name);
    }

    /// <inheritdoc />
    public ISingle<TResult> MapAwait<TResult>(Func<T, ValueTask<TResult>> selector)
    {
        return new Single<TResult>(mapAwait(selector), clock, name);
    }

    /// <inheritdoc />
    public ISingle<TResult> Map<TResult>(Func<T, TResult> selector)
    {
        return new Single<TResult>(map(selector), clock, name);
    }

    /// <inheritdoc />
    public ISingle<TResult> FlatMapAwait<TResult>(Func<T, ValueTask<ISingle<TResult>>> selector)
    {
        return new Single<TResult>(flatMapAwait(selector), clock, name);
    }

    /// <inheritdoc />
    public ISingle<TResult> Select<TResult>(Func<T, TResult> selector) => Map(selector);

    /// <inheritdoc />
    public ISingle<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector)
    {
        return new Single<TResult>(flatMap(selector), clock, name);
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, IStream<TResult>> selector)
    {
        return Streamix.Stream.From(concatMapInternal(selector), clock, name);
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMapAwait<TResult>(Func<T, ValueTask<IStream<TResult>>> selector)
    {
        return Streamix.Stream.From(concatMapAwaitInternal(selector), clock, name);
    }

    /// <inheritdoc />
    public ISingle<T> OnErrorResume(Func<Exception, ISingle<T>> errorHandler)
    {
        return new Single<T>(onErrorResume(errorHandler), clock, name);
    }

    /// <inheritdoc />
    public ISingle<T> OnErrorReturn(T value)
    {
        return OnErrorResume(_ => Single.Just<T>(value).Named(name ?? ""));
    }

    /// <inheritdoc />
    public ISingle<T> OnErrorMap(Func<Exception, Exception> mapper)
    {
        return OnErrorResume(ex => Single.Error<T>(mapper(ex)));
    }

    /// <inheritdoc />
    public ISingle<T> RunOn(TaskScheduler scheduler)
    {
        return new Single<T>(runOn(scheduler), clock, name);
    }

    /// <inheritdoc />
    public async Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default)
    {
        bool hasValue = false;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (hasValue)
                throw new InvalidOperationException("Sequence contains more than one element.");

            action(item);
            hasValue = true;
        }
    }

    /// <inheritdoc />
    public async Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default)
    {
        bool hasValue = false;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (hasValue)
                throw new InvalidOperationException("Sequence contains more than one element.");

            await action(item);
            hasValue = true;
        }
    }

    /// <inheritdoc />
    public async Task<T> ToTask(CancellationToken cancellationToken = default)
    {
        T result = default!;
        bool hasValue = false;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (hasValue)
                throw new InvalidOperationException("Sequence contains more than one element.");

            result = item;
            hasValue = true;
        }
        return result;
    }

    /// <inheritdoc />
    public ISingle<T> Retry(int retryCount = 1)
    {
        return Single.From(retry(retryCount), clock, name);
    }

    /// <inheritdoc />
    public ISingle<T> Timeout(TimeSpan interval)
    {
        return Single.From(timeout(interval), clock, name);
    }

    /// <inheritdoc />
    public ISingle<T> DoOnNext(Action<T> onNext)
    {
        return new Single<T>(doOnNext(onNext), clock, name);
    }

    /// <inheritdoc />
    public ISingle<T> Do(Action<T> onNext) => DoOnNext(onNext);

    /// <inheritdoc />
    public ISingle<T> Tap(Action<T> onNext) => DoOnNext(onNext);

    /// <inheritdoc />
    public ISingle<T> DoOnError(Action<Exception> onError)
    {
        return new Single<T>(doOnError(onError), clock, name);
    }

    /// <inheritdoc />
    public ISingle<T> DoOnComplete(Action onComplete)
    {
        return new Single<T>(doOnComplete(onComplete), clock, name);
    }

    /// <inheritdoc />
    public ISingle<T> DoOnTerminate(Action onTerminate)
    {
        return new Single<T>(doOnTerminate(onTerminate), clock, name);
    }

    /// <inheritdoc />
    public ISingle<T> Log() => Log(name ?? "");

    /// <inheritdoc />
    public ISingle<T> Log(string prefix) => LogActionInternal(s => Console.WriteLine(s), prefix);

    /// <inheritdoc />
    public ISingle<T> LogAction(Action<string> loggerAction) => LogActionInternal(loggerAction, name ?? "");

    /// <inheritdoc />
    public ISingle<T> Log(ILogger logger, string? prefix = null)
    {
        var p = prefix ?? name ?? "";
        var pref = string.IsNullOrEmpty(p) ? "" : $"[{p}] ";
        return DoOnNext(x => logger.LogInformation("{Prefix}Next({Value})", pref, x))
              .DoOnError(ex => logger.LogError(ex, "{Prefix}Error({Message})", pref, ex.Message))
              .DoOnComplete(() => logger.LogInformation("{Prefix}Completed", pref));
    }

    private ISingle<T> LogActionInternal(Action<string> logger, string prefix)
    {
        var pref = string.IsNullOrEmpty(prefix) ? "" : $"[{prefix}] ";
        return DoOnNext(x => logger($"{pref}Next({x})"))
              .DoOnError(ex => logger($"{pref}Error({ex.Message})"))
              .DoOnComplete(() => logger($"{pref}Completed"));
    }

    /// <inheritdoc />
    public ISingle<T> Debug() => Debug(name ?? "");

    /// <inheritdoc />
    public ISingle<T> Debug(string prefix)
    {
        var pref = string.IsNullOrEmpty(prefix) ? "" : $"[{prefix}] ";
        return DoOnNext(x => System.Diagnostics.Debug.WriteLine($"{pref}Next({x})"))
              .DoOnError(ex => System.Diagnostics.Debug.WriteLine($"{pref}Error({ex.Message})"))
              .DoOnComplete(() => System.Diagnostics.Debug.WriteLine($"{pref}Completed"));
    }
}
