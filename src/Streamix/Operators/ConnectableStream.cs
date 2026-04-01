using Streamix.Abstractions;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix.Operators;

/// <summary>
/// Implementation of <see cref="IConnectableStream{T}"/> that allows multicasting a single source to multiple subscribers.
/// This class is internal as it's intended to be created via the <see cref="Stream{T}.Publish"/> method.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
sealed class ConnectableStream<T> : IConnectableStream<T>
{
    class ConnectionDisposable : IDisposable
    {
        readonly ConnectableStream<T> stream;
        int disposed = 0;
        public ConnectionDisposable(ConnectableStream<T> stream) => this.stream = stream;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
                stream.disconnect();
        }
    }

    readonly IStream<T> source;
    readonly ConcurrentDictionary<Guid, Channel<T>> subscribers = new();
    readonly object _lock = new();
    int refCounter = 0;
    CancellationTokenSource? cts;
    Task? connectionTask;
    IDisposable? autoConnection;

    public ConnectableStream(IStream<T> source)
    {
        this.source = source;
    }

    async Task runConnectionInternal(CancellationToken token)
    {
        await Task.Yield();
        await runConnection(token);
    }

    async Task runConnection(CancellationToken cancellationToken)
    {
        try
        {
            await using var enumerator = source.GetAsyncEnumerator(cancellationToken);
            while (await enumerator.MoveNextAsync())
            {
                var item = enumerator.Current;
                var subscribers = this.subscribers.Values.ToArray();

                foreach (var subscriber in subscribers)
                {
                    try
                    {
                        await subscriber.Writer.WriteAsync(item, cancellationToken);
                    }
                    catch { }
                }
            }

            var finalSubscribers = subscribers.Values.ToArray();
            foreach (var subscriber in finalSubscribers)
                subscriber.Writer.TryComplete();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            var finalSubscribers = subscribers.Values.ToArray();
            foreach (var subscriber in finalSubscribers)
                subscriber.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            var finalSubscribers = subscribers.Values.ToArray();
            foreach (var subscriber in finalSubscribers)
                subscriber.Writer.TryComplete(ex);
        }
        finally
        {
            lock (_lock)
            {
                cts?.Dispose();
                cts = null;
                connectionTask = null;
            }
        }
    }

    async IAsyncEnumerable<TResult> map<TResult>(Func<T, TResult> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            yield return selector(item);
    }

    async IAsyncEnumerable<T> filter(Func<T, bool> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (predicate(item))
                yield return item;
        }
    }

    async IAsyncEnumerable<TResult> flatMap<TResult>(Func<T, ISingle<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                yield return innerItem;
    }

    async IAsyncEnumerable<TResult> flatMapManyConcurrent<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var semaphore = new SemaphoreSlim(maxConcurrency);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var tasks = new List<Task<List<TResult>>>();

        try
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
            {
                await semaphore.WaitAsync(cancellationToken);
                var task = processItemAsync(item, selector, semaphore, cts.Token);
                tasks.Add(task);
            }

            var results = await Task.WhenAll(tasks);
            foreach (var list in results)
                foreach (var result in list)
                    yield return result;
        }
        finally
        {
            semaphore.Dispose();
        }
    }

    async Task<List<TResult>> processItemAsync<TResult>(T item, Func<T, ISingle<TResult>> selector, SemaphoreSlim semaphore, CancellationToken cancellationToken)
    {
        try
        {
            var results = new List<TResult>();
            await foreach (var result in selector(item).WithCancellation(cancellationToken))
                results.Add(result);
            return results;
        }
        finally
        {
            semaphore.Release();
        }
    }

    async IAsyncEnumerable<T> refCount([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        bool incremented = false;

        try
        {
            lock (_lock)
            {
                refCounter++;
                incremented = true;
                if (refCounter == 1)
                    autoConnection = Connect();
                enumerator = this.GetAsyncEnumerator(cancellationToken);
            }

            while (await enumerator.MoveNextAsync())
                yield return enumerator.Current;
        }
        finally
        {
            if (enumerator != null)
                await enumerator.DisposeAsync();

            if (incremented)
            {
                lock (_lock)
                {
                    refCounter--;

                    if (refCounter == 0)
                    {
                        autoConnection?.Dispose();
                        autoConnection = null;
                    }
                }
            }
        }
    }

    async IAsyncEnumerable<TResult> flatMapConcurrent<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
                yield return await selector(item);
        }
        else
        {
            var semaphore = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task<TResult>>();

            try
            {
                await foreach (var item in this.WithCancellation(cancellationToken))
                {
                    await semaphore.WaitAsync(cancellationToken);
                    tasks.Add(executeSelectorAsync(item, selector, semaphore, cancellationToken));
                }

                var results = await Task.WhenAll(tasks);
                foreach (var result in results)
                    yield return result;
            }
            finally
            {
                semaphore.Dispose();
            }
        }
    }

    async Task<TResult> executeSelectorAsync<TResult>(T item, Func<T, Task<TResult>> selector, SemaphoreSlim semaphore, CancellationToken cancellationToken)
    {
        try
        {
            return await selector(item);
        }
        finally
        {
            semaphore.Release();
        }
    }

    async IAsyncEnumerable<TResult> flatMapMany<TResult>(Func<T, IStream<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                yield return innerItem;
        }
    }

    async IAsyncEnumerable<TResult> flatMapManyConcurrentMany<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var semaphore = new SemaphoreSlim(maxConcurrency);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var tasks = new List<Task<List<TResult>>>();

        try
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
            {
                await semaphore.WaitAsync(cancellationToken);
                var task = processItemManyAsync(item, selector, semaphore, cts.Token);
                tasks.Add(task);
            }

            var results = await Task.WhenAll(tasks);
            foreach (var list in results)
                foreach (var result in list)
                    yield return result;
        }
        finally
        {
            semaphore.Dispose();
        }
    }

    async Task<List<TResult>> processItemManyAsync<TResult>(T item, Func<T, IStream<TResult>> selector, SemaphoreSlim semaphore, CancellationToken cancellationToken)
    {
        try
        {
            var results = new List<TResult>();
            await foreach (var result in selector(item).WithCancellation(cancellationToken))
                results.Add(result);
            return results;
        }
        finally
        {
            semaphore.Release();
        }
    }

    async IAsyncEnumerable<T> take(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var remaining = count;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (remaining <= 0) yield break;
            yield return item;
            remaining--;
        }
    }

    async IAsyncEnumerable<T> skip(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var remaining = count;
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

    async IAsyncEnumerable<T> mergeWith(IStream<T>[] others, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var streams = new IAsyncEnumerable<T>[others.Length + 1];
        streams[0] = this;
        Array.Copy(others, 0, streams, 1, others.Length);

        foreach (var stream in streams)
            await foreach (var item in stream.WithCancellation(cancellationToken))
                yield return item;
    }

    async IAsyncEnumerable<TResult> zipWith<TOther, TResult>(IStream<TOther> other, Func<T, TOther, TResult> resultSelector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var enumerator1 = this.GetAsyncEnumerator(cancellationToken);
        var enumerator2 = other.GetAsyncEnumerator(cancellationToken);

        try
        {
            while (await enumerator1.MoveNextAsync() && await enumerator2.MoveNextAsync())
                yield return resultSelector(enumerator1.Current, enumerator2.Current);
        }
        finally
        {
            await enumerator1.DisposeAsync();
            await enumerator2.DisposeAsync();
        }
    }

    async IAsyncEnumerable<IList<T>> buffer(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var buffer = new List<T>();
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            buffer.Add(item);
            if (buffer.Count >= count)
            {
                yield return buffer;
                buffer = new List<T>();
            }
        }

        if (buffer.Count > 0)
            yield return buffer;
    }

    async IAsyncEnumerable<IStream<T>> window(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var window = new List<T>();
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            window.Add(item);
            if (window.Count >= count)
            {
                yield return Stream.From(window.ToAsyncEnumerable());
                window.RemoveAt(0);
            }
        }
    }

    async IAsyncEnumerable<T> throttle(TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var lastEmit = DateTime.UtcNow;
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            var now = DateTime.UtcNow;
            var timeSinceLastEmit = now - lastEmit;

            if (timeSinceLastEmit < interval)
                await Task.Delay(interval - timeSinceLastEmit, cancellationToken);

            yield return item;
            lastEmit = DateTime.UtcNow;
        }
    }

    async IAsyncEnumerable<T> delay(TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            await Task.Delay(interval, cancellationToken);
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
                try
                {
                    enumerator = this.GetAsyncEnumerator(cancellationToken);
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
            }
            finally
            { }

            if (!failed) yield break;
        }
    }

    async IAsyncEnumerable<T> timeout(TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var enumerator = this.GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            var moveNextTask = enumerator.MoveNextAsync().AsTask();
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var timeoutTask = Task.Delay(interval, timeoutCts.Token);

            try
            {
                var completedTask = await Task.WhenAny(moveNextTask, timeoutTask);
                await timeoutCts.CancelAsync();

                if (completedTask == timeoutTask)
                    throw new TimeoutException($"The operation has timed out after {interval}.");

                if (await moveNextTask)
                    yield return enumerator.Current;
                else
                    break;
            }
            finally
            {
                await timeoutCts.CancelAsync();
            }
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
                enumerator = this.GetAsyncEnumerator(cancellationToken);
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
            await foreach (var item in resumeSource.WithCancellation(cancellationToken))
                yield return item;
    }

    async IAsyncEnumerable<T> onErrorReturn(T value, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        Exception? caughtException = null;
        try
        {
            try
            {
                enumerator = this.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception ex)
            {
                caughtException = ex;
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
                        caughtException = ex;
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

        if (caughtException != null)
            yield return value;
    }

    async IAsyncEnumerable<T> onErrorMap(Func<Exception, Exception> mapper, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        Exception? mappedException = null;
        try
        {
            try
            {
                enumerator = this.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception ex)
            {
                mappedException = mapper(ex);
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
                        mappedException = mapper(ex);
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

        if (mappedException != null)
            throw mappedException;
    }

    async IAsyncEnumerable<T> runOn(TaskScheduler scheduler, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            yield return await Task.Factory.StartNew(
                () => item,
                cancellationToken,
                TaskCreationOptions.DenyChildAttach,
                scheduler);
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
                enumerator = this.GetAsyncEnumerator(cancellationToken);
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

    async Task forEachAsync(Action<T> action, CancellationToken cancellationToken)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            action(item);
    }

    async Task forEachAsync(Func<T, Task> action, CancellationToken cancellationToken)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            await action(item);
    }

    void disconnect()
    {
        lock (_lock)
        {
            cts?.Cancel();
        }
    }

    /// <inheritdoc />
    public IDisposable Connect()
    {
        lock (_lock)
        {
            if (connectionTask != null && !connectionTask.IsCompleted)
                return new ConnectionDisposable(this);

            cts = new CancellationTokenSource();
            var token = cts.Token;
            connectionTask = runConnectionInternal(token);
            return new ConnectionDisposable(this);
        }
    }

    /// <inheritdoc />
    public IStream<T> RefCount()
    {
        return Stream.From(refCount());
    }

    /// <inheritdoc />
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<T>();
        subscribers.TryAdd(id, channel);

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
                while (channel.Reader.TryRead(out var item))
                    yield return item;

            // Check for error
            await channel.Reader.Completion;
        }
        finally
        {
            subscribers.TryRemove(id, out _);
        }
    }

    /// <inheritdoc />
    public IStream<TResult> Map<TResult>(Func<T, TResult> selector)
    {
        return Stream.From(map(selector));
    }

    /// <inheritdoc />
    public IStream<TResult> Select<TResult>(Func<T, TResult> selector) => Map(selector);

    /// <inheritdoc />
    public IStream<T> Filter(Func<T, bool> predicate)
    {
        return Stream.From(filter(predicate));
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
            : Stream.From(flatMapManyConcurrentMany(selector, maxConcurrency));
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

    /// <inheritdoc />
    public IStream<T> MergeWith(params IStream<T>[] others)
    {
        return Stream.From(mergeWith(others));
    }

    /// <inheritdoc />
    public IStream<TResult> ZipWith<TOther, TResult>(IStream<TOther> other, Func<T, TOther, TResult> resultSelector)
    {
        return Stream.From(zipWith(other, resultSelector));
    }

    /// <inheritdoc />
    public IStream<IList<T>> Buffer(int count)
    {
        return Stream.From(buffer(count));
    }

    /// <inheritdoc />
    public IStream<IStream<T>> Window(int count)
    {
        return Stream.From(window(count));
    }

    /// <inheritdoc />
    public IStream<T> Throttle(TimeSpan interval)
    {
        return Stream.From(throttle(interval));
    }

    /// <inheritdoc />
    public IStream<T> Delay(TimeSpan interval)
    {
        return Stream.From(delay(interval));
    }

    /// <inheritdoc />
    public IStream<T> Retry(int retryCount = 1)
    {
        return Stream.From(retry(retryCount));
    }

    /// <inheritdoc />
    public IStream<T> Timeout(TimeSpan interval)
    {
        return Stream.From(timeout(interval));
    }

    /// <inheritdoc />
    public IStream<T> OnErrorResume(Func<Exception, IStream<T>> errorHandler)
    {
        return Stream.From(onErrorResume(errorHandler));
    }

    /// <inheritdoc />
    public IStream<T> OnErrorReturn(T value)
    {
        return Stream.From(onErrorReturn(value));
    }

    /// <inheritdoc />
    public IStream<T> OnErrorMap(Func<Exception, Exception> mapper)
    {
        return Stream.From(onErrorMap(mapper));
    }

    /// <inheritdoc />
    public IConnectableStream<T> Publish() => this;

    /// <inheritdoc />
    public IStream<T> RunOn(TaskScheduler scheduler)
    {
        return Stream.From(runOn(scheduler));
    }

    /// <inheritdoc />
    public Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default)
    {
        return forEachAsync(action, cancellationToken);
    }

    /// <inheritdoc />
    public Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default)
    {
        return forEachAsync(action, cancellationToken);
    }

    /// <inheritdoc />
    public IStream<T> DoOnNext(Action<T> onNext)
    {
        return Stream.From(doOnNext(onNext));
    }

    /// <inheritdoc />
    public IStream<T> DoOnError(Action<Exception> onError)
    {
        return Stream.From(doOnError(onError));
    }

    /// <inheritdoc />
    public IStream<T> DoOnTerminate(Action onTerminate)
    {
        return Stream.From(doOnTerminate(onTerminate));
    }
}

internal static class EnumerableExtensions
{
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> source)
    {
        foreach (var item in source)
            yield return item;

        await Task.Yield();
    }
}
