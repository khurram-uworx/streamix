using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix.Implementations;

/// <summary>
/// Implementation of <see cref="IConnectableStream{T}"/> that allows multicasting a single source to multiple subscribers.
/// This class is internal as it's intended to be created via the <see cref="Stream{T}.Publish"/> method.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
class ConnectableStream<T> : IConnectableStream<T>
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
    readonly IClock clock;
    readonly ConcurrentDictionary<Guid, Channel<T>> subscribers = new();
    readonly object _lock = new();
    readonly int replayBufferSize;
    readonly Queue<T> replayBuffer = new();
    bool isCompleted;
    Exception? error;
    int refCounter = 0;
    CancellationTokenSource? cts;
    Task? connectionTask;
    IDisposable? autoConnection;
    TaskCompletionSource<bool>? refCountDisconnectedTcs;

    public ConnectableStream(IStream<T> source, int bufferSize = 0, IClock? clock = null)
    {
        if (bufferSize < 0) throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer size must be non-negative.");
        this.source = source;
        this.replayBufferSize = bufferSize;
        this.clock = clock ?? (source is Stream<T> s ? s.Clock : Streamix.Implementations.SystemClock.Instance);
    }

    internal IClock Clock => clock;

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
                Channel<T>[] currentSubscribers;

                lock (_lock)
                {
                    if (replayBufferSize > 0)
                    {
                        replayBuffer.Enqueue(item);
                        while (replayBuffer.Count > replayBufferSize)
                            replayBuffer.Dequeue();
                    }
                    currentSubscribers = this.subscribers.Values.ToArray();
                }

                foreach (var subscriber in currentSubscribers)
                {
                    try
                    {
                        await subscriber.Writer.WriteAsync(item, cancellationToken);
                    }
                    catch { }
                }
            }

            lock (_lock)
            {
                isCompleted = true;
                var finalSubscribers = subscribers.Values.ToArray();
                foreach (var subscriber in finalSubscribers)
                    subscriber.Writer.TryComplete();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            lock (_lock)
            {
                isCompleted = true;
                var finalSubscribers = subscribers.Values.ToArray();
                foreach (var subscriber in finalSubscribers)
                    subscriber.Writer.TryComplete();
            }
        }
        catch (Exception ex)
        {
            lock (_lock)
            {
                isCompleted = true;
                error = ex;
                var finalSubscribers = subscribers.Values.ToArray();
                foreach (var subscriber in finalSubscribers)
                    subscriber.Writer.TryComplete(ex);
            }
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

    async IAsyncEnumerable<TResult> mapAwait<TResult>(Func<T, ValueTask<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            yield return await selector(item);
    }

    async IAsyncEnumerable<T> filter(Func<T, bool> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (predicate(item))
                yield return item;
        }
    }

    async IAsyncEnumerable<T> filterAwait(Func<T, ValueTask<bool>> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (await predicate(item))
                yield return item;
        }
    }

    async IAsyncEnumerable<TResult> flatMap<TResult>(Func<T, ISingle<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                yield return innerItem;
    }

    async IAsyncEnumerable<TResult> parallelMapEnumerable<TResult>(Func<T, IAsyncEnumerable<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
                await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                    yield return innerItem;
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
                        refCountDisconnectedTcs?.TrySetResult(true);
                        refCountDisconnectedTcs = null;
                    }
                }
            }
        }
    }

    /// <summary>
    /// Returns a task that completes when all RefCount subscribers have disconnected.
    /// This is useful for testing RefCount behavior without relying on timing assumptions.
    /// </summary>
    public Task WhenRefCountDisconnectedAsync()
    {
        lock (_lock)
        {
            if (refCounter == 0)
                return Task.CompletedTask;

            refCountDisconnectedTcs = new TaskCompletionSource<bool>();
            return refCountDisconnectedTcs.Task;
        }
    }

    async IAsyncEnumerable<TResult> parallelMapTask<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
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
                try { await Task.WhenAll(tasks); } catch { }
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

    async IAsyncEnumerable<TResult> concatMapInternal<TResult>(Func<T, IStream<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                yield return innerItem;
        }
    }

    async IAsyncEnumerable<TResult> concatMapAwaitInternal<TResult>(Func<T, ValueTask<IStream<TResult>>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            var innerStream = await selector(item);
            await foreach (var innerItem in innerStream.WithCancellation(cancellationToken))
                yield return innerItem;
        }
    }

    async IAsyncEnumerable<TResult> flatMapOrdered<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency, int maxBufferedItemsPerInner, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in concatMapInternal(selector, cancellationToken))
                yield return item;
            yield break;
        }

        var channel = Channel.CreateBounded<ChannelReader<TResult>>(maxConcurrency);

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
                        var innerChannel = Channel.CreateBounded<TResult>(maxBufferedItemsPerInner);

                        var task = Task.Run(async () =>
                        {
                            try
                            {
                                var innerStream = selector(item);
                                await foreach (var innerItem in innerStream.WithCancellation(cts.Token))
                                {
                                    await innerChannel.Writer.WriteAsync(innerItem, cts.Token);
                                }
                                innerChannel.Writer.Complete();
                            }
                            catch (Exception ex)
                            {
                                innerChannel.Writer.TryComplete(ex);
                                throw;
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }, cts.Token);

                        tasks.Add(task);
                        await channel.Writer.WriteAsync(innerChannel.Reader, cts.Token);
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
            {
                while (channel.Reader.TryRead(out var innerReader))
                {
                    await foreach (var result in innerReader.ReadAllAsync(cancellationToken))
                        yield return result;
                }
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

    async IAsyncEnumerable<TResult> parallelMapTaskOrdered<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in this.WithCancellation(cancellationToken))
                yield return await selector(item);
            yield break;
        }

        var channel = Channel.CreateBounded<Task<TResult>>(maxConcurrency);

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
                                return await selector(item);
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }, cts.Token);

                        tasks.Add(task);
                        await channel.Writer.WriteAsync(task, cts.Token);
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

                while (channel.Reader.TryRead(out var task))
                    yield return await task;
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

    async IAsyncEnumerable<TResult> flatMapAwaitConcurrent<TResult>(Func<T, ValueTask<IStream<TResult>>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in concatMapAwaitInternal(selector, cancellationToken))
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
                enumerator = this.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception ex)
            {
                lastException = ex;
                failed = true;
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
                            yield return current;
                        else
                        {
                            yield break;
                        }
                    }
                }
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

        while (true)
        {
            var moveNextTask = enumerator.MoveNextAsync().AsTask();
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var timeoutTask = clock.Delay(interval, timeoutCts.Token);

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

    async IAsyncEnumerable<T> onBackpressureBuffer(int capacity, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(capacity);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in this.WithCancellation(cts.Token))
                {
                    if (!channel.Writer.TryWrite(item))
                    {
                        throw new BackpressureException($"Buffer overflow: capacity of {capacity} reached.");
                    }
                }
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
        }, cts.Token);

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
            {
                while (channel.Reader.TryRead(out var item))
                {
                    yield return item;
                }
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

    async IAsyncEnumerable<T> onBackpressureLatest([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in this.WithCancellation(cts.Token))
                {
                    channel.Writer.TryWrite(item);
                }
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
        }, cts.Token);

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
            {
                while (channel.Reader.TryRead(out var item))
                {
                    yield return item;
                }
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

    async IAsyncEnumerable<T> doOnComplete(Action onComplete, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
            yield return item;

        onComplete();
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
    public IStream<T> OnBackpressureBuffer(int capacity)
    {
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than 0.");
        return Streamix.Stream.From(onBackpressureBuffer(capacity), clock);
    }

    /// <inheritdoc />
    public IStream<T> OnBackpressureDrop()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public IStream<T> OnBackpressureLatest()
    {
        return Streamix.Stream.From(onBackpressureLatest(), clock);
    }

    /// <inheritdoc />
    public IStream<T> OnBackpressureError()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public IStream<T> Retry(int retryCount, Func<int, Exception, TimeSpan> backoffStrategy)
    {
        return Stream.From(retry(retryCount, backoffStrategy), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> MapAwait<TResult>(Func<T, ValueTask<TResult>> selector)
    {
        return Stream.From(mapAwait(selector), clock);
    }

    /// <inheritdoc />
    public IDisposable Connect()
    {
        lock (_lock)
        {
            if (connectionTask != null && !connectionTask.IsCompleted)
                return new ConnectionDisposable(this);

            replayBuffer.Clear();
            isCompleted = false;
            error = null;

            cts = new CancellationTokenSource();
            var token = cts.Token;
            connectionTask = runConnectionInternal(token);
            return new ConnectionDisposable(this);
        }
    }

    /// <inheritdoc />
    public IStream<T> FilterAwait(Func<T, ValueTask<bool>> predicate)
    {
        return Stream.From(filterAwait(predicate), clock);
    }

    /// <inheritdoc />
    public IStream<T> RefCount()
    {
        return Stream.From(refCount(), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMapAwait<TResult>(Func<T, ValueTask<ISingle<TResult>>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(flatMapAwaitConcurrent(selector, maxConcurrency), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> Map<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(parallelMapTask(selector, maxConcurrency), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> MapOrdered<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(parallelMapTaskOrdered(selector, maxConcurrency), clock);
    }

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<T>();

        lock (_lock)
        {
            if (replayBufferSize > 0)
            {
                foreach (var item in replayBuffer)
                    channel.Writer.TryWrite(item);
            }

            if (isCompleted)
            {
                channel.Writer.TryComplete(error);
            }
            else
            {
                subscribers.TryAdd(id, channel);
            }
        }

        return getAsyncEnumeratorImpl(id, channel, cancellationToken);
    }

    async IAsyncEnumerator<T> getAsyncEnumeratorImpl(Guid id, Channel<T> channel, CancellationToken cancellationToken)
    {
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
        return Stream.From(map(selector), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> Select<TResult>(Func<T, TResult> selector) => Map(selector);

    /// <inheritdoc />
    public IStream<T> Filter(Func<T, bool> predicate)
    {
        return Stream.From(filter(predicate), clock);
    }

    /// <inheritdoc />
    public IStream<T> Where(Func<T, bool> predicate) => Filter(predicate);

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(flatMap(selector), clock)
            : Stream.From(parallelMapEnumerable(selector, maxConcurrency), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(parallelMapTask(selector, 1), clock)
            : Stream.From(parallelMapTask(selector, maxConcurrency), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> SelectMany<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = int.MaxValue) => FlatMap(selector, maxConcurrency);

    /// <inheritdoc />
    public IStream<TResult> FlatMap<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(concatMapInternal(selector), clock)
            : Stream.From(parallelMapEnumerable(selector, maxConcurrency), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> ConcatMap<TResult>(Func<T, IStream<TResult>> selector)
    {
        return Stream.From(concatMapInternal(selector), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> FlatMapOrdered<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency = int.MaxValue, int maxBufferedItemsPerInner = 16)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        if (maxBufferedItemsPerInner <= 0) throw new ArgumentOutOfRangeException(nameof(maxBufferedItemsPerInner), "Max buffered items per inner must be greater than 0.");
        return Stream.From(flatMapOrdered(selector, maxConcurrency, maxBufferedItemsPerInner), clock);
    }

    /// <inheritdoc />
    public IStream<T> Take(int count)
    {
        return Stream.From(take(count), clock);
    }

    /// <inheritdoc />
    public IStream<T> Skip(int count)
    {
        return Stream.From(skip(count), clock);
    }

    /// <inheritdoc />
    public IStream<T> MergeWith(params IStream<T>[] others)
    {
        return Stream.From(mergeWith(others), clock);
    }

    /// <inheritdoc />
    public IStream<TResult> ZipWith<TOther, TResult>(IStream<TOther> other, Func<T, TOther, TResult> resultSelector)
    {
        return Stream.From(zipWith(other, resultSelector), clock);
    }

    /// <inheritdoc />
    public IStream<IList<T>> Buffer(int count)
    {
        return Stream.From(buffer(count), clock);
    }

    /// <inheritdoc />
    public IStream<IStream<T>> Window(int count)
    {
        return Stream.From(window(count), clock);
    }

    /// <inheritdoc />
    public IStream<T> Throttle(TimeSpan interval)
    {
        return Stream.From(throttle(interval), clock);
    }

    /// <inheritdoc />
    public IStream<T> Delay(TimeSpan interval)
    {
        return Stream.From(delay(interval), clock);
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
        return Stream.From(onErrorResume(errorHandler), clock);
    }

    /// <inheritdoc />
    public IStream<T> OnErrorReturn(T value)
    {
        return Stream.From(onErrorReturn(value), clock);
    }

    /// <inheritdoc />
    public IStream<T> OnErrorMap(Func<Exception, Exception> mapper)
    {
        return Stream.From(onErrorMap(mapper), clock);
    }

    /// <inheritdoc />
    public IConnectableStream<T> Publish() => this;

    /// <inheritdoc />
    public IConnectableStream<T> Replay(int bufferSize) => new ConnectableStream<T>(this, bufferSize);

    /// <inheritdoc />
    public IStream<T> RunOn(TaskScheduler scheduler)
    {
        return Stream.From(runOn(scheduler), clock);
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
        return Stream.From(doOnNext(onNext), clock);
    }

    /// <inheritdoc />
    public IStream<T> Do(Action<T> onNext) => DoOnNext(onNext);

    /// <inheritdoc />
    public IStream<T> Tap(Action<T> onNext) => DoOnNext(onNext);

    /// <inheritdoc />
    public IStream<T> DoOnError(Action<Exception> onError)
    {
        return Stream.From(doOnError(onError), clock);
    }

    /// <inheritdoc />
    public IStream<T> DoOnComplete(Action onComplete)
    {
        return Stream.From(doOnComplete(onComplete), clock);
    }

    /// <inheritdoc />
    public IStream<T> DoOnTerminate(Action onTerminate)
    {
        return Stream.From(doOnTerminate(onTerminate), clock);
    }

    /// <inheritdoc />
    public Task ToChannel(ChannelWriter<T> writer, bool completeWriter = true, CancellationToken cancellationToken = default)
    {
        return SinkHelper.WriteSinkAsync(
            this,
            new ChannelWriterSink<T>(writer),
            completeWriter ? SinkCompletionMode.CompleteSink : SinkCompletionMode.LeaveSinkOpen,
            cancellationToken);
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
