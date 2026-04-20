using Microsoft.Extensions.Logging;
using Streamix.Implementations;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Provides extension methods for <see cref="IStream{T}"/> implementing higher-order operations and convenience methods.
/// This follows the composition-based design pattern similar to <see cref="ILogger"/>, where the core interface
/// is minimal and all higher-level functionality is provided through extension methods.
/// </summary>
public static class StreamExtensions
{
    static async IAsyncEnumerable<TResult> map<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, TResult> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            yield return selector(item);
    }

    static async IAsyncEnumerable<TResult> mapAwait<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, ValueTask<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            yield return await selector(item);
    }

    static async IAsyncEnumerable<TResult> flatMap<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, ISingle<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                yield return innerItem;
    }

    static async IAsyncEnumerable<TResult> concatMapInternal<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, IStream<TResult>> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                yield return innerItem;
    }

    static async IAsyncEnumerable<TResult> parallelMapTask<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, Task<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in enumerable.WithCancellation(cancellationToken))
                yield return await selector(item);
            yield break;
        }

        var channel = Channel.CreateBounded<TResult>(new BoundedChannelOptions(maxConcurrency) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        var scope = new StreamScope(cancellationToken);
        var semaphore = new SemaphoreSlim(maxConcurrency);

        try
        {
            scope.Run(async ct =>
            {
                try
                {
                    var tasks = new List<Task>();
                    await foreach (var item in enumerable.WithCancellation(ct).ConfigureAwait(false))
                    {
                        await semaphore.WaitAsync(ct).ConfigureAwait(false);

                        tasks.Add(scope.RunAsync(async innerCt =>
                        {
                            try
                            {
                                var result = await selector(item);
                                try
                                {
                                    await channel.Writer.WriteAsync(result, innerCt).ConfigureAwait(false);
                                }
                                catch (ChannelClosedException) { }
                                catch (OperationCanceledException) { }
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }));

                        if (tasks.Count > 1000) tasks.RemoveAll(t => t.IsCompleted);
                    }

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    channel.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    throw;
                }
            });

            await foreach (var item in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            try
            {
                await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Dispose();
            }
        }
    }

    static async IAsyncEnumerable<TResult> parallelMapTaskOrdered<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, Task<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in enumerable.WithCancellation(cancellationToken))
                yield return await selector(item);
            yield break;
        }

        var channel = Channel.CreateBounded<Task<TResult>>(new BoundedChannelOptions(maxConcurrency) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        var scope = new StreamScope(cancellationToken);
        var semaphore = new SemaphoreSlim(maxConcurrency);

        try
        {
            scope.Run(async ct =>
            {
                try
                {
                    await foreach (var item in enumerable.WithCancellation(ct).ConfigureAwait(false))
                    {
                        await semaphore.WaitAsync(ct).ConfigureAwait(false);

                        var task = scope.RunAsync(async innerCt =>
                        {
                            try
                            {
                                return await selector(item);
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        });

                        try
                        {
                            await channel.Writer.WriteAsync(task, ct).ConfigureAwait(false);
                        }
                        catch (ChannelClosedException) { }
                        catch (OperationCanceledException) { }
                    }

                    channel.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    throw;
                }
            });

            await foreach (var task in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return await task;
            }
        }
        finally
        {
            try
            {
                await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Dispose();
            }
        }
    }

    static async IAsyncEnumerable<TResult> parallelMapEnumerable<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, IAsyncEnumerable<TResult>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in enumerable.WithCancellation(cancellationToken))
                await foreach (var innerItem in selector(item).WithCancellation(cancellationToken))
                    yield return innerItem;
            yield break;
        }

        var channel = Channel.CreateBounded<TResult>(new BoundedChannelOptions(maxConcurrency) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        var scope = new StreamScope(cancellationToken);
        var semaphore = new SemaphoreSlim(maxConcurrency);

        try
        {
            scope.Run(async ct =>
            {
                try
                {
                    var tasks = new List<Task>();
                    await foreach (var item in enumerable.WithCancellation(ct).ConfigureAwait(false))
                    {
                        await semaphore.WaitAsync(ct).ConfigureAwait(false);

                        tasks.Add(scope.RunAsync(async innerCt =>
                        {
                            try
                            {
                                await foreach (var result in selector(item).WithCancellation(innerCt).ConfigureAwait(false))
                                {
                                    if (innerCt.IsCancellationRequested) break;
                                    try
                                    {
                                        await channel.Writer.WriteAsync(result, innerCt).ConfigureAwait(false);
                                    }
                                    catch (ChannelClosedException)
                                    {
                                        break;
                                    }
                                }
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }));

                        if (tasks.Count > 1000) tasks.RemoveAll(t => t.IsCompleted);
                    }

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    channel.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    throw;
                }
            });

            await foreach (var result in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return result;
            }
        }
        finally
        {
            try
            {
                await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Dispose();
            }
        }
    }

    static async IAsyncEnumerable<TResult> flatMapOrdered<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, IStream<TResult>> selector, int maxConcurrency, int maxBufferedItemsPerInner, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in concatMapInternal(enumerable, selector, cancellationToken))
                yield return item;
            yield break;
        }

        var channel = Channel.CreateBounded<ChannelReader<TResult>>(new BoundedChannelOptions(maxConcurrency) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        var scope = new StreamScope(cancellationToken);
        var semaphore = new SemaphoreSlim(maxConcurrency);

        try
        {
            scope.Run(async ct =>
            {
                try
                {
                    await foreach (var item in enumerable.WithCancellation(ct).ConfigureAwait(false))
                    {
                        await semaphore.WaitAsync(ct).ConfigureAwait(false);

                        var innerChannel = Channel.CreateBounded<TResult>(new BoundedChannelOptions(maxBufferedItemsPerInner) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });

                        scope.Run(async innerCt =>
                        {
                            try
                            {
                                var innerStream = selector(item);
                                await foreach (var innerItem in innerStream.WithCancellation(innerCt).ConfigureAwait(false))
                                {
                                    if (innerCt.IsCancellationRequested) break;
                                    try
                                    {
                                        await innerChannel.Writer.WriteAsync(innerItem, innerCt).ConfigureAwait(false);
                                    }
                                    catch (ChannelClosedException)
                                    {
                                        break;
                                    }
                                }
                                innerChannel.Writer.TryComplete();
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
                        });

                        try
                        {
                            await channel.Writer.WriteAsync(innerChannel.Reader, ct).ConfigureAwait(false);
                        }
                        catch (ChannelClosedException) { }
                        catch (OperationCanceledException) { }
                    }

                    channel.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    throw;
                }
            });

            await foreach (var innerReader in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                await foreach (var result in innerReader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    yield return result;
                    if (scope.IsFaulted) break;
                }
                if (scope.IsFaulted) break;
            }
        }
        finally
        {
            try
            {
                await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Dispose();
            }
        }
    }

    static async IAsyncEnumerable<TResult> flatMapAwaitConcurrent<T, TResult>(IAsyncEnumerable<T> enumerable, Func<T, ValueTask<ISingle<TResult>>> selector, int maxConcurrency, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (maxConcurrency == 1)
        {
            await foreach (var item in enumerable.WithCancellation(cancellationToken))
            {
                var innerSingle = await selector(item);
                await foreach (var innerItem in innerSingle.WithCancellation(cancellationToken))
                    yield return innerItem;
            }
            yield break;
        }

        var channel = Channel.CreateBounded<TResult>(new BoundedChannelOptions(maxConcurrency) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        var scope = new StreamScope(cancellationToken);
        var semaphore = new SemaphoreSlim(maxConcurrency);

        try
        {
            scope.Run(async ct =>
            {
                try
                {
                    var tasks = new List<Task>();
                    await foreach (var item in enumerable.WithCancellation(ct).ConfigureAwait(false))
                    {
                        await semaphore.WaitAsync(ct).ConfigureAwait(false);

                        tasks.Add(scope.RunAsync(async innerCt =>
                        {
                            try
                            {
                                var innerSingle = await selector(item);
                                await foreach (var result in innerSingle.WithCancellation(innerCt).ConfigureAwait(false))
                                {
                                    if (innerCt.IsCancellationRequested) break;
                                    try
                                    {
                                        await channel.Writer.WriteAsync(result, innerCt).ConfigureAwait(false);
                                    }
                                    catch (ChannelClosedException)
                                    {
                                        break;
                                    }
                                }
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }));

                        if (tasks.Count > 1000) tasks.RemoveAll(t => t.IsCompleted);
                    }

                    await Task.WhenAll(tasks).ConfigureAwait(false);
                    channel.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    throw;
                }
            });

            await foreach (var result in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return result;
            }
        }
        finally
        {
            try
            {
                await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Dispose();
            }
        }
    }

    static async IAsyncEnumerable<T> filter<T>(IAsyncEnumerable<T> enumerable, Func<T, bool> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            if (predicate(item))
                yield return item;
    }

    static async IAsyncEnumerable<T> filterAsync<T>(IAsyncEnumerable<T> enumerable, Func<T, ValueTask<bool>> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            if (await predicate(item))
                yield return item;
    }

    static async IAsyncEnumerable<IList<T>> buffer<T>(IAsyncEnumerable<T> enumerable, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var buffer = new List<T>(count);
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
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

    static async IAsyncEnumerable<T> retry<T>(IAsyncEnumerable<T> enumerable, IClock clock, int retryCount, Func<int, Exception, TimeSpan>? backoffStrategy = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int attempts = 0;
        while (true)
        {
            bool failed = false;
            IAsyncEnumerator<T>? enumerator = null;
            Exception? lastException = null;
            try
            {
                enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
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
                            yield return current;
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

    static async IAsyncEnumerable<T> timeout<T>(IAsyncEnumerable<T> enumerable, IClock clock, TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var enumerator = enumerable.GetAsyncEnumerator(cancellationToken);

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

    static async IAsyncEnumerable<T> onBackpressureLatest<T>(IAsyncEnumerable<T> enumerable, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });
        var scope = new StreamScope(cancellationToken);

        scope.Run(async ct =>
        {
            try
            {
                await foreach (var item in enumerable.WithCancellation(ct))
                    channel.Writer.TryWrite(item);

                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        });

        try
        {
            await foreach (var item in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
        }
    }

    static async IAsyncEnumerable<T> onBackpressureError<T>(IAsyncEnumerable<T> enumerable, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(1);
        var scope = new StreamScope(cancellationToken);

        scope.Run(async ct =>
        {
            try
            {
                await foreach (var item in enumerable.WithCancellation(ct))
                    if (!channel.Writer.TryWrite(item))
                        throw new BackpressureException("Downstream cannot keep pace.");

                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        });

        try
        {
            await foreach (var item in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
        }
    }

    static async IAsyncEnumerable<T> onBackpressureDrop<T>(IAsyncEnumerable<T> enumerable, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite
        });
        var scope = new StreamScope(cancellationToken);

        scope.Run(async ct =>
        {
            try
            {
                await foreach (var item in enumerable.WithCancellation(ct))
                    channel.Writer.TryWrite(item);

                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        });

        try
        {
            await foreach (var item in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
        }
    }

    static async IAsyncEnumerable<T> onBackpressureBuffer<T>(IAsyncEnumerable<T> enumerable, int capacity, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(capacity);
        var scope = new StreamScope(cancellationToken);

        scope.Run(async ct =>
        {
            try
            {
                await foreach (var item in enumerable.WithCancellation(ct))
                {
                    if (!channel.Writer.TryWrite(item))
                        throw new BackpressureException($"Buffer overflow: capacity of {capacity} reached.");
                }
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        });

        try
        {
            await foreach (var item in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
        }
    }

    static async IAsyncEnumerable<T> onErrorResume<T>(IAsyncEnumerable<T> enumerable, Func<Exception, IStream<T>> errorHandler, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        IStream<T>? resumeSource = null;
        try
        {
            try
            {
                enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
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

    static async IAsyncEnumerable<T> doOnNext<T>(IAsyncEnumerable<T> enumerable, Action<T> onNext, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
        {
            onNext(item);
            yield return item;
        }
    }

    static async IAsyncEnumerable<T> doOnError<T>(IAsyncEnumerable<T> enumerable, Action<Exception> onError, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IAsyncEnumerator<T>? enumerator = null;
        try
        {
            try
            {
                enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
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

    static async IAsyncEnumerable<T> doOnTerminate<T>(IAsyncEnumerable<T> enumerable, Action onTerminate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        try
        {
            await foreach (var item in enumerable.WithCancellation(cancellationToken))
                yield return item;
        }
        finally
        {
            onTerminate();
        }
    }

    static async IAsyncEnumerable<T> doOnComplete<T>(IAsyncEnumerable<T> enumerable, Action onComplete, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
            yield return item;

        onComplete();
    }

    static async IAsyncEnumerable<T> trace<T>(IAsyncEnumerable<T> enumerable, string prefix, Action<string> logger, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var pref = string.IsNullOrEmpty(prefix) ? "" : $"[{prefix}] ";
        logger($"{pref}Subscribe");

        IAsyncEnumerator<T>? enumerator = null;
        try
        {
            try
            {
                enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception ex)
            {
                logger($"{pref}Error({ex.Message})");
                throw;
            }

            while (true)
            {
                logger($"{pref}Request(1)");
                T item;
                bool hasNext;
                try
                {
                    hasNext = await enumerator.MoveNextAsync();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    logger($"{pref}Cancelled");
                    throw;
                }
                catch (Exception ex)
                {
                    logger($"{pref}Error({ex.Message})");
                    throw;
                }

                if (!hasNext) break;
                item = enumerator.Current;

                logger($"{pref}Next({item})");
                yield return item;
            }

            logger($"{pref}Completed");
        }
        finally
        {
            if (enumerator != null)
            {
                await enumerator.DisposeAsync();
                logger($"{pref}Dispose");
            }
        }
    }

    static async IAsyncEnumerable<T> checkpoint<T>(IAsyncEnumerable<T> enumerable, IClock clock, string checkpointName, Action<string> logger, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var startTime = clock.Now;
        var lastItemTime = startTime;

        IAsyncEnumerator<T>? enumerator = null;
        try
        {
            try
            {
                enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
            }
            catch (Exception ex)
            {
                var errorTime = clock.Now;
                var totalErrorElapsed = errorTime - startTime;
                logger($"[Checkpoint: {checkpointName}] Error({ex.Message}) | Total: {totalErrorElapsed.TotalMilliseconds:F2}ms");
                throw;
            }

            while (true)
            {
                T item;
                bool hasNext;
                try
                {
                    hasNext = await enumerator.MoveNextAsync();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    var cancelTime = clock.Now;
                    var totalCancelElapsed = cancelTime - startTime;
                    logger($"[Checkpoint: {checkpointName}] Cancelled | Total: {totalCancelElapsed.TotalMilliseconds:F2}ms");
                    throw;
                }
                catch (Exception ex)
                {
                    var errorTime = clock.Now;
                    var totalErrorElapsed = errorTime - startTime;
                    logger($"[Checkpoint: {checkpointName}] Error({ex.Message}) | Total: {totalErrorElapsed.TotalMilliseconds:F2}ms");
                    throw;
                }

                if (!hasNext) break;
                item = enumerator.Current;

                var now = clock.Now;
                var totalElapsed = now - startTime;
                var sinceLastElapsed = now - lastItemTime;
                lastItemTime = now;

                logger($"[Checkpoint: {checkpointName}] Next({item}) | Total: {totalElapsed.TotalMilliseconds:F2}ms | Since last: {sinceLastElapsed.TotalMilliseconds:F2}ms");
                yield return item;
            }

            var completeTime = clock.Now;
            var totalCompleteElapsed = completeTime - startTime;
            logger($"[Checkpoint: {checkpointName}] Completed | Total: {totalCompleteElapsed.TotalMilliseconds:F2}ms");
        }
        finally
        {
            if (enumerator != null)
                await enumerator.DisposeAsync();
        }
    }

    static async IAsyncEnumerable<T> teeToChannel<T>(IAsyncEnumerable<T> enumerable, ChannelWriter<T> writer, bool completeWriter, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Exception? completionError = null;
        var canceled = false;
        IAsyncEnumerator<T>? enumerator = null;

        try
        {
            enumerator = enumerable.GetAsyncEnumerator(cancellationToken);

            while (true)
            {
                T item;

                try
                {
                    if (!await enumerator.MoveNextAsync())
                        break;

                    item = enumerator.Current;
                    await writer.WriteAsync(item, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    canceled = true;
                    throw;
                }
                catch (Exception ex)
                {
                    completionError = ex;
                    throw;
                }

                yield return item;
            }
        }
        finally
        {
            if (enumerator != null)
                await enumerator.DisposeAsync();

            if (completeWriter && !canceled)
                writer.TryComplete(completionError);
        }
    }

    static async IAsyncEnumerable<T> delay<T>(IAsyncEnumerable<T> enumerable, IClock clock, TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
        {
            await clock.Delay(interval, cancellationToken);
            yield return item;
        }
    }

    static async IAsyncEnumerable<T> throttle<T>(IAsyncEnumerable<T> enumerable, IClock clock, TimeSpan interval, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        DateTimeOffset? nextAllowedEmission = null;

        await foreach (var item in enumerable.WithCancellation(cancellationToken))
        {
            var now = clock.Now;
            if (nextAllowedEmission == null || now >= nextAllowedEmission.Value)
            {
                yield return item;
                nextAllowedEmission = now + interval;
            }
        }
    }

    static async IAsyncEnumerable<T> skip<T>(IAsyncEnumerable<T> enumerable, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        int remaining = count;
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
        {
            if (remaining > 0)
            {
                remaining--;
                continue;
            }

            yield return item;
        }
    }

    static async IAsyncEnumerable<T> take<T>(IAsyncEnumerable<T> enumerable, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (count <= 0) yield break;

        int remaining = count;
        await foreach (var item in enumerable.WithCancellation(cancellationToken))
        {
            yield return item;
            if (--remaining == 0) break;
        }
    }

    /// <summary>
    /// Projects each element of a stream into a new form using a synchronous selector function.
    /// This overload is sequential and preserves source ordering.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the transform function on each element of source.</returns>
    public static IStream<TResult> Map<T, TResult>(this IStream<T> source, Func<T, TResult> selector)
        => Stream.From(map(source, selector), source.Clock, source.Name);

    /// <summary>
    /// Projects each element of a stream into a new form by applying an asynchronous selector concurrently while preserving upstream ordering.
    /// Results are buffered as necessary to ensure they are emitted in source order.
    /// This is the concurrent ordered <c>Map</c> overload.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent selector invocations.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits mapped results in source order.</returns>
    public static IStream<TResult> MapOrdered<T, TResult>(this IStream<T> source, Func<T, Task<TResult>> selector, int maxConcurrency)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(parallelMapTaskOrdered(source, selector, maxConcurrency), source.Clock, source.Name);
    }

    /// <summary>
    /// Projects each element of a stream into a new form using an asynchronous selector function.
    /// This overload is sequential and preserves source ordering by awaiting each selector invocation before advancing.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async transform function on each element of source.</returns>
    public static IStream<TResult> MapAwait<T, TResult>(this IStream<T> source, Func<T, ValueTask<TResult>> selector)
        => Stream.From(mapAwait(source, selector), source.Clock, source.Name);

    /// <summary>
    /// Projects each element of a stream into a new form by applying an asynchronous selector concurrently.
    /// Results are emitted as soon as they complete, so upstream ordering is not preserved.
    /// This is the concurrent unordered <c>Map</c> overload and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits mapped results in completion order.</returns>
    public static IStream<TResult> FlatMap<T, TResult>(this IStream<T> source, Func<T, Task<TResult>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(parallelMapTask(source, selector, maxConcurrency), source.Clock, source.Name);
    }

    /// <summary>
    /// Projects each element of a stream to another stream and merges the inner streams concurrently.
    /// Results are emitted as soon as inner streams produce them, so outer ordering is not preserved.
    /// This is the highest-throughput 1-to-N flattening variant and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent inner streams. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits items from inner streams in completion order.</returns>
    public static IStream<TResult> FlatMap<T, TResult>(this IStream<T> source, Func<T, IStream<TResult>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(concatMapInternal(source, selector), source.Clock, source.Name)
            : Stream.From(parallelMapEnumerable(source, selector, maxConcurrency), source.Clock, source.Name);
    }

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream.
    /// Results are emitted as soon as they complete, so upstream ordering is not preserved.
    /// This is the highest-throughput variant for 1-to-1 async transforms and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the one-to-many transform function on each element of the input stream.</returns>
    public static IStream<TResult> FlatMap<T, TResult>(this IStream<T> source, Func<T, ISingle<TResult>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return maxConcurrency == 1
            ? Stream.From(flatMap(source, selector), source.Clock, source.Name)
            : Stream.From(parallelMapEnumerable(source, selector, maxConcurrency), source.Clock, source.Name);
    }

    /// <summary>
    /// Projects each element of a stream to another stream and merges the inner streams concurrently while preserving outer source ordering.
    /// Results from inner streams are buffered as necessary to ensure they are emitted in the same order as the source elements that produced them.
    /// Each later inner stream may buffer up to <paramref name="maxBufferedItemsPerInner"/> items while waiting for earlier inners to drain.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent inner streams. Defaults to unbounded concurrency.</param>
    /// <param name="maxBufferedItemsPerInner">The maximum number of buffered items allowed per inner stream while preserving outer ordering. Defaults to 16.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits inner stream items grouped in original source order.</returns>
    public static IStream<TResult> FlatMapOrdered<T, TResult>(this IStream<T> source, Func<T, IStream<TResult>> selector, int maxConcurrency = int.MaxValue, int maxBufferedItemsPerInner = 16)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        if (maxBufferedItemsPerInner <= 0) throw new ArgumentOutOfRangeException(nameof(maxBufferedItemsPerInner), "Max buffered items per inner must be greater than 0.");
        return Stream.From(flatMapOrdered(source, selector, maxConcurrency, maxBufferedItemsPerInner), source.Clock, source.Name);
    }

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> using an asynchronous selector and flattens the resulting streams into one stream.
    /// Results are emitted as soon as they complete, so upstream ordering is not preserved.
    /// This is a high-throughput variant for async transforms and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async one-to-one transform function on each element of the input stream.</returns>
    public static IStream<TResult> FlatMapAwait<T, TResult>(this IStream<T> source, Func<T, ValueTask<ISingle<TResult>>> selector, int maxConcurrency = int.MaxValue)
    {
        if (maxConcurrency <= 0) throw new ArgumentOutOfRangeException(nameof(maxConcurrency), "Max concurrency must be greater than 0.");
        return Stream.From(flatMapAwaitConcurrent(source, selector, maxConcurrency), source.Clock, source.Name);
    }

    /// <summary>
    /// Projects each element of a stream to another stream and concatenates the inner streams sequentially.
    /// Only one inner stream is active at a time, so results are emitted strictly in source order.
    /// This is equivalent to <see cref="FlatMap{T, TResult}(IStream{T}, Func{T, IStream{TResult}}, int)"/> with maxConcurrency of 1.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="source">The stream to filter.</param>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits items from each inner stream before moving to the next source item.</returns>
    public static IStream<TResult> ConcatMap<T, TResult>(this IStream<T> source, Func<T, IStream<TResult>> selector)
        => Stream.From(concatMapInternal(source, selector), source.Clock, source.Name);

    /// <summary>
    /// Filters a stream of values based on a predicate.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="predicate">A function to test each element for a condition.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains elements from the input stream that satisfy the condition.</returns>
    public static IStream<T> Filter<T>(this IStream<T> source, Func<T, bool> predicate)
        => Stream.From(filter(source, predicate), source.Clock, source.Name);

    /// <summary>
    /// Filters a stream of values based on an asynchronous predicate.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="predicate">An asynchronous function to test each element for a condition.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains elements from the input stream that satisfy the condition.</returns>
    public static IStream<T> FilterAsync<T>(this IStream<T> source, Func<T, ValueTask<bool>> predicate)
        => Stream.From(filterAsync(source, predicate), source.Clock, source.Name);

    /// <summary>
    /// Retries a stream if it fails, up to a specified number of times.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="retryCount">The number of times to retry.</param>
    /// <returns>A retrying <see cref="IStream{T}"/>.</returns>
    public static IStream<T> Retry<T>(this IStream<T> source, int retryCount = 1)
        => Stream.From(retry(source, source.Clock, retryCount), source.Clock, source.Name);

    /// <summary>
    /// Retries a stream if it fails, up to a specified number of times, with a backoff strategy.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="retryCount">The number of times to retry.</param>
    /// <param name="backoffStrategy">A function that computes the delay before the next retry attempt based on the attempt number (1-based) and the exception that caused the failure.</param>
    /// <returns>A retrying <see cref="IStream{T}"/> with backoff.</returns>
    public static IStream<T> Retry<T>(this IStream<T> source, int retryCount, Func<int, Exception, TimeSpan> backoffStrategy)
        => Stream.From(retry(source, source.Clock, retryCount, backoffStrategy), source.Clock, source.Name);

    /// <summary>
    /// Groups elements of a stream into lists of a specified size.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="count">The maximum size of each buffer.</param>
    /// <returns>An <see cref="IStream{T}"/> of <see cref="IList{T}"/>.</returns>
    public static IStream<IList<T>> Buffer<T>(this IStream<T> source, int count)
    {
        if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than 0.");
        return Stream.From(buffer(source, count), source.Clock, source.Name);
    }

    /// <summary>
    /// Terminates a stream with a <see cref="TimeoutException"/> if it doesn't emit an element within a specified time interval.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="interval">The maximum time interval between elements.</param>
    /// <returns>A timeout-monitored <see cref="IStream{T}"/>.</returns>
    public static IStream<T> Timeout<T>(this IStream<T> source, TimeSpan interval)
        => Stream.From(timeout(source, source.Clock, interval), source.Clock, source.Name);

    /// <summary>
    /// Tracks progress through a specific stage of the pipeline with timing information using a custom logging action.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="checkpointName">The name of the checkpoint.</param>
    /// <param name="loggerAction">The action to use for logging.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Checkpoint<T>(this IStream<T> source, string checkpointName, Action<string> loggerAction)
        => Stream.From(checkpoint(source, source.Clock, checkpointName, loggerAction), source.Clock, source.Name);

    /// <summary>
    /// Provides a comprehensive trace of every stream signal using a custom logging action.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="loggerAction">The action to use for logging.</param>
    /// <param name="prefix">Optional prefix. If not provided, the stream name is used.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> TraceAction<T>(this IStream<T> source, Action<string> loggerAction, string? prefix = null)
        => Stream.From(trace(source, prefix ?? source.Name ?? "", loggerAction), source.Clock, source.Name);

    /// <summary>
    /// Resumes a stream with another stream if an error occurs.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="errorHandler">A function that returns a fallback stream given the exception.</param>
    /// <returns>A resilient <see cref="IStream{T}"/>.</returns>
    public static IStream<T> OnErrorResume<T>(this IStream<T> source, Func<Exception, IStream<T>> errorHandler)
        => Stream.From(onErrorResume(source, errorHandler), source.Clock, source.Name);

    /// <summary>
    /// Resumes a stream with a single constant value if an error occurs.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="value">The value to emit on error.</param>
    /// <returns>A resilient <see cref="IStream{T}"/>.</returns>
    public static IStream<T> OnErrorReturn<T>(this IStream<T> source, T value)
        => OnErrorResume(source, _ => Stream.Just<T>(value).Named(source.Name ?? ""));

    /// <summary>
    /// Maps a stream error into another exception.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="mapper">A function to map the exception.</param>
    /// <returns>An <see cref="IStream{T}"/> with mapped errors.</returns>
    public static IStream<T> OnErrorMap<T>(this IStream<T> source, Func<Exception, Exception> mapper)
        => OnErrorResume(source, ex => Stream.Error<T>(mapper(ex)).Named(source.Name ?? ""));

    /// <summary>
    /// Executes an action for each element of the stream without modifying it.
    /// This operator does not catch exceptions thrown by the action.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="onNext">The action to execute for each element.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> DoOnNext<T>(this IStream<T> source, Action<T> onNext)
        => Stream.From(doOnNext(source, onNext), source.Clock, source.Name);

    /// <summary>
    /// Alias for <see cref="DoOnNext{T}(IStream{T}, Action{T})"/>.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="onNext">The action to execute for each element.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Do<T>(this IStream<T> source, Action<T> onNext)
        => source.DoOnNext(onNext);

    /// <summary>
    /// Alias for <see cref="DoOnNext{T}(IStream{T}, Action{T})"/>.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="onNext">The action to execute for each element.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Tap<T>(this IStream<T> source, Action<T> onNext)
        => source.DoOnNext(onNext);

    /// <summary>
    /// Executes an action when the stream fails.
    /// This hook does not fire if the stream is cancelled or completes successfully.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="onError">The action to execute with the exception.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> DoOnError<T>(this IStream<T> source, Action<Exception> onError)
        => Stream.From(doOnError(source, onError), source.Clock, source.Name);

    /// <summary>
    /// Executes an action when the stream completes successfully.
    /// This hook does not fire if an error occurs or the stream is cancelled.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="onComplete">The action to execute.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> DoOnComplete<T>(this IStream<T> source, Action onComplete)
        => Stream.From(doOnComplete(source, onComplete), source.Clock, source.Name);

    /// <summary>
    /// Executes an action when the stream terminates (either successfully or with an error).
    /// This hook also fires if the stream is cancelled during enumeration.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="onTerminate">The action to execute.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> DoOnTerminate<T>(this IStream<T> source, Action onTerminate)
        => Stream.From(doOnTerminate(source, onTerminate), source.Clock, source.Name);

    /// <summary>
    /// Mirrors items into a channel writer while preserving the main stream.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="writer">The side-channel writer.</param>
    /// <param name="completeWriter">Whether this operator owns writer completion.</param>
    /// <returns>The original stream with side-channel writes applied.</returns>
    public static IStream<T> TeeToChannel<T>(this IStream<T> source, ChannelWriter<T> writer, bool completeWriter = false)
        => Stream.From(teeToChannel(source, writer, completeWriter), source.Clock, source.Name);

    /// <summary>
    /// Shares the source stream among multiple subscribers.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <returns>An <see cref="IConnectableStream{T}"/>.</returns>
    public static IConnectableStream<T> Publish<T>(this IStream<T> source) => new ConnectableStream<T>(source);

    /// <summary>
    /// Shares the source stream among multiple subscribers and replays the last <paramref name="bufferSize"/> elements to late subscribers.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="bufferSize">The maximum number of elements to replay to late subscribers.</param>
    /// <returns>An <see cref="IConnectableStream{T}"/>.</returns>
    public static IConnectableStream<T> Replay<T>(this IStream<T> source, int bufferSize)
        => new ConnectableStream<T>(source, bufferSize);

    /// <summary>
    /// Tracks progress through a specific stage of the pipeline with timing information.
    /// Logs when items pass through, including time since stream start and time since last item.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="checkpointName">The name of the checkpoint.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Checkpoint<T>(this IStream<T> source, string checkpointName)
        => source.Checkpoint(checkpointName, s => Console.WriteLine(s));

    /// <summary>
    /// Provides a comprehensive trace of every stream signal using an <see cref="ILogger"/>.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="logger">The logger to use.</param>
    /// <param name="prefix">Optional prefix. If not provided, the stream name is used.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Trace<T>(this IStream<T> source, ILogger logger, string? prefix = null)
        => source.TraceAction(s => logger.LogInformation(s), prefix ?? source.Name ?? "");

    /// <summary>
    /// Provides a comprehensive trace of every stream signal with a specified prefix.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="prefix">The prefix to use in traces.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Trace<T>(this IStream<T> source, string prefix)
        => source.TraceAction(s => Console.WriteLine(s), prefix);

    /// <summary>
    /// Provides a comprehensive trace of every stream signal (Subscribe, Next, Error, Complete, Cancel, Dispose).
    /// Uses the stream name as a prefix if available.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Trace<T>(this IStream<T> source) => source.Trace(source.Name ?? "");

    /// <summary>
    /// Logs items, errors, and completion of the stream using a custom logging action.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="loggerAction">The action to use for logging.</param>
    /// <param name="prefix">Optional prefix. If not provided, the stream name is used.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> LogAction<T>(this IStream<T> source, Action<string> loggerAction, string? prefix = null)
    {
        prefix = prefix ?? source.Name ?? null;
        var pref = string.IsNullOrEmpty(prefix) ? "" : $"[{prefix}] ";
        return source.DoOnNext(x => loggerAction($"{pref}Next({x})"))
              .DoOnError(ex => loggerAction($"{pref}Error({ex.Message})"))
              .DoOnComplete(() => loggerAction($"{pref}Completed"));
    }

    /// <summary>
    /// Logs items, errors, and completion of the stream using a custom logging action.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="loggerAction">The action to use for logging.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> LogAction<T>(this IStream<T> source, Action<string> loggerAction)
        => LogAction(source, loggerAction, source.Name ?? "");

    /// <summary>
    /// Logs items, errors, and completion of the stream to standard output with a specified prefix.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="prefix">The prefix to use in logs.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Log<T>(this IStream<T> source, string prefix)
        => source.LogAction(s => Console.WriteLine(s), prefix);

    /// <summary>
    /// Logs items, errors, and completion of the stream to standard output.
    /// Uses the stream name as a prefix if available.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Log<T>(this IStream<T> source) => Log(source, source.Name ?? "");

    /// <summary>
    /// Logs items, errors, and completion of the stream using an <see cref="ILogger"/>.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="logger">The logger to use.</param>
    /// <param name="prefix">Optional prefix. If not provided, the stream name is used.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Log<T>(this IStream<T> source, ILogger logger, string? prefix = null)
    {
        var p = prefix ?? source.Name ?? "";
        var pref = string.IsNullOrEmpty(p) ? "" : $"[{p}] ";
        return source.DoOnNext(x => logger.LogInformation("{Prefix}Next({Value})", pref, x))
              .DoOnError(ex => logger.LogError(ex, "{Prefix}Error({Message})", pref, ex.Message))
              .DoOnComplete(() => logger.LogInformation("{Prefix}Completed", pref));
    }

    /// <summary>
    /// Logs items, errors, and completion of the stream to debug output.
    /// Uses the stream name as a prefix if available.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Debug<T>(this IStream<T> source) => source.Debug(source.Name ?? "");

    /// <summary>
    /// Logs items, errors, and completion of the stream to debug output with a specified prefix.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="prefix">The prefix to use in logs.</param>
    /// <returns>The same stream.</returns>
    public static IStream<T> Debug<T>(this IStream<T> source, string prefix)
    {
        var pref = string.IsNullOrEmpty(prefix) ? "" : $"[{prefix}] ";
        return source.DoOnNext(x => System.Diagnostics.Debug.WriteLine($"{pref}Next({x})"))
              .DoOnError(ex => System.Diagnostics.Debug.WriteLine($"{pref}Error({ex.Message})"))
              .DoOnComplete(() => System.Diagnostics.Debug.WriteLine($"{pref}Completed"));
    }

    /// <summary>
    /// Inserts a bounded channel-backed execution boundary into the pipeline.
    /// This decouples upstream production from downstream consumption using explicit channel semantics.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="capacity">The bounded channel capacity.</param>
    /// <param name="mode">The backpressure policy used when the boundary is full.</param>
    /// <returns>A stream that crosses an explicit channel boundary before continuing downstream.</returns>
    public static IStream<T> PipeThroughChannel<T>(this IStream<T> source, int capacity, ChannelBackpressureMode mode = ChannelBackpressureMode.Wait)
        => Stream.From(ChannelExecution.PipeThroughChannel(source, capacity, mode), source.Clock, source.Name);

    /// <summary>
    /// Keeps only the latest item when downstream is slow.
    /// Older items in the buffer are discarded in favor of newer ones.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <returns>A stream with backpressure latest strategy applied.</returns>
    public static IStream<T> OnBackpressureLatest<T>(this IStream<T> source)
        => Stream.From(onBackpressureLatest(source), source.Clock, source.Name);

    /// <summary>
    /// Throws a <see cref="BackpressureException"/> when downstream cannot keep pace.
    /// Signals immediate failure rather than buffering or dropping items.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <returns>A stream with backpressure error strategy applied.</returns>
    public static IStream<T> OnBackpressureError<T>(this IStream<T> source)
        => Stream.From(onBackpressureError(source), source.Clock, source.Name);

    /// <summary>
    /// Drops items when downstream cannot keep pace.
    /// Items already buffered are preserved while new arrivals are discarded.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <returns>A stream with backpressure drop strategy applied.</returns>
    public static IStream<T> OnBackpressureDrop<T>(this IStream<T> source)
        => Stream.From(onBackpressureDrop(source), source.Clock, source.Name);

    /// <summary>
    /// Buffers items up to <paramref name="capacity"/> when downstream is slow.
    /// Throws <see cref="BackpressureException"/> if buffer overflows.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="capacity">Maximum number of items to buffer.</param>
    /// <returns>A stream with backpressure buffering strategy applied.</returns>
    public static IStream<T> OnBackpressureBuffer<T>(this IStream<T> source, int capacity)
    {
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than 0.");
        return Stream.From(onBackpressureBuffer(source, capacity), source.Clock, source.Name);
    }

    /// <summary>
    /// Groups elements of a stream into lists of a specified size after crossing a bounded channel-backed boundary.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="count">The maximum size of each buffer.</param>
    /// <param name="capacity">The bounded channel capacity used by the buffering boundary.</param>
    /// <param name="mode">The backpressure policy used when the boundary is full.</param>
    /// <returns>An <see cref="IStream{T}"/> of <see cref="IList{T}"/>.</returns>
    public static IStream<IList<T>> Buffer<T>(this IStream<T> source, int count, int capacity, ChannelBackpressureMode mode = ChannelBackpressureMode.Wait)
    {
        if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than 0.");
        return PipeThroughChannel(source, capacity, mode).Buffer(count);
    }

    /// <summary>
    /// Groups elements of a stream into windows of a specified size after crossing a bounded channel-backed boundary.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="count">The maximum size of each window.</param>
    /// <param name="capacity">The bounded channel capacity used by the windowing boundary.</param>
    /// <param name="mode">The backpressure policy used when the boundary is full.</param>
    /// <returns>An <see cref="IStream{T}"/> of <see cref="IStream{T}"/>.</returns>
    public static IStream<IStream<T>> Window<T>(this IStream<T> source, int count, int capacity, ChannelBackpressureMode mode = ChannelBackpressureMode.Wait)
    {
        if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than 0.");
        return PipeThroughChannel(source, capacity, mode).Window(count);
    }

    /// <summary>
    /// Inserts a channel-backed execution boundary and relays items through a worker pool while preserving source ordering.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="capacity">The bounded channel capacity.</param>
    /// <param name="degreeOfParallelism">The number of workers draining the channel-backed boundary.</param>
    /// <param name="mode">The backpressure policy used when the boundary is full.</param>
    /// <returns>A stream that runs across a channel-backed worker boundary.</returns>
    public static IStream<T> RunOnChannel<T>(this IStream<T> source, int capacity, int degreeOfParallelism = 1, ChannelBackpressureMode mode = ChannelBackpressureMode.Wait)
        => Stream.From(ChannelExecution.RunOnChannel(source, capacity, degreeOfParallelism, mode), source.Clock, source.Name);

    /// <summary>
    /// Terminal operation that writes all items of the stream to the specified <see cref="ChannelWriter{T}"/>.
    /// Supports backpressure: if the channel is bounded and full, this method will asynchronously wait for space to become available.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="writer">The channel writer to write items to.</param>
    /// <param name="completeWriter">Whether to complete the writer when the stream completes (either successfully or with an error).</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all items have been written to the channel.</returns>
    public static Task ToChannel<T>(this IStream<T> source, ChannelWriter<T> writer, bool completeWriter = true, CancellationToken cancellationToken = default)
    {
        return SinkHelper.WriteSinkAsync(
            source,
            new ChannelWriterSink<T>(writer),
            completeWriter ? SinkCompletionMode.CompleteSink : SinkCompletionMode.LeaveSinkOpen,
            cancellationToken);
    }

    /// <summary>
    /// Delays the emission of each element in a stream by a specified time interval.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="interval">The time interval to delay each element by.</param>
    /// <returns>A delayed <see cref="IStream{T}"/>.</returns>
    public static IStream<T> Delay<T>(this IStream<T> source, TimeSpan interval)
        => Stream.From(delay(source, source.Clock, interval), source.Clock, source.Name);

    /// <summary>
    /// Throttles a stream by emitting only the first element in each time interval.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="interval">The time interval to throttle by.</param>
    /// <returns>A throttled <see cref="IStream{T}"/>.</returns>
    public static IStream<T> Throttle<T>(this IStream<T> source, TimeSpan interval)
        => Stream.From(throttle(source, source.Clock, interval), source.Clock, source.Name);

    /// <summary>
    /// Returns a specified number of contiguous elements from the start of a stream.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="count">The number of elements to return.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains the specified number of elements from the start of the input stream.</returns>
    public static IStream<T> Take<T>(this IStream<T> source, int count)
        => Stream.From(take(source, count), source.Clock, source.Name);

    /// <summary>
    /// Bypasses a specified number of elements in a stream and then returns the remaining elements.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="count">The number of elements to skip before returning the remaining elements.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains the elements that occur after the specified index in the input stream.</returns>
    public static IStream<T> Skip<T>(this IStream<T> source, int count)
        => Stream.From(skip(source, count), source.Clock, source.Name);

    /// <summary>
    /// Terminal operation that executes an action for each element of the stream.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="action">The action to execute for each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all elements have been processed.</returns>
    public static async Task ForEachAsync<T>(this IStream<T> source, Action<T> action, CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken))
            action(item);
    }

    /// <summary>
    /// Terminal operation that executes an asynchronous action for each element of the stream.
    /// </summary>
    /// <param name="source">The stream to filter.</param>
    /// <param name="action">The asynchronous action to execute for each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all elements have been processed.</returns>
    public static async Task ForEachAsync<T>(this IStream<T> source, Func<T, Task> action, CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken))
            await action(item);
    }
}
