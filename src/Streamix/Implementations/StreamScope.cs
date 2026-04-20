using System.Collections.Concurrent;

namespace Streamix.Implementations;

internal sealed class StreamScope : IStreamScope, IAsyncDisposable
{
    readonly CancellationToken externalToken;
    readonly CancellationTokenSource cts;
    readonly ConcurrentDictionary<object, Task> tasks = new();
    readonly object syncRoot = new();
    readonly object errorSyncRoot = new();
    Exception? firstException;
    bool disposed;

    public StreamScope(CancellationToken externalToken)
    {
        this.externalToken = externalToken;
        this.cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
    }

    public CancellationToken CancellationToken => cts.Token;

    public bool IsFaulted
    {
        get
        {
            lock (errorSyncRoot)
            {
                return firstException != null;
            }
        }
    }

    internal void RecordException(Exception ex)
    {
        if (ex is OperationCanceledException && cts.IsCancellationRequested)
        {
            return;
        }

        lock (errorSyncRoot)
        {
            firstException ??= ex;
        }

        try
        {
            cts.Cancel();
        }
        catch (ObjectDisposedException) { }
    }

    public void Run(Func<CancellationToken, Task> work)
    {
        _ = RunAsync(work);
    }

    public Task RunAsync(Func<CancellationToken, Task> work)
    {
        ArgumentNullException.ThrowIfNull(work);

        lock (syncRoot)
        {
            if (disposed) throw new ObjectDisposedException(nameof(StreamScope));

            var key = new object();
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var task = Task.Run(async () =>
            {
                try
                {
                    cts.Token.ThrowIfCancellationRequested();
                    await work(cts.Token);
                    tcs.TrySetResult();
                }
                catch (OperationCanceledException ex) when (ex.CancellationToken == cts.Token)
                {
                    tcs.TrySetCanceled(ex.CancellationToken);
                }
                catch (Exception ex)
                {
                    RecordException(ex);
                    tcs.TrySetException(ex);
                }
                finally
                {
                    tasks.TryRemove(key, out _);
                }
            });

            tasks.TryAdd(key, task);
            return tcs.Task;
        }
    }

    public Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> work)
    {
        ArgumentNullException.ThrowIfNull(work);

        lock (syncRoot)
        {
            if (disposed) throw new ObjectDisposedException(nameof(StreamScope));

            var key = new object();
            var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            var task = Task.Run(async () =>
            {
                try
                {
                    cts.Token.ThrowIfCancellationRequested();
                    var result = await work(cts.Token);
                    tcs.TrySetResult(result);
                    return result;
                }
                catch (OperationCanceledException ex) when (ex.CancellationToken == cts.Token)
                {
                    tcs.TrySetCanceled(ex.CancellationToken);
                    throw;
                }
                catch (Exception ex)
                {
                    RecordException(ex);
                    tcs.TrySetException(ex);
                    throw;
                }
                finally
                {
                    tasks.TryRemove(key, out _);
                }
            });

            tasks.TryAdd(key, task);
            return tcs.Task;
        }
    }

    public async Task WaitAllAsync()
    {
        while (true)
        {
            Task[] toWait;
            toWait = tasks.Values.Where(t => !t.IsCompleted).ToArray();

            if (toWait.Length == 0) break;

            try
            {
                await Task.WhenAll(toWait).ConfigureAwait(false);
            }
            catch
            {
                // We wait for all to settle
            }
        }
    }

    public void ThrowIfFailed()
    {
        lock (errorSyncRoot)
        {
            if (firstException != null)
            {
                var ex = firstException;
                firstException = null; // Prevent re-throw
                throw ex;
            }
        }

        if (externalToken.IsCancellationRequested)
        {
            throw new TaskCanceledException(null, null, externalToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        lock (syncRoot)
        {
            if (disposed) return;
            disposed = true;
        }
        await cts.CancelAsync();
        cts.Dispose();
    }
}
