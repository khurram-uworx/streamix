using System.Collections.Concurrent;

namespace Streamix.Implementations;

internal sealed class StreamScope : IStreamScope, IAsyncDisposable
{
    readonly CancellationTokenSource cts;
    readonly ConcurrentBag<Task> tasks = new();
    readonly object syncRoot = new();
    readonly object errorSyncRoot = new();
    Exception? firstException;
    bool disposed;

    public StreamScope(CancellationToken externalToken)
    {
        this.cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
    }

    public CancellationToken CancellationToken => cts.Token;

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
        ArgumentNullException.ThrowIfNull(work);

        lock (syncRoot)
        {
            if (disposed) throw new ObjectDisposedException(nameof(StreamScope));

            // If already cancelled, we still want to track the task but it will likely exit immediately
            var task = Task.Run(async () =>
            {
                try
                {
                    await work(cts.Token);
                }
                catch (Exception ex)
                {
                    RecordException(ex);
                    throw;
                }
            }, cts.Token);

            tasks.Add(task);
        }
    }

    public async Task WaitAllAsync()
    {
        // Continuously wait for all registered tasks to complete.
        // We use a loop because new tasks might be registered while we are waiting for current ones.
        while (true)
        {
            Task[] toWait;
            lock (syncRoot)
            {
                // Snapshot the current set of incomplete tasks.
                toWait = tasks.Where(t => !t.IsCompleted).ToArray();
            }

            // If no incomplete tasks remain, all children have reached a terminal state.
            if (toWait.Length == 0) break;

            try
            {
                // Wait for the current batch of tasks to settle.
                // We use WhenAll but catch exceptions because we want all tasks to settle
                // before we propagate the first failure recorded via RecordException.
                await Task.WhenAll(toWait);
            }
            catch
            {
                // Exceptions are captured by individual tasks and reported via RecordException,
                // so we ignore them here to continue waiting for other siblings to settle.
            }
        }

        // After all tasks have settled, propagate the first failure if one occurred.
        if (firstException != null)
        {
            throw firstException;
        }

        if (cts.IsCancellationRequested)
        {
            throw new OperationCanceledException(cts.Token);
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
