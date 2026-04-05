namespace Streamix;

internal sealed class DelegateAsyncSink<T> : IAsyncSink<T>
{
    readonly Func<T, CancellationToken, ValueTask> writeAsync;
    readonly Func<Exception?, CancellationToken, ValueTask>? completeAsync;

    public DelegateAsyncSink(
        Func<T, CancellationToken, ValueTask> writeAsync,
        Func<Exception?, CancellationToken, ValueTask>? completeAsync = null)
    {
        this.writeAsync = writeAsync ?? throw new ArgumentNullException(nameof(writeAsync));
        this.completeAsync = completeAsync;
    }

    public ValueTask WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        return writeAsync(item, cancellationToken);
    }

    public ValueTask CompleteAsync(Exception? error = null, CancellationToken cancellationToken = default)
    {
        if (completeAsync is null)
        {
            return ValueTask.CompletedTask;
        }

        return completeAsync(error, cancellationToken);
    }
}
