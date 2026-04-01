namespace Streamix.Abstractions;

/// <summary>
/// Represents a stream that can have 0 or 1 item.
/// </summary>
/// <typeparam name="T">The type of item in the single-item stream.</typeparam>
public interface ISingle<T> : IAsyncEnumerable<T>
{
    /// <summary>
    /// Projects the element of a single-item stream into a new form.
    /// </summary>
    ISingle<TResult> Map<TResult>(Func<T, TResult> selector);

    /// <summary>
    /// Projects the element of a single-item stream into a new form. Alias for <see cref="Map{TResult}"/>.
    /// </summary>
    ISingle<TResult> Select<TResult>(Func<T, TResult> selector);

    /// <summary>
    /// Projects the element of a single-item stream to another <see cref="ISingle{TResult}"/> and flattens it.
    /// </summary>
    ISingle<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector);

    /// <summary>
    /// Projects the element of a single-item stream to an <see cref="IStream{TResult}"/> and flattens it.
    /// </summary>
    IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector);

    /// <summary>
    /// Resumes a single-item stream with another single-item stream if an error occurs.
    /// </summary>
    ISingle<T> OnErrorResume(Func<Exception, ISingle<T>> errorHandler);

    /// <summary>
    /// Resumes a single-item stream with a single value if an error occurs.
    /// </summary>
    ISingle<T> OnErrorReturn(T value);

    /// <summary>
    /// Maps the error into another exception.
    /// </summary>
    ISingle<T> OnErrorMap(Func<Exception, Exception> mapper);

    /// <summary>
    /// Executes upstream operations (including source enumeration) on the specified scheduler.
    /// This is equivalent to SubscribeOn in other reactive libraries.
    /// </summary>
    ISingle<T> RunOn(TaskScheduler scheduler);

    /// <summary>
    /// Terminal operation that executes an action for the element of the stream.
    /// </summary>
    Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Terminal operation that executes an async action for the element of the stream.
    /// </summary>
    Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Converts the single-item stream to a <see cref="Task{T}"/>.
    /// </summary>
    Task<T> ToTask(CancellationToken cancellationToken = default);

    /// <summary>
    /// Retries a single-item stream if it fails.
    /// </summary>
    ISingle<T> Retry(int retryCount = 1);

    /// <summary>
    /// Terminates a single-item stream with an error if it doesn't emit an element within a specified time interval.
    /// </summary>
    ISingle<T> Timeout(TimeSpan interval);
}
