namespace Streamix.Abstractions;

/// <summary>
/// Represents a stream that can have 0 or 1 item, backed by <see cref="IAsyncEnumerable{T}"/>.
/// </summary>
/// <typeparam name="T">The type of item in the single-item stream.</typeparam>
public interface ISingle<T> : IAsyncEnumerable<T>
{
    /// <summary>
    /// Projects the element of a single-item stream into a new form using a synchronous selector function.
    /// </summary>
    /// <typeparam name="TResult">The type of the element in the resulting single-item stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="ISingle{TResult}"/> whose element is the result of invoking the transform function on the element of source.</returns>
    ISingle<TResult> Map<TResult>(Func<T, TResult> selector);

    /// <summary>
    /// Projects the element of a single-item stream into a new form. Alias for <see cref="Map{TResult}"/>.
    /// </summary>
    /// <typeparam name="TResult">The type of the element in the resulting single-item stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="ISingle{TResult}"/> whose element is the result of invoking the transform function on the element of source.</returns>
    ISingle<TResult> Select<TResult>(Func<T, TResult> selector);

    /// <summary>
    /// Projects the element of a single-item stream to another <see cref="ISingle{TResult}"/> and flattens it.
    /// </summary>
    /// <typeparam name="TResult">The type of the element in the resulting single-item stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="ISingle{TResult}"/> whose element is the result of invoking the one-to-one transform function on the element of source.</returns>
    ISingle<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector);

    /// <summary>
    /// Projects the element of a single-item stream to an <see cref="IStream{TResult}"/> and flattens it.
    /// </summary>
    /// <typeparam name="TResult">The type of elements in the resulting stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the one-to-many transform function on the element of source.</returns>
    IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector);

    /// <summary>
    /// Resumes a single-item stream with another single-item stream if an error occurs.
    /// </summary>
    /// <param name="errorHandler">A function that returns a fallback single-item stream given the exception.</param>
    /// <returns>A resilient <see cref="ISingle{T}"/>.</returns>
    ISingle<T> OnErrorResume(Func<Exception, ISingle<T>> errorHandler);

    /// <summary>
    /// Resumes a single-item stream with a single constant value if an error occurs.
    /// </summary>
    /// <param name="value">The value to emit on error.</param>
    /// <returns>A resilient <see cref="ISingle{T}"/>.</returns>
    ISingle<T> OnErrorReturn(T value);

    /// <summary>
    /// Maps a single-item stream error into another exception.
    /// </summary>
    /// <param name="mapper">A function to map the exception.</param>
    /// <returns>An <see cref="ISingle{T}"/> with mapped errors.</returns>
    ISingle<T> OnErrorMap(Func<Exception, Exception> mapper);

    /// <summary>
    /// Executes upstream operations (including source enumeration) on the specified scheduler.
    /// This is equivalent to SubscribeOn in other reactive libraries.
    /// </summary>
    /// <param name="scheduler">The task scheduler to run the operations on.</param>
    /// <returns>An <see cref="ISingle{T}"/> scheduled on the specified scheduler.</returns>
    ISingle<T> RunOn(TaskScheduler scheduler);

    /// <summary>
    /// Terminal operation that executes an action for the element of the stream.
    /// </summary>
    /// <param name="action">The action to execute for each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the element has been processed.</returns>
    Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Terminal operation that executes an asynchronous action for the element of the stream.
    /// </summary>
    /// <param name="action">The asynchronous action to execute for each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the element has been processed.</returns>
    Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Converts the single-item stream to a <see cref="Task{T}"/>.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the completion of the single-item stream and its value.</returns>
    Task<T> ToTask(CancellationToken cancellationToken = default);

    /// <summary>
    /// Retries a single-item stream if it fails, up to a specified number of times.
    /// </summary>
    /// <param name="retryCount">The number of times to retry.</param>
    /// <returns>A retrying <see cref="ISingle{T}"/>.</returns>
    ISingle<T> Retry(int retryCount = 1);

    /// <summary>
    /// Terminates a single-item stream with a <see cref="TimeoutException"/> if it doesn't emit an element within a specified time interval.
    /// </summary>
    /// <param name="interval">The maximum time interval before an element is emitted.</param>
    /// <returns>A timeout-monitored <see cref="ISingle{T}"/>.</returns>
    ISingle<T> Timeout(TimeSpan interval);

    /// <summary>
    /// Executes an action for the element of the stream without modifying it.
    /// </summary>
    /// <param name="onNext">The action to execute for the element.</param>
    /// <returns>The same single-item stream.</returns>
    ISingle<T> DoOnNext(Action<T> onNext);

    /// <summary>
    /// Executes an action when the single-item stream fails.
    /// </summary>
    /// <param name="onError">The action to execute with the exception.</param>
    /// <returns>The same single-item stream.</returns>
    ISingle<T> DoOnError(Action<Exception> onError);

    /// <summary>
    /// Executes an action when the single-item stream terminates (either successfully or with an error).
    /// </summary>
    /// <param name="onTerminate">The action to execute.</param>
    /// <returns>The same single-item stream.</returns>
    ISingle<T> DoOnTerminate(Action onTerminate);
}
