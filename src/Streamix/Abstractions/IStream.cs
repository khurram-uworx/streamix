namespace Streamix.Abstractions;

/// <summary>
/// Represents a stream of 0..N values.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public interface IStream<T> : IAsyncEnumerable<T>
{
    /// <summary>
    /// Projects each element of a stream into a new form.
    /// </summary>
    IStream<TResult> Map<TResult>(Func<T, TResult> selector);

    /// <summary>
    /// Projects each element of a stream into a new form. Alias for <see cref="Map{TResult}"/>.
    /// </summary>
    IStream<TResult> Select<TResult>(Func<T, TResult> selector);

    /// <summary>
    /// Filters a stream of values based on a predicate.
    /// </summary>
    IStream<T> Filter(Func<T, bool> predicate);

    /// <summary>
    /// Filters a stream of values based on a predicate. Alias for <see cref="Filter"/>.
    /// </summary>
    IStream<T> Where(Func<T, bool> predicate);

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream.
    /// Supports concurrency control.
    /// </summary>
    IStream<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = 1);

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream.
    /// Supports asynchronous projection and concurrency control.
    /// </summary>
    IStream<TResult> FlatMap<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency = 1);

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream. Alias for <see cref="FlatMap{TResult}(Func{T, ISingle{TResult}}, int)"/>.
    /// </summary>
    IStream<TResult> SelectMany<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = 1);

    /// <summary>
    /// Projects each element of a stream to an <see cref="IStream{TResult}"/> and flattens the resulting streams into one stream.
    /// Supports concurrency control.
    /// </summary>
    IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency = 1);

    /// <summary>
    /// Returns a specified number of contiguous elements from the start of a stream.
    /// </summary>
    IStream<T> Take(int count);

    /// <summary>
    /// Bypasses a specified number of elements in a stream and then returns the remaining elements.
    /// </summary>
    IStream<T> Skip(int count);

    /// <summary>
    /// Merges this stream with other streams.
    /// </summary>
    IStream<T> MergeWith(params IStream<T>[] others);

    /// <summary>
    /// Zips this stream with another stream using a result selector.
    /// </summary>
    IStream<TResult> ZipWith<TOther, TResult>(IStream<TOther> other, Func<T, TOther, TResult> resultSelector);

    /// <summary>
    /// Groups elements of a stream into buffers.
    /// </summary>
    IStream<IList<T>> Buffer(int count);

    /// <summary>
    /// Groups elements of a stream into windows.
    /// </summary>
    IStream<IStream<T>> Window(int count);

    /// <summary>
    /// Throttles a stream by emitting only the first element in each time interval.
    /// </summary>
    IStream<T> Throttle(TimeSpan interval);

    /// <summary>
    /// Delays the emission of each element in a stream by a specified time interval.
    /// </summary>
    IStream<T> Delay(TimeSpan interval);

    /// <summary>
    /// Retries a stream if it fails.
    /// </summary>
    IStream<T> Retry(int retryCount = 1);

    /// <summary>
    /// Terminates a stream with an error if it doesn't emit an element within a specified time interval.
    /// </summary>
    IStream<T> Timeout(TimeSpan interval);

    /// <summary>
    /// Resumes a stream with another stream if an error occurs.
    /// </summary>
    IStream<T> OnErrorResume(Func<Exception, IStream<T>> errorHandler);

    /// <summary>
    /// Resumes a stream with a single element if an error occurs.
    /// </summary>
    IStream<T> OnErrorReturn(T value);

    /// <summary>
    /// Maps the error into another exception.
    /// </summary>
    IStream<T> OnErrorMap(Func<Exception, Exception> mapper);

    /// <summary>
    /// Shares the source stream.
    /// </summary>
    IConnectableStream<T> Publish();

    /// <summary>
    /// Executes upstream operations (including source enumeration) on the specified scheduler.
    /// This is equivalent to SubscribeOn in other reactive libraries.
    /// </summary>
    IStream<T> RunOn(TaskScheduler scheduler);

    /// <summary>
    /// Terminal operation that executes an action for each element of the stream.
    /// </summary>
    Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Terminal operation that executes an async action for each element of the stream.
    /// </summary>
    Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default);
}
