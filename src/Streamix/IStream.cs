using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Represents a stream of 0..N values, backed by <see cref="IAsyncEnumerable{T}"/>.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public interface IStream<T> : IAsyncEnumerable<T>
{
    /// <summary>
    /// Gets the name of the stream, if any.
    /// </summary>
    string? Name { get; }

    /// <summary>
    /// Sets the name of the stream.
    /// </summary>
    /// <param name="name">The name to set.</param>
    /// <returns>A new <see cref="IStream{T}"/> with the specified name.</returns>
    IStream<T> Named(string name);

    /// <summary>
    /// Projects each element of a stream into a new form using a synchronous selector function.
    /// This overload is sequential and preserves source ordering.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the transform function on each element of source.</returns>
    IStream<TResult> Map<TResult>(Func<T, TResult> selector);

    /// <summary>
    /// Projects each element of a stream into a new form using an asynchronous selector function.
    /// This overload is sequential and preserves source ordering by awaiting each selector invocation before advancing.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async transform function on each element of source.</returns>
    IStream<TResult> MapAwait<TResult>(Func<T, ValueTask<TResult>> selector);

    /// <summary>
    /// Projects each element of a stream into a new form by applying an asynchronous selector concurrently.
    /// Results are emitted as soon as they complete, so upstream ordering is not preserved.
    /// This is the concurrent unordered <c>Map</c> overload and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent selector invocations. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits mapped results in completion order.</returns>
    IStream<TResult> Map<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency = int.MaxValue);

    /// <summary>
    /// Projects each element of a stream into a new form by applying an asynchronous selector concurrently while preserving upstream ordering.
    /// Results are buffered as necessary to ensure they are emitted in source order.
    /// This is the concurrent ordered <c>Map</c> overload.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent selector invocations.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits mapped results in source order.</returns>
    IStream<TResult> MapOrdered<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency);

    /// <summary>
    /// Filters a stream of values based on a predicate.
    /// </summary>
    /// <param name="predicate">A function to test each element for a condition.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains elements from the input stream that satisfy the condition.</returns>
    IStream<T> Filter(Func<T, bool> predicate);

    /// <summary>
    /// Filters a stream of values based on an asynchronous predicate.
    /// </summary>
    /// <param name="predicate">An asynchronous function to test each element for a condition.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains elements from the input stream that satisfy the condition.</returns>
    IStream<T> FilterAwait(Func<T, ValueTask<bool>> predicate);

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream.
    /// Results are emitted as soon as they complete, so upstream ordering is not preserved.
    /// This is the highest-throughput variant for 1-to-1 async transforms and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the one-to-many transform function on each element of the input stream.</returns>
    IStream<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector, int maxConcurrency = int.MaxValue);

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> and flattens the resulting streams into one stream.
    /// Results are emitted as soon as they complete, so upstream ordering is not preserved.
    /// This is a high-throughput variant for async transforms and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the one-to-many transform function on each element of the input stream.</returns>
    IStream<TResult> FlatMap<TResult>(Func<T, Task<TResult>> selector, int maxConcurrency = int.MaxValue);

    /// <summary>
    /// Projects each element of a stream to an <see cref="ISingle{TResult}"/> using an asynchronous selector and flattens the resulting streams into one stream.
    /// Results are emitted as soon as they complete, so upstream ordering is not preserved.
    /// This is a high-throughput variant for async transforms and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">An asynchronous transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> whose elements are the result of invoking the async one-to-one transform function on each element of the input stream.</returns>
    IStream<TResult> FlatMapAwait<TResult>(Func<T, ValueTask<ISingle<TResult>>> selector, int maxConcurrency = int.MaxValue);

    /// <summary>
    /// Projects each element of a stream to another stream and merges the inner streams concurrently.
    /// Results are emitted as soon as inner streams produce them, so outer ordering is not preserved.
    /// This is the highest-throughput 1-to-N flattening variant and defaults to unbounded concurrency.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent inner streams. Defaults to unbounded concurrency.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits items from inner streams in completion order.</returns>
    IStream<TResult> FlatMap<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency = int.MaxValue);

    /// <summary>
    /// Projects each element of a stream to another stream and concatenates the inner streams sequentially.
    /// Only one inner stream is active at a time, so results are emitted strictly in source order.
    /// This is equivalent to <see cref="FlatMap{TResult}(Func{T, IStream{TResult}}, int)"/> with maxConcurrency of 1.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits items from each inner stream before moving to the next source item.</returns>
    IStream<TResult> ConcatMap<TResult>(Func<T, IStream<TResult>> selector);

    /// <summary>
    /// Projects each element of a stream to another stream and merges the inner streams concurrently while preserving outer source ordering.
    /// Results from inner streams are buffered as necessary to ensure they are emitted in the same order as the source elements that produced them.
    /// Each later inner stream may buffer up to <paramref name="maxBufferedItemsPerInner"/> items while waiting for earlier inners to drain.
    /// </summary>
    /// <typeparam name="TResult">The type of the elements in the resulting stream.</typeparam>
    /// <param name="selector">A transform function to apply to each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent inner streams. Defaults to unbounded concurrency.</param>
    /// <param name="maxBufferedItemsPerInner">The maximum number of buffered items allowed per inner stream while preserving outer ordering. Defaults to 16.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that emits inner stream items grouped in original source order.</returns>
    IStream<TResult> FlatMapOrdered<TResult>(Func<T, IStream<TResult>> selector, int maxConcurrency = int.MaxValue, int maxBufferedItemsPerInner = 16);

    /// <summary>
    /// Returns a specified number of contiguous elements from the start of a stream.
    /// </summary>
    /// <param name="count">The number of elements to return.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains the specified number of elements from the start of the input stream.</returns>
    IStream<T> Take(int count);

    /// <summary>
    /// Bypasses a specified number of elements in a stream and then returns the remaining elements.
    /// </summary>
    /// <param name="count">The number of elements to skip before returning the remaining elements.</param>
    /// <returns>An <see cref="IStream{T}"/> that contains the elements that occur after the specified index in the input stream.</returns>
    IStream<T> Skip(int count);

    /// <summary>
    /// Merges this stream with other streams into a single stream.
    /// </summary>
    /// <param name="others">The other streams to merge with.</param>
    /// <returns>A merged <see cref="IStream{T}"/>.</returns>
    IStream<T> MergeWith(params IStream<T>[] others);

    /// <summary>
    /// Zips this stream with another stream using a result selector function.
    /// </summary>
    /// <typeparam name="TOther">The type of elements in the other stream.</typeparam>
    /// <typeparam name="TResult">The type of elements in the resulting stream.</typeparam>
    /// <param name="other">The other stream to zip with.</param>
    /// <param name="resultSelector">A function that specifies how to combine the elements from the two streams.</param>
    /// <returns>An <see cref="IStream{TResult}"/> that contains zipped elements of the two streams.</returns>
    IStream<TResult> ZipWith<TOther, TResult>(IStream<TOther> other, Func<T, TOther, TResult> resultSelector);

    /// <summary>
    /// Groups elements of a stream into lists of a specified size.
    /// </summary>
    /// <param name="count">The maximum size of each buffer.</param>
    /// <returns>An <see cref="IStream{T}"/> of <see cref="IList{T}"/>.</returns>
    IStream<IList<T>> Buffer(int count);

    /// <summary>
    /// Groups elements of a stream into lists of a specified size after crossing a bounded channel-backed boundary.
    /// </summary>
    /// <param name="count">The maximum size of each buffer.</param>
    /// <param name="capacity">The bounded channel capacity used by the buffering boundary.</param>
    /// <param name="mode">The backpressure policy used when the boundary is full.</param>
    /// <returns>An <see cref="IStream{T}"/> of <see cref="IList{T}"/>.</returns>
    IStream<IList<T>> Buffer(int count, int capacity, ChannelBackpressureMode mode = ChannelBackpressureMode.Wait);

    /// <summary>
    /// Groups elements of a stream into windows of a specified size.
    /// </summary>
    /// <param name="count">The maximum size of each window.</param>
    /// <returns>An <see cref="IStream{T}"/> of <see cref="IStream{T}"/>.</returns>
    IStream<IStream<T>> Window(int count);

    /// <summary>
    /// Groups elements of a stream into windows of a specified size after crossing a bounded channel-backed boundary.
    /// </summary>
    /// <param name="count">The maximum size of each window.</param>
    /// <param name="capacity">The bounded channel capacity used by the windowing boundary.</param>
    /// <param name="mode">The backpressure policy used when the boundary is full.</param>
    /// <returns>An <see cref="IStream{T}"/> of <see cref="IStream{T}"/>.</returns>
    IStream<IStream<T>> Window(int count, int capacity, ChannelBackpressureMode mode = ChannelBackpressureMode.Wait);

    /// <summary>
    /// Throttles a stream by emitting only the first element in each time interval.
    /// </summary>
    /// <param name="interval">The time interval to throttle by.</param>
    /// <returns>A throttled <see cref="IStream{T}"/>.</returns>
    IStream<T> Throttle(TimeSpan interval);

    /// <summary>
    /// Delays the emission of each element in a stream by a specified time interval.
    /// </summary>
    /// <param name="interval">The time interval to delay each element by.</param>
    /// <returns>A delayed <see cref="IStream{T}"/>.</returns>
    IStream<T> Delay(TimeSpan interval);

    /// <summary>
    /// Retries a stream if it fails, up to a specified number of times.
    /// </summary>
    /// <param name="retryCount">The number of times to retry.</param>
    /// <returns>A retrying <see cref="IStream{T}"/>.</returns>
    IStream<T> Retry(int retryCount = 1);

    /// <summary>
    /// Retries a stream if it fails, up to a specified number of times, with a backoff strategy.
    /// </summary>
    /// <param name="retryCount">The number of times to retry.</param>
    /// <param name="backoffStrategy">A function that computes the delay before the next retry attempt based on the attempt number (1-based) and the exception that caused the failure.</param>
    /// <returns>A retrying <see cref="IStream{T}"/> with backoff.</returns>
    IStream<T> Retry(int retryCount, Func<int, Exception, TimeSpan> backoffStrategy);

    /// <summary>
    /// Terminates a stream with a <see cref="TimeoutException"/> if it doesn't emit an element within a specified time interval.
    /// </summary>
    /// <param name="interval">The maximum time interval between elements.</param>
    /// <returns>A timeout-monitored <see cref="IStream{T}"/>.</returns>
    IStream<T> Timeout(TimeSpan interval);

    /// <summary>
    /// Buffers items up to <paramref name="capacity"/> when downstream is slow.
    /// Throws <see cref="BackpressureException"/> if buffer overflows.
    /// </summary>
    /// <param name="capacity">Maximum number of items to buffer.</param>
    /// <returns>A stream with backpressure buffering strategy applied.</returns>
    IStream<T> OnBackpressureBuffer(int capacity);

    /// <summary>
    /// Drops items when downstream cannot keep pace.
    /// Items already buffered are preserved while new arrivals are discarded.
    /// </summary>
    /// <returns>A stream with backpressure drop strategy applied.</returns>
    IStream<T> OnBackpressureDrop();

    /// <summary>
    /// Keeps only the latest item when downstream is slow.
    /// Older items in the buffer are discarded in favor of newer ones.
    /// </summary>
    /// <returns>A stream with backpressure latest strategy applied.</returns>
    IStream<T> OnBackpressureLatest();

    /// <summary>
    /// Throws a <see cref="BackpressureException"/> when downstream cannot keep pace.
    /// Signals immediate failure rather than buffering or dropping items.
    /// </summary>
    /// <returns>A stream with backpressure error strategy applied.</returns>
    IStream<T> OnBackpressureError();

    /// <summary>
    /// Resumes a stream with another stream if an error occurs.
    /// </summary>
    /// <param name="errorHandler">A function that returns a fallback stream given the exception.</param>
    /// <returns>A resilient <see cref="IStream{T}"/>.</returns>
    IStream<T> OnErrorResume(Func<Exception, IStream<T>> errorHandler);

    /// <summary>
    /// Resumes a stream with a single constant value if an error occurs.
    /// </summary>
    /// <param name="value">The value to emit on error.</param>
    /// <returns>A resilient <see cref="IStream{T}"/>.</returns>
    IStream<T> OnErrorReturn(T value);

    /// <summary>
    /// Maps a stream error into another exception.
    /// </summary>
    /// <param name="mapper">A function to map the exception.</param>
    /// <returns>An <see cref="IStream{T}"/> with mapped errors.</returns>
    IStream<T> OnErrorMap(Func<Exception, Exception> mapper);

    /// <summary>
    /// Shares the source stream among multiple subscribers.
    /// </summary>
    /// <returns>An <see cref="IConnectableStream{T}"/>.</returns>
    IConnectableStream<T> Publish();

    /// <summary>
    /// Shares the source stream among multiple subscribers and replays the last <paramref name="bufferSize"/> elements to late subscribers.
    /// </summary>
    /// <param name="bufferSize">The maximum number of elements to replay to late subscribers.</param>
    /// <returns>An <see cref="IConnectableStream{T}"/>.</returns>
    IConnectableStream<T> Replay(int bufferSize);

    /// <summary>
    /// Executes upstream operations (including source enumeration) on the specified scheduler.
    /// This is equivalent to SubscribeOn in other reactive libraries.
    /// </summary>
    /// <param name="scheduler">The task scheduler to run the operations on.</param>
    /// <returns>An <see cref="IStream{T}"/> scheduled on the specified scheduler.</returns>
    IStream<T> RunOn(TaskScheduler scheduler);

    /// <summary>
    /// Inserts a bounded channel-backed execution boundary into the pipeline.
    /// This decouples upstream production from downstream consumption using explicit channel semantics.
    /// </summary>
    /// <param name="capacity">The bounded channel capacity.</param>
    /// <param name="mode">The backpressure policy used when the boundary is full.</param>
    /// <returns>A stream that crosses an explicit channel boundary before continuing downstream.</returns>
    IStream<T> PipeThroughChannel(int capacity, ChannelBackpressureMode mode = ChannelBackpressureMode.Wait);

    /// <summary>
    /// Inserts a channel-backed execution boundary and relays items through a worker pool while preserving source ordering.
    /// </summary>
    /// <param name="capacity">The bounded channel capacity.</param>
    /// <param name="degreeOfParallelism">The number of workers draining the channel-backed boundary.</param>
    /// <param name="mode">The backpressure policy used when the boundary is full.</param>
    /// <returns>A stream that runs across a channel-backed worker boundary.</returns>
    IStream<T> RunOnChannel(int capacity, int degreeOfParallelism = 1, ChannelBackpressureMode mode = ChannelBackpressureMode.Wait);

    /// <summary>
    /// Terminal operation that executes an action for each element of the stream.
    /// </summary>
    /// <param name="action">The action to execute for each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all elements have been processed.</returns>
    Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Terminal operation that executes an asynchronous action for each element of the stream.
    /// </summary>
    /// <param name="action">The asynchronous action to execute for each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all elements have been processed.</returns>
    Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Logs items, errors, and completion of the stream to standard output.
    /// Uses the stream name as a prefix if available.
    /// </summary>
    /// <returns>The same stream.</returns>
    IStream<T> Log();

    /// <summary>
    /// Logs items, errors, and completion of the stream to standard output with a specified prefix.
    /// </summary>
    /// <param name="prefix">The prefix to use in logs.</param>
    /// <returns>The same stream.</returns>
    IStream<T> Log(string prefix);

    /// <summary>
    /// Logs items, errors, and completion of the stream using a custom logging action.
    /// </summary>
    /// <param name="loggerAction">The action to use for logging.</param>
    /// <returns>The same stream.</returns>
    IStream<T> LogAction(Action<string> loggerAction);

    /// <summary>
    /// Logs items, errors, and completion of the stream using an <see cref="Microsoft.Extensions.Logging.ILogger"/>.
    /// </summary>
    /// <param name="logger">The logger to use.</param>
    /// <param name="prefix">Optional prefix. If not provided, the stream name is used.</param>
    /// <returns>The same stream.</returns>
    IStream<T> Log(Microsoft.Extensions.Logging.ILogger logger, string? prefix = null);

    /// <summary>
    /// Logs items, errors, and completion of the stream to debug output.
    /// Uses the stream name as a prefix if available.
    /// </summary>
    /// <returns>The same stream.</returns>
    IStream<T> Debug();

    /// <summary>
    /// Logs items, errors, and completion of the stream to debug output with a specified prefix.
    /// </summary>
    /// <param name="prefix">The prefix to use in logs.</param>
    /// <returns>The same stream.</returns>
    IStream<T> Debug(string prefix);

    /// <summary>
    /// Executes an action for each element of the stream without modifying it.
    /// This operator does not catch exceptions thrown by the action.
    /// </summary>
    /// <param name="onNext">The action to execute for each element.</param>
    /// <returns>The same stream.</returns>
    IStream<T> DoOnNext(Action<T> onNext);

    /// <summary>
    /// Alias for <see cref="DoOnNext(Action{T})"/>.
    /// </summary>
    /// <param name="onNext">The action to execute for each element.</param>
    /// <returns>The same stream.</returns>
    IStream<T> Do(Action<T> onNext);

    /// <summary>
    /// Alias for <see cref="DoOnNext(Action{T})"/>.
    /// </summary>
    /// <param name="onNext">The action to execute for each element.</param>
    /// <returns>The same stream.</returns>
    IStream<T> Tap(Action<T> onNext);

    /// <summary>
    /// Mirrors items into a channel writer while preserving the main stream.
    /// </summary>
    /// <param name="writer">The side-channel writer.</param>
    /// <param name="completeWriter">Whether this operator owns writer completion.</param>
    /// <returns>The original stream with side-channel writes applied.</returns>
    IStream<T> TeeToChannel(ChannelWriter<T> writer, bool completeWriter = false);

    /// <summary>
    /// Executes an action when the stream fails.
    /// This hook does not fire if the stream is cancelled or completes successfully.
    /// </summary>
    /// <param name="onError">The action to execute with the exception.</param>
    /// <returns>The same stream.</returns>
    IStream<T> DoOnError(Action<Exception> onError);

    /// <summary>
    /// Executes an action when the stream completes successfully.
    /// This hook does not fire if an error occurs or the stream is cancelled.
    /// </summary>
    /// <param name="onComplete">The action to execute.</param>
    /// <returns>The same stream.</returns>
    IStream<T> DoOnComplete(Action onComplete);

    /// <summary>
    /// Executes an action when the stream terminates (either successfully or with an error).
    /// This hook also fires if the stream is cancelled during enumeration.
    /// </summary>
    /// <param name="onTerminate">The action to execute.</param>
    /// <returns>The same stream.</returns>
    IStream<T> DoOnTerminate(Action onTerminate);

    /// <summary>
    /// Terminal operation that writes all items of the stream to the specified <see cref="ChannelWriter{T}"/>.
    /// Supports backpressure: if the channel is bounded and full, this method will asynchronously wait for space to become available.
    /// </summary>
    /// <param name="writer">The channel writer to write items to.</param>
    /// <param name="completeWriter">Whether to complete the writer when the stream completes (either successfully or with an error).</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all items have been written to the channel.</returns>
    Task ToChannel(ChannelWriter<T> writer, bool completeWriter = true, CancellationToken cancellationToken = default);
}
