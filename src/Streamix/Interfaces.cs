namespace Streamix;

/// <summary>
/// Defines a clock abstraction for time-based operations.
/// </summary>
public interface IClock
{
    /// <summary>
    /// Gets the current time.
    /// </summary>
    DateTimeOffset Now { get; }

    /// <summary>
    /// Returns a task that completes after a specified time interval.
    /// </summary>
    Task Delay(TimeSpan delay, CancellationToken cancellationToken = default);
}

/// <summary>
/// Provides methods to emit items, complete the stream, or signal an error.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public interface IStreamEmitter<T>
{
    /// <summary>
    /// Gets the cancellation token for the current subscriber.
    /// This token is cancelled when the consumer cancels the subscription,
    /// or when the stream reaches a terminal state via <see cref="Complete"/> or <see cref="Fail"/>.
    /// Producers should monitor this token to stop any ongoing work.
    /// </summary>
    CancellationToken CancellationToken { get; }

    /// <summary>
    /// Emits an item to the stream.
    /// This method suspends if the consumer is slow and the internal buffer is full.
    /// </summary>
    /// <param name="item">The item to emit.</param>
    /// <returns>A value task that completes when the item has been accepted into the buffer.</returns>
    ValueTask EmitAsync(T item);

    /// <summary>
    /// Signals successful completion of the stream.
    /// Any subsequent calls to <see cref="EmitAsync"/>, <see cref="Complete"/>, or <see cref="Fail"/> will be ignored.
    /// </summary>
    void Complete();

    /// <summary>
    /// Signals a failure to the stream.
    /// Any subsequent calls to <see cref="EmitAsync"/>, <see cref="Complete"/>, or <see cref="Fail"/> will be ignored.
    /// </summary>
    /// <param name="error">The exception that caused the failure.</param>
    void Fail(Exception error);
}

/// <summary>
/// Represents an asynchronous destination for stream items.
/// </summary>
/// <typeparam name="T">The type of items written to the sink.</typeparam>
public interface IAsyncSink<in T>
{
    /// <summary>
    /// Writes a single item to the sink.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the write has finished.</returns>
    ValueTask WriteAsync(T item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Completes the sink, optionally passing through an upstream failure.
    /// </summary>
    /// <param name="error">The upstream failure, or <see langword="null"/> on successful completion.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when sink completion has finished.</returns>
    ValueTask CompleteAsync(Exception? error = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents a stream that can be connected to a shared source.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public interface IConnectableStream<T> : IStream<T>
{
    /// <summary>
    /// Connects to the shared source.
    /// </summary>
    /// <returns>A disposable to disconnect from the source.</returns>
    IDisposable Connect();

    /// <summary>
    /// Returns a stream that stays connected as long as there is at least one subscriber.
    /// </summary>
    IStream<T> RefCount();

    /// <summary>
    /// Returns a task that completes when all RefCount subscribers have disconnected.
    /// This is useful for testing RefCount behavior without relying on timing assumptions.
    /// </summary>
    Task WhenRefCountDisconnectedAsync();
}
