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
