namespace Streamix.Abstractions;

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
