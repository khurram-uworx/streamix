namespace Streamix.Concurrency;

/// <summary>
/// A default implementation of <see cref="IClock"/> using system time.
/// </summary>
public sealed class SystemClock : IClock
{
    static readonly Lazy<SystemClock> instance = new(() => new SystemClock());

    /// <summary>
    /// Gets the singleton instance of <see cref="SystemClock"/>.
    /// </summary>
    public static SystemClock Instance => instance.Value;

    SystemClock() { }

    /// <inheritdoc />
    public DateTimeOffset Now => DateTimeOffset.UtcNow;

    /// <inheritdoc />
    public Task Delay(TimeSpan delay, CancellationToken cancellationToken = default)
    {
        return Task.Delay(delay, cancellationToken);
    }
}
