using Streamix.Abstractions;

namespace Streamix.Concurrency;

/// <summary>
/// A default implementation of <see cref="IClock"/> using system time.
/// </summary>
public sealed class SystemClock : IClock
{
    private static readonly Lazy<SystemClock> _instance = new(() => new SystemClock());

    /// <summary>
    /// Gets the singleton instance of <see cref="SystemClock"/>.
    /// </summary>
    public static SystemClock Instance => _instance.Value;

    private SystemClock() { }

    /// <inheritdoc />
    public DateTimeOffset Now => DateTimeOffset.UtcNow;

    /// <inheritdoc />
    public Task Delay(TimeSpan delay, CancellationToken cancellationToken = default)
    {
        return Task.Delay(delay, cancellationToken);
    }
}
