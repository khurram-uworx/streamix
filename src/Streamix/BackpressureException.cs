namespace Streamix;

/// <summary>
/// Exception thrown when a backpressure overflow occurs in a stream.
/// </summary>
public sealed class BackpressureException : InvalidOperationException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="BackpressureException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public BackpressureException(string message) : base(message) { }
}

/// <summary>
/// Internal representation of backpressure strategies.
/// </summary>
internal enum BackpressureStrategy
{
    /// <summary>
    /// Buffers items up to a fixed capacity and throws on overflow.
    /// </summary>
    Buffer,

    /// <summary>
    /// Drops items when downstream cannot keep pace.
    /// </summary>
    Drop,

    /// <summary>
    /// Keeps only the latest item when downstream is slow.
    /// </summary>
    Latest,

    /// <summary>
    /// Signals immediate failure when downstream cannot keep pace.
    /// </summary>
    Error
}
