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
