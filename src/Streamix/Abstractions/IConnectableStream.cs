namespace Streamix.Abstractions;

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
}
