namespace Streamix.Abstractions;

/// <summary>
/// Represents a stream of 0..N values.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public interface IStream<out T> : IAsyncEnumerable<T>
{
}
