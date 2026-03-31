using Streamix.Abstractions;

namespace Streamix;

/// <summary>
/// Default implementation of <see cref="IStream{T}"/>.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public class Stream<T> : IStream<T>
{
    private readonly IAsyncEnumerable<T> _source;

    internal Stream(IAsyncEnumerable<T> source)
    {
        _source = source;
    }

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return _source.GetAsyncEnumerator(cancellationToken);
    }
}

/// <summary>
/// Provides static methods for creating streams.
/// </summary>
public static class Stream
{
    /// <summary>
    /// Creates a stream from an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    public static IStream<T> From<T>(IAsyncEnumerable<T> source) => new Stream<T>(source);

    /// <summary>
    /// Creates an empty stream.
    /// </summary>
    public static IStream<T> Empty<T>() => From(AsyncEnumerable.Empty<T>());
}

internal static class AsyncEnumerable
{
    public static async IAsyncEnumerable<T> Empty<T>()
    {
        yield break;
    }
}
