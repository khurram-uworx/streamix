using Streamix.Abstractions;

namespace Streamix;

/// <summary>
/// Default implementation of <see cref="ISingle{T}"/>.
/// This class is sealed to provide a stable API surface and ensure consistent behavior across operator chains.
/// </summary>
/// <typeparam name="T">The type of item in the stream.</typeparam>
public sealed class Single<T> : ISingle<T>
{
    private readonly IAsyncEnumerable<T> _source;

    internal Single(IAsyncEnumerable<T> source)
    {
        _source = source;
    }

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return _source.GetAsyncEnumerator(cancellationToken);
    }

    /// <inheritdoc />
    public ISingle<TResult> Map<TResult>(Func<T, TResult> selector) => throw new NotImplementedException();

    /// <inheritdoc />
    public ISingle<TResult> Select<TResult>(Func<T, TResult> selector) => Map(selector);

    /// <inheritdoc />
    public ISingle<TResult> FlatMap<TResult>(Func<T, ISingle<TResult>> selector) => throw new NotImplementedException();

    /// <inheritdoc />
    public IStream<TResult> FlatMapMany<TResult>(Func<T, IStream<TResult>> selector) => throw new NotImplementedException();

    /// <inheritdoc />
    public ISingle<T> OnErrorResume(Func<Exception, ISingle<T>> errorHandler) => throw new NotImplementedException();

    /// <inheritdoc />
    public ISingle<T> RunOn(TaskScheduler scheduler) => throw new NotImplementedException();

    /// <inheritdoc />
    public Task ForEachAsync(Action<T> action, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    /// <inheritdoc />
    public Task ForEachAsync(Func<T, Task> action, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    /// <inheritdoc />
    public Task<T> ToTask(CancellationToken cancellationToken = default) => throw new NotImplementedException();
}

/// <summary>
/// Provides static methods for creating single-item streams.
/// </summary>
public static class Single
{
    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    public static ISingle<T> From<T>(IAsyncEnumerable<T> source) => new Single<T>(source);

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a <see cref="Task{T}"/>.
    /// </summary>
    public static ISingle<T> From<T>(Task<T> task) => From(ToAsyncEnumerable(task));

    private static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(Task<T> task)
    {
        yield return await task;
    }
}
