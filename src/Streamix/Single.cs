using Streamix.Implementations;
using System.Runtime.CompilerServices;

namespace Streamix;

/// <summary>
/// Provides static methods for creating single-item streams.
/// </summary>
public static class Single
{
    static class AsyncEnumerableInternal
    {
        public static async IAsyncEnumerable<T> Empty<T>([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield break;
        }

        public static async IAsyncEnumerable<T> Just<T>(T value, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield return value;
        }

        public static async IAsyncEnumerable<T> Error<T>(Exception exception, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            throw exception;
            yield break;
        }
    }

    static async IAsyncEnumerable<TValue> toAsyncEnumerableFromTask<TValue>(Task<TValue> task, [EnumeratorCancellation] CancellationToken ct = default)
    {
        yield return await task.WaitAsync(ct);
    }

    static async IAsyncEnumerable<TValue> toAsyncEnumerableFromTaskFunc<TValue>(Func<Task<TValue>> taskFactory, [EnumeratorCancellation] CancellationToken ct = default)
    {
        yield return await taskFactory().WaitAsync(ct);
    }

    static async IAsyncEnumerable<TValue> toAsyncEnumerableFromTaskFuncWithCt<TValue>(Func<CancellationToken, Task<TValue>> taskFactory, [EnumeratorCancellation] CancellationToken ct = default)
    {
        yield return await taskFactory(ct).WaitAsync(ct);
    }

    static async IAsyncEnumerable<TValue> toAsyncEnumerableFromValueTaskFunc<TValue>(Func<ValueTask<TValue>> taskFactory, [EnumeratorCancellation] CancellationToken ct = default)
    {
        yield return await taskFactory().AsTask().WaitAsync(ct);
    }

    static async IAsyncEnumerable<TValue> toAsyncEnumerableFromValueTaskFuncWithCt<TValue>(Func<CancellationToken, ValueTask<TValue>> taskFactory, [EnumeratorCancellation] CancellationToken ct = default)
    {
        yield return await taskFactory(ct).AsTask().WaitAsync(ct);
    }

    static async IAsyncEnumerable<TValue> defer<TValue>(Func<ISingle<TValue>> factory, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in factory().WithCancellation(ct))
            yield return item;
    }

    static async IAsyncEnumerable<TValue> deferWithCt<TValue>(Func<CancellationToken, ISingle<TValue>> factory, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in factory(ct).WithCancellation(ct))
            yield return item;
    }

    internal static async IAsyncEnumerable<T> EnforceAtMostOne<T>(IAsyncEnumerable<T> source, [EnumeratorCancellation] CancellationToken ct = default)
    {
        bool hasValue = false;
        await foreach (var item in source.WithCancellation(ct))
        {
            if (hasValue)
                throw new InvalidOperationException("Sequence contains more than one element.");

            yield return item;
            hasValue = true;
        }
    }

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from an <see cref="IAsyncEnumerable{T}"/>.
    /// Cardinality is strictly enforced to 0..1 items.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="source">The source asynchronous enumerable.</param>
    /// <returns>A single-item stream wrapping the source.</returns>
    /// <exception cref="InvalidOperationException">The source emits more than one element during enumeration.</exception>
    public static ISingle<T> From<T>(IAsyncEnumerable<T> source) => new Single<T>(source);

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from an <see cref="IAsyncEnumerable{T}"/> with a specific clock.
    /// </summary>
    internal static ISingle<T> From<T>(IAsyncEnumerable<T> source, IClock clock) => new Single<T>(source, clock);

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a single value.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>A single-item stream that emits the specified value and then completes.</returns>
    public static ISingle<T> From<T>(T value) => From(AsyncEnumerableInternal.Just(value));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a single value. Alias for <see cref="From{T}(T)"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>A single-item stream that emits the specified value and then completes.</returns>
    public static ISingle<T> Just<T>(T value) => From(value);

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a <see cref="Task{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="task">The task to wrap.</param>
    /// <returns>A single-item stream that emits the result of the task and then completes.</returns>
    public static ISingle<T> From<T>(Task<T> task) => From(toAsyncEnumerableFromTask(task));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a function that returns a <see cref="Task{T}"/>.
    /// The function is invoked lazily when the stream is subscribed to.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="taskFactory">The function to invoke.</param>
    /// <returns>A single-item stream that emits the result of the task and then completes.</returns>
    public static ISingle<T> From<T>(Func<Task<T>> taskFactory) => From(toAsyncEnumerableFromTaskFunc(taskFactory));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a function that returns a <see cref="Task{T}"/> and accepts a <see cref="CancellationToken"/>.
    /// The function is invoked lazily when the stream is subscribed to.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="taskFactory">The function to invoke.</param>
    /// <returns>A single-item stream that emits the result of the task and then completes.</returns>
    public static ISingle<T> From<T>(Func<CancellationToken, Task<T>> taskFactory) => From(toAsyncEnumerableFromTaskFuncWithCt(taskFactory));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a <see cref="ValueTask{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="task">The task to wrap.</param>
    /// <returns>A single-item stream that emits the result of the task and then completes.</returns>
    public static ISingle<T> From<T>(ValueTask<T> task) => From(task.AsTask());

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a function that returns a <see cref="ValueTask{T}"/>.
    /// The function is invoked lazily when the stream is subscribed to.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="taskFactory">The function to invoke.</param>
    /// <returns>A single-item stream that emits the result of the task and then completes.</returns>
    public static ISingle<T> From<T>(Func<ValueTask<T>> taskFactory) => From(toAsyncEnumerableFromValueTaskFunc(taskFactory));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> from a function that returns a <see cref="ValueTask{T}"/> and accepts a <see cref="CancellationToken"/>.
    /// The function is invoked lazily when the stream is subscribed to.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="taskFactory">The function to invoke.</param>
    /// <returns>A single-item stream that emits the result of the task and then completes.</returns>
    public static ISingle<T> From<T>(Func<CancellationToken, ValueTask<T>> taskFactory) => From(toAsyncEnumerableFromValueTaskFuncWithCt(taskFactory));

    /// <summary>
    /// Creates an empty <see cref="ISingle{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <returns>An empty single-item stream.</returns>
    public static ISingle<T> Empty<T>() => From(AsyncEnumerableInternal.Empty<T>());

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> that fails with the specified exception.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="exception">The exception to fail with.</param>
    /// <returns>A failing single-item stream.</returns>
    public static ISingle<T> Error<T>(Exception exception) => From(AsyncEnumerableInternal.Error<T>(exception));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> by invoking a factory function for each subscription.
    /// This is used to defer the creation of the single-item stream until it is subscribed to.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="factory">The factory function to invoke.</param>
    /// <returns>A single-item stream that is created lazily for each subscriber.</returns>
    public static ISingle<T> Defer<T>(Func<ISingle<T>> factory) => From(defer(factory));

    /// <summary>
    /// Creates a <see cref="ISingle{T}"/> by invoking a factory function that accepts a <see cref="CancellationToken"/> for each subscription.
    /// This is used to defer the creation of the single-item stream until it is subscribed to.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="factory">The factory function to invoke.</param>
    /// <returns>A single-item stream that is created lazily for each subscriber.</returns>
    public static ISingle<T> Defer<T>(Func<CancellationToken, ISingle<T>> factory) => From(deferWithCt(factory));
}
