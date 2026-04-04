using Streamix.Concurrency;
using Streamix.Implementations;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Provides static methods for creating streams.
/// </summary>
public static class Stream
{
    static class AsyncEnumerable
    {
        public static async IAsyncEnumerable<long> Interval(TimeSpan dueTime, TimeSpan period, IClock clock, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (dueTime < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(dueTime));
            if (period <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(period));

            if (dueTime > TimeSpan.Zero)
            {
                await clock.Delay(dueTime, cancellationToken);
            }

            long counter = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return counter++;
                await clock.Delay(period, cancellationToken);
            }
        }

        public static async IAsyncEnumerable<T> Defer<T>(Func<CancellationToken, IStream<T>> factory, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var source = factory(cancellationToken);
            await foreach (var item in source.WithCancellation(cancellationToken))
            {
                yield return item;
            }
        }

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

        public static async IAsyncEnumerable<int> Range(int start, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            for (int i = 0; i < count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return start + i;
            }
        }

        public static async IAsyncEnumerable<T> Generate<TState, T>(TState initialState, Func<TState, GenerationResult<TState, T>> generator, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var state = initialState;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var result = generator(state);
                if (result.IsComplete)
                {
                    yield break;
                }

                if (result.HasValue)
                {
                    yield return result.Value!;
                }

                state = result.NextState;
            }
        }

        public static async IAsyncEnumerable<T> Generate<TState, T>(TState initialState, Func<TState, CancellationToken, ValueTask<GenerationResult<TState, T>>> generator, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var state = initialState;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var result = await generator(state, cancellationToken);
                if (result.IsComplete)
                {
                    yield break;
                }

                if (result.HasValue)
                {
                    yield return result.Value!;
                }

                state = result.NextState;
            }
        }
    }

    internal static IStream<T> From<T>(IAsyncEnumerable<T> source, IClock clock) => new Stream<T>(source, clock);

    /// <summary>
    /// Creates a stream from an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="source">The source asynchronous enumerable.</param>
    /// <returns>A stream wrapping the source.</returns>
    public static IStream<T> From<T>(IAsyncEnumerable<T> source)
    {
        if (source is IStream<T> stream) return stream;
        return new Stream<T>(source);
    }

    /// <summary>
    /// Creates a stream from a <see cref="ISingle{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="source">The source single-item stream.</param>
    /// <returns>A stream wrapping the source.</returns>
    public static IStream<T> From<T>(ISingle<T> source) => new Stream<T>(source);

    /// <summary>
    /// Creates a stream from a single value.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>A stream that emits the specified value and then completes.</returns>
    public static IStream<T> From<T>(T value) => From(AsyncEnumerable.Just(value));

    /// <summary>
    /// Creates a stream from a single value. Alias for <see cref="From{T}(T)"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>A stream that emits the specified value and then completes.</returns>
    public static IStream<T> Just<T>(T value) => From(value);

    /// <summary>
    /// Creates a stream from a <see cref="Task{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="task">The task to wrap.</param>
    /// <returns>A stream that emits the result of the task and then completes.</returns>
    public static IStream<T> From<T>(Task<T> task) => From(Single.From(task));

    /// <summary>
    /// Creates a stream from a function that returns a <see cref="Task{T}"/>.
    /// The function is invoked lazily when the stream is subscribed to.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="taskFactory">The function to invoke.</param>
    /// <returns>A stream that emits the result of the task and then completes.</returns>
    public static IStream<T> From<T>(Func<Task<T>> taskFactory) => From(Single.From(taskFactory));

    /// <summary>
    /// Creates a stream from a function that returns a <see cref="Task{T}"/> and accepts a <see cref="CancellationToken"/>.
    /// The function is invoked lazily when the stream is subscribed to.
    /// The provided <see cref="CancellationToken"/> will be cancelled if the subscriber cancels their subscription.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="taskFactory">The function to invoke.</param>
    /// <returns>A stream that emits the result of the task and then completes.</returns>
    public static IStream<T> From<T>(Func<CancellationToken, Task<T>> taskFactory) => From(Single.From(taskFactory));

    /// <summary>
    /// Creates an empty stream.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <returns>An empty stream.</returns>
    public static IStream<T> Empty<T>() => From(AsyncEnumerable.Empty<T>());

    /// <summary>
    /// Creates a stream that fails with the specified exception.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="exception">The exception to fail with.</param>
    /// <returns>A failing stream.</returns>
    public static IStream<T> Error<T>(Exception exception) => From(AsyncEnumerable.Error<T>(exception));

    /// <summary>
    /// Creates a stream that emits a range of sequential integers.
    /// </summary>
    /// <param name="start">The value of the first integer in the sequence.</param>
    /// <param name="count">The number of sequential integers to generate.</param>
    /// <returns>A stream that contains a range of sequential integers.</returns>
    public static IStream<int> Range(int start, int count) => From(AsyncEnumerable.Range(start, count));

    /// <summary>
    /// Returns a stream that emits a sequential long integer every specified time interval.
    /// </summary>
    /// <param name="period">The time interval between emissions.</param>
    /// <returns>A stream that emits sequential long integers.</returns>
    public static IStream<long> Interval(TimeSpan period) => Interval(period, period);

    /// <summary>
    /// Returns a stream that emits a sequential long integer after an initial delay, and then every specified time interval.
    /// </summary>
    /// <param name="dueTime">The initial delay before the first emission.</param>
    /// <param name="period">The time interval between subsequent emissions.</param>
    /// <returns>A stream that emits sequential long integers.</returns>
    public static IStream<long> Interval(TimeSpan dueTime, TimeSpan period) => From(AsyncEnumerable.Interval(dueTime, period, SystemClock.Instance));

    internal static IStream<long> Interval(TimeSpan dueTime, TimeSpan period, IClock clock) => From(AsyncEnumerable.Interval(dueTime, period, clock), clock);

    /// <summary>
    /// Creates a stream that reads all items from the specified <see cref="ChannelReader{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="reader">The channel reader to read from.</param>
    /// <returns>A stream that emits all items from the channel reader.</returns>
    public static IStream<T> FromChannel<T>(ChannelReader<T> reader) => From(reader.ReadAllAsync());

    /// <summary>
    /// Creates a stream that reads all items from the specified <see cref="Channel{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="channel">The channel to read from.</param>
    /// <returns>A stream that emits all items from the channel.</returns>
    public static IStream<T> FromChannel<T>(Channel<T> channel) => FromChannel(channel.Reader);

    /// <summary>
    /// Merges multiple streams into one by combining their elements.
    /// </summary>
    /// <typeparam name="T">The type of items in the streams.</typeparam>
    /// <param name="streams">The streams to merge.</param>
    /// <returns>A merged stream.</returns>
    public static IStream<T> Merge<T>(params IStream<T>[] streams) => Stream<T>.Merge(streams);

    /// <summary>
    /// Combines elements from multiple streams using a specified function.
    /// </summary>
    /// <typeparam name="T1">The type of items in the first stream.</typeparam>
    /// <typeparam name="T2">The type of items in the second stream.</typeparam>
    /// <typeparam name="TResult">The type of items in the resulting stream.</typeparam>
    /// <param name="first">The first stream.</param>
    /// <param name="second">The second stream.</param>
    /// <param name="resultSelector">The result selector function.</param>
    /// <returns>A zipped stream.</returns>
    public static IStream<TResult> Zip<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector) => Stream<TResult>.Zip(first, second, resultSelector);

    /// <summary>
    /// Returns a stream that is created by a factory function for each subscriber.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="factory">The factory function to create the stream.</param>
    /// <returns>A deferred stream.</returns>
    public static IStream<T> Defer<T>(Func<IStream<T>> factory) => From(AsyncEnumerable.Defer<T>(_ => factory()));

    /// <summary>
    /// Returns a stream that is created by a factory function for each subscriber, with access to the subscriber's cancellation token.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="factory">The factory function to create the stream.</param>
    /// <returns>A deferred stream.</returns>
    public static IStream<T> Defer<T>(Func<CancellationToken, IStream<T>> factory) => From(AsyncEnumerable.Defer<T>(factory));

    /// <summary>
    /// Creates a stream by providing an emitter that can be used to push items, complete, or signal errors.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="producer">A function that uses the emitter to produce items.</param>
    /// <returns>A stream created from the emitter.</returns>
    public static IStream<T> Create<T>(Func<IStreamEmitter<T>, Task> producer) => Stream<T>.Create(producer);

    /// <summary>
    /// Creates a stream by statefully generating elements.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="initialState">The initial state.</param>
    /// <param name="generator">The state transition and emission function.</param>
    /// <returns>A statefully generated stream.</returns>
    public static IStream<T> Generate<TState, T>(TState initialState, Func<TState, GenerationResult<TState, T>> generator)
        => From(AsyncEnumerable.Generate(initialState, generator));

    /// <summary>
    /// Creates a stream by statefully generating elements asynchronously.
    /// </summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="initialState">The initial state.</param>
    /// <param name="generator">The asynchronous state transition and emission function.</param>
    /// <returns>A statefully generated stream.</returns>
    public static IStream<T> Generate<TState, T>(TState initialState, Func<TState, CancellationToken, ValueTask<GenerationResult<TState, T>>> generator)
        => From(AsyncEnumerable.Generate(initialState, generator));
}
