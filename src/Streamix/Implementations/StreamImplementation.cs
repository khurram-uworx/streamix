using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix.Implementations;

/// <summary>
/// Default implementation of <see cref="IStream{T}"/>.
/// This class is sealed to provide a stable API surface and ensure consistent behavior across operator chains.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
class StreamImplementation<T> : IStream<T>
{
    class Emitter : IStreamEmitter<T>, IDisposable
    {
        readonly ChannelWriter<T> writer;
        readonly CancellationTokenSource cts;
        int terminalState = 0; // 0: active, 1: terminal

        public Emitter(ChannelWriter<T> writer, CancellationToken externalToken)
        {
            this.writer = writer;
            this.cts = CancellationTokenSource.CreateLinkedTokenSource(externalToken);
        }

        public CancellationToken CancellationToken => cts.Token;

        public async ValueTask EmitAsync(T item)
        {
            if (Volatile.Read(ref terminalState) != 0 || cts.IsCancellationRequested)
                throw new OperationCanceledException(cts.Token);

            try
            {
                await writer.WriteAsync(item, cts.Token);
            }
            catch (ChannelClosedException)
            {
                throw new OperationCanceledException(cts.Token);
            }
            catch (OperationCanceledException ex)
            {
                if (cts.IsCancellationRequested)
                    throw;

                throw new OperationCanceledException(ex.Message, ex, cts.Token);
            }
        }

        public void Complete()
        {
            if (Interlocked.CompareExchange(ref terminalState, 1, 0) == 0)
            {
                writer.TryComplete();
                cts.Cancel();
            }
        }

        public void Fail(Exception error)
        {
            if (Interlocked.CompareExchange(ref terminalState, 1, 0) == 0)
            {
                writer.TryComplete(error);
                cts.Cancel();
            }
        }

        internal void Cancel()
        {
            if (Interlocked.Exchange(ref terminalState, 1) == 0)
            {
                writer.TryComplete();
                cts.Cancel();
            }
        }

        public void Dispose()
        {
            cts.Dispose();
        }
    }

    static async IAsyncEnumerable<T> merge(IStream<T>[] streams, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (streams == null || streams.Length == 0) yield break;

        var channel = Channel.CreateUnbounded<T>();
        var tasks = new List<Task>();

        foreach (var stream in streams)
        {
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in stream.WithCancellation(cancellationToken))
                        await channel.Writer.WriteAsync(item, cancellationToken);
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    throw;
                }
            }, cancellationToken));
        }

        _ = Task.WhenAll(tasks).ContinueWith(t =>
        {
            if (t.IsFaulted)
                channel.Writer.TryComplete(t.Exception?.InnerException);
            else
                channel.Writer.TryComplete();
        }, cancellationToken);

        while (await channel.Reader.WaitToReadAsync(cancellationToken))
            while (channel.Reader.TryRead(out var item))
                yield return item;

        // Ensure any exception that completed the channel is rethrown
        await channel.Reader.Completion;
    }

    static async IAsyncEnumerable<TResult> zip<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var e1 = first.GetAsyncEnumerator(cancellationToken);
        await using var e2 = second.GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            var t1 = e1.MoveNextAsync();
            var t2 = e2.MoveNextAsync();

            var h1 = await t1;
            var h2 = await t2;

            if (!h1 || !h2)
                yield break;

            yield return resultSelector(e1.Current, e2.Current);
        }
    }

    static async IAsyncEnumerable<T> create(Func<IStreamEmitter<T>, Task> producer, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(1);
        var emitter = new Emitter(channel.Writer, cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            try
            {
                await producer(emitter);
                emitter.Complete();
            }
            catch (OperationCanceledException) when (emitter.CancellationToken.IsCancellationRequested)
            {
                emitter.Complete();
            }
            catch (Exception ex)
            {
                if (emitter.CancellationToken.IsCancellationRequested)
                    return;

                emitter.Fail(ex);
            }
        }, cancellationToken);

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
            {
                while (channel.Reader.TryRead(out var item))
                {
                    yield return item;
                }
            }

            await channel.Reader.Completion;
        }
        finally
        {
            emitter.Cancel();

            try { await producerTask; } catch { }

            emitter.Dispose();
        }
    }

    static async IAsyncEnumerable<T> create(Func<IStreamEmitter<T>, CancellationToken, ValueTask> producer, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(1);
        var emitter = new Emitter(channel.Writer, cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            try
            {
                await producer(emitter, emitter.CancellationToken);
                emitter.Complete();
            }
            catch (OperationCanceledException) when (emitter.CancellationToken.IsCancellationRequested)
            {
                emitter.Complete();
            }
            catch (Exception ex)
            {
                if (emitter.CancellationToken.IsCancellationRequested)
                    return;

                emitter.Fail(ex);
            }
        }, cancellationToken);

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
                while (channel.Reader.TryRead(out var item))
                    yield return item;

            await channel.Reader.Completion;
        }
        finally
        {
            emitter.Cancel();

            try { await producerTask; } catch { }

            emitter.Dispose();
        }
    }

    /// <summary>
    /// Merges multiple streams into one by combining their elements.
    /// </summary>
    /// <param name="streams">The streams to merge.</param>
    /// <returns>A merged stream.</returns>
    public static IStream<T> Merge(params IStream<T>[] streams)
    {
        return Stream.From(merge(streams));
    }

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
    public static IStream<TResult> Zip<T1, T2, TResult>(IStream<T1> first, IStream<T2> second, Func<T1, T2, TResult> resultSelector)
    {
        return Stream.From(zip(first, second, resultSelector));
    }

    /// <summary>
    /// Creates a stream by providing an emitter that can be used to push items, complete, or signal errors.
    /// </summary>
    /// <param name="producer">A function that uses the emitter to produce items.</param>
    /// <returns>A stream created from the emitter.</returns>
    public static IStream<T> Create(Func<IStreamEmitter<T>, Task> producer)
    {
        return Stream.From(create(producer));
    }

    /// <summary>
    /// Creates a stream by providing an emitter that can be used to push items, complete, or signal errors.
    /// </summary>
    /// <param name="producer">A function that uses the emitter to produce items.</param>
    /// <returns>A stream created from the emitter.</returns>
    public static IStream<T> Create(Func<IStreamEmitter<T>, CancellationToken, ValueTask> producer)
    {
        return Stream.From(create(producer));
    }

    readonly IAsyncEnumerable<T> source;
    readonly IClock clock;
    readonly string? name;

    internal StreamImplementation(IAsyncEnumerable<T> source, IClock? clock = null, string? name = null)
    {
        this.source = source;
        this.clock = clock ?? SystemClock.Instance;
        this.name = name;
    }

    /// <inheritdoc />
    public IClock Clock => clock;

    /// <inheritdoc />
    public string? Name => name;

    async IAsyncEnumerable<IStream<T>> window(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var items = new List<T>(count);
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            items.Add(item);
            if (items.Count == count)
            {
                yield return Stream.From(toAsyncEnumerable(items));
                items = new List<T>(count);
            }
        }

        if (items.Count > 0)
        {
            yield return Stream.From(toAsyncEnumerable(items));
        }
    }

    async IAsyncEnumerable<T> toAsyncEnumerable(IEnumerable<T> items)
    {
        foreach (var item in items)
            yield return item;

        await Task.Yield();
    }

    async IAsyncEnumerable<T> runOn(TaskScheduler scheduler, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var enumerator = await Task.Factory.StartNew(() => source.GetAsyncEnumerator(cancellationToken), cancellationToken, TaskCreationOptions.None, scheduler);
        try
        {
            while (true)
            {
                var hasNext = await Task.Factory.StartNew(() => enumerator.MoveNextAsync().AsTask(), cancellationToken, TaskCreationOptions.None, scheduler).Unwrap();
                if (hasNext)
                    yield return enumerator.Current;
                else
                    break;
            }
        }
        finally
        {
            await Task.Factory.StartNew(() => enumerator.DisposeAsync().AsTask(), cancellationToken, TaskCreationOptions.None, scheduler).Unwrap();
        }
    }

    /// <inheritdoc />
    public IStream<T> Named(string name)
    {
        return new StreamImplementation<T>(source, clock, name);
    }

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return source.GetAsyncEnumerator(cancellationToken);
    }

    /// <inheritdoc />
    public IStream<T> MergeWith(params IStream<T>[] others)
    {
        var all = new IStream<T>[others.Length + 1];
        all[0] = this;
        others.CopyTo(all, 1);
        return Merge(all).Named(name ?? "");
    }

    /// <inheritdoc />
    public IStream<TResult> ZipWith<TOther, TResult>(IStream<TOther> other, Func<T, TOther, TResult> resultSelector)
    {
        return Zip(this, other, resultSelector).Named(name ?? "");
    }

    /// <inheritdoc />
    public IStream<IStream<T>> Window(int count)
    {
        if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than 0.");
        return Stream.From(window(count), clock, name);
    }

    /// <inheritdoc />
    public IStream<T> RunOn(TaskScheduler scheduler)
    {
        return Stream.From(runOn(scheduler), clock, name);
    }
}
