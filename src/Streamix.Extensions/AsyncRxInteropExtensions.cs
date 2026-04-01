using Streamix.Abstractions;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;

namespace Streamix;

/// <summary>
/// Provides extension methods for interoperability between Streamix and AsyncRx.NET.
/// </summary>
public static class AsyncRxInteropExtensions
{
    static async IAsyncEnumerable<T> toAsyncEnumerable<T>(IAsyncObservable<T> source, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = System.Threading.Channels.Channel.CreateUnbounded<T>();

        var subscription = await source.SubscribeAsync(
            async x => await channel.Writer.WriteAsync(x, cancellationToken),
            async ex => channel.Writer.TryComplete(ex),
            async () => channel.Writer.TryComplete()
        );

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
            {
                while (channel.Reader.TryRead(out var item))
                {
                    yield return item;
                }
            }

            // Ensure any exception that completed the channel is rethrown
            await channel.Reader.Completion;
        }
        finally
        {
            await subscription.DisposeAsync();
        }
    }

    /// <summary>
    /// Converts an <see cref="IStream{T}"/> to an <see cref="IAsyncObservable{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of elements in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <returns>An asynchronous observable that emits elements from the stream.</returns>
    public static IAsyncObservable<T> ToAsyncObservable<T>(this IStream<T> stream)
    {
        return AsyncObservable.Create<T>(async observer =>
        {
            var cts = new CancellationTokenSource();

            // We use a separate task to run the enumeration because AsyncObservable.Create
            // expects us to return an IAsyncDisposable immediately.
            var runTask = Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in stream.WithCancellation(cts.Token))
                    {
                        await observer.OnNextAsync(item);
                    }
                    await observer.OnCompletedAsync();
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    // Normal cancellation
                }
                catch (Exception ex)
                {
                    await observer.OnErrorAsync(ex);
                }
            });

            return AsyncDisposable.Create(async () =>
            {
                await cts.CancelAsync();
                try
                {
                    await runTask;
                }
                catch
                {
                    // Ignore errors during cleanup
                }
                cts.Dispose();
            });
        });
    }

    /// <summary>
    /// Converts an <see cref="ISingle{T}"/> to an <see cref="IAsyncObservable{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of item in the stream.</typeparam>
    /// <param name="single">The source single-item stream.</param>
    /// <returns>An asynchronous observable that emits the element from the single-item stream.</returns>
    public static IAsyncObservable<T> ToAsyncObservable<T>(this ISingle<T> single)
    {
        return AsyncObservable.Create<T>(async observer =>
        {
            var cts = new CancellationTokenSource();

            var runTask = Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in single.WithCancellation(cts.Token))
                    {
                        await observer.OnNextAsync(item);
                        break; // Single should only emit one item
                    }
                    await observer.OnCompletedAsync();
                }
                catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
                {
                    // Normal cancellation
                }
                catch (Exception ex)
                {
                    await observer.OnErrorAsync(ex);
                }
            });

            return AsyncDisposable.Create(async () =>
            {
                await cts.CancelAsync();
                try
                {
                    await runTask;
                }
                catch
                {
                    // Ignore errors during cleanup
                }
                cts.Dispose();
            });
        });
    }

    /// <summary>
    /// Converts an <see cref="IAsyncObservable{T}"/> to an <see cref="IStream{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable.</typeparam>
    /// <param name="source">The source asynchronous observable.</param>
    /// <returns>A stream that emits elements from the observable.</returns>
    public static IStream<T> ToStream<T>(this IAsyncObservable<T> source)
    {
        return Stream.From(toAsyncEnumerable(source));
    }

    /// <summary>
    /// Converts an <see cref="IAsyncObservable{T}"/> to an <see cref="ISingle{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of elements in the observable.</typeparam>
    /// <param name="source">The source asynchronous observable.</param>
    /// <returns>A single-item stream that emits the first element from the observable.</returns>
    public static ISingle<T> ToSingle<T>(this IAsyncObservable<T> source)
    {
        return Single.From(toAsyncEnumerable(source));
    }
}
