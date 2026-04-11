using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Describes how a bounded channel-backed boundary behaves when downstream falls behind.
/// </summary>
public enum ChannelBackpressureMode
{
    /// <summary>
    /// Wait for capacity before accepting more items.
    /// </summary>
    Wait,

    /// <summary>
    /// Drop the newest buffered item to make room for the incoming item.
    /// </summary>
    DropNewest,

    /// <summary>
    /// Drop the oldest buffered item to make room for the incoming item.
    /// </summary>
    DropOldest,

    /// <summary>
    /// Keep only the latest pending item. This uses an effective capacity of 1.
    /// </summary>
    LatestOnly,

    /// <summary>
    /// Fail immediately when the boundary is full.
    /// </summary>
    Fail
}

static class ChannelExecution
{
    readonly record struct IndexedItem<T>(long Index, T Item);

    public static int GetEffectiveCapacity(int capacity, ChannelBackpressureMode mode)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than 0.");

        return mode == ChannelBackpressureMode.LatestOnly ? 1 : capacity;
    }

    public static Channel<T> CreateChannel<T>(int capacity, ChannelBackpressureMode mode, bool singleWriter = false, bool singleReader = false)
    {
        var effectiveCapacity = GetEffectiveCapacity(capacity, mode);
        return Channel.CreateBounded<T>(new BoundedChannelOptions(effectiveCapacity)
        {
            FullMode = GetFullMode(mode),
            SingleWriter = singleWriter,
            SingleReader = singleReader
        });
    }

    public static async ValueTask WriteAsync<T>(ChannelWriter<T> writer, T item, ChannelBackpressureMode mode, CancellationToken cancellationToken)
    {
        if (mode == ChannelBackpressureMode.Fail)
        {
            if (!writer.TryWrite(item))
                throw new BackpressureException("Channel boundary is full.");

            return;
        }

        if (mode == ChannelBackpressureMode.Wait)
        {
            await writer.WriteAsync(item, cancellationToken);
            return;
        }

        writer.TryWrite(item);
    }

    public static async IAsyncEnumerable<T> PipeThroughChannel<T>(
        IAsyncEnumerable<T> source,
        int capacity,
        ChannelBackpressureMode mode,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = CreateChannel<T>(capacity, mode, singleWriter: true, singleReader: true);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in source.WithCancellation(cts.Token))
                {
                    await WriteAsync(channel.Writer, item, mode, cts.Token);
                }

                channel.Writer.TryComplete();
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
            }
        }, cts.Token);

        try
        {
            while (await channel.Reader.WaitToReadAsync(cancellationToken))
            {
                while (channel.Reader.TryRead(out var item))
                {
                    yield return item;
                }
            }

            await producerTask;
            await channel.Reader.Completion;
        }
        finally
        {
            await cts.CancelAsync();
            try { await producerTask; } catch { }
        }
    }

    public static async IAsyncEnumerable<T> RunOnChannel<T>(
        IAsyncEnumerable<T> source,
        int capacity,
        int degreeOfParallelism,
        ChannelBackpressureMode mode,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (degreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(degreeOfParallelism), "Degree of parallelism must be greater than 0.");

        var input = CreateChannel<IndexedItem<T>>(capacity, mode, singleWriter: true, singleReader: degreeOfParallelism == 1);
        var output = Channel.CreateBounded<IndexedItem<T>>(new BoundedChannelOptions(Math.Max(GetEffectiveCapacity(capacity, mode), degreeOfParallelism))
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = degreeOfParallelism == 1
        });
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var producerTask = Task.Run(async () =>
        {
            long index = 0;

            try
            {
                await foreach (var item in source.WithCancellation(cts.Token))
                {
                    await WriteAsync(input.Writer, new IndexedItem<T>(index++, item), mode, cts.Token);
                }

                input.Writer.TryComplete();
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                input.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                input.Writer.TryComplete(ex);
            }
        }, cts.Token);

        var workerTasks = Enumerable.Range(0, degreeOfParallelism)
            .Select(_ => Task.Run(async () =>
            {
                await foreach (var entry in input.Reader.ReadAllAsync(cts.Token))
                {
                    await output.Writer.WriteAsync(entry, cts.Token);
                }
            }, cts.Token))
            .ToArray();

        var completionTask = Task.Run(async () =>
        {
            try
            {
                await producerTask;
                await Task.WhenAll(workerTasks);
                output.Writer.TryComplete();
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                output.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                output.Writer.TryComplete(ex);
            }
        }, cts.Token);

        var pending = new SortedDictionary<long, T>();
        long nextIndex = 0;

        try
        {
            while (await output.Reader.WaitToReadAsync(cancellationToken))
            {
                while (output.Reader.TryRead(out var entry))
                {
                    pending[entry.Index] = entry.Item;

                    while (pending.Remove(nextIndex, out var item))
                    {
                        yield return item;
                        nextIndex++;
                    }
                }
            }

            await completionTask;
            await output.Reader.Completion;

            while (pending.Remove(nextIndex, out var item))
            {
                yield return item;
                nextIndex++;
            }
        }
        finally
        {
            await cts.CancelAsync();
            try { await completionTask; } catch { }
            try { await producerTask; } catch { }
            try { await Task.WhenAll(workerTasks); } catch { }
        }
    }

    static BoundedChannelFullMode GetFullMode(ChannelBackpressureMode mode)
    {
        return mode switch
        {
            ChannelBackpressureMode.Wait => BoundedChannelFullMode.Wait,
            ChannelBackpressureMode.DropNewest => BoundedChannelFullMode.DropNewest,
            ChannelBackpressureMode.DropOldest => BoundedChannelFullMode.DropOldest,
            ChannelBackpressureMode.LatestOnly => BoundedChannelFullMode.DropOldest,
            ChannelBackpressureMode.Fail => BoundedChannelFullMode.Wait,
            _ => throw new ArgumentOutOfRangeException(nameof(mode))
        };
    }
}
