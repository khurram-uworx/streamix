using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Streamix.Implementations;

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
        var scope = new StreamScope(cancellationToken);

        try
        {
            scope.Run(async ct =>
            {
                try
                {
                    await foreach (var item in source.WithCancellation(ct))
                    {
                        await WriteAsync(channel.Writer, item, mode, ct);
                    }

                    channel.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    throw;
                }
            });

            await foreach (var item in ScopeHelper.ReadAllSupervisedAsync(channel.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }
        finally
        {
            await ScopeHelper.FinalizeScopeAsync(scope);
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

        var scope = new StreamScope(cancellationToken);

        scope.Run(async ct =>
        {
            long index = 0;
            try
            {
                await foreach (var item in source.WithCancellation(ct))
                {
                    await WriteAsync(input.Writer, new IndexedItem<T>(index++, item), mode, ct);
                }
                input.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                input.Writer.TryComplete(ex);
                throw;
            }
        });

        var workerTasks = Enumerable.Range(0, degreeOfParallelism)
            .Select(_ => scope.RunAsync(async ct =>
            {
                await foreach (var entry in input.Reader.ReadAllAsync(ct))
                {
                    await output.Writer.WriteAsync(entry, ct);
                }
            }))
            .ToArray();

        scope.Run(async ct =>
        {
            try
            {
                await Task.WhenAll(workerTasks);
                output.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                output.Writer.TryComplete(ex);
                throw;
            }
        });

        var pending = new SortedDictionary<long, T>();
        long nextIndex = 0;

        try
        {
            await foreach (var entry in ScopeHelper.ReadAllSupervisedAsync(output.Reader, scope, cancellationToken).ConfigureAwait(false))
            {
                pending[entry.Index] = entry.Item;

                while (pending.Remove(nextIndex, out var item))
                {
                    yield return item;
                    nextIndex++;
                }
            }

            while (pending.Remove(nextIndex, out var item))
            {
                yield return item;
                nextIndex++;
            }
        }
        finally
        {
            await ScopeHelper.FinalizeScopeAsync(scope);
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
