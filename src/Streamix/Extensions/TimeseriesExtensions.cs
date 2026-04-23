using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Provides time-series extension methods for <see cref="IStream{T}"/>.
/// </summary>
public static class TimeseriesExtensions
{
    /// <summary>
    /// Projects each element of a stream into a <see cref="Timestamped{T}"/> using the specified timestamp selector.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="source">The source stream.</param>
    /// <param name="timestampSelector">A function to extract the timestamp from each element.</param>
    /// <returns>A stream of timestamped items.</returns>
    public static IStream<Timestamped<T>> MapWithTimestamp<T>(this IStream<T> source, Func<T, DateTimeOffset> timestampSelector)
    {
        return source.Map(x => Timestamped.Create(x, timestampSelector(x)));
    }

    /// <summary>
    /// Groups elements of a stream into windows based on their timestamps.
    /// Supports both tumbling and sliding windows.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="source">The source stream of timestamped items.</param>
    /// <param name="duration">The duration of each window.</param>
    /// <param name="slide">The interval at which windows are started. If null, tumbling windows are used (slide = duration).</param>
    /// <param name="capacity">The capacity of the buffer for each window.</param>
    /// <param name="mode">The backpressure mode for each window.</param>
    /// <param name="outOfOrderness">The maximum out-of-orderness allowed before an event is considered late. If not null, enables watermark-aware behavior.</param>
    /// <returns>A stream of window streams.</returns>
    public static IStream<IStream<Timestamped<T>>> WindowByTime<T>(
        this IStream<Timestamped<T>> source,
        TimeSpan duration,
        TimeSpan? slide = null,
        int capacity = 16,
        ChannelBackpressureMode mode = ChannelBackpressureMode.Wait,
        TimeSpan? outOfOrderness = null)
    {
        if (duration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(duration));
        if (slide.HasValue && slide.Value <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(slide));
        if (outOfOrderness.HasValue && outOfOrderness.Value < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(outOfOrderness));

        return Stream.Create<IStream<Timestamped<T>>>(async emitter =>
        {
            var ct = emitter.CancellationToken;

            if (slide == null || slide == duration)
            {
                // Tumbling window logic
                if (outOfOrderness.HasValue)
                {
                    var activeWindows = new SortedDictionary<long, Channel<Timestamped<T>>>();
                    long? maxObservedTicks = null;

                    try
                    {
                        await foreach (var item in source.WithCancellation(ct))
                        {
                            var currentWatermark = maxObservedTicks.HasValue ? maxObservedTicks.Value - outOfOrderness.Value.Ticks : long.MinValue;

                            if (item.Timestamp.UtcTicks <= currentWatermark)
                                continue;

                            if (!maxObservedTicks.HasValue || item.Timestamp.UtcTicks > maxObservedTicks.Value)
                                maxObservedTicks = item.Timestamp.UtcTicks;

                            var newWatermark = maxObservedTicks.Value - outOfOrderness.Value.Ticks;

                            // Clean up
                            while (activeWindows.Count > 0)
                            {
                                var first = activeWindows.First();
                                if (first.Key + duration.Ticks <= newWatermark)
                                {
                                    first.Value.Writer.TryComplete();
                                    activeWindows.Remove(first.Key);
                                }
                                else break;
                            }

                            var startTicks = (item.Timestamp.UtcTicks / duration.Ticks) * duration.Ticks;
                            if (!activeWindows.TryGetValue(startTicks, out var channel))
                            {
                                channel = ChannelExecution.CreateChannel<Timestamped<T>>(capacity, mode, singleWriter: true);
                                activeWindows[startTicks] = channel;
                                await emitter.EmitAsync(Stream.From(innerCt => channel.Reader.ReadAllAsync(innerCt)));
                            }

                            await ChannelExecution.WriteAsync(channel.Writer, item, mode, ct);
                        }
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
                    catch (Exception ex)
                    {
                        foreach (var window in activeWindows.Values) window.Writer.TryComplete(ex);
                        throw;
                    }
                    finally
                    {
                        foreach (var window in activeWindows.Values) window.Writer.TryComplete();
                        activeWindows.Clear();
                    }
                }
                else
                {
                    Channel<Timestamped<T>>? currentWindowChannel = null;
                    DateTimeOffset? currentWindowEnd = null;

                    try
                    {
                        await foreach (var item in source.WithCancellation(ct))
                        {
                            if (currentWindowChannel == null || item.Timestamp >= currentWindowEnd)
                            {
                                currentWindowChannel?.Writer.TryComplete();

                                var startTicks = (item.Timestamp.UtcTicks / duration.Ticks) * duration.Ticks;
                                var start = new DateTimeOffset(startTicks, TimeSpan.Zero);
                                currentWindowEnd = start + duration;

                                var channel = ChannelExecution.CreateChannel<Timestamped<T>>(capacity, mode, singleWriter: true);
                                currentWindowChannel = channel;

                                await emitter.EmitAsync(Stream.From(innerCt => channel.Reader.ReadAllAsync(innerCt)));
                            }

                            await ChannelExecution.WriteAsync(currentWindowChannel.Writer, item, mode, ct);
                        }
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                    }
                    catch (Exception ex)
                    {
                        currentWindowChannel?.Writer.TryComplete(ex);
                        throw;
                    }
                    finally
                    {
                        currentWindowChannel?.Writer.TryComplete();
                    }
                }
            }
            else
            {
                // Sliding window logic
                var activeWindows = new SortedDictionary<long, Channel<Timestamped<T>>>();
                long? firstStartTicks = null;
                long? maxObservedTicks = null;

                try
                {
                    await foreach (var item in source.WithCancellation(ct))
                    {
                        var currentWatermark = outOfOrderness.HasValue
                            ? (maxObservedTicks.HasValue ? maxObservedTicks.Value - outOfOrderness.Value.Ticks : long.MinValue)
                            : long.MinValue;

                        if (outOfOrderness.HasValue && item.Timestamp.UtcTicks <= currentWatermark)
                            continue;

                        if (!maxObservedTicks.HasValue || item.Timestamp.UtcTicks > maxObservedTicks.Value)
                            maxObservedTicks = item.Timestamp.UtcTicks;

                        var effectiveWatermark = outOfOrderness.HasValue
                            ? maxObservedTicks.Value - outOfOrderness.Value.Ticks
                            : item.Timestamp.UtcTicks;

                        // Clean up expired windows
                        while (activeWindows.Count > 0)
                        {
                            var first = activeWindows.First();
                            if (first.Key + duration.Ticks <= effectiveWatermark)
                            {
                                first.Value.Writer.TryComplete();
                                activeWindows.Remove(first.Key);
                            }
                            else break;
                        }

                        var latestStartTicks = (item.Timestamp.UtcTicks / slide.Value.Ticks) * slide.Value.Ticks;

                        if (firstStartTicks == null)
                        {
                            var earliestPossible = item.Timestamp.UtcTicks - duration.Ticks + 1;
                            firstStartTicks = (earliestPossible / slide.Value.Ticks) * slide.Value.Ticks;
                            if (firstStartTicks + duration.Ticks <= item.Timestamp.UtcTicks)
                                firstStartTicks += slide.Value.Ticks;
                        }

                        var effectiveMinStart = outOfOrderness.HasValue
                             ? (item.Timestamp.UtcTicks - duration.Ticks + 1)
                             : Math.Max(item.Timestamp.UtcTicks - duration.Ticks + 1, firstStartTicks.Value);

                        // Identify windows to create (in chronological order)
                        var windowsToCreate = new List<long>();
                        for (var s = latestStartTicks; s >= effectiveMinStart; s -= slide.Value.Ticks)
                        {
                            if (outOfOrderness.HasValue && s + duration.Ticks <= effectiveWatermark)
                                break;

                            if (!activeWindows.ContainsKey(s)) windowsToCreate.Add(s);
                        }
                        windowsToCreate.Reverse();

                        foreach (var s in windowsToCreate)
                        {
                            var channel = ChannelExecution.CreateChannel<Timestamped<T>>(capacity, mode, singleWriter: true);
                            activeWindows[s] = channel;
                            await emitter.EmitAsync(Stream.From(innerCt => channel.Reader.ReadAllAsync(innerCt)));
                        }

                        // Write to all active windows that contain this item
                        foreach (var kvp in activeWindows)
                        {
                            if (item.Timestamp.UtcTicks >= kvp.Key && item.Timestamp.UtcTicks < kvp.Key + duration.Ticks)
                            {
                                await ChannelExecution.WriteAsync(kvp.Value.Writer, item, mode, ct);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                }
                catch (Exception ex)
                {
                    foreach (var window in activeWindows.Values) window.Writer.TryComplete(ex);
                    throw;
                }
                finally
                {
                    foreach (var window in activeWindows.Values) window.Writer.TryComplete();
                    activeWindows.Clear();
                }
            }
        });
    }
}
