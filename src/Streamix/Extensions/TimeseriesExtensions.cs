using Streamix.Implementations;
using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Provides time-series extension methods for <see cref="IStream{T}"/>.
/// </summary>
public static class TimeseriesExtensions
{
    class SessionState<T>
    {
        public long MinTicks { get; set; }
        public long MaxTicks { get; set; }
        public List<Timestamped<T>> Items { get; } = new();

        public SessionState(long min, long max)
        {
            MinTicks = min;
            MaxTicks = max;
        }
    }

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

    /// <summary>
    /// Groups elements of a stream into session windows based on their timestamps and a gap of inactivity.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="source">The source stream of timestamped items.</param>
    /// <param name="gap">The maximum gap of inactivity between elements in a session.</param>
    /// <param name="capacity">The capacity of the buffer for each window.</param>
    /// <param name="mode">The backpressure mode for each window.</param>
    /// <param name="outOfOrderness">The maximum out-of-orderness allowed before an event is considered late. If not null, enables watermark-aware behavior and session merging.</param>
    /// <returns>A stream of session window streams.</returns>
    public static IStream<IStream<Timestamped<T>>> WindowBySession<T>(
        this IStream<Timestamped<T>> source,
        TimeSpan gap,
        int capacity = 16,
        ChannelBackpressureMode mode = ChannelBackpressureMode.Wait,
        TimeSpan? outOfOrderness = null)
    {
        if (gap <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(gap));
        if (outOfOrderness.HasValue && outOfOrderness.Value < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(outOfOrderness));

        return Stream.Create<IStream<Timestamped<T>>>(async emitter =>
        {
            var ct = emitter.CancellationToken;

            if (!outOfOrderness.HasValue)
            {
                // Ordered/Sequential session logic
                Channel<Timestamped<T>>? currentSessionChannel = null;
                long? sessionMinTicks = null;
                long? sessionMaxTicks = null;

                try
                {
                    await foreach (var item in source.WithCancellation(ct))
                    {
                        var itemTicks = item.Timestamp.UtcTicks;

                        if (currentSessionChannel == null || itemTicks > sessionMaxTicks + gap.Ticks || itemTicks < sessionMinTicks - gap.Ticks)
                        {
                            // New session
                            currentSessionChannel?.Writer.TryComplete();

                            currentSessionChannel = ChannelExecution.CreateChannel<Timestamped<T>>(capacity, mode, singleWriter: true);
                            sessionMinTicks = itemTicks;
                            sessionMaxTicks = itemTicks;

                            var channelToEmit = currentSessionChannel;
                            await emitter.EmitAsync(Stream.From(innerCt => channelToEmit.Reader.ReadAllAsync(innerCt)));
                        }
                        else
                        {
                            // Extend current session
                            if (itemTicks < sessionMinTicks) sessionMinTicks = itemTicks;
                            if (itemTicks > sessionMaxTicks) sessionMaxTicks = itemTicks;
                        }

                        await ChannelExecution.WriteAsync(currentSessionChannel.Writer, item, mode, ct);
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    currentSessionChannel?.Writer.TryComplete(ex);
                    throw;
                }
                finally
                {
                    currentSessionChannel?.Writer.TryComplete();
                }
            }
            else
            {
                // Watermark-aware session logic with merging
                var activeSessions = new List<SessionState<T>>();
                long? maxObservedTicks = null;

                try
                {
                    await foreach (var item in source.WithCancellation(ct))
                    {
                        var itemTicks = item.Timestamp.UtcTicks;
                        var currentWatermark = maxObservedTicks.HasValue ? maxObservedTicks.Value - outOfOrderness.Value.Ticks : long.MinValue;

                        if (itemTicks <= currentWatermark)
                            continue;

                        if (!maxObservedTicks.HasValue || itemTicks > maxObservedTicks.Value)
                            maxObservedTicks = itemTicks;

                        var newWatermark = maxObservedTicks.Value - outOfOrderness.Value.Ticks;

                        // Find overlapping sessions
                        var overlapping = new List<SessionState<T>>();
                        var nonOverlapping = new List<SessionState<T>>();

                        foreach (var session in activeSessions)
                        {
                            // Overlap if item is within gap of session range [min, max]
                            // i.e., itemTicks >= session.Min - gap AND itemTicks <= session.Max + gap
                            if (itemTicks >= session.MinTicks - gap.Ticks && itemTicks <= session.MaxTicks + gap.Ticks)
                            {
                                overlapping.Add(session);
                            }
                            else
                            {
                                nonOverlapping.Add(session);
                            }
                        }

                        if (overlapping.Count == 0)
                        {
                            // Create new session
                            var newSession = new SessionState<T>(itemTicks, itemTicks);
                            newSession.Items.Add(item);
                            nonOverlapping.Add(newSession);
                        }
                        else
                        {
                            // Merge all overlapping into one
                            var mergedMin = Math.Min(itemTicks, overlapping.Min(s => s.MinTicks));
                            var mergedMax = Math.Max(itemTicks, overlapping.Max(s => s.MaxTicks));
                            var mergedSession = new SessionState<T>(mergedMin, mergedMax);

                            foreach (var s in overlapping)
                            {
                                mergedSession.Items.AddRange(s.Items);
                            }
                            mergedSession.Items.Add(item);
                            // Sort items to ensure final stream is ordered
                            mergedSession.Items.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));

                            nonOverlapping.Add(mergedSession);
                        }

                        activeSessions = nonOverlapping;

                        // Emit and remove finalized sessions
                        var sessionsToEmit = new List<SessionState<T>>();
                        for (int i = activeSessions.Count - 1; i >= 0; i--)
                        {
                            var session = activeSessions[i];
                            if (newWatermark >= session.MaxTicks + gap.Ticks)
                            {
                                sessionsToEmit.Add(session);
                                activeSessions.RemoveAt(i);
                            }
                        }

                        // Emit in chronological order
                        foreach (var session in sessionsToEmit.OrderBy(s => s.MinTicks))
                        {
                            await emitter.EmitAsync(Stream.From(session.Items.OrderBy(x => x.Timestamp).AsEnumerable()));
                        }
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
                finally
                {
                    // Flush remaining sessions
                    activeSessions.Sort((a, b) => a.MinTicks.CompareTo(b.MinTicks));
                    foreach (var session in activeSessions)
                    {
                        await emitter.EmitAsync(Stream.From(session.Items.OrderBy(x => x.Timestamp).AsEnumerable()));
                    }
                    activeSessions.Clear();
                }
            }
        });
    }

    /// <summary>
    /// Groups elements of a stream into lists based on a time interval.
    /// </summary>
    /// <param name="source">The stream</param>
    /// <param name="interval">The time interval to buffer items.</param>
    /// <param name="maxCount">The maximum number of items per buffer.</param>
    /// <returns>An <see cref="IStream{T}"/> of <see cref="IList{T}"/>.</returns>
    public static IStream<IList<T>> BufferByTime<T>(this IStream<T> source, TimeSpan interval, int? maxCount = null)
    {
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval), "Interval must be greater than 0.");
        if (maxCount.HasValue && maxCount.Value <= 0) throw new ArgumentOutOfRangeException(nameof(maxCount), "Max count must be greater than 0.");

        return Stream.Create<IList<T>>(async (emitter, ct) =>
        {
            var clock = source.Clock;
            var channel = Channel.CreateUnbounded<object?>();
            var tick = new object();

            using var internalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var scope = new StreamScope(internalCts.Token);
            try
            {
                // Timer
                scope.Run(async innerCt =>
                {
                    try
                    {
                        while (!innerCt.IsCancellationRequested)
                        {
                            await clock.Delay(interval, innerCt).ConfigureAwait(false);
                            channel.Writer.TryWrite(tick);
                        }
                    }
                    catch (OperationCanceledException) { }
                });

                // Source
                scope.Run(async innerCt =>
                {
                    try
                    {
                        await foreach (var item in source.WithCancellation(innerCt).ConfigureAwait(false))
                        {
                            channel.Writer.TryWrite(item);
                        }
                    }
                    finally
                    {
                        channel.Writer.TryComplete();
                    }
                });

                var buffer = new List<T>(maxCount ?? 16);
                try
                {
                    await foreach (var itemOrTick in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                    {
                        if (ReferenceEquals(itemOrTick, tick))
                        {
                            if (buffer.Count > 0)
                            {
                                await emitter.EmitAsync(buffer).ConfigureAwait(false);
                                buffer = new List<T>(maxCount ?? 16);
                            }
                        }
                        else
                        {
                            buffer.Add((T)itemOrTick!);
                            if (maxCount.HasValue && buffer.Count >= maxCount.Value)
                            {
                                await emitter.EmitAsync(buffer).ConfigureAwait(false);
                                buffer = new List<T>(maxCount ?? 16);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    emitter.Fail(ex);
                }
                finally
                {
                    internalCts.Cancel();
                    if (buffer.Count > 0 && !ct.IsCancellationRequested)
                    {
                        try { await emitter.EmitAsync(buffer).ConfigureAwait(false); } catch { }
                    }
                }
            }
            finally
            {
                await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
                await scope.DisposeAsync().ConfigureAwait(false);
                emitter.Complete();
            }
        }).Named(source.Name ?? "");
    }

    /// <summary>
    /// Emits the latest item within each periodic time interval.
    /// </summary>
    /// <param name="source">The stream</param>
    /// <param name="interval">The sampling interval.</param>
    /// <returns>A sampled <see cref="IStream{T}"/>.</returns>
    public static IStream<T> Sample<T>(this IStream<T> source, TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval), "Interval must be greater than 0.");

        return Stream.Create<T>(async (emitter, ct) =>
        {
            var clock = source.Clock;
            var channel = Channel.CreateUnbounded<object?>();
            var tick = new object();

            using var internalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var scope = new StreamScope(internalCts.Token);
            try
            {
                // Timer
                scope.Run(async innerCt =>
                {
                    try
                    {
                        while (!innerCt.IsCancellationRequested)
                        {
                            await clock.Delay(interval, innerCt).ConfigureAwait(false);
                            channel.Writer.TryWrite(tick);
                        }
                    }
                    catch (OperationCanceledException) { }
                });

                // Source
                scope.Run(async innerCt =>
                {
                    try
                    {
                        await foreach (var item in source.WithCancellation(innerCt).ConfigureAwait(false))
                        {
                            channel.Writer.TryWrite(item);
                        }
                    }
                    finally
                    {
                        channel.Writer.TryComplete();
                    }
                });

                T? latestItem = default;
                bool hasItem = false;
                try
                {
                    await foreach (var itemOrTick in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                    {
                        if (ReferenceEquals(itemOrTick, tick))
                        {
                            if (hasItem)
                            {
                                await emitter.EmitAsync(latestItem!).ConfigureAwait(false);
                                hasItem = false;
                                latestItem = default;
                            }
                        }
                        else
                        {
                            latestItem = (T)itemOrTick!;
                            hasItem = true;
                        }
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    emitter.Fail(ex);
                }
                finally
                {
                    internalCts.Cancel();
                    if (hasItem && !ct.IsCancellationRequested)
                    {
                        try { await emitter.EmitAsync(latestItem!).ConfigureAwait(false); } catch { }
                    }
                }
            }
            finally
            {
                await ScopeHelper.FinalizeScopeAsync(scope).ConfigureAwait(false);
                await scope.DisposeAsync().ConfigureAwait(false);
                emitter.Complete();
            }
        }).Named(source.Name ?? "");
    }
}
