using System.Threading.Channels;

namespace Streamix;

enum ProcessingTimeSignalKind
{
    Item,
    Tick,
    SourceCompleted,
    SourceFault,
}

readonly record struct ProcessingTimeSignal<T>(
    ProcessingTimeSignalKind Kind,
    T? Item = default,
    Exception? Error = null)
{
    public static ProcessingTimeSignal<T> FromItem(T item) => new(ProcessingTimeSignalKind.Item, item);
    public static ProcessingTimeSignal<T> Tick() => new(ProcessingTimeSignalKind.Tick);
    public static ProcessingTimeSignal<T> SourceCompleted() => new(ProcessingTimeSignalKind.SourceCompleted);
    public static ProcessingTimeSignal<T> SourceFault(Exception error) => new(ProcessingTimeSignalKind.SourceFault, default, error);
}

static class ProcessingTimeOperatorCoordinator
{
    public static async Task RunAsync<T>(
        IStream<T> source,
        TimeSpan interval,
        CancellationToken cancellationToken,
        Func<ProcessingTimeSignal<T>, ValueTask<bool>> onSignal)
    {
        var clock = source.Clock;
        using var internalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var signalChannel = Channel.CreateBounded<ProcessingTimeSignal<T>>(new BoundedChannelOptions(1)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        var sourceTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in source.WithCancellation(internalCts.Token).ConfigureAwait(false))
                {
                    await signalChannel.Writer.WriteAsync(ProcessingTimeSignal<T>.FromItem(item), internalCts.Token).ConfigureAwait(false);
                }

                await signalChannel.Writer.WriteAsync(ProcessingTimeSignal<T>.SourceCompleted(), internalCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (internalCts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                try
                {
                    await signalChannel.Writer.WriteAsync(ProcessingTimeSignal<T>.SourceFault(ex), internalCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (internalCts.IsCancellationRequested)
                {
                }
            }
        }, CancellationToken.None);

        var timerTask = Task.Run(async () =>
        {
            try
            {
                while (!internalCts.IsCancellationRequested)
                {
                    await clock.Delay(interval, internalCts.Token).ConfigureAwait(false);
                    await signalChannel.Writer.WriteAsync(ProcessingTimeSignal<T>.Tick(), internalCts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (internalCts.IsCancellationRequested)
            {
            }
        }, CancellationToken.None);

        try
        {
            while (true)
            {
                ProcessingTimeSignal<T> signal;
                try
                {
                    signal = await signalChannel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                if (!await onSignal(signal).ConfigureAwait(false))
                {
                    return;
                }
            }
        }
        finally
        {
            await internalCts.CancelAsync().ConfigureAwait(false);
            signalChannel.Writer.TryComplete();
            try { await sourceTask.ConfigureAwait(false); } catch { }
            try { await timerTask.ConfigureAwait(false); } catch { }
        }
    }
}
