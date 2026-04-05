using System.Threading.Channels;

namespace Streamix;

internal sealed class ChannelWriterSink<T> : IAsyncSink<T>
{
    readonly ChannelWriter<T> writer;

    public ChannelWriterSink(ChannelWriter<T> writer)
    {
        this.writer = writer ?? throw new ArgumentNullException(nameof(writer));
    }

    public ValueTask WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        return writer.WriteAsync(item, cancellationToken);
    }

    public ValueTask CompleteAsync(Exception? error = null, CancellationToken cancellationToken = default)
    {
        writer.TryComplete(error);
        return ValueTask.CompletedTask;
    }
}
