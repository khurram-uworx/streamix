using System.Runtime.ExceptionServices;

namespace Streamix.Implementations;

/// <summary>
/// Internal helper class that provides core sink writing functionality.
/// This class encapsulates the common logic for writing stream items to sinks,
/// allowing both core stream classes and extensions to depend on this shared implementation
/// rather than creating circular dependencies.
/// </summary>
internal static class SinkHelper
{
    /// <summary>
    /// Writes all items from a stream to a sink with proper error and cancellation handling.
    /// This is the core implementation shared by Stream, ConnectableStream, and TerminalExtensions.
    /// </summary>
    public static async Task WriteSinkAsync<T>(
        IAsyncEnumerable<T> source,
        IAsyncSink<T> sink,
        SinkCompletionMode completionMode,
        CancellationToken cancellationToken)
    {
        Exception? completionError = null;
        ExceptionDispatchInfo? capturedException = null;
        var shouldCompleteSink = completionMode == SinkCompletionMode.CompleteSink;
        var canceled = false;

        try
        {
            await foreach (var item in source.WithCancellation(cancellationToken))
            {
                await sink.WriteAsync(item, cancellationToken);
            }
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
        {
            canceled = true;
            capturedException = ExceptionDispatchInfo.Capture(ex);
        }
        catch (Exception ex)
        {
            completionError = ex;
            capturedException = ExceptionDispatchInfo.Capture(ex);
        }

        if (shouldCompleteSink && !canceled)
        {
            try
            {
                await sink.CompleteAsync(completionError, cancellationToken);
            }
            catch when (capturedException is not null)
            {
                // Preserve the original upstream or write failure when completion also fails.
            }
        }

        capturedException?.Throw();
    }
}
