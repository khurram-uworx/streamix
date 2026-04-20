using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Streamix.Implementations;

internal static class ScopeHelper
{
    public static async IAsyncEnumerable<T> ReadAllSupervisedAsync<T>(
        ChannelReader<T> reader,
        StreamScope scope,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (true)
        {
            if (scope.IsFaulted) break;
            bool hasMore;
            try
            {
                hasMore = await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (ChannelClosedException)
            {
                break;
            }

            if (!hasMore) break;

            while (reader.TryRead(out var item))
            {
                yield return item;
            }
        }
    }

    public static async Task FinalizeScopeAsync(StreamScope scope)
    {
        try
        {
            await scope.WaitAllAsync().ConfigureAwait(false);
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
            scope.ThrowIfFailed();
        }
    }
}
