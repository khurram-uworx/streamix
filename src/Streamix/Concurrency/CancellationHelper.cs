namespace Streamix.Concurrency;

/// <summary>
/// Internal helper for cancellation token management.
/// </summary>
internal static class CancellationHelper
{
    /// <summary>
    /// Creates a linked cancellation token from multiple sources.
    /// </summary>
    public static CancellationTokenSource Link(CancellationToken token1, CancellationToken token2)
    {
        if (token1 == CancellationToken.None) return CancellationTokenSource.CreateLinkedTokenSource(token2);
        if (token2 == CancellationToken.None) return CancellationTokenSource.CreateLinkedTokenSource(token1);
        return CancellationTokenSource.CreateLinkedTokenSource(token1, token2);
    }
}
