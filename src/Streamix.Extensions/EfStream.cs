using Microsoft.EntityFrameworkCore;
using System.Runtime.CompilerServices;

namespace Streamix.Extensions;

/// <summary>
/// Provides factory methods for creating Streamix streams from Entity Framework queries.
/// </summary>
public static class EfStream
{
    static async IAsyncEnumerable<T> executeBufferedQuery<T>(Func<DbContext, IQueryable<T>> query, Func<DbContext> dbContextFactory,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
    {
        await using var context = dbContextFactory();
        var efQuery = query(context) ?? throw new InvalidOperationException("The EF query delegate returned null.");
        var results = await efQuery.ToListAsync(cancellationToken);

        foreach (var item in results)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    static async IAsyncEnumerable<T> executeStreamedQuery<T>(Func<DbContext, IQueryable<T>> query, Func<DbContext> dbContextFactory,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
    {
        await using var context = dbContextFactory();
        var efQuery = query(context) ?? throw new InvalidOperationException("The EF query delegate returned null.");

        await foreach (var item in EntityFrameworkQueryableExtensions
            .AsAsyncEnumerable(efQuery)
            .WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    /// Creates a stream from an Entity Framework query builder and a context factory.
    /// A new <see cref="DbContext"/> is created per subscription, used to build and execute the query,
    /// then disposed when enumeration completes, fails, or is cancelled.
    /// </summary>
    /// <typeparam name="T">The entity type emitted by the stream.</typeparam>
    /// <param name="query">Builds a query using the context instance created for the current subscription.</param>
    /// <param name="dbContextFactory">Creates a context instance per subscription.</param>
    /// <param name="name">Optional stream name used by diagnostics operators.</param>
    /// <returns>A cold stream that executes the query per subscription.</returns>
    /// <remarks>
    /// The query must be composed from the same context instance created by <paramref name="dbContextFactory"/> for that
    /// subscription. Query execution uses <c>ToListAsync</c>, so the full result set is materialized before items are emitted.
    /// </remarks>
    public static IStream<T> From<T>(Func<DbContext, IQueryable<T>> query, Func<DbContext> dbContextFactory,
        string? name = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(dbContextFactory);

        return Stream.From(executeBufferedQuery(query, dbContextFactory), name);
    }

    /// <summary>
    /// Creates a stream from an Entity Framework query builder and a context factory.
    /// A new <see cref="DbContext"/> is created per subscription, used to build and execute the query,
    /// then disposed when enumeration completes, fails, or is cancelled.
    /// </summary>
    /// <typeparam name="T">The entity type emitted by the stream.</typeparam>
    /// <param name="query">Builds a query using the context instance created for the current subscription.</param>
    /// <param name="dbContextFactory">Creates a context instance per subscription.</param>
    /// <param name="clock">Clock used by the produced stream metadata.</param>
    /// <param name="name">Optional stream name used by diagnostics operators.</param>
    /// <returns>A cold stream that executes the query per subscription.</returns>
    /// <remarks>
    /// The query must be composed from the same context instance created by <paramref name="dbContextFactory"/> for that
    /// subscription. Query execution uses <c>ToListAsync</c>, so the full result set is materialized before items are emitted.
    /// </remarks>
    public static IStream<T> From<T>(Func<DbContext, IQueryable<T>> query, Func<DbContext> dbContextFactory, IClock clock,
        string? name = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(dbContextFactory);
        ArgumentNullException.ThrowIfNull(clock);

        return Stream.From(
            executeBufferedQuery(query, dbContextFactory),
            clock,
            name);
    }

    /// <summary>
    /// Creates a streamed stream from an Entity Framework query builder and a context factory.
    /// A new <see cref="DbContext"/> is created per subscription, used to build and execute the query,
    /// then disposed when enumeration completes, fails, or is cancelled.
    /// </summary>
    /// <typeparam name="T">The entity type emitted by the stream.</typeparam>
    /// <param name="query">Builds a query using the context instance created for the current subscription.</param>
    /// <param name="dbContextFactory">Creates a context instance per subscription.</param>
    /// <param name="name">Optional stream name used by diagnostics operators.</param>
    /// <returns>A cold stream that executes the query per subscription.</returns>
    /// <remarks>
    /// The query must be composed from the same context instance created by <paramref name="dbContextFactory"/> for that
    /// subscription. Query execution uses <c>AsAsyncEnumerable</c>, so items are emitted as the provider enumerates them.
    /// </remarks>
    public static IStream<T> FromStreamed<T>(Func<DbContext, IQueryable<T>> query, Func<DbContext> dbContextFactory,
        string? name = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(dbContextFactory);

        return Stream.From(executeStreamedQuery(query, dbContextFactory), name);
    }

    /// <summary>
    /// Creates a streamed stream from an Entity Framework query builder and a context factory.
    /// A new <see cref="DbContext"/> is created per subscription, used to build and execute the query,
    /// then disposed when enumeration completes, fails, or is cancelled.
    /// </summary>
    /// <typeparam name="T">The entity type emitted by the stream.</typeparam>
    /// <param name="query">Builds a query using the context instance created for the current subscription.</param>
    /// <param name="dbContextFactory">Creates a context instance per subscription.</param>
    /// <param name="clock">Clock used by the produced stream metadata.</param>
    /// <param name="name">Optional stream name used by diagnostics operators.</param>
    /// <returns>A cold stream that executes the query per subscription.</returns>
    /// <remarks>
    /// The query must be composed from the same context instance created by <paramref name="dbContextFactory"/> for that
    /// subscription. Query execution uses <c>AsAsyncEnumerable</c>, so items are emitted as the provider enumerates them.
    /// </remarks>
    public static IStream<T> FromStreamed<T>(Func<DbContext, IQueryable<T>> query, Func<DbContext> dbContextFactory, IClock clock,
        string? name = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(dbContextFactory);
        ArgumentNullException.ThrowIfNull(clock);

        return Stream.From(
            executeStreamedQuery(query, dbContextFactory),
            clock,
            name);
    }
}
