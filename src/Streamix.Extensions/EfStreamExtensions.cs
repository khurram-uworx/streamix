using Microsoft.EntityFrameworkCore;

namespace Streamix.Extensions;

/// <summary>
/// Provides fluent extension methods for creating Streamix streams from Entity Framework queries.
/// </summary>
public static class EfStreamExtensions
{
    /// <summary>
    /// Creates a stream by building an EF query from the same <see cref="DbContext"/> instance that executes it.
    /// </summary>
    /// <typeparam name="T">The entity type emitted by the stream.</typeparam>
    /// <param name="dbContextFactory">Creates a new context per subscription.</param>
    /// <param name="query">Builds a query from the context created for that subscription.</param>
    /// <param name="name">Optional stream name used by diagnostics operators.</param>
    /// <returns>A cold stream that executes the query per subscription.</returns>
    /// <remarks>
    /// This overload delegates to <see cref="EfStream.From{T}(Func{DbContext, IQueryable{T}}, Func{DbContext}, string?)"/>.
    /// Query results are materialized with <c>ToListAsync</c> before items are yielded.
    /// </remarks>
    public static IStream<T> ToStream<T>(this Func<DbContext> dbContextFactory, Func<DbContext, IQueryable<T>> query,
        string? name = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(dbContextFactory);
        ArgumentNullException.ThrowIfNull(query);

        return EfStream.From(query, dbContextFactory, name);
    }

    /// <summary>
    /// Creates a stream by building an EF query from the same <see cref="DbContext"/> instance that executes it.
    /// </summary>
    /// <typeparam name="T">The entity type emitted by the stream.</typeparam>
    /// <param name="dbContextFactory">Creates a new context per subscription.</param>
    /// <param name="query">Builds a query from the context created for that subscription.</param>
    /// <param name="clock">Clock used by the produced stream metadata.</param>
    /// <param name="name">Optional stream name used by diagnostics operators.</param>
    /// <returns>A cold stream that executes the query per subscription.</returns>
    /// <remarks>
    /// This overload delegates to <see cref="EfStream.From{T}(Func{DbContext, IQueryable{T}}, Func{DbContext}, IClock, string?)"/>.
    /// Query results are materialized with <c>ToListAsync</c> before items are yielded.
    /// </remarks>
    public static IStream<T> ToStream<T>(this Func<DbContext> dbContextFactory, Func<DbContext, IQueryable<T>> query,
        IClock clock, string? name = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(dbContextFactory);
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(clock);

        return EfStream.From(query, dbContextFactory, clock, name);
    }

    /// <summary>
    /// Creates a streamed stream by building an EF query from the same <see cref="DbContext"/> instance that executes it.
    /// </summary>
    /// <typeparam name="T">The entity type emitted by the stream.</typeparam>
    /// <param name="dbContextFactory">Creates a new context per subscription.</param>
    /// <param name="query">Builds a query from the context created for that subscription.</param>
    /// <param name="name">Optional stream name used by diagnostics operators.</param>
    /// <returns>A cold stream that executes the query per subscription.</returns>
    /// <remarks>
    /// This overload delegates to <see cref="EfStream.FromStreamed{T}(Func{DbContext, IQueryable{T}}, Func{DbContext}, string?)"/>.
    /// Query results are emitted as the provider enumerates them.
    /// </remarks>
    public static IStream<T> ToStreamed<T>(this Func<DbContext> dbContextFactory, Func<DbContext, IQueryable<T>> query,
        string? name = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(dbContextFactory);
        ArgumentNullException.ThrowIfNull(query);

        return EfStream.FromStreamed(query, dbContextFactory, name);
    }

    /// <summary>
    /// Creates a streamed stream by building an EF query from the same <see cref="DbContext"/> instance that executes it.
    /// </summary>
    /// <typeparam name="T">The entity type emitted by the stream.</typeparam>
    /// <param name="dbContextFactory">Creates a new context per subscription.</param>
    /// <param name="query">Builds a query from the context created for that subscription.</param>
    /// <param name="clock">Clock used by the produced stream metadata.</param>
    /// <param name="name">Optional stream name used by diagnostics operators.</param>
    /// <returns>A cold stream that executes the query per subscription.</returns>
    /// <remarks>
    /// This overload delegates to <see cref="EfStream.FromStreamed{T}(Func{DbContext, IQueryable{T}}, Func{DbContext}, IClock, string?)"/>.
    /// Query results are emitted as the provider enumerates them.
    /// </remarks>
    public static IStream<T> ToStreamed<T>(this Func<DbContext> dbContextFactory, Func<DbContext, IQueryable<T>> query,
        IClock clock, string? name = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(dbContextFactory);
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(clock);

        return EfStream.FromStreamed(query, dbContextFactory, clock, name);
    }
}
