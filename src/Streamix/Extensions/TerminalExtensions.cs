using Streamix.Implementations;
using System.Collections.ObjectModel;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace Streamix;

/// <summary>
/// Controls whether a sink terminal owns sink completion.
/// </summary>
public enum SinkCompletionMode
{
    /// <summary>
    /// Complete the sink when the stream finishes successfully or fails.
    /// Cancellation does not complete the sink.
    /// </summary>
    CompleteSink,

    /// <summary>
    /// Leave the sink open after the stream stops writing.
    /// </summary>
    LeaveSinkOpen
}

/// <summary>
/// Provides static extension methods for <see cref="IStream{T}"/> to offer Sink / Terminal variety.
/// </summary>
public static class TerminalExtensions
{
    private static IClock GetClock<T>(IStream<T> stream)
    {
        if (stream is Stream<T> s) return s.Clock;
        if (stream is ConnectableStream<T> cs) return cs.Clock;
        return Streamix.Concurrency.SystemClock.Instance;
    }

    /// <summary>
    /// Determines whether the stream contains a specific element by using the default equality comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="value">The value to locate in the stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns true if the stream contains an element that has the specified value; otherwise, false.</returns>
    public static Task<bool> ContainsAsync<T>(this IStream<T> stream, T value, CancellationToken cancellationToken = default)
    {
        return stream.ContainsAsync(value, null, cancellationToken);
    }

    /// <summary>
    /// Determines whether the stream contains a specific element by using a specified <see cref="IEqualityComparer{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="value">The value to locate in the stream.</param>
    /// <param name="comparer">An <see cref="IEqualityComparer{T}"/> to compare values.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns true if the stream contains an element that has the specified value; otherwise, false.</returns>
    public static async Task<bool> ContainsAsync<T>(this IStream<T> stream, T value, IEqualityComparer<T>? comparer, CancellationToken cancellationToken = default)
    {
        comparer ??= EqualityComparer<T>.Default;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (comparer.Equals(item, value))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Collects all items from the stream into a <see cref="List{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a list containing all items from the stream.</returns>
    public static async Task<List<T>> ToListAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        var list = new List<T>();
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            list.Add(item);
        }
        return list;
    }

    /// <summary>
    /// Collects all items from the stream into an array.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an array containing all items from the stream.</returns>
    public static async Task<T[]> ToArrayAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        var list = await stream.ToListAsync(cancellationToken);
        return list.ToArray();
    }

    /// <summary>
    /// Collects all items from the stream into a <see cref="HashSet{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a hash set containing all items from the stream.</returns>
    public static Task<HashSet<T>> ToHashSetAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        return stream.ToHashSetAsync(null, cancellationToken);
    }

    /// <summary>
    /// Collects all items from the stream into a <see cref="HashSet{T}"/> using a specified comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="comparer">An <see cref="IEqualityComparer{T}"/> to compare elements.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a hash set containing all items from the stream.</returns>
    public static async Task<HashSet<T>> ToHashSetAsync<T>(this IStream<T> stream, IEqualityComparer<T>? comparer, CancellationToken cancellationToken = default)
    {
        var hashSet = new HashSet<T>(comparer);
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            hashSet.Add(item);
        }
        return hashSet;
    }

    /// <summary>
    /// Collects all items from the stream into a read-only collection.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a read-only collection containing all items from the stream.</returns>
    public static async Task<ReadOnlyCollection<T>> ToReadOnlyCollectionAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        var list = await stream.ToListAsync(cancellationToken);
        return new ReadOnlyCollection<T>(list);
    }

    /// <summary>
    /// Collects all items from the stream into a dictionary using a key selector.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="keySelector">A function to extract the key from each item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a dictionary with keys from the selector and values as stream items.</returns>
    public static Task<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> keySelector, CancellationToken cancellationToken = default) where TKey : notnull
    {
        return stream.ToDictionaryAsync(keySelector, null, cancellationToken);
    }

    /// <summary>
    /// Collects all items from the stream into a dictionary using a key selector and specified comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="keySelector">A function to extract the key from each item.</param>
    /// <param name="comparer">An <see cref="IEqualityComparer{TKey}"/> to compare keys.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a dictionary with keys from the selector and values as stream items.</returns>
    public static async Task<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer, CancellationToken cancellationToken = default) where TKey : notnull
    {
        var dict = new Dictionary<TKey, T>(comparer);
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            dict.Add(keySelector(item), item);
        }
        return dict;
    }

    /// <summary>
    /// Collects all items from the stream into a dictionary using key and value selectors.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="keySelector">A function to extract the key from each item.</param>
    /// <param name="valueSelector">A function to extract the value from each item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a dictionary with selected keys and values.</returns>
    public static Task<Dictionary<TKey, TValue>> ToDictionaryAsync<T, TKey, TValue>(this IStream<T> stream, Func<T, TKey> keySelector, Func<T, TValue> valueSelector, CancellationToken cancellationToken = default) where TKey : notnull
    {
        return stream.ToDictionaryAsync(keySelector, valueSelector, null, cancellationToken);
    }

    /// <summary>
    /// Collects all items from the stream into a dictionary using key and value selectors and specified comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="keySelector">A function to extract the key from each item.</param>
    /// <param name="valueSelector">A function to extract the value from each item.</param>
    /// <param name="comparer">An <see cref="IEqualityComparer{TKey}"/> to compare keys.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a dictionary with selected keys and values.</returns>
    public static async Task<Dictionary<TKey, TValue>> ToDictionaryAsync<T, TKey, TValue>(this IStream<T> stream, Func<T, TKey> keySelector, Func<T, TValue> valueSelector, IEqualityComparer<TKey>? comparer, CancellationToken cancellationToken = default) where TKey : notnull
    {
        var dict = new Dictionary<TKey, TValue>(comparer);
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            dict.Add(keySelector(item), valueSelector(item));
        }
        return dict;
    }

    /// <summary>
    /// Collects all items from the stream into a <see cref="ILookup{TKey, T}"/> using a key selector.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="keySelector">A function to extract the key from each item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a lookup with keys from the selector and values as stream items.</returns>
    public static Task<ILookup<TKey, T>> ToLookupAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> keySelector, CancellationToken cancellationToken = default)
    {
        return stream.ToLookupAsync(keySelector, x => x, null, cancellationToken);
    }

    /// <summary>
    /// Collects all items from the stream into a <see cref="ILookup{TKey, T}"/> using a key selector and specified comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="keySelector">A function to extract the key from each item.</param>
    /// <param name="comparer">An <see cref="IEqualityComparer{TKey}"/> to compare keys.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a lookup with keys from the selector and values as stream items.</returns>
    public static Task<ILookup<TKey, T>> ToLookupAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer, CancellationToken cancellationToken = default)
    {
        return stream.ToLookupAsync(keySelector, x => x, comparer, cancellationToken);
    }

    /// <summary>
    /// Collects all items from the stream into a <see cref="ILookup{TKey, TValue}"/> using key and value selectors.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="keySelector">A function to extract the key from each item.</param>
    /// <param name="valueSelector">A function to extract the value from each item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a lookup with selected keys and values.</returns>
    public static Task<ILookup<TKey, TValue>> ToLookupAsync<T, TKey, TValue>(this IStream<T> stream, Func<T, TKey> keySelector, Func<T, TValue> valueSelector, CancellationToken cancellationToken = default)
    {
        return stream.ToLookupAsync(keySelector, valueSelector, null, cancellationToken);
    }

    /// <summary>
    /// Collects all items from the stream into a <see cref="ILookup{TKey, TValue}"/> using key and value selectors and specified comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="keySelector">A function to extract the key from each item.</param>
    /// <param name="valueSelector">A function to extract the value from each item.</param>
    /// <param name="comparer">An <see cref="IEqualityComparer{TKey}"/> to compare keys.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a lookup with selected keys and values.</returns>
    public static async Task<ILookup<TKey, TValue>> ToLookupAsync<T, TKey, TValue>(this IStream<T> stream, Func<T, TKey> keySelector, Func<T, TValue> valueSelector, IEqualityComparer<TKey>? comparer, CancellationToken cancellationToken = default)
    {
        var list = await stream.ToListAsync(cancellationToken);
        return list.ToLookup(keySelector, valueSelector, comparer);
    }

    // LINQ Terminals

    /// <summary>
    /// Returns the first item from the stream, or throws if the stream is empty.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the first item from the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> FirstAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            return item;
        }
        throw new InvalidOperationException("Sequence contains no elements.");
    }

    /// <summary>
    /// Returns the first item from the stream that satisfies a condition, or throws if no such item is found.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the first matching item.</returns>
    /// <exception cref="InvalidOperationException">No item matches the condition or the stream is empty.</exception>
    public static async Task<T> FirstAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
                return item;
        }
        throw new InvalidOperationException("Sequence contains no matching element.");
    }

    /// <summary>
    /// Returns the first item from the stream, or a default value if the stream is empty.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the first item from the stream, or the default value of T if empty.</returns>
    public static async Task<T?> FirstOrDefaultAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            return item;
        }
        return default;
    }

    /// <summary>
    /// Returns the first item from the stream that satisfies a condition, or a default value if no such item is found.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the first matching item, or the default value of T if none found.</returns>
    public static async Task<T?> FirstOrDefaultAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
                return item;
        }
        return default;
    }

    /// <summary>
    /// Returns the last item from the stream, or throws if the stream is empty.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the last item from the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> LastAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        T? last = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            last = item;
            hasValue = true;
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return last!;
    }

    /// <summary>
    /// Returns the last item from the stream that satisfies a condition, or throws if no such item is found.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the last matching item.</returns>
    /// <exception cref="InvalidOperationException">No item matches the condition or the stream is empty.</exception>
    public static async Task<T> LastAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        T? last = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
            {
                last = item;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no matching element.");

        return last!;
    }

    /// <summary>
    /// Returns the last item from the stream, or a default value if the stream is empty.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the last item from the stream, or the default value of T if empty.</returns>
    public static async Task<T?> LastOrDefaultAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        T? last = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            last = item;
        }

        return last;
    }

    /// <summary>
    /// Returns the last item from the stream that satisfies a condition, or a default value if no such item is found.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the last matching item, or the default value of T if none found.</returns>
    public static async Task<T?> LastOrDefaultAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        T? last = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
            {
                last = item;
            }
        }

        return last;
    }

    /// <summary>
    /// Returns the count of items in the stream.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the count of items in the stream.</returns>
    public static async Task<int> CountAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        int count = 0;
        await foreach (var _ in stream.WithCancellation(cancellationToken))
        {
            count++;
        }
        return count;
    }

    /// <summary>
    /// Returns the count of items in the stream that satisfy a condition.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the count of items that satisfy the condition.</returns>
    public static async Task<int> CountAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        int count = 0;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
                count++;
        }
        return count;
    }

    /// <summary>
    /// Determines whether any item in the stream satisfies a condition.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns true if any item satisfies the condition; otherwise, false.</returns>
    public static async Task<bool> AnyAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Determines whether all items in the stream satisfy a condition.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns true if all items satisfy the condition; otherwise, false.</returns>
    public static async Task<bool> AllAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (!predicate(item))
                return false;
        }
        return true;
    }

    /// <summary>
    /// Determines whether the stream contains any items.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns true if the stream contains any items; otherwise, false.</returns>
    public static async Task<bool> AnyAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        await foreach (var _ in stream.WithCancellation(cancellationToken))
        {
            return true;
        }
        return false;
    }

    /// <summary>
    /// Applies an accumulator function over the stream and returns the aggregate value.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="accumulator">An accumulator function.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the aggregate value.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> AggregateAsync<T>(this IStream<T> stream, Func<T, T, T> accumulator, CancellationToken cancellationToken = default)
    {
        bool hasValue = false;
        T? accumulate = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (!hasValue)
            {
                accumulate = item;
                hasValue = true;
            }
            else
            {
                accumulate = accumulator(accumulate!, item);
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return accumulate!;
    }

    /// <summary>
    /// Applies an accumulator function over the stream with a specified seed value and returns the aggregate value.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">An accumulator function.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the aggregate value.</returns>
    public static async Task<TAccumulate> AggregateAsync<T, TAccumulate>(this IStream<T> stream, TAccumulate seed, Func<TAccumulate, T, TAccumulate> accumulator, CancellationToken cancellationToken = default)
    {
        TAccumulate accumulate = seed;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            accumulate = accumulator(accumulate, item);
        }

        return accumulate;
    }

    /// <summary>
    /// Applies an accumulator function over the stream with a specified seed value and returns a transformed aggregate value.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
    /// <typeparam name="TResult">The type of the final result value.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">An accumulator function.</param>
    /// <param name="resultSelector">A function to transform the final accumulator value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the transformed aggregate value.</returns>
    public static async Task<TResult> AggregateAsync<T, TAccumulate, TResult>(this IStream<T> stream, TAccumulate seed, Func<TAccumulate, T, TAccumulate> accumulator, Func<TAccumulate, TResult> resultSelector, CancellationToken cancellationToken = default)
    {
        TAccumulate accumulate = seed;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            accumulate = accumulator(accumulate, item);
        }

        return resultSelector(accumulate);
    }

    /// <summary>
    /// Returns the maximum value from the stream.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the maximum value from the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> MaxAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default) where T : IComparable<T>
    {
        bool hasValue = false;
        T? max = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (!hasValue || item.CompareTo(max!) > 0)
            {
                max = item;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return max!;
    }

    /// <summary>
    /// Returns the maximum value from the stream using a comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="comparer">A comparer to compare items.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the maximum value from the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> MaxAsync<T>(this IStream<T> stream, IComparer<T> comparer, CancellationToken cancellationToken = default)
    {
        bool hasValue = false;
        T? max = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (!hasValue || comparer.Compare(item, max!) > 0)
            {
                max = item;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return max!;
    }

    /// <summary>
    /// Returns the minimum value from the stream.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the minimum value from the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> MinAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default) where T : IComparable<T>
    {
        bool hasValue = false;
        T? min = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (!hasValue || item.CompareTo(min!) < 0)
            {
                min = item;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return min!;
    }

    /// <summary>
    /// Returns the minimum value from the stream using a comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="comparer">A comparer to compare items.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the minimum value from the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> MinAsync<T>(this IStream<T> stream, IComparer<T> comparer, CancellationToken cancellationToken = default)
    {
        bool hasValue = false;
        T? min = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (!hasValue || comparer.Compare(item, min!) < 0)
            {
                min = item;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return min!;
    }

    /// <summary>
    /// Computes the sum of numeric values in the stream.
    /// </summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the sum of all numeric values in the stream.</returns>
    public static async Task<int> SumAsync(this IStream<int> stream, CancellationToken cancellationToken = default)
    {
        int sum = 0;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            sum += item;
        }
        return sum;
    }

    /// <summary>
    /// Computes the sum of numeric values in the stream.
    /// </summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the sum of all numeric values in the stream.</returns>
    public static async Task<long> SumAsync(this IStream<long> stream, CancellationToken cancellationToken = default)
    {
        long sum = 0;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            sum += item;
        }
        return sum;
    }

    /// <summary>
    /// Computes the sum of numeric values in the stream.
    /// </summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the sum of all numeric values in the stream.</returns>
    public static async Task<decimal> SumAsync(this IStream<decimal> stream, CancellationToken cancellationToken = default)
    {
        decimal sum = 0;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            sum += item;
        }
        return sum;
    }

    /// <summary>
    /// Computes the sum of numeric values in the stream.
    /// </summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the sum of all numeric values in the stream.</returns>
    public static async Task<double> SumAsync(this IStream<double> stream, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            sum += item;
        }
        return sum;
    }

    /// <summary>
    /// Computes the average of numeric values in the stream.
    /// </summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the average of all numeric values in the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<double> AverageAsync(this IStream<int> stream, CancellationToken cancellationToken = default)
    {
        long sum = 0;
        int count = 0;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            sum += item;
            count++;
        }

        if (count == 0)
            throw new InvalidOperationException("Sequence contains no elements.");

        return (double)sum / count;
    }

    /// <summary>
    /// Computes the average of numeric values in the stream.
    /// </summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the average of all numeric values in the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<double> AverageAsync(this IStream<double> stream, CancellationToken cancellationToken = default)
    {
        double sum = 0;
        int count = 0;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            sum += item;
            count++;
        }

        if (count == 0)
            throw new InvalidOperationException("Sequence contains no elements.");

        return sum / count;
    }

    /// <summary>
    /// Computes the average of numeric values in the stream.
    /// </summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the average of all numeric values in the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<decimal> AverageAsync(this IStream<decimal> stream, CancellationToken cancellationToken = default)
    {
        decimal sum = 0;
        int count = 0;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            sum += item;
            count++;
        }

        if (count == 0)
            throw new InvalidOperationException("Sequence contains no elements.");

        return sum / count;
    }

    /// <summary>
    /// Returns the only item from the stream, or throws if the stream does not contain exactly one item.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the only item from the stream.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty or contains more than one item.</exception>
    public static async Task<T> SingleAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        T? first = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (hasValue)
                throw new InvalidOperationException("Sequence contains more than one element.");

            first = item;
            hasValue = true;
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return first!;
    }

    /// <summary>
    /// Returns the only item from the stream that satisfies a condition, or throws if no such item exists or multiple items match.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the single matching item.</returns>
    /// <exception cref="InvalidOperationException">No item matches the condition, or more than one item matches.</exception>
    public static async Task<T> SingleAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        T? first = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
            {
                if (hasValue)
                    throw new InvalidOperationException("Sequence contains more than one matching element.");

                first = item;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no matching element.");

        return first!;
    }

    /// <summary>
    /// Returns the only item from the stream, or a default value if the stream is empty. Throws if the stream contains more than one item.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the only item from the stream, or the default value of T if empty.</returns>
    /// <exception cref="InvalidOperationException">The stream contains more than one item.</exception>
    public static async Task<T?> SingleOrDefaultAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        T? first = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (hasValue)
                throw new InvalidOperationException("Sequence contains more than one element.");

            first = item;
            hasValue = true;
        }

        return first;
    }

    /// <summary>
    /// Returns the only item from the stream that satisfies a condition, or a default value if no such item exists. Throws if more than one item matches.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the single matching item, or the default value of T if none found.</returns>
    /// <exception cref="InvalidOperationException">More than one item matches the condition.</exception>
    public static async Task<T?> SingleOrDefaultAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        T? first = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
            {
                if (hasValue)
                    throw new InvalidOperationException("Sequence contains more than one matching element.");

                first = item;
                hasValue = true;
            }
        }

        return first;
    }

    /// <summary>
    /// Returns the item at a specified index in a stream.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="index">The zero-based index of the element to retrieve.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the element at the specified position in the source stream.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is less than 0.</exception>
    /// <exception cref="InvalidOperationException">The stream contains fewer than <paramref name="index"/> + 1 elements.</exception>
    public static async Task<T> ElementAtAsync<T>(this IStream<T> stream, int index, CancellationToken cancellationToken = default)
    {
        if (index < 0)
            throw new ArgumentOutOfRangeException(nameof(index));

        int current = 0;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (current == index)
                return item;
            current++;
        }

        throw new InvalidOperationException("Sequence contains no elements at the specified index.");
    }

    /// <summary>
    /// Returns the item at a specified index in a stream or a default value if the index is out of range.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="index">The zero-based index of the element to retrieve.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the element at the specified position in the source stream, or a default value if the index is out of range.</returns>
    public static async Task<T?> ElementAtOrDefaultAsync<T>(this IStream<T> stream, int index, CancellationToken cancellationToken = default)
    {
        if (index < 0)
            return default;

        int current = 0;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (current == index)
                return item;
            current++;
        }

        return default;
    }

    /// <summary>
    /// Returns an <see cref="Option{T}"/> containing the first item from the stream, or an empty option if the stream is empty.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an option containing the first item if it exists.</returns>
    public static async Task<Option<T>> FirstOrNoneAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            return Option<T>.Some(item);
        }
        return Option<T>.None;
    }

    /// <summary>
    /// Returns an <see cref="Option{T}"/> containing the first item from the stream that satisfies a condition, or an empty option if no such item exists.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an option containing the first matching item if it exists.</returns>
    public static async Task<Option<T>> FirstOrNoneAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
                return Option<T>.Some(item);
        }
        return Option<T>.None;
    }

    /// <summary>
    /// Returns an <see cref="Option{T}"/> containing the last item from the stream, or an empty option if the stream is empty.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an option containing the last item if it exists.</returns>
    public static async Task<Option<T>> LastOrNoneAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        T? last = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            last = item;
            hasValue = true;
        }

        return hasValue ? Option<T>.Some(last!) : Option<T>.None;
    }

    /// <summary>
    /// Returns an <see cref="Option{T}"/> containing the last item from the stream that satisfies a condition, or an empty option if no such item exists.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an option containing the last matching item if it exists.</returns>
    public static async Task<Option<T>> LastOrNoneAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        T? last = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
            {
                last = item;
                hasValue = true;
            }
        }

        return hasValue ? Option<T>.Some(last!) : Option<T>.None;
    }

    /// <summary>
    /// Returns an <see cref="Option{T}"/> containing the only item from the stream, or an empty option if the stream is empty or contains more than one item.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an option containing the only item if it exists.</returns>
    public static async Task<Option<T>> SingleOrNoneAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        T? first = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (hasValue)
                return Option<T>.None;

            first = item;
            hasValue = true;
        }

        return hasValue ? Option<T>.Some(first!) : Option<T>.None;
    }

    /// <summary>
    /// Returns an <see cref="Option{T}"/> containing the only item from the stream that satisfies a condition, or an empty option if no such item exists or multiple items match.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="predicate">A function to test each item for a condition.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an option containing the single matching item if it exists.</returns>
    public static async Task<Option<T>> SingleOrNoneAsync<T>(this IStream<T> stream, Func<T, bool> predicate, CancellationToken cancellationToken = default)
    {
        T? first = default;
        bool hasValue = false;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (predicate(item))
            {
                if (hasValue)
                    return Option<T>.None;

                first = item;
                hasValue = true;
            }
        }

        return hasValue ? Option<T>.Some(first!) : Option<T>.None;
    }

    /// <summary>
    /// Returns an <see cref="Option{T}"/> containing the item at a specified index in a stream, or an empty option if the index is out of range.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="index">The zero-based index of the element to retrieve.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns an option containing the element at the specified position if it exists.</returns>
    public static async Task<Option<T>> ElementAtOrNoneAsync<T>(this IStream<T> stream, int index, CancellationToken cancellationToken = default)
    {
        if (index < 0)
            return Option<T>.None;

        int current = 0;
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            if (current == index)
                return Option<T>.Some(item);
            current++;
        }

        return Option<T>.None;
    }

    // Reduction Beyond Aggregate

    /// <summary>
    /// Applies an accumulator function over the stream and returns the final result.
    /// Alias for <see cref="AggregateAsync{T}(IStream{T}, Func{T, T, T}, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="accumulator">An accumulator function.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the final accumulated value.</returns>
    public static Task<T> ReduceAsync<T>(this IStream<T> stream, Func<T, T, T> accumulator, CancellationToken cancellationToken = default)
    {
        return stream.AggregateAsync(accumulator, cancellationToken);
    }

    /// <summary>
    /// Applies an accumulator function over the stream with a seed and returns the final state only.
    /// Alias for <see cref="AggregateAsync{T, TAccumulate}(IStream{T}, TAccumulate, Func{TAccumulate, T, TAccumulate}, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">An accumulator function.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the final accumulated state.</returns>
    public static Task<TAccumulate> ScanLastAsync<T, TAccumulate>(this IStream<T> stream, TAccumulate seed, Func<TAccumulate, T, TAccumulate> accumulator, CancellationToken cancellationToken = default)
    {
        return stream.AggregateAsync(seed, accumulator, cancellationToken);
    }

    /// <summary>
    /// Returns the element from the stream that has the maximum value according to a specified selector.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key used for comparison.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="selector">A function to extract the comparison key from each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the element with the maximum key value.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> MaxByAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> selector, CancellationToken cancellationToken = default) where TKey : IComparable<TKey>
    {
        return await stream.MaxByAsync(selector, comparer: null, cancellationToken);
    }

    /// <summary>
    /// Returns the element from the stream that has the maximum value according to a specified selector and comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key used for comparison.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="selector">A function to extract the comparison key from each element.</param>
    /// <param name="comparer">The comparer used to order keys.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the element with the maximum key value.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> MaxByAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> selector, IComparer<TKey>? comparer, CancellationToken cancellationToken = default)
    {
        comparer ??= Comparer<TKey>.Default;
        bool hasValue = false;
        T? maxItem = default;
        TKey? maxKey = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            var key = selector(item);
            if (!hasValue || comparer.Compare(key, maxKey!) > 0)
            {
                maxItem = item;
                maxKey = key;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return maxItem!;
    }

    /// <summary>
    /// Returns the element from the stream that has the minimum value according to a specified selector.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key used for comparison.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="selector">A function to extract the comparison key from each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the element with the minimum key value.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> MinByAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> selector, CancellationToken cancellationToken = default) where TKey : IComparable<TKey>
    {
        return await stream.MinByAsync(selector, comparer: null, cancellationToken);
    }

    /// <summary>
    /// Returns the element from the stream that has the minimum value according to a specified selector and comparer.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <typeparam name="TKey">The type of the key used for comparison.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="selector">A function to extract the comparison key from each element.</param>
    /// <param name="comparer">The comparer used to order keys.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the element with the minimum key value.</returns>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> MinByAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> selector, IComparer<TKey>? comparer, CancellationToken cancellationToken = default)
    {
        comparer ??= Comparer<TKey>.Default;
        bool hasValue = false;
        T? minItem = default;
        TKey? minKey = default;

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            var key = selector(item);
            if (!hasValue || comparer.Compare(key, minKey!) < 0)
            {
                minItem = item;
                minKey = key;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return minItem!;
    }

    // Time-aware Terminals

    /// <summary>
    /// Returns the first item from the stream, or throws if the stream is empty or the timeout is reached.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="timeout">The maximum time to wait for the first item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the first item from the stream.</returns>
    /// <exception cref="TimeoutException">The timeout is reached before the first item is emitted.</exception>
    /// <exception cref="InvalidOperationException">The stream is empty.</exception>
    public static async Task<T> FirstAsync<T>(this IStream<T> stream, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var clock = GetClock(stream);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var delayTask = clock.Delay(timeout, delayCts.Token).ContinueWith(t =>
        {
            if (t.IsCompletedSuccessfully)
            {
                timeoutCts.Cancel();
            }
        }, TaskContinuationOptions.ExecuteSynchronously);

        try
        {
            return await stream.FirstAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"The operation has timed out after {timeout}.");
        }
        finally
        {
            delayCts.Cancel();
            try { await delayTask; } catch { }
        }
    }

    /// <summary>
    /// Returns the first item from the stream, or a default value if the stream is empty or the timeout is reached.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="timeout">The maximum time to wait for the first item.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the first item from the stream, or the default value of T if empty or timed out.</returns>
    public static async Task<T?> FirstOrDefaultAsync<T>(this IStream<T> stream, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var clock = GetClock(stream);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var delayTask = clock.Delay(timeout, delayCts.Token).ContinueWith(t =>
        {
            if (t.IsCompletedSuccessfully)
            {
                timeoutCts.Cancel();
            }
        }, TaskContinuationOptions.ExecuteSynchronously);

        try
        {
            return await stream.FirstOrDefaultAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            return default;
        }
        finally
        {
            delayCts.Cancel();
            try { await delayTask; } catch { }
        }
    }

    /// <summary>
    /// Collects all items emitted by the stream within a specified duration into a <see cref="List{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="duration">The duration for which to collect items.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns a list of items collected within the specified duration.</returns>
    public static async Task<List<T>> CollectAsync<T>(this IStream<T> stream, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        var list = new List<T>();
        var clock = GetClock(stream);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var delayTask = clock.Delay(duration, delayCts.Token).ContinueWith(t =>
        {
            if (t.IsCompletedSuccessfully)
            {
                timeoutCts.Cancel();
            }
        }, TaskContinuationOptions.ExecuteSynchronously);

        try
        {
            await foreach (var item in stream.WithCancellation(timeoutCts.Token))
            {
                list.Add(item);
            }
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // Expected when duration is reached
        }
        finally
        {
            delayCts.Cancel();
            try { await delayTask; } catch { }
        }

        return list;
    }

    // Bridging Terminals

    /// <summary>
    /// Terminal operation that writes all items of the stream to the specified <see cref="IAsyncSink{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="sink">The destination sink.</param>
    /// <param name="completionMode">Controls whether the sink is completed by this terminal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all items have been written to the sink.</returns>
    public static async Task ToSinkAsync<T>(this IStream<T> stream, IAsyncSink<T> sink, SinkCompletionMode completionMode = SinkCompletionMode.CompleteSink, CancellationToken cancellationToken = default)
    {
        Exception? completionError = null;
        ExceptionDispatchInfo? capturedException = null;
        var shouldCompleteSink = completionMode == SinkCompletionMode.CompleteSink;
        var canceled = false;

        try
        {
            await foreach (var item in stream.WithCancellation(cancellationToken))
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

    /// <summary>
    /// Terminal operation that writes all items of the stream to a delegate-backed sink.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="writeAsync">The asynchronous callback invoked for each item.</param>
    /// <param name="completeAsync">An optional asynchronous callback invoked when sink completion is owned by this terminal.</param>
    /// <param name="completionMode">Controls whether the sink is completed by this terminal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all items have been written to the sink.</returns>
    public static Task ToSinkAsync<T>(
        this IStream<T> stream,
        Func<T, CancellationToken, ValueTask> writeAsync,
        Func<Exception?, CancellationToken, ValueTask>? completeAsync = null,
        SinkCompletionMode completionMode = SinkCompletionMode.CompleteSink,
        CancellationToken cancellationToken = default)
    {
        return stream.ToSinkAsync(new DelegateAsyncSink<T>(writeAsync, completeAsync), completionMode, cancellationToken);
    }

    /// <summary>
    /// Terminal operation that writes all items of the stream to a new <see cref="ChannelReader{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="capacity">The capacity of the channel. If not specified, an unbounded channel is created.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A channel reader that provides items from the stream.</returns>
    public static ChannelReader<T> ToChannel<T>(this IStream<T> stream, int? capacity = null, CancellationToken cancellationToken = default)
    {
        var channel = capacity.HasValue
            ? Channel.CreateBounded<T>(capacity.Value)
            : Channel.CreateUnbounded<T>();

        _ = stream.ToChannel(channel.Writer, true, cancellationToken);

        return channel.Reader;
    }

    /// <summary>
    /// Converts the stream into a blocking <see cref="IEnumerable{T}"/>.
    /// This should be used with caution as it blocks the calling thread.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <returns>A blocking enumerable.</returns>
    public static IEnumerable<T> ToEnumerableBlocking<T>(this IStream<T> stream)
    {
        // Using Task.Run to avoid deadlocks in environments with a SynchronizationContext
        var enumerator = Task.Run(() => stream.GetAsyncEnumerator()).GetAwaiter().GetResult();
        try
        {
            while (true)
            {
                if (!Task.Run(() => enumerator.MoveNextAsync().AsTask()).GetAwaiter().GetResult())
                    yield break;

                yield return enumerator.Current;
            }
        }
        finally
        {
            Task.Run(() => enumerator.DisposeAsync().AsTask()).GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Explicitly returns the stream as an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <returns>The stream as an <see cref="IAsyncEnumerable{T}"/>.</returns>
    public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IStream<T> stream)
    {
        return stream;
    }

    // Consumption Control

    /// <summary>
    /// Terminal operation that executes an asynchronous action for each element of the stream with concurrency control.
    /// This is an ergonomic shortcut for <c>stream.FlatMap(x => Process(x), maxConcurrency).DrainAsync()</c>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="action">The asynchronous action to execute for each element.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all elements have been processed.</returns>
    public static Task ForEachAsync<T>(this IStream<T> stream, Func<T, Task> action, int maxConcurrency, CancellationToken cancellationToken = default)
    {
        return stream.FlatMap(async x => { await action(x); return 0; }, maxConcurrency).DrainAsync(cancellationToken);
    }

    /// <summary>
    /// Explicit version of <see cref="ForEachAsync{T}(IStream{T}, Func{T, Task}, int, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="maxConcurrency">The maximum number of concurrent operations.</param>
    /// <param name="action">The asynchronous action to execute for each element.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when all elements have been processed.</returns>
    public static Task ParallelForEachAsync<T>(this IStream<T> stream, int maxConcurrency, Func<T, Task> action, CancellationToken cancellationToken = default)
    {
        return stream.ForEachAsync(action, maxConcurrency, cancellationToken);
    }

    // Subscription-style Terminals

    /// <summary>
    /// Subscribes to the stream with callbacks for items, errors, and completion.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="onNext">The asynchronous action to execute for each element.</param>
    /// <param name="onError">The asynchronous action to execute when the stream fails.</param>
    /// <param name="onComplete">The asynchronous action to execute when the stream completes successfully.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the subscription lifecycle.</returns>
    public static async Task SubscribeAsync<T>(this IStream<T> stream, Func<T, Task> onNext, Func<Exception, Task>? onError = null, Func<Task>? onComplete = null, CancellationToken cancellationToken = default)
    {
        try
        {
            await foreach (var item in stream.WithCancellation(cancellationToken))
            {
                await onNext(item);
            }

            if (onComplete != null)
            {
                await onComplete();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected
        }
        catch (Exception ex)
        {
            if (onError != null)
            {
                await onError(ex);
            }
            else
            {
                throw;
            }
        }
    }

    // Drain / Ignore

    /// <summary>
    /// Terminal operation that consumes the stream and discards all items.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the stream has been fully consumed.</returns>
    public static async Task DrainAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        await foreach (var _ in stream.WithCancellation(cancellationToken))
        {
            // Discard
        }
    }

    // Diagnostics-aware Terminals

    /// <summary>
    /// Executes the stream and returns a <see cref="StreamResult{T}"/> containing items, error, completion status, and duration.
    /// </summary>
    /// <typeparam name="T">The type of items in the stream.</typeparam>
    /// <param name="stream">The source stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that returns the execution results.</returns>
    public static async Task<StreamResult<T>> ExecuteAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        var clock = GetClock(stream);

        var items = new List<T>();
        Exception? error = null;
        bool completed = false;
        var startTime = clock.Now;

        try
        {
            await foreach (var item in stream.WithCancellation(cancellationToken))
            {
                items.Add(item);
            }
            completed = true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Partial completion
        }
        catch (Exception ex)
        {
            error = ex;
        }

        var duration = clock.Now - startTime;
        return new StreamResult<T>(items, error, completed, duration);
    }
}
