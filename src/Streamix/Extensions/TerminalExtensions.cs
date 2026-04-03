using Streamix.Abstractions;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace Streamix;

/// <summary>
/// Provides static extension methods for <see cref="Stream{T}"/> to offer Sink / Terminal variety.
/// </summary>
public static class TerminalExtensions
{
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
    public static async Task<HashSet<T>> ToHashSetAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        var hashSet = new HashSet<T>();
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
    public static async Task<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(this IStream<T> stream, Func<T, TKey> keySelector, CancellationToken cancellationToken = default) where TKey : notnull
    {
        var dict = new Dictionary<TKey, T>();
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            dict[keySelector(item)] = item;
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
    public static async Task<Dictionary<TKey, TValue>> ToDictionaryAsync<T, TKey, TValue>(this IStream<T> stream, Func<T, TKey> keySelector, Func<T, TValue> valueSelector, CancellationToken cancellationToken = default) where TKey : notnull
    {
        var dict = new Dictionary<TKey, TValue>();
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            dict[keySelector(item)] = valueSelector(item);
        }
        return dict;
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
}
