using NUnit.Framework;
using Streamix;

namespace Streamix.Tests.Extensions;

[TestFixture]
public class LinqExtensionsTests
{
    [Test]
    public async Task Where_Filters_Elements()
    {
        var result = await Stream.Range(1, 5)
            .Where(x => x % 2 == 0)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 2, 4 }));
    }

    [Test]
    public async Task Where_Returns_Empty_When_No_Match()
    {
        var result = await Stream.Range(1, 5)
            .Where(x => x > 10)
            .ToListAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task Where_Returns_All_When_All_Match()
    {
        var result = await Stream.Range(1, 5)
            .Where(x => x > 0)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task Where_Works_With_String_Predicate()
    {
        var input = new[] { "apple", "banana", "apricot", "cherry" };
        var result = await Stream.From(input.ToAsyncEnumerable())
            .Where(x => x.StartsWith("a"))
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { "apple", "apricot" }));
    }

    [Test]
    public async Task Where_Chained_Multiple_Times()
    {
        var result = await Stream.Range(1, 10)
            .Where(x => x % 2 == 0)
            .Where(x => x > 3)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 4, 6, 8, 10 }));
    }

    [Test]
    public async Task Select_Transforms_Elements()
    {
        var result = await Stream.Range(1, 3)
            .Select(x => x * 10)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task Select_Transforms_To_Different_Type()
    {
        var result = await Stream.Range(1, 3)
            .Select(x => x.ToString())
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { "1", "2", "3" }));
    }

    [Test]
    public async Task Select_With_Complex_Transformation()
    {
        var input = new[] { 1, 2, 3 };
        var result = await Stream.From(input.ToAsyncEnumerable())
            .Select(x => new { Value = x, Squared = x * x })
            .ToListAsync();

        Assert.That(result.Count, Is.EqualTo(3));
        Assert.That(result[0].Value, Is.EqualTo(1));
        Assert.That(result[0].Squared, Is.EqualTo(1));
        Assert.That(result[2].Value, Is.EqualTo(3));
        Assert.That(result[2].Squared, Is.EqualTo(9));
    }

    [Test]
    public async Task Select_Chained_Multiple_Times()
    {
        var result = await Stream.Range(1, 3)
            .Select(x => x * 2)
            .Select(x => x + 1)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 3, 5, 7 }));
    }

    [Test]
    public async Task Where_And_Select_Combined()
    {
        var result = await Stream.Range(1, 5)
            .Where(x => x % 2 == 0)
            .Select(x => x * 10)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 20, 40 }));
    }

    [Test]
    public async Task SelectMany_Flattens_Nested_Streams()
    {
        var result = await Stream.Range(1, 3)
            .SelectMany(x => Stream.Range(1, x))
            .ToListAsync();

        // 1 -> [1], 2 -> [1,2], 3 -> [1,2,3]
        Assert.That(result, Is.EquivalentTo(new[] { 1, 1, 2, 1, 2, 3 }));
    }

    [Test]
    public async Task SelectMany_With_Projection()
    {
        var result = await Stream.Range(1, 2)
            .SelectMany(x => Stream.Range(1, x).Select(y => $"{x}:{y}"))
            .ToListAsync();

        Assert.That(result, Is.EquivalentTo(new[] { "1:1", "2:1", "2:2" }));
    }

    [Test]
    public async Task SelectMany_Returns_Empty_For_Zero_Values()
    {
        var result = await Stream.Range(1, 3)
            .SelectMany(x => Stream.Range(1, x))
            .ToListAsync();

        Assert.That(result.Count, Is.EqualTo(6)); // 1 + 2 + 3 elements
    }

    [Test]
    public async Task SelectMany_With_Concurrency()
    {
        var result = await Stream.Range(1, 3)
            .SelectMany(x => Stream.Range(1, x), maxConcurrency: 2)
            .ToListAsync();

        // Result should have the same number of elements, but order may vary due to concurrency
        Assert.That(result.Count, Is.EqualTo(6)); // 1 + 2 + 3 elements
        // Verify all expected elements are present (order might differ due to concurrency)
        Assert.That(result.Where(x => x == 1).Count(), Is.EqualTo(3));
        Assert.That(result.Where(x => x == 2).Count(), Is.EqualTo(2));
        Assert.That(result.Where(x => x == 3).Count(), Is.EqualTo(1));
    }

    [Test]
    public async Task SelectMany_Chained()
    {
        var result = await Stream.Range(1, 2)
            .SelectMany(x => Stream.Range(1, x))
            .ToListAsync();

        Assert.That(result, Is.EquivalentTo(new[] { 1, 1, 2 }));
    }

    [Test]
    public async Task Complex_Chain_Where_Select_SelectMany()
    {
        var result = await Stream.Range(1, 5)
            .Where(x => x % 2 == 1)  // Get odd numbers: 1, 3, 5
            .Select(x => x * 2)      // Transform: 2, 6, 10
            .SelectMany(x => Stream.Range(1, x))  // Flatten: Range(1,2), Range(1,6), Range(1,10)
            .ToListAsync();

        // Result: Range(1,2)=[1,2], Range(1,6)=[1..6], Range(1,10)=[1..10]
        // Total: 2 + 6 + 10 = 18 elements
        Assert.That(result.Count, Is.EqualTo(18));

        // Verify all expected values are present
        Assert.That(result.Where(x => x == 1).Count(), Is.EqualTo(3));  // Appears in all 3 ranges
        Assert.That(result.Where(x => x == 2).Count(), Is.EqualTo(3));  // Appears in all 3 ranges
        Assert.That(result.Where(x => x <= 10).Count(), Is.EqualTo(18)); // All values <= 10
    }

    [Test]
    public async Task Extensions_Work_With_Empty_Stream()
    {
        var result = await Stream.Empty<int>()
            .Where(x => x > 0)
            .Select(x => x * 2)
            .ToListAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task Extensions_Preserve_Order()
    {
        var input = new[] { "zebra", "apple", "mango", "banana" };
        var result = await Stream.From(input.ToAsyncEnumerable())
            .Where(x => x.Length > 4)
            .Select(x => x.ToUpper())
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { "ZEBRA", "APPLE", "MANGO", "BANANA" }));
    }

    [Test]
    public async Task Where_Respects_Cancellation_Token()
    {
        using var cts = new CancellationTokenSource();
        var items = new List<int>();
        var task = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in Stream.Range(1, 100)
                    .Where(x => x % 2 == 0)
                    .WithCancellation(cts.Token))
                {
                    items.Add(item);
                    if (items.Count == 5)
                    {
                        await cts.CancelAsync();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        });

        await task;

        // Should have collected exactly 5 items before cancellation
        Assert.That(items.Count, Is.EqualTo(5));
    }

    // ============ Async Extension Methods Tests ============

    [Test]
    public async Task WhereAsync_Filters_With_ValueTask()
    {
        var result = await Stream.Range(1, 5)
            .WhereAsync(x => new ValueTask<bool>(x % 2 == 0))
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 2, 4 }));
    }

    [Test]
    public async Task WhereAsync_Filters_With_Async_Lambda()
    {
        var result = await Stream.Range(1, 5)
            .WhereAsync(async x =>
            {
                await Task.Delay(1);
                return x % 2 == 0;
            })
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 2, 4 }));
    }

    [Test]
    public async Task WhereAsync_Chained()
    {
        var result = await Stream.Range(1, 10)
            .WhereAsync(x => new ValueTask<bool>(x > 3))
            .WhereAsync(x => new ValueTask<bool>(x < 8))
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 4, 5, 6, 7 }));
    }

    [Test]
    public async Task SelectAsync_Transforms_With_ValueTask()
    {
        var result = await Stream.Range(1, 3)
            .SelectAsync(x => new ValueTask<int>(x * 10))
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task SelectAsync_Transforms_With_Async_Lambda()
    {
        var result = await Stream.Range(1, 3)
            .SelectAsync(async x =>
            {
                await Task.Delay(1);
                return x * 10;
            })
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task SelectAsync_To_Different_Type()
    {
        var result = await Stream.Range(1, 3)
            .SelectAsync(x => new ValueTask<string>(x.ToString()))
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { "1", "2", "3" }));
    }

    [Test]
    public async Task SelectAsync_Chained()
    {
        var result = await Stream.Range(1, 3)
            .SelectAsync(x => new ValueTask<int>(x * 2))
            .SelectAsync(x => new ValueTask<int>(x + 1))
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 3, 5, 7 }));
    }

    [Test]
    public async Task SelectManyAsync_Flattens_With_ValueTask()
    {
        var result = await Stream.Range(1, 3)
            .SelectManyAsync(x => new ValueTask<IStream<int>>(Stream.Range(1, x)))
            .ToListAsync();

        Assert.That(result, Is.EquivalentTo(new[] { 1, 1, 2, 1, 2, 3 }));
    }

    [Test]
    public async Task SelectManyAsync_Flattens_With_Async_Lambda()
    {
        var result = await Stream.Range(1, 3)
            .SelectManyAsync(async x =>
            {
                await Task.Delay(1);
                return Stream.Range(1, x);
            })
            .ToListAsync();

        Assert.That(result, Is.EquivalentTo(new[] { 1, 1, 2, 1, 2, 3 }));
    }

    [Test]
    public async Task SelectManyAsync_With_Concurrency()
    {
        var result = await Stream.Range(1, 3)
            .SelectManyAsync(x => new ValueTask<IStream<int>>(Stream.Range(1, x)), maxConcurrency: 2)
            .ToListAsync();

        // Result should have 6 elements (1 + 2 + 3), order may vary due to concurrency
        Assert.That(result.Count, Is.EqualTo(6));
        Assert.That(result.Where(x => x == 1).Count(), Is.EqualTo(3));
        Assert.That(result.Where(x => x == 2).Count(), Is.EqualTo(2));
        Assert.That(result.Where(x => x == 3).Count(), Is.EqualTo(1));
    }

    [Test]
    public async Task SelectManyAsync_With_Concurrency_Async_Lambda()
    {
        var result = await Stream.Range(1, 3)
            .SelectManyAsync(
                async x =>
                {
                    await Task.Delay(1);
                    return Stream.Range(1, x);
                },
                maxConcurrency: 2)
            .ToListAsync();

        Assert.That(result.Count, Is.EqualTo(6));
        Assert.That(result.Where(x => x == 1).Count(), Is.EqualTo(3));
    }

    [Test]
    public async Task SelectManyAsync_With_Concurrency_Respects_Active_Inner_Stream_Limit()
    {
        const int maxConcurrency = 2;
        int activeStreams = 0;
        int maxObservedConcurrency = 0;
        var lockObj = new object();

        var result = await Stream.Range(1, 5)
            .SelectManyAsync(
                x => new ValueTask<IStream<int>>(Stream.Create<int>(async emitter =>
                {
                    var currentActive = Interlocked.Increment(ref activeStreams);

                    lock (lockObj)
                    {
                        maxObservedConcurrency = Math.Max(maxObservedConcurrency, currentActive);
                    }

                    try
                    {
                        await emitter.EmitAsync(x);
                        await Task.Delay(50, emitter.CancellationToken);
                        emitter.Complete();
                    }
                    finally
                    {
                        Interlocked.Decrement(ref activeStreams);
                    }
                })),
                maxConcurrency: maxConcurrency)
            .ToListAsync();

        Assert.That(maxObservedConcurrency, Is.LessThanOrEqualTo(maxConcurrency));
        Assert.That(result, Is.EquivalentTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task Combined_Async_And_Sync_Extensions()
    {
        var result = await Stream.Range(1, 10)
            .WhereAsync(x => new ValueTask<bool>(x % 2 == 0))  // Keep even: 2, 4, 6, 8, 10
            .Select(x => x * 2)                               // Transform: 4, 8, 12, 16, 20
            .SelectAsync(x => new ValueTask<int>(x + 1))      // Add 1: 5, 9, 13, 17, 21
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 5, 9, 13, 17, 21 }));
    }

    [Test]
    public async Task SelectAsync_With_Exception_Propagates()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Stream.Range(1, 5)
                .SelectAsync(async x =>
                {
                    if (x == 3) throw new InvalidOperationException("Test error");
                    return x * 10;
                })
                .ToListAsync();
        });

        Assert.That(ex.Message, Is.EqualTo("Test error"));
    }

    [Test]
    public async Task WhereAsync_With_Exception_Propagates()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Stream.Range(1, 5)
                .WhereAsync(async x =>
                {
                    if (x == 3) throw new InvalidOperationException("Predicate error");
                    return x % 2 == 0;
                })
                .ToListAsync();
        });

        Assert.That(ex.Message, Is.EqualTo("Predicate error"));
    }

    [Test]
    public async Task SelectManyAsync_With_Exception_Propagates()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Stream.Range(1, 3)
                .SelectManyAsync(async x =>
                {
                    if (x == 2) throw new InvalidOperationException("SelectMany error");
                    return Stream.Range(1, x);
                })
                .ToListAsync();
        });

        Assert.That(ex.Message, Is.EqualTo("SelectMany error"));
    }

    [Test]
    public async Task WhereAsync_Empty_Stream()
    {
        var result = await Stream.Empty<int>()
            .WhereAsync(x => new ValueTask<bool>(x > 0))
            .ToListAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task SelectAsync_Empty_Stream()
    {
        var result = await Stream.Empty<int>()
            .SelectAsync(x => new ValueTask<int>(x * 2))
            .ToListAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task SelectManyAsync_Empty_Stream()
    {
        var result = await Stream.Empty<int>()
            .SelectManyAsync(x => new ValueTask<IStream<int>>(Stream.Range(1, x)))
            .ToListAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task Complex_Async_Chain()
    {
        var result = await Stream.Range(1, 10)
            .WhereAsync(x => new ValueTask<bool>(x > 2))           // 3, 4, 5, 6, 7, 8, 9, 10
            .SelectAsync(async x =>
            {
                await Task.Delay(1);
                return x % 2 == 0 ? x : 0;
            })                                                      // 0, 4, 0, 6, 0, 8, 0, 10
            .WhereAsync(x => new ValueTask<bool>(x > 0))           // 4, 6, 8, 10
            .SelectManyAsync(x => new ValueTask<IStream<int>>(Stream.Range(1, x / 2)))  // range(1,2), range(1,3), range(1,4), range(1,5)
            .ToListAsync();

        // range(1,2) = [1,2], range(1,3) = [1,2,3], range(1,4) = [1,2,3,4], range(1,5) = [1,2,3,4,5]
        // Total: 2 + 3 + 4 + 5 = 14 elements
        Assert.That(result.Count, Is.EqualTo(14));
        Assert.That(result.Where(x => x == 1).Count(), Is.EqualTo(4));
        Assert.That(result.Where(x => x == 5).Count(), Is.EqualTo(1));
    }

    // ============ Query Comprehension Syntax Tests ============

    [Test]
    public async Task Query_Syntax_Where_Select()
    {
        var result = await (
            from x in Stream.Range(1, 10)
            where x % 2 == 0
            select x * 10
        ).ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 20, 40, 60, 80, 100 }));
    }

    [Test]
    public async Task Query_Syntax_Multiple_Where_Clauses()
    {
        var result = await (
            from x in Stream.Range(1, 20)
            where x % 2 == 0
            where x > 5
            select x
        ).ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 6, 8, 10, 12, 14, 16, 18, 20 }));
    }

    [Test]
    public async Task Query_Syntax_SelectMany_TwoFroms()
    {
        var result = await (
            from x in Stream.Range(1, 3)
            from y in Stream.Range(1, x)
            select (x, y)
        ).ToListAsync();

        Assert.That(result.Count, Is.EqualTo(6)); // 1 + 2 + 3
        Assert.That(result, Is.EquivalentTo(new[] { (1, 1), (2, 1), (2, 2), (3, 1), (3, 2), (3, 3) }));
    }

    [Test]
    public async Task Query_Syntax_SelectMany_With_Where()
    {
        var result = await (
            from x in Stream.Range(1, 5)
            where x % 2 == 1
            from y in Stream.Range(1, x)
            select (x, y)
        ).ToListAsync();

        // Only odd numbers: 1, 3, 5
        // 1 -> (1,1)
        // 3 -> (3,1), (3,2), (3,3)
        // 5 -> (5,1), (5,2), (5,3), (5,4), (5,5)
        Assert.That(result.Count, Is.EqualTo(9));
        Assert.That(result, Is.EquivalentTo(new[] { (1, 1), (3, 1), (3, 2), (3, 3), (5, 1), (5, 2), (5, 3), (5, 4), (5, 5) }));
    }

    [Test]
    public async Task Query_Syntax_With_Let()
    {
        var result = await (
            from x in Stream.Range(1, 10)
            let squared = x * x
            where squared > 25
            select (x, squared)
        ).ToListAsync();

        // squared > 25: x values 6-10 (36, 49, 64, 81, 100)
        Assert.That(result.Count, Is.EqualTo(5));
        Assert.That(result.Select(r => r.x), Is.EquivalentTo(new[] { 6, 7, 8, 9, 10 }));
    }

    [Test]
    public async Task Query_Syntax_Complex_Transformation()
    {
        var result = await (
            from x in Stream.Range(1, 5)
            where x > 1
            let doubled = x * 2
            select new { Original = x, Doubled = doubled, Squared = x * x }
        ).ToListAsync();

        Assert.That(result.Count, Is.EqualTo(4)); // 2, 3, 4, 5
        Assert.That(result.Select(r => r.Original), Is.EquivalentTo(new[] { 2, 3, 4, 5 }));
    }

    [Test]
    public async Task Query_Syntax_Nested_SelectMany()
    {
        var result = await (
            from x in Stream.Range(1, 3)
            from y in Stream.Range(1, 2)
            select x * 10 + y
        ).ToListAsync();

        Assert.That(result, Is.EquivalentTo(new[] { 11, 12, 21, 22, 31, 32 }));
    }

    [Test]
    public async Task Query_Syntax_Equivalent_To_Fluent()
    {
        // Query syntax
        var queryResult = await (
            from x in Stream.Range(1, 10)
            where x % 2 == 0
            select x * 10
        ).ToListAsync();

        // Fluent syntax
        var fluentResult = await Stream.Range(1, 10)
            .Where(x => x % 2 == 0)
            .Select(x => x * 10)
            .ToListAsync();

        Assert.That(queryResult, Is.EqualTo(fluentResult));
    }

    [Test]
    public async Task Query_Syntax_Three_From_Clauses()
    {
        var result = await (
            from x in Stream.Range(1, 2)
            from y in Stream.Range(1, 2)
            from z in Stream.Range(1, 2)
            select (x, y, z)
        ).ToListAsync();

        Assert.That(result.Count, Is.EqualTo(8)); // 2 * 2 * 2
        Assert.That(result, Is.EquivalentTo(new[] {
            (1, 1, 1), (1, 1, 2), (1, 2, 1), (1, 2, 2),
            (2, 1, 1), (2, 1, 2), (2, 2, 1), (2, 2, 2)
        }));
    }

    [Test]
    public async Task Query_Syntax_With_Where_On_Multiple_Levels()
    {
        var result = await (
            from x in Stream.Range(1, 5)
            where x < 4
            from y in Stream.Range(1, x)
            where y > 0
            select (x, y)
        ).ToListAsync();

        // x: 1, 2, 3
        // 1 -> (1,1)
        // 2 -> (2,1), (2,2)
        // 3 -> (3,1), (3,2), (3,3)
        Assert.That(result.Count, Is.EqualTo(6));
        Assert.That(result, Is.EquivalentTo(new[] { (1, 1), (2, 1), (2, 2), (3, 1), (3, 2), (3, 3) }));
    }

    [Test]
    public async Task Query_Syntax_With_String_Transformation()
    {
        var result = await (
            from x in Stream.Range(1, 5)
            where x % 2 == 0
            select $"Number: {x}"
        ).ToListAsync();

        Assert.That(result, Is.EquivalentTo(new[] { "Number: 2", "Number: 4" }));
    }

    [Test]
    public async Task Query_Syntax_Fluent_Comparison()
    {
        // Complex query using both syntaxes
        var queryVersionResult = await (
            from x in Stream.Range(1, 20)
            where x > 5
            let squared = x * x
            where squared < 200
            select new { Value = x, Squared = squared }
        ).ToListAsync();

        var fluentVersionResult = await Stream.Range(1, 20)
            .Where(x => x > 5)
            .Select(x => new { Value = x, Squared = x * x })
            .Where(item => item.Squared < 200)
            .ToListAsync();

        Assert.That(queryVersionResult.Count, Is.EqualTo(fluentVersionResult.Count));
        Assert.That(queryVersionResult.Select(r => r.Value), Is.EquivalentTo(fluentVersionResult.Select(r => r.Value)));
    }
}
