using NUnit.Framework;
using Streamix;

namespace Streamix.Tests;

[TestFixture]
public class TerminalExtensionsTests
{
    // Materialization Tests

    [Test]
    public async Task ToHashSetAsync_With_Comparer_Works()
    {
        var input = new[] { "a", "A", "b" };
        var stream = Stream.From(input.ToAsyncEnumerable());

        var set = await stream.ToHashSetAsync(StringComparer.OrdinalIgnoreCase);

        Assert.That(set.Count, Is.EqualTo(2));
        Assert.That(set.Contains("a"), Is.True);
        Assert.That(set.Contains("b"), Is.True);
    }

    [Test]
    public async Task ToDictionaryAsync_Throws_On_Duplicate_Key()
    {
        var input = new[] { 1, 2, 3 };
        var stream = Stream.From(input.ToAsyncEnumerable());

        // Both 1 and 3 map to "odd"
        Assert.ThrowsAsync<ArgumentException>(async () =>
            await stream.ToDictionaryAsync(x => x % 2 == 0 ? "even" : "odd"));
    }

    [Test]
    public async Task ToDictionaryAsync_With_ValueSelector_Throws_On_Duplicate_Key()
    {
        var input = new[] { 1, 2, 3 };
        var stream = Stream.From(input.ToAsyncEnumerable());

        Assert.ThrowsAsync<ArgumentException>(async () =>
            await stream.ToDictionaryAsync(x => x % 2 == 0 ? "even" : "odd", x => x * 10));
    }

    [Test]
    public async Task ToDictionaryAsync_With_Comparer_Works()
    {
        var input = new[] { "a", "b" };
        var stream = Stream.From(input.ToAsyncEnumerable());

        var dict = await stream.ToDictionaryAsync(x => x, StringComparer.OrdinalIgnoreCase);

        Assert.That(dict.ContainsKey("A"), Is.True);
        Assert.That(dict["A"], Is.EqualTo("a"));
    }

    [Test]
    public async Task ToDictionaryAsync_Empty_Stream_Returns_Empty_Dictionary()
    {
        var stream = Stream.Empty<int>();
        var dict = await stream.ToDictionaryAsync<int, int>(x => x, (IEqualityComparer<int>?)null);
        Assert.That(dict.Count, Is.EqualTo(0));
    }

    [Test]
    public void ToDictionaryAsync_Propagates_Upstream_Exception()
    {
        var stream = Stream.Error<int>(new InvalidOperationException("Upstream failure"));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToDictionaryAsync<int, int>(x => x, (IEqualityComparer<int>?)null));
    }

    [Test]
    public void ToDictionaryAsync_Propagates_KeySelector_Exception()
    {
        var stream = Stream.Range(1, 5);
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await stream.ToDictionaryAsync<int, int>(x => throw new InvalidOperationException("Selector failure"), (IEqualityComparer<int>?)null));
    }

    [Test]
    public void ToDictionaryAsync_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 100).DoOnNext(x => { if (x == 5) cts.Cancel(); });

        Assert.CatchAsync<OperationCanceledException>(async () =>
            await stream.ToDictionaryAsync<int, int>(x => x, (IEqualityComparer<int>?)null, cts.Token));
    }

    // LINQ Terminals with Predicates Tests

    [Test]
    public async Task FirstAsync_With_Predicate_Returns_Correct_Element()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.FirstAsync(x => x > 2);
        Assert.That(result, Is.EqualTo(3));
    }

    [Test]
    public async Task FirstAsync_With_Predicate_Throws_If_No_Match()
    {
        var stream = Stream.Range(1, 5);
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.FirstAsync(x => x > 10));
    }

    [Test]
    public async Task FirstOrDefaultAsync_With_Predicate_Returns_Correct_Element()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.FirstOrDefaultAsync(x => x > 2);
        Assert.That(result, Is.EqualTo(3));
    }

    [Test]
    public async Task FirstOrDefaultAsync_With_Predicate_Returns_Default_If_No_Match()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.FirstOrDefaultAsync(x => x > 10);
        Assert.That(result, Is.EqualTo(0));
    }

    [Test]
    public async Task LastAsync_With_Predicate_Returns_Correct_Element()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.LastAsync(x => x < 4);
        Assert.That(result, Is.EqualTo(3));
    }

    [Test]
    public async Task LastOrDefaultAsync_With_Predicate_Returns_Correct_Element()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.LastOrDefaultAsync(x => x < 4);
        Assert.That(result, Is.EqualTo(3));
    }

    [Test]
    public async Task SingleAsync_With_Predicate_Returns_Correct_Element()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.SingleAsync(x => x == 3);
        Assert.That(result, Is.EqualTo(3));
    }

    [Test]
    public async Task SingleAsync_With_Predicate_Throws_If_Multiple_Matches()
    {
        var stream = Stream.Range(1, 5);
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.SingleAsync(x => x > 2));
    }

    [Test]
    public async Task SingleOrDefaultAsync_With_Predicate_Returns_Correct_Element()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.SingleOrDefaultAsync(x => x == 3);
        Assert.That(result, Is.EqualTo(3));
    }

    // Single-Value Semantics Tests

    [Test]
    public async Task ElementAtAsync_Returns_Correct_Element()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.ElementAtAsync(2);
        Assert.That(result, Is.EqualTo(3));
    }

    [Test]
    public async Task ElementAtAsync_Throws_If_Out_Of_Range()
    {
        var stream = Stream.Range(1, 3);
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ElementAtAsync(5));
    }

    [Test]
    public async Task ElementAtAsync_Throws_If_Index_Negative()
    {
        var stream = Stream.Range(1, 3);
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await stream.ElementAtAsync(-1));
    }

    [Test]
    public async Task ElementAtOrDefaultAsync_Returns_Correct_Element()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.ElementAtOrDefaultAsync(2);
        Assert.That(result, Is.EqualTo(3));
    }

    [Test]
    public async Task ElementAtOrDefaultAsync_Returns_Default_If_Out_Of_Range()
    {
        var stream = Stream.Range(1, 3);
        var result = await stream.ElementAtOrDefaultAsync(5);
        Assert.That(result, Is.EqualTo(0));
    }

    [Test]
    public void ElementAtAsync_Propagates_Upstream_Exception()
    {
        var stream = Stream.Error<int>(new InvalidOperationException("Upstream failure"));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ElementAtAsync(0));
    }

    [Test]
    public void ElementAtAsync_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 100).DoOnNext(x => { if (x == 5) cts.Cancel(); });

        Assert.CatchAsync<OperationCanceledException>(async () =>
            await stream.ElementAtAsync(10, cts.Token));
    }

    [Test]
    public void ElementAtOrDefaultAsync_Propagates_Upstream_Exception()
    {
        var stream = Stream.Error<int>(new InvalidOperationException("Upstream failure"));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ElementAtOrDefaultAsync(0));
    }

    [Test]
    public void ElementAtOrDefaultAsync_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 100).DoOnNext(x => { if (x == 5) cts.Cancel(); });

        Assert.CatchAsync<OperationCanceledException>(async () =>
            await stream.ElementAtOrDefaultAsync(10, cts.Token));
    }

    [Test]
    public async Task ElementAtOrDefaultAsync_Returns_Default_If_Index_Negative()
    {
        var stream = Stream.Range(1, 3);
        var result = await stream.ElementAtOrDefaultAsync(-1);
        Assert.That(result, Is.EqualTo(0));
    }

    // Try-style (Non-throwing) Tests

    [Test]
    public async Task FirstOrNoneAsync_Returns_Some_If_NotEmpty()
    {
        var stream = Stream.Range(1, 3);
        var result = await stream.FirstOrNoneAsync();
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value, Is.EqualTo(1));
    }

    [Test]
    public async Task FirstOrNoneAsync_With_Predicate_Returns_Some_If_Match()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.FirstOrNoneAsync(x => x > 2);
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value, Is.EqualTo(3));
    }

    [Test]
    public async Task FirstOrNoneAsync_Returns_None_If_Empty()
    {
        var stream = Stream.Empty<int>();
        var result = await stream.FirstOrNoneAsync();
        Assert.That(result.HasValue, Is.False);
    }

    [Test]
    public async Task LastOrNoneAsync_Returns_Some_If_NotEmpty()
    {
        var stream = Stream.Range(1, 3);
        var result = await stream.LastOrNoneAsync();
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value, Is.EqualTo(3));
    }

    [Test]
    public async Task LastOrNoneAsync_With_Predicate_Returns_Some_If_Match()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.LastOrNoneAsync(x => x < 4);
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value, Is.EqualTo(3));
    }

    [Test]
    public async Task LastOrNoneAsync_Returns_None_If_Empty()
    {
        var stream = Stream.Empty<int>();
        var result = await stream.LastOrNoneAsync();
        Assert.That(result.HasValue, Is.False);
    }

    [Test]
    public async Task SingleOrNoneAsync_Returns_Some_If_OneElement()
    {
        var stream = Stream.Just(1);
        var result = await stream.SingleOrNoneAsync();
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value, Is.EqualTo(1));
    }

    [Test]
    public async Task SingleOrNoneAsync_With_Predicate_Returns_Some_If_OneMatch()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.SingleOrNoneAsync(x => x == 3);
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value, Is.EqualTo(3));
    }

    [Test]
    public async Task SingleOrNoneAsync_Returns_None_If_Empty()
    {
        var stream = Stream.Empty<int>();
        var result = await stream.SingleOrNoneAsync();
        Assert.That(result.HasValue, Is.False);
    }

    [Test]
    public async Task SingleOrNoneAsync_Returns_None_If_MoreThanOneElement()
    {
        var stream = Stream.Range(1, 2);
        var result = await stream.SingleOrNoneAsync();
        Assert.That(result.HasValue, Is.False);
    }

    [Test]
    public async Task SingleOrNoneAsync_With_Predicate_Returns_None_If_MoreThanOneMatch()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.SingleOrNoneAsync(x => x > 2);
        Assert.That(result.HasValue, Is.False);
    }

    [Test]
    public async Task ElementAtOrNoneAsync_Returns_Some_If_In_Range()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.ElementAtOrNoneAsync(2);
        Assert.That(result.HasValue, Is.True);
        Assert.That(result.Value, Is.EqualTo(3));
    }

    [Test]
    public async Task ElementAtOrNoneAsync_Returns_None_If_Out_Of_Range()
    {
        var stream = Stream.Range(1, 3);
        var result = await stream.ElementAtOrNoneAsync(5);
        Assert.That(result.HasValue, Is.False);
    }

    // Reduction Beyond Aggregate Tests

    [Test]
    public async Task ReduceAsync_Works()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.ReduceAsync((acc, x) => acc + x);
        Assert.That(result, Is.EqualTo(15));
    }

    [Test]
    public async Task ScanLastAsync_Works()
    {
        var stream = Stream.Range(1, 5);
        var result = await stream.ScanLastAsync(10, (acc, x) => acc + x);
        Assert.That(result, Is.EqualTo(25));
    }

    [Test]
    public async Task MaxByAsync_Works()
    {
        var stream = Stream.From(new[] { (1, "a"), (3, "c"), (2, "b") }.ToAsyncEnumerable());
        var result = await stream.MaxByAsync(x => x.Item1);
        Assert.That(result, Is.EqualTo((3, "c")));
    }

    [Test]
    public async Task MinByAsync_Works()
    {
        var stream = Stream.From(new[] { (1, "a"), (3, "c"), (2, "b") }.ToAsyncEnumerable());
        var result = await stream.MinByAsync(x => x.Item1);
        Assert.That(result, Is.EqualTo((1, "a")));
    }

    [Test]
    public void MaxByAsync_Empty_Stream_Throws_InvalidOperationException()
    {
        var stream = Stream.Empty<int>();
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.MaxByAsync(x => x));
    }

    [Test]
    public void MaxByAsync_Propagates_Upstream_Exception()
    {
        var stream = Stream.Error<int>(new InvalidOperationException("Upstream failure"));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.MaxByAsync(x => x));
    }

    [Test]
    public void MaxByAsync_Propagates_Selector_Exception()
    {
        var stream = Stream.Range(1, 5);
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await stream.MaxByAsync<int, int>(x => throw new InvalidOperationException("Selector failure")));
    }

    [Test]
    public void MaxByAsync_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 100).DoOnNext(x => { if (x == 5) cts.Cancel(); });

        Assert.CatchAsync<OperationCanceledException>(async () =>
            await stream.MaxByAsync(x => x, cts.Token));
    }

    [Test]
    public void MinByAsync_Empty_Stream_Throws_InvalidOperationException()
    {
        var stream = Stream.Empty<int>();
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.MinByAsync(x => x));
    }

    [Test]
    public void MinByAsync_Propagates_Upstream_Exception()
    {
        var stream = Stream.Error<int>(new InvalidOperationException("Upstream failure"));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.MinByAsync(x => x));
    }

    [Test]
    public void MinByAsync_Propagates_Selector_Exception()
    {
        var stream = Stream.Range(1, 5);
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await stream.MinByAsync<int, int>(x => throw new InvalidOperationException("Selector failure")));
    }

    [Test]
    public void MinByAsync_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 100).DoOnNext(x => { if (x == 5) cts.Cancel(); });

        Assert.CatchAsync<OperationCanceledException>(async () =>
            await stream.MinByAsync(x => x, cts.Token));
    }

    // Bridging Terminals Tests

    [Test]
    public async Task ToChannel_Works()
    {
        var stream = Stream.Range(1, 5);
        var reader = stream.ToChannel();
        var list = new List<int>();
        await foreach (var item in reader.ReadAllAsync())
        {
            list.Add(item);
        }
        Assert.That(list, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void ToEnumerableBlocking_Works()
    {
        var stream = Stream.Range(1, 5);
        var result = stream.ToEnumerableBlocking().ToList();
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task AsAsyncEnumerable_Works()
    {
        var stream = Stream.Range(1, 3);
        IAsyncEnumerable<int> asyncEnumerable = stream.AsAsyncEnumerable();
        var list = new List<int>();
        await foreach (var item in asyncEnumerable)
        {
            list.Add(item);
        }
        Assert.That(list, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    // Consumption Control Tests

    [Test]
    public async Task ForEachAsync_WithConcurrency_Works()
    {
        var stream = Stream.Range(1, 10);
        var processed = new List<int>();
        await stream.ForEachAsync(async x =>
        {
            await Task.Delay(10);
            lock (processed) processed.Add(x);
        }, maxConcurrency: 3);

        Assert.That(processed.Count, Is.EqualTo(10));
        Assert.That(processed, Is.EquivalentTo(Enumerable.Range(1, 10)));
    }

    // Subscription-style Terminals Tests

    [Test]
    public async Task SubscribeAsync_Works()
    {
        var stream = Stream.Range(1, 3);
        var items = new List<int>();
        bool completed = false;

        await stream.SubscribeAsync(
            onNext: async x => { items.Add(x); await Task.Yield(); },
            onComplete: async () => { completed = true; await Task.Yield(); }
        );

        Assert.That(items, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(completed, Is.True);
    }

    [Test]
    public async Task SubscribeAsync_HandlesError()
    {
        var stream = Stream.Error<int>(new Exception("test"));
        Exception? caught = null;

        await stream.SubscribeAsync(
            onNext: x => Task.CompletedTask,
            onError: async ex => { caught = ex; await Task.Yield(); }
        );

        Assert.That(caught?.Message, Is.EqualTo("test"));
    }

    // Drain / Ignore Tests

    [Test]
    public async Task DrainAsync_Works()
    {
        var count = 0;
        var stream = Stream.Range(1, 5).DoOnNext(_ => count++);
        await stream.DrainAsync();
        Assert.That(count, Is.EqualTo(5));
    }

    [Test]
    public async Task DrainAsync_Empty_Stream_Works()
    {
        var count = 0;
        var stream = Stream.Empty<int>().DoOnNext(_ => count++);
        await stream.DrainAsync();
        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public void DrainAsync_Propagates_Upstream_Exception()
    {
        var stream = Stream.Error<int>(new InvalidOperationException("Upstream failure"));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.DrainAsync());
    }

    [Test]
    public void DrainAsync_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 100).DoOnNext(x => { if (x == 5) cts.Cancel(); });

        Assert.CatchAsync<OperationCanceledException>(async () =>
            await stream.DrainAsync(cts.Token));
    }

    // Diagnostics-aware Terminals Tests

    [Test]
    public async Task ExecuteAsync_Works_Success()
    {
        var stream = Stream.Range(1, 3);
        var result = await stream.ExecuteAsync();

        Assert.That(result.Items, Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(result.Completed, Is.True);
        Assert.That(result.Error, Is.Null);
        Assert.That(result.Duration, Is.GreaterThan(TimeSpan.Zero));
    }

    [Test]
    public async Task ExecuteAsync_Works_Error()
    {
        var stream = Stream.Range(1, 2).MergeWith(Stream.Error<int>(new Exception("test")));
        var result = await stream.ExecuteAsync();

        Assert.That(result.Items, Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(result.Completed, Is.False);
        Assert.That(result.Error?.Message, Is.EqualTo("test"));
    }
}
