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
}
