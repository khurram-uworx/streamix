using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

[TestFixture]
public class FlatteningOperatorTests
{
    [Test]
    public async Task Stream_FlatMap_WithSingle_Sequential()
    {
        var result = await Stream.Range(1, 3)
            .FlatMap(x => Single.From(x * 10))
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task Stream_FlatMapMany_Sequential()
    {
        var result = await Stream.Range(1, 3)
            .FlatMapMany(x => Stream.Range(x * 10, 2))
            .ToListAsync();

        // 1 -> 10, 11
        // 2 -> 20, 21
        // 3 -> 30, 31
        Assert.That(result, Is.EqualTo(new[] { 10, 11, 20, 21, 30, 31 }));
    }

    [Test]
    public async Task Stream_FlatMap_WithTask_Sequential()
    {
        var result = await Stream.Range(1, 3)
            .FlatMap(async x =>
            {
                await Task.Delay(10);
                return x * 10;
            }, maxConcurrency: 1)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task Stream_FlatMap_WithTask_Concurrent()
    {
        var result = await Stream.Range(1, 5)
            .FlatMap(async x =>
            {
                // Delays are such that they might finish out of order if concurrent
                await Task.Delay(100 - (x * 10));
                return x * 10;
            }, maxConcurrency: 5)
            .ToListAsync();

        // Since it's concurrent, we don't guarantee order, but we expect all elements
        Assert.That(result, Has.Count.EqualTo(5));
        Assert.That(result, Is.EquivalentTo(new[] { 10, 20, 30, 40, 50 }));

        // With these delays, 50 (delay 50) should likely come before 10 (delay 90)
        // But we just want to ensure it works.
    }

    [Test]
    public async Task Single_FlatMap_ReturnsSingle()
    {
        var result = await Single.From(1)
            .FlatMap(x => Single.From(x * 10))
            .ToTask();

        Assert.That(result, Is.EqualTo(10));
    }

    [Test]
    public async Task Single_FlatMapMany_ReturnsStream()
    {
        var result = await Single.From(1)
            .FlatMapMany(x => Stream.Range(x * 10, 3))
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 11, 12 }));
    }

    [Test]
    public async Task Readme_AsyncComposition_Example()
    {
        // Mocking the README example
        Func<int, ISingle<string>> GetUser = id => Single.From($"User{id}");
        Func<string, IStream<int>> GetOrders = user => Stream.Range(1, 2); // Orders 1, 2

        var orders = await GetUser(1)
            .FlatMapMany(user => GetOrders(user))
            .FlatMapMany(o => Stream.From(new[] { o }.ToAsyncEnumerable())) // Using an internal helper or similar
            .ToListAsync();

        Assert.That(orders, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public async Task FlatMap_Handles_Empty_Inner()
    {
        var result = await Stream.Range(1, 3)
            .FlatMapMany(x => x == 2 ? Stream.Empty<int>() : Stream.Range(x, 1))
            .ToListAsync();

        // 1 -> [1]
        // 2 -> []
        // 3 -> [3]
        Assert.That(result, Is.EqualTo(new[] { 1, 3 }));
    }

    [Test]
    public void FlatMap_Propagates_Inner_Failure()
    {
        var stream = Stream.Range(1, 3)
            .FlatMapMany(x => x == 2 ? Stream.From(ThrowError()) : Stream.Range(x, 1));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    private static async IAsyncEnumerable<int> ThrowError()
    {
        yield return 1;
        throw new InvalidOperationException("Inner fail");
    }

    [Test]
    public void FlatMap_Propagates_Outer_Failure()
    {
        var source = Stream.From(ThrowErrorAfter(1));
        var stream = source.FlatMapMany(x => Stream.Range(x, 1));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    private static async IAsyncEnumerable<int> ThrowErrorAfter(int count)
    {
        for (int i = 0; i < count; i++) yield return i;
        throw new InvalidOperationException("Outer fail");
    }

    [Test]
    public void FlatMap_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 100).FlatMapMany(x => Stream.Range(x, 10));

        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in stream.WithCancellation(cts.Token)) { }
        });
    }
}

internal static class TestExtensions
{
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> source)
    {
        foreach (var item in source)
        {
            yield return item;
            await Task.Yield();
        }
    }
}
