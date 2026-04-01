using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

static class AsyncEnumerableExtensions
{
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source)
            list.Add(item);
        return list;
    }
}

[TestFixture]
public class BatchOperatorTests
{
    [Test]
    public async Task Buffer_Exact_Division()
    {
        var result = await Stream.Range(1, 4).Buffer(2).Map(list => string.Join(",", list)).ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { "1,2", "3,4" }));
    }

    [Test]
    public async Task Buffer_Trailing_Remainder()
    {
        var result = await Stream.Range(1, 5).Buffer(2).Map(list => string.Join(",", list)).ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { "1,2", "3,4", "5" }));
    }

    [Test]
    public async Task Buffer_Empty_Source()
    {
        var result = await Stream.Empty<int>().Buffer(2).ToListAsync();
        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Buffer_Throws_When_Cancelled()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 10).Buffer(2);

        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in stream.WithCancellation(cts.Token)) { }
        });
    }

    [Test]
    public async Task Window_Exact_Division()
    {
        var windows = await Stream.Range(1, 4).Window(2).ToListAsync();
        Assert.That(windows.Count, Is.EqualTo(2));

        var w1Task = windows[0].ToListAsync();
        var w2Task = windows[1].ToListAsync();

        Assert.That(await w1Task, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(await w2Task, Is.EqualTo(new[] { 3, 4 }));
    }

    [Test]
    public async Task Window_Trailing_Remainder()
    {
        var windows = await Stream.Range(1, 5).Window(2).ToListAsync();
        Assert.That(windows.Count, Is.EqualTo(3));

        var w1Task = windows[0].ToListAsync();
        var w2Task = windows[1].ToListAsync();
        var w3Task = windows[2].ToListAsync();

        Assert.That(await w1Task, Is.EqualTo(new[] { 1, 2 }));
        Assert.That(await w2Task, Is.EqualTo(new[] { 3, 4 }));
        Assert.That(await w3Task, Is.EqualTo(new[] { 5 }));
    }

    [Test]
    public async Task Window_Empty_Source()
    {
        var result = await Stream.Empty<int>().Window(2).ToListAsync();
        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Window_Throws_When_Cancelled()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 10).Window(2);

        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in stream.WithCancellation(cts.Token)) { }
        });
    }
}
