using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

[TestFixture]
public class TransformationOperatorTests
{
    [Test]
    public async Task Map_Transforms_Elements()
    {
        var result = await Stream.Range(1, 3)
            .Map(x => x * 10)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task Select_Is_Alias_For_Map()
    {
        var result = await Stream.Range(1, 3)
            .Select(x => x * 10)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task Filter_Filters_Elements()
    {
        var result = await Stream.Range(1, 5)
            .Filter(x => x % 2 == 0)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 2, 4 }));
    }

    [Test]
    public async Task MapAwait_Transforms_Elements_Asynchronously()
    {
        var result = await Stream.Range(1, 3)
            .MapAwait(async x =>
            {
                await Task.Yield();
                return x * 10;
            })
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 10, 20, 30 }));
    }

    [Test]
    public async Task FilterAwait_Filters_Elements_Asynchronously()
    {
        var result = await Stream.Range(1, 5)
            .FilterAwait(async x =>
            {
                await Task.Yield();
                return x % 2 == 0;
            })
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 2, 4 }));
    }

    [Test]
    public async Task Where_Is_Alias_For_Filter()
    {
        var result = await Stream.Range(1, 5)
            .Where(x => x % 2 == 0)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 2, 4 }));
    }

    [Test]
    public async Task Take_Returns_Specified_Number_Of_Elements()
    {
        var result = await Stream.Range(1, 10)
            .Take(3)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task Take_Returns_All_Elements_If_Count_Greater_Than_Length()
    {
        var result = await Stream.Range(1, 3)
            .Take(5)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task Skip_Bypasses_Specified_Number_Of_Elements()
    {
        var result = await Stream.Range(1, 5)
            .Skip(2)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { 3, 4, 5 }));
    }

    [Test]
    public async Task Skip_Returns_Empty_If_Count_Greater_Than_Length()
    {
        var result = await Stream.Range(1, 3)
            .Skip(5)
            .ToListAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task Chained_Operators_Work_Correctly()
    {
        var result = await Stream.Range(1, 10)
            .Filter(x => x % 2 != 0)
            .Map(x => x * 10)
            .Skip(1)
            .Take(2)
            .ToListAsync();

        // 1, 3, 5, 7, 9 -> 10, 30, 50, 70, 90 -> 30, 50, 70, 90 -> 30, 50
        Assert.That(result, Is.EqualTo(new[] { 30, 50 }));
    }

    [Test]
    public void Map_Propagates_Exceptions()
    {
        var stream = Stream.Range(1, 5).Map(x =>
        {
            if (x == 3) throw new InvalidOperationException("Oops");
            return x;
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public void Filter_Propagates_Exceptions()
    {
        var stream = Stream.Range(1, 5).Filter(x =>
        {
            if (x == 3) throw new InvalidOperationException("Oops");
            return true;
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public async Task Operators_Handle_Empty_Source()
    {
        var result = await Stream.Empty<int>()
            .Map(x => x * 10)
            .Filter(x => true)
            .Take(5)
            .Skip(2)
            .ToListAsync();

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Take_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var stream = Stream.Range(1, 100).Take(50);

        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in stream.WithCancellation(cts.Token)) { }
        });
    }
}

internal static class StreamExtensions
{
    public static async Task<List<T>> ToListAsync<T>(this IStream<T> stream, CancellationToken cancellationToken = default)
    {
        var list = new List<T>();
        await foreach (var item in stream.WithCancellation(cancellationToken))
            list.Add(item);

        return list;
    }
}
