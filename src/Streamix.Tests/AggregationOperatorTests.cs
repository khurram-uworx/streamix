using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class AggregationOperatorTests
{
    [Test]
    public async Task Merge_Combines_Multiple_Streams()
    {
        var s1 = Stream.Range(1, 3); // 1, 2, 3
        var s2 = Stream.Range(10, 3); // 10, 11, 12

        var merged = Stream.Merge(s1, s2);
        var result = new List<int>();
        await foreach (var item in merged)
        {
            result.Add(item);
        }

        Assert.That(result.Count, Is.EqualTo(6));
        Assert.That(result, Contains.Item(1));
        Assert.That(result, Contains.Item(2));
        Assert.That(result, Contains.Item(3));
        Assert.That(result, Contains.Item(10));
        Assert.That(result, Contains.Item(11));
        Assert.That(result, Contains.Item(12));
    }

    [Test]
    public async Task Merge_Handles_Empty_Streams()
    {
        var s1 = Stream.Empty<int>();
        var s2 = Stream.Empty<int>();

        var merged = Stream.Merge(s1, s2);
        var result = new List<int>();
        await foreach (var item in merged)
        {
            result.Add(item);
        }

        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task Merge_Handles_Mixed_Empty_And_NonEmpty()
    {
        var s1 = Stream.Range(1, 3);
        var s2 = Stream.Empty<int>();

        var merged = Stream.Merge(s1, s2);
        var result = new List<int>();
        await foreach (var item in merged)
        {
            result.Add(item);
        }

        Assert.That(result.Count, Is.EqualTo(3));
        Assert.That(result, Is.EquivalentTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void Merge_Propagates_Exceptions()
    {
        async IAsyncEnumerable<int> FaultySource()
        {
            yield return 1;
            throw new InvalidOperationException("Faulty stream");
        }

        var s1 = Stream.From(FaultySource());
        var s2 = Stream.Range(10, 3);

        var merged = Stream.Merge(s1, s2);

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in merged)
            {
            }
        });
    }

    [Test]
    public void Merge_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var s1 = Stream.Range(1, 100);
        var s2 = Stream.Range(100, 100);

        var merged = Stream.Merge(s1, s2);

        Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await foreach (var item in merged.WithCancellation(cts.Token))
            {
                cts.Cancel();
            }
        });
    }

    [Test]
    public async Task MergeWith_Works_As_Instance_Method()
    {
        var s1 = Stream.Range(1, 2);
        var s2 = Stream.Range(10, 2);

        var merged = s1.MergeWith(s2);
        var result = new List<int>();
        await foreach (var item in merged)
        {
            result.Add(item);
        }

        Assert.That(result.Count, Is.EqualTo(4));
        Assert.That(result, Is.EquivalentTo(new[] { 1, 2, 10, 11 }));
    }

    [Test]
    public async Task Zip_Combines_Streams_Of_Same_Length()
    {
        var s1 = Stream.Range(1, 3);
        var s2 = Stream.Range(10, 3);

        var zipped = Stream.Zip(s1, s2, (a, b) => a + b);
        var result = new List<int>();
        await foreach (var item in zipped)
        {
            result.Add(item);
        }

        Assert.That(result, Is.EqualTo(new[] { 11, 13, 15 }));
    }

    [Test]
    public async Task Zip_Completes_When_First_Stream_Completes()
    {
        var s1 = Stream.Range(1, 2);
        var s2 = Stream.Range(10, 5);

        var zipped = Stream.Zip(s1, s2, (a, b) => a + b);
        var result = new List<int>();
        await foreach (var item in zipped)
        {
            result.Add(item);
        }

        Assert.That(result, Is.EqualTo(new[] { 11, 13 }));
    }

    [Test]
    public async Task Zip_Completes_When_Second_Stream_Completes()
    {
        var s1 = Stream.Range(1, 5);
        var s2 = Stream.Range(10, 2);

        var zipped = Stream.Zip(s1, s2, (a, b) => a + b);
        var result = new List<int>();
        await foreach (var item in zipped)
        {
            result.Add(item);
        }

        Assert.That(result, Is.EqualTo(new[] { 11, 13 }));
    }

    [Test]
    public async Task Zip_Handles_Empty_Streams()
    {
        var s1 = Stream.Empty<int>();
        var s2 = Stream.Range(1, 5);

        var zipped = Stream.Zip(s1, s2, (a, b) => a + b);
        var result = new List<int>();
        await foreach (var item in zipped)
        {
            result.Add(item);
        }

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Zip_Propagates_Exceptions()
    {
        async IAsyncEnumerable<int> FaultySource()
        {
            yield return 1;
            throw new InvalidOperationException("Faulty stream");
        }

        var s1 = Stream.From(FaultySource());
        var s2 = Stream.Range(10, 3);

        var zipped = Stream.Zip(s1, s2, (a, b) => a + b);

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in zipped)
            {
            }
        });
    }

    [Test]
    public void Zip_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        var s1 = Stream.Range(1, 100);
        var s2 = Stream.Range(100, 100);

        var zipped = Stream.Zip(s1, s2, (a, b) => a + b);

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var item in zipped.WithCancellation(cts.Token))
            {
                cts.Cancel();
            }
        });
    }

    [Test]
    public async Task ZipWith_Works_As_Instance_Method()
    {
        var s1 = Stream.Range(1, 2);
        var s2 = Stream.Range(10, 2);

        var zipped = s1.ZipWith(s2, (a, b) => a + b);
        var result = new List<int>();
        await foreach (var item in zipped)
        {
            result.Add(item);
        }

        Assert.That(result, Is.EqualTo(new[] { 11, 13 }));
    }

    [Test]
    public async Task SingleAsync_Returns_Element_When_One_Element_Exists()
    {
        var result = await Stream.Range(1, 1).SingleAsync(); // Just 1
        Assert.That(result, Is.EqualTo(1));
    }

    [Test]
    public void SingleAsync_Throws_When_Empty()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Stream.Empty<int>().SingleAsync());
        Assert.That(ex?.Message, Is.EqualTo("Sequence contains no elements."));
    }

    [Test]
    public void SingleAsync_Throws_When_Multiple_Elements()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Stream.Range(1, 2).SingleAsync()); // 1, 2
        Assert.That(ex?.Message, Is.EqualTo("Sequence contains more than one element."));
    }

    [Test]
    public async Task SingleOrDefaultAsync_Returns_Element_When_One_Element_Exists()
    {
        var result = await Stream.Range(1, 1).SingleOrDefaultAsync();
        Assert.That(result, Is.EqualTo(1));
    }

    [Test]
    public async Task SingleOrDefaultAsync_Returns_Default_When_Empty()
    {
        var result = await Stream.Empty<int>().SingleOrDefaultAsync();
        Assert.That(result, Is.EqualTo(0));
    }

    [Test]
    public void SingleOrDefaultAsync_Throws_When_Multiple_Elements()
    {
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Stream.Range(1, 2).SingleOrDefaultAsync());
        Assert.That(ex?.Message, Is.EqualTo("Sequence contains more than one element."));
    }

    [Test]
    public void SingleAsync_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await Stream.Range(1, 1).SingleAsync(cts.Token));
    }
}
