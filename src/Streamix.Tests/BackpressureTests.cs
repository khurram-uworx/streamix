using Streamix.Implementations;

using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class BackpressureTests
{
    static async Task<IReadOnlyList<T>> ReadAllAsync<T>(IAsyncEnumerable<T> source, Func<int, Task>? onItem = null, CancellationToken cancellationToken = default)
    {
        var results = new List<T>();
        int index = 0;

        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            results.Add(item);
            if (onItem != null)
            {
                await onItem(index);
            }

            index++;
        }

        return results;
    }

    [Test]
    public async Task OnBackpressureBuffer_HappyPath_BuffersItems()
    {
        // Arrange
        var stream = Stream.Range(1, 10).OnBackpressureBuffer(10);

        // Act
        var results = new List<int>();
        await foreach (var item in stream)
        {
            results.Add(item);
        }

        // Assert
        Assert.That(results, Is.EqualTo(Enumerable.Range(1, 10)));
    }

    [Test]
    public async Task OnBackpressureBuffer_OverflowThrows_BackpressureException()
    {
        // Arrange
        // We need a stream that produces items faster than they are consumed.
        // We use a custom producer to control emission and ensure overflow.
        var stream = Stream.Create<int>(async emitter =>
        {
            for (int i = 0; i < 20; i++)
            {
                await emitter.EmitAsync(i);
            }
        }).OnBackpressureBuffer(5);

        // Act & Assert
        var ex = Assert.ThrowsAsync<BackpressureException>(async () =>
        {
            await foreach (var item in stream)
            {
                // Slow down consumption to trigger overflow
                await Task.Delay(20);
            }
        });

        Assert.That(ex!.Message, Does.Contain("Buffer overflow"));
    }

    [Test]
    public void OnBackpressureBuffer_InvalidCapacity_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var stream = Stream.Range(1, 10);

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => stream.OnBackpressureBuffer(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => stream.OnBackpressureBuffer(-1));
    }

    [Test]
    public async Task OnBackpressureBuffer_Chaining_Works()
    {
        // Arrange
        var stream = Stream.Range(1, 10)
            .OnBackpressureBuffer(20)
            .Map(x => x * 2);

        // Act
        var results = new List<int>();
        await foreach (var item in stream)
        {
            results.Add(item);
        }

        // Assert
        Assert.That(results, Is.EqualTo(Enumerable.Range(1, 10).Select(x => x * 2)));
    }

    [Test]
    public async Task OnBackpressureBuffer_ConnectableStream_HappyPath()
    {
        // Arrange
        var connectable = Stream.Range(1, 10).Replay(10);
        var stream = connectable.OnBackpressureBuffer(10);

        var resultsTask = stream.ToListAsync();
        using var connection = connectable.Connect();

        // Act
        var results = await resultsTask;

        // Assert
        Assert.That(results, Is.EqualTo(Enumerable.Range(1, 10)));
    }

    [Test]
    public async Task OnBackpressureLatest_HappyPath_PreservesLatest()
    {
        // Arrange
        var consumerReceivedFirstTcs = new TaskCompletionSource<bool>();

        var stream = Stream.Create<int>(async (emitter, ct) =>
        {
            // Send first item, which should be received by consumer
            await emitter.EmitAsync(1);

            // Wait until consumer has received the first item and is "busy"
            await consumerReceivedFirstTcs.Task;

            // These should be dropped, except the last one (5)
            await emitter.EmitAsync(2);
            await emitter.EmitAsync(3);
            await emitter.EmitAsync(4);
            await emitter.EmitAsync(5);
        }).OnBackpressureLatest();

        // Act
        var results = new List<int>();
        await foreach (var item in stream)
        {
            results.Add(item);
            if (results.Count == 1)
            {
                consumerReceivedFirstTcs.SetResult(true);
                // Slow down consumer to force backpressure for subsequent items
                await Task.Delay(100);
            }
        }

        // Assert
        // Item 1 is received, then 2, 3, 4 are dropped by the channel (DropOldest with capacity 1),
        // leaving 5 as the latest available.
        Assert.That(results, Is.EqualTo(new[] { 1, 5 }));
    }

    [Test]
    public async Task OnBackpressureLatest_ConnectableStream_HappyPath()
    {
        // Arrange
        var consumerReceivedFirstTcs = new TaskCompletionSource<bool>();

        var connectable = Stream.Create<int>(async (emitter, ct) =>
        {
            // Send first item
            await emitter.EmitAsync(1);

            // Wait until consumer has received the first item
            await consumerReceivedFirstTcs.Task;

            // These should be dropped, except the last one (5)
            await emitter.EmitAsync(2);
            await emitter.EmitAsync(3);
            await emitter.EmitAsync(4);
            await emitter.EmitAsync(5);
        }).Replay(1);

        var stream = connectable.OnBackpressureLatest();

        // Act
        var resultsTask = ReadAllAsync(
            stream,
            async index =>
            {
                if (index == 0)
                {
                    consumerReceivedFirstTcs.SetResult(true);
                    await Task.Delay(100);
                }
            });

        using var connection = connectable.Connect();
        var results = await resultsTask;

        // Assert
        Assert.That(results, Is.EqualTo(new[] { 1, 5 }));
    }

    [Test]
    public async Task OnBackpressureDrop_NoDropsWhenProducerIsSlow()
    {
        // Arrange
        var stream = Stream.Create<int>(async emitter =>
        {
            for (int i = 1; i <= 5; i++)
            {
                await emitter.EmitAsync(i);
                await Task.Delay(100); // Plenty of time for consumer
            }
        }).OnBackpressureDrop();

        // Act
        var results = await stream.ToListAsync();

        // Assert
        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task OnBackpressureDrop_FastProducerSlowConsumer_DropsNewItems()
    {
        // Arrange
        var stream = Stream.Range(1, 500).OnBackpressureDrop();

        // Act
        var results = new List<int>();
        await foreach (var item in stream)
        {
            results.Add(item);
            // Slow down consumer to force drops
            await Task.Delay(20);
        }

        // Assert
        // We should have dropped some items.
        Assert.That(results.Count, Is.LessThan(500));
        // With DropWrite, we should have the earlier items and NOT the last item.
        Assert.That(results.First(), Is.EqualTo(1));
        Assert.That(results, Does.Not.Contain(500));
    }

    [Test]
    public async Task OnBackpressureDrop_ConnectableStream_DropsNewItems()
    {
        // Arrange
        var connectable = Stream.Range(1, 100).Replay(100);
        var stream = connectable.OnBackpressureDrop();

        // Act
        var resultsTask = ReadAllAsync(stream, _ => Task.Delay(50));

        using var connection = connectable.Connect();
        var results = await resultsTask;

        // Assert
        Assert.That(results.Count, Is.LessThan(500));
        Assert.That(results.First(), Is.EqualTo(1));
        Assert.That(results, Does.Not.Contain(500));
    }

    [Test]
    public async Task OnBackpressureError_ThrowsOnBackpressure()
    {
        // Arrange
        var stream = Stream.Create<int>(async emitter =>
        {
            for (int i = 0; i < 20; i++)
            {
                await emitter.EmitAsync(i);
            }
        }).OnBackpressureError();

        // Act & Assert
        var ex = Assert.ThrowsAsync<BackpressureException>(async () =>
        {
            await foreach (var item in stream)
            {
                // Slow down consumption to trigger overflow
                await Task.Delay(50);
            }
        });

        Assert.That(ex!.Message, Does.Contain("Downstream cannot keep pace."));
    }

    [Test]
    public async Task OnBackpressureError_ConnectableStream_ThrowsOnBackpressure()
    {
        // Arrange
        var connectable = Stream.Create<int>(async emitter =>
        {
            for (int i = 0; i < 20; i++)
            {
                await emitter.EmitAsync(i);
            }
        }).Replay(20);

        var stream = connectable.OnBackpressureError();

        // Act & Assert
        var ex = Assert.ThrowsAsync<BackpressureException>(async () =>
        {
            var resultsTask = Task.Run(async () =>
            {
                await foreach (var item in stream)
                {
                    await Task.Delay(50);
                }
            });

            using var connection = connectable.Connect();
            await resultsTask;
        });

        Assert.That(ex!.Message, Does.Contain("Downstream cannot keep pace."));
    }

    [Test]
    public async Task MultipleBackpressureOperators_ComposeRatherThanOverride()
    {
        // Arrange
        // The inner Drop() can discard items before the outer Buffer() sees them.
        // This demonstrates that chained strategies compose as nested operators.
        var stream = Stream.Range(1, 100)
            .OnBackpressureDrop()
            .OnBackpressureBuffer(100);

        // Act
        var results = await ReadAllAsync(stream, _ => Task.Delay(20));

        // Assert
        Assert.That(results.Count, Is.LessThan(500));
        Assert.That(results.First(), Is.EqualTo(1));
        Assert.That(results, Does.Not.Contain(500));
    }

    [Test]
    [TestCase("Buffer")]
    [TestCase("Drop")]
    [TestCase("Latest")]
    [TestCase("Error")]
    public async Task BackpressureStrategies_EmptyStream_HandlesCorrectly(string strategy)
    {
        // Arrange
        var source = Stream.Empty<int>();
        var stream = strategy switch
        {
            "Buffer" => source.OnBackpressureBuffer(10),
            "Drop" => source.OnBackpressureDrop(),
            "Latest" => source.OnBackpressureLatest(),
            "Error" => source.OnBackpressureError(),
            _ => throw new ArgumentException()
        };

        // Act
        var results = await stream.ToListAsync();

        // Assert
        Assert.That(results, Is.Empty);
    }

    [Test]
    [TestCase("Buffer")]
    [TestCase("Drop")]
    [TestCase("Latest")]
    [TestCase("Error")]
    public async Task BackpressureStrategies_SingleItemStream_HandlesCorrectly(string strategy)
    {
        // Arrange
        var source = Stream.Just(1);
        var stream = strategy switch
        {
            "Buffer" => source.OnBackpressureBuffer(10),
            "Drop" => source.OnBackpressureDrop(),
            "Latest" => source.OnBackpressureLatest(),
            "Error" => source.OnBackpressureError(),
            _ => throw new ArgumentException()
        };

        // Act
        var results = await stream.ToListAsync();

        // Assert
        Assert.That(results, Is.EqualTo(new[] { 1 }));
    }
}
