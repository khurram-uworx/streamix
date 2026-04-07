using Streamix.Implementations;

using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class BackpressureTests
{
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

        Assert.That(ex.Message, Does.Contain("Buffer overflow"));
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
        var connectable = Stream.Range(1, 10).Publish();
        var stream = connectable.OnBackpressureBuffer(10);

        var resultsTask = stream.ToListAsync();
        using var connection = connectable.Connect();

        // Act
        var results = await resultsTask;

        // Assert
        Assert.That(results, Is.EqualTo(Enumerable.Range(1, 10)));
    }
}
