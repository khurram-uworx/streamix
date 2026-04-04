using NUnit.Framework;

namespace Streamix.Tests;

public class TestSubscriber<T>
{
    private readonly List<T> items = new();
    private Exception? error;
    private bool completed;

    public IReadOnlyList<T> Items => items;
    public Exception? Error => error;
    public bool Completed => completed;

    public static async Task<TestSubscriber<T>> SubscribeAsync(IStream<T> stream, CancellationToken ct = default)
    {
        var subscriber = new TestSubscriber<T>();
        await subscriber.RunAsync(stream, ct);
        return subscriber;
    }

    public static async Task<TestSubscriber<T>> SubscribeAsync(ISingle<T> single, CancellationToken ct = default)
    {
        var subscriber = new TestSubscriber<T>();
        await subscriber.RunAsync(single, ct);
        return subscriber;
    }

    public async Task RunAsync(IAsyncEnumerable<T> source, CancellationToken ct)
    {
        try
        {
            await foreach (var item in source.WithCancellation(ct))
            {
                items.Add(item);
            }
            completed = true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal cancellation, not recorded as error unless specifically needed
        }
        catch (Exception ex)
        {
            error = ex;
        }
    }

    public TestSubscriber<T> AssertValues(params T[] expectedValues)
    {
        Assert.That(items, Is.EqualTo(expectedValues));
        return this;
    }

    public TestSubscriber<T> AssertValueCount(int count)
    {
        Assert.That(items, Has.Count.EqualTo(count));
        return this;
    }

    public TestSubscriber<T> AssertComplete()
    {
        Assert.That(completed, Is.True, "Stream should have completed");
        Assert.That(error, Is.Null, "Stream should not have errored");
        return this;
    }

    public TestSubscriber<T> AssertError<TException>(Action<TException>? assert = null) where TException : Exception
    {
        Assert.That(error, Is.Not.Null, "Stream should have errored");
        Assert.That(error, Is.InstanceOf<TException>());
        assert?.Invoke((TException)error!);
        return this;
    }

    public TestSubscriber<T> AssertNoError()
    {
        Assert.That(error, Is.Null, $"Stream should not have errored: {error?.Message}");
        return this;
    }

    public TestSubscriber<T> AssertNotComplete()
    {
        Assert.That(completed, Is.False, "Stream should not have completed");
        return this;
    }
}
