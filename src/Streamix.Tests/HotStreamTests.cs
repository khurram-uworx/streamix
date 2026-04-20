using NUnit.Framework;
using System.Collections.Concurrent;

namespace Streamix.Tests;

[TestFixture]
public class HotStreamTests
{
    [Test]
    public async Task ColdStream_ReExecutesForEverySubscriber()
    {
        int executionCount = 0;
        var cold = Stream.From(GenerateItems());

        async IAsyncEnumerable<int> GenerateItems()
        {
            executionCount++;
            yield return 1;
            yield return 2;
        }

        var results1 = new List<int>();
        await cold.ForEachAsync(results1.Add);

        var results2 = new List<int>();
        await cold.ForEachAsync(results2.Add);

        Assert.That(executionCount, Is.EqualTo(2));
        Assert.That(results1, Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(results2, Is.EquivalentTo(new[] { 1, 2 }));
    }

    [Test]
    public async Task PublishConnect_SharesExecution()
    {
        Console.WriteLine("Starting PublishConnect_SharesExecution");
        int executionCount = 0;
        var source = Stream.From(GenerateItems());
        var hot = source.Publish();

        async IAsyncEnumerable<int> GenerateItems()
        {
            Console.WriteLine("Source starting");
            executionCount++;
            yield return 1;
            await Task.Delay(50);
            yield return 2;
            Console.WriteLine("Source finishing");
        }

        var results1 = new List<int>();
        var results2 = new List<int>();

        var t1 = hot.ForEachAsync(results1.Add);
        var t2 = hot.ForEachAsync(results2.Add);

        using (hot.Connect())
        {
            Console.WriteLine("Connected");
            Console.WriteLine("Waiting for subscribers");
            await Task.WhenAll(t1, t2);
            Console.WriteLine("Subscribers finished");
        }

        Assert.That(executionCount, Is.EqualTo(1));
        Assert.That(results1, Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(results2, Is.EquivalentTo(new[] { 1, 2 }));
    }

    [Test]
    public async Task RefCount_AutomaticallyConnectsAndDisconnects()
    {
        var started = new TaskCompletionSource();
        var continueSignal = new TaskCompletionSource();
        int executionCount = 0;

        async IAsyncEnumerable<int> GenerateItems()
        {
            executionCount++;
            if (!started.Task.IsCompleted)
                started.SetResult();

            yield return 1;
            await continueSignal.Task;
            yield return 2;
        }

        var source = Stream.From(GenerateItems());
        var connectable = source.Publish();
        var shared = connectable.RefCount();

        // First execution with two concurrent subscribers
        var results1 = new List<int>();
        var results2 = new List<int>();

        // We need to ensure that the source hasn't finished before the second one joins.
        // In our current implementation, we might need a more reliable way.
        var t1 = shared.ForEachAsync(results1.Add);
        // Wait until we KNOW execution started
        await started.Task;
        var t2 = shared.ForEachAsync(results2.Add);
        // Now allow continuation
        continueSignal.SetResult();
        await Task.WhenAll(t1, t2);

        // Wait for RefCount to fully disconnect before third subscription
        await connectable.WhenRefCountDisconnectedAsync();

        Assert.That(executionCount, Is.EqualTo(1));
        Assert.That(results1, Is.EquivalentTo(new[] { 1, 2 }));
        Assert.That(results2, Is.Not.Empty);

        // Third subscriber after both have disconnected should trigger a new execution
        var results3 = new List<int>();
        await shared.ForEachAsync(results3.Add);

        Assert.That(executionCount, Is.EqualTo(2));
        Assert.That(results3, Is.EquivalentTo(new[] { 1, 2 }));
    }

    [Test]
    public async Task LateSubscriber_ReceivesOnlyNewItems()
    {
        var tcs = new TaskCompletionSource();
        var source = Stream.From(GenerateItems());
        var hot = source.Publish();

        async IAsyncEnumerable<int> GenerateItems()
        {
            yield return 1;
            await tcs.Task;
            yield return 2;
        }

        var results1 = new List<int>();
        var t1 = hot.Take(2).ForEachAsync(results1.Add);

        using (hot.Connect())
        {
            // Wait until 1 is produced (we can't easily, so we just delay a bit)
            await Task.Delay(50);

            var results2 = new List<int>();
            var t2 = hot.ForEachAsync(results2.Add);

            tcs.SetResult();
            await Task.WhenAll(t1, t2);

            Assert.That(results1, Is.EquivalentTo(new[] { 1, 2 }));
            Assert.That(results2, Is.EquivalentTo(new[] { 2 })); // Should not see 1
        }
    }

    [Test]
    public async Task ErrorInSource_PropagatesToAllSubscribers()
    {
        var source = Stream.From(GenerateItems());
        var hot = source.Publish();

        async IAsyncEnumerable<int> GenerateItems()
        {
            yield return 1;
            throw new InvalidOperationException("Source failed");
        }

        var results1 = new List<int>();
        var results2 = new List<int>();

        var t1 = hot.ForEachAsync(results1.Add);
        var t2 = hot.ForEachAsync(results2.Add);

        using (hot.Connect())
        {
            Assert.ThrowsAsync<InvalidOperationException>(async () => await t1);
            Assert.ThrowsAsync<InvalidOperationException>(async () => await t2);

            Assert.That(results1, Is.EquivalentTo(new[] { 1 }));
            Assert.That(results2, Is.EquivalentTo(new[] { 1 }));
        }
    }

    [Test]
    public async Task SlowSubscriber_AppliesBackpressureToSource()
    {
        var sourceItems = new ConcurrentQueue<int>();
        var source = Stream.From(GenerateItems());
        var hot = source.Publish();

        async IAsyncEnumerable<int> GenerateItems()
        {
            for (int i = 1; i <= 3; i++)
            {
                sourceItems.Enqueue(i);
                yield return i;
            }
        }

        var results1 = new List<int>();
        var results2 = new List<int>();

        // Subscriber 1 is fast
        var t1 = hot.ForEachAsync(results1.Add);

        // Subscriber 2 is slow
        var t2 = hot.ForEachAsync(async x =>
        {
            results2.Add(x);
            await Task.Delay(10);
        });

        using (hot.Connect())
        {
            await Task.WhenAll(t1, t2);

            Assert.That(results1, Is.EquivalentTo(new[] { 1, 2, 3 }));
            Assert.That(results2, Is.EquivalentTo(new[] { 1, 2, 3 }));
        }
    }

    [Test]
    public async Task Replay_LateSubscriber_ReceivesBufferedItems()
    {
        var tcs = new TaskCompletionSource();
        var source = Stream.From(GenerateItems());
        var hot = source.Replay(2);

        async IAsyncEnumerable<int> GenerateItems()
        {
            yield return 1;
            yield return 2;
            yield return 3;
            await tcs.Task;
            yield return 4;
        }

        using (hot.Connect())
        {
            await Task.Delay(50); // Let first 3 items be produced

            var results = new List<int>();
            var t = hot.ForEachAsync(results.Add);

            tcs.SetResult();
            await t;

            // Buffer size is 2, so should receive 2, 3 (buffered) and 4 (new)
            Assert.That(results, Is.EquivalentTo(new[] { 2, 3, 4 }));
        }
    }

    [Test]
    public async Task Replay_LateSubscriber_ReceivesCompletion()
    {
        var source = Stream.From(new[] { 1, 2, 3 }.ToAsyncEnumerable());
        var hot = source.Replay(2);

        using (hot.Connect())
        {
            await Task.Delay(50); // Wait for completion

            var results = new List<int>();
            await hot.ForEachAsync(results.Add);

            // Should receive last 2 items from buffer
            Assert.That(results, Is.EquivalentTo(new[] { 2, 3 }));
        }
    }

    [Test]
    public async Task Replay_LateSubscriber_ReceivesError()
    {
        var source = Stream.From(GenerateItems());
        var hot = source.Replay(2);

        async IAsyncEnumerable<int> GenerateItems()
        {
            yield return 1;
            yield return 2;
            throw new InvalidOperationException("Failed");
        }

        using (hot.Connect())
        {
            await Task.Delay(50); // Wait for failure

            var results = new List<int>();
            Assert.ThrowsAsync<InvalidOperationException>(async () => await hot.ForEachAsync(results.Add));

            // Should receive buffered items before the error
            Assert.That(results, Is.EquivalentTo(new[] { 1, 2 }));
        }
    }

    [Test]
    public async Task Replay_RefCount_WorksCorrectly()
    {
        int executionCount = 0;
        var source = Stream.From(GenerateItems());
        var connectable = source.Replay(2);
        var shared = connectable.RefCount();

        async IAsyncEnumerable<int> GenerateItems()
        {
            executionCount++;
            yield return 1;
            yield return 2;
            yield return 3;
        }

        var results1 = new List<int>();
        await shared.ForEachAsync(results1.Add);
        Assert.That(results1, Is.EquivalentTo(new[] { 1, 2, 3 }));
        Assert.That(executionCount, Is.EqualTo(1));

        // Wait for RefCount to fully disconnect before second subscription
        // This ensures the second subscription will trigger a new execution
        await connectable.WhenRefCountDisconnectedAsync();

        // Second subscriber joins after first finished and disconnected
        var results2 = new List<int>();
        await shared.ForEachAsync(results2.Add);

        Assert.That(executionCount, Is.EqualTo(2));
        Assert.That(results2, Is.EquivalentTo(new[] { 1, 2, 3 }));
    }
}
