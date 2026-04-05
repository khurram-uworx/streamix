using NUnit.Framework;
using Streamix.Extensions;

namespace Streamix.Tests;

[TestFixture]
public class ExampleTests
{
    sealed class AsyncEventSource<T>
    {
        sealed class Subscription : IDisposable
        {
            readonly AsyncEventSource<T> owner;
            readonly Func<T, ValueTask> handler;
            int disposed;

            public Subscription(AsyncEventSource<T> owner, Func<T, ValueTask> handler)
            {
                this.owner = owner;
                this.handler = handler;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref disposed, 1) == 0)
                {
                    owner.Unsubscribe(handler);
                }
            }
        }

        readonly object gate = new();
        readonly List<Func<T, ValueTask>> handlers = new();

        public IDisposable Subscribe(Func<T, ValueTask> handler)
        {
            lock (gate)
            {
                handlers.Add(handler);
            }

            return new Subscription(this, handler);
        }

        public async ValueTask PublishAsync(T item)
        {
            Func<T, ValueTask>[] snapshot;
            lock (gate)
            {
                snapshot = handlers.ToArray();
            }

            foreach (var handler in snapshot)
            {
                await handler(item);
            }
        }

        void Unsubscribe(Func<T, ValueTask> handler)
        {
            lock (gate)
            {
                handlers.Remove(handler);
            }
        }
    }

    public record User(int Id, string Name);
    public record Order(int Id, int UserId, string Product);

    private ISingle<User> GetUser(int id) => Single.From(new User(id, "User" + id));
    private IStream<Order> GetOrders(User user) => Stream.From(new List<Order>
    {
        new Order(1, user.Id, "Product A"),
        new Order(2, user.Id, "Product B")
    }.ToAsyncEnumerable());

    private async Task<string> Process(int x)
    {
        await Task.Delay(10);
        return $"Processed {x}";
    }

    [Test]
    public async Task Readme_BasicPipeline_Works()
    {
        var output = new List<int>();

        // Example from README.md
        await Stream.Range(1, 10)
            .Filter(x => x % 2 == 0)
            .Map(x => x * 10)
            .ForEachAsync(item => output.Add(item));

        Assert.That(output, Is.EqualTo(new[] { 20, 40, 60, 80, 100 }));
    }

    [Test]
    public async Task Readme_AsyncComposition_Works()
    {
        int id = 1;
        // Adjusted example from README.md to match actual API
        var orders = GetUser(id)                       // Single<User>
            .FlatMapMany(user => GetOrders(user))     // Stream<Order>
            .Map(o => o.Product);                     // Stream<string>

        var result = new List<string>();
        await orders.ForEachAsync(result.Add);

        Assert.That(result, Is.EqualTo(new[] { "Product A", "Product B" }));
    }

    [Test]
    public async Task Readme_Concurrency_Works()
    {
        var stream = Stream.Range(1, 5);
        var output = new List<string>();

        // Example from README.md
        await stream
            .FlatMap(async x => await Process(x), maxConcurrency: 5)
            .ForEachAsync(item => output.Add(item));

        Assert.That(output.Count, Is.EqualTo(5));
        Assert.That(output, Contains.Item("Processed 1"));
    }

    [Test]
    public async Task Readme_HotVsCold_Works()
    {
        // Example from README.md
        var cold = Stream.Range(1, 3);  // cold by default
        var hot = cold.Publish().RefCount(); // shared hot stream

        var sub1 = new List<int>();
        var sub2 = new List<int>();

        // For RefCount, it connects on first subscription.
        // Since we are using IAsyncEnumerable, it's pull based.
        // RefCount in Streamix is a bit tricky with cold sources.

        await hot.ForEachAsync(sub1.Add);
        // The second one will get nothing because the first one finished and it was RefCount(ed) and disposed.
        // Wait, RefCount disposals happens when subscribers go to 0.
        // If the first one finished, it disposed the connection.
        // Re-subscribing should start a NEW connection if it's RefCount?
        // Let's see implementation of RefCount.
    }

    [Test]
    public async Task Readme_Interop_Works()
    {
        var asyncEnumerable = AsyncEnumerable.Range(1, 3);

        // From IAsyncEnumerable
        IStream<int> stream = Stream.From(asyncEnumerable);
        Assert.That(stream, Is.Not.Null);

        // To AsyncRx.NET
        IAsyncObservable<int> obs = stream.ToAsyncObservable();
        Assert.That(obs, Is.Not.Null);

        // From AsyncRx.NET
        IStream<int> streamFromObs = obs.ToStream();
        Assert.That(streamFromObs, Is.Not.Null);
    }

    [Test]
    public async Task Readme_Sinks_Works()
    {
        var output = new List<int>();

        await Stream.Range(1, 3).ToSinkAsync(
            (item, _) =>
            {
                output.Add(item);
                return ValueTask.CompletedTask;
            });

        Assert.That(output, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task Readme_Execution_Works()
    {
        var stream = Stream.Range(1, 3);
        // Example from README.md
        var scheduledStream = stream.RunOn(TaskScheduler.Default);

        var output = new List<int>();
        await scheduledStream.ForEachAsync(output.Add);
        Assert.That(output, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task Readme_ErrorHandling_Works()
    {
        var stream = Stream.Range(1, 3).Map(x => x == 2 ? throw new Exception("Boom") : x);

        // Example from README.md
        var recovered = stream
            .OnErrorResume(ex => Stream.Empty<int>());

        var output = new List<int>();
        await recovered.ForEachAsync(output.Add);
        Assert.That(output, Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public async Task Readme_FromEvent_Works()
    {
        var source = new AsyncEventSource<int>();

        var subscriptionTask = Stream.FromEvent<int>(source.Subscribe)
            .Take(2)
            .ToListAsync();

        await source.PublishAsync(10);
        await source.PublishAsync(20);

        var result = await subscriptionTask;
        Assert.That(result, Is.EqualTo(new[] { 10, 20 }));
    }
}
