using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using Streamix.Extensions;
using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace Streamix.Tests;

[TestFixture]
public class EfStreamTests
{
    class InstrumentedAsyncQueryable<T> : EnumerableQuery<T>, IAsyncEnumerable<T>, IQueryable<T>
    {
        private readonly IReadOnlyList<T> items;
        private readonly Action<T>? onYield;
        private readonly int? failOnMoveNextCall;

        public InstrumentedAsyncQueryable(
            IReadOnlyList<T> items,
            Action<T>? onYield = null,
            int? failOnMoveNextCall = null)
            : base(items)
        {
            this.items = items;
            this.onYield = onYield;
            this.failOnMoveNextCall = failOnMoveNextCall;
        }

        public InstrumentedAsyncQueryable(Expression expression, IReadOnlyList<T> items, Action<T>? onYield = null, int? failOnMoveNextCall = null)
            : base(expression)
        {
            this.items = items;
            this.onYield = onYield;
            this.failOnMoveNextCall = failOnMoveNextCall;
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new Enumerator(items, onYield, failOnMoveNextCall, cancellationToken);
        }

        class Enumerator : IAsyncEnumerator<T>
        {
            private readonly IReadOnlyList<T> items;
            private readonly Action<T>? onYield;
            private readonly int? failOnMoveNextCall;
            private readonly CancellationToken cancellationToken;
            private int index;
            private int moveNextCallCount;

            public Enumerator(IReadOnlyList<T> items, Action<T>? onYield, int? failOnMoveNextCall, CancellationToken cancellationToken)
            {
                this.items = items;
                this.onYield = onYield;
                this.failOnMoveNextCall = failOnMoveNextCall;
                this.cancellationToken = cancellationToken;
                index = -1;
            }

            public T Current { get; private set; } = default!;

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                cancellationToken.ThrowIfCancellationRequested();

                moveNextCallCount++;
                if (failOnMoveNextCall.HasValue && moveNextCallCount == failOnMoveNextCall.Value)
                {
                    throw new InvalidOperationException("streamed query failed");
                }

                var nextIndex = index + 1;
                if (nextIndex >= items.Count)
                {
                    return ValueTask.FromResult(false);
                }

                index = nextIndex;
                Current = items[index];
                onYield?.Invoke(Current);
                return ValueTask.FromResult(true);
            }
        }
    }

    class TestEntity
    {
        public int Id { get; set; }
        public bool IsActive { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    class TrackingDbContext : DbContext
    {
        private readonly Action<int> onDispose;

        public TrackingDbContext(DbContextOptions<TrackingDbContext> options, int instanceId, Action<int> onDispose)
            : base(options)
        {
            InstanceId = instanceId;
            this.onDispose = onDispose;
        }

        public int InstanceId { get; }

        public DbSet<TestEntity> Entities => Set<TestEntity>();

        public override async ValueTask DisposeAsync()
        {
            onDispose(InstanceId);
            await base.DisposeAsync();
        }
    }

    [Test]
    public async Task From_EmitsExpectedEntities()
    {
        var stream = EfStream.From(
            ctx => ctx.Set<TestEntity>()
                .Where(x => x.IsActive)
                .OrderBy(x => x.Id),
            createSeededFactory(),
            name: "ActiveEntities");

        var result = await stream
            .Map(x => x.Name)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { "A", "C" }));
    }

    [Test]
    public async Task From_Cancellation_ThrowsAndDisposesContext()
    {
        var disposedContextIds = new ConcurrentBag<int>();
        var stream = EfStream.From(
            ctx => ctx.Set<TestEntity>().OrderBy(x => x.Id),
            createSeededFactory(disposedContextIds));

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in stream.WithCancellation(cts.Token))
            {
            }
        });

        Assert.That(disposedContextIds, Has.Count.EqualTo(1));
    }

    [Test]
    public void From_QueryBuilderFailure_PropagatesAndDisposesContext()
    {
        var disposedContextIds = new ConcurrentBag<int>();
        var stream = EfStream.From<TestEntity>(
            _ => throw new InvalidOperationException("query failed"),
            createSeededFactory(disposedContextIds));

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
        Assert.That(exception!.Message, Is.EqualTo("query failed"));
        Assert.That(disposedContextIds, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task From_CreatesDistinctContextPerSubscription()
    {
        var queryContextIds = new ConcurrentBag<int>();
        var disposedContextIds = new ConcurrentBag<int>();
        var stream = EfStream.From(
            ctx =>
            {
                var typed = (TrackingDbContext)ctx;
                queryContextIds.Add(typed.InstanceId);
                return typed.Entities.OrderBy(x => x.Id);
            },
            createSeededFactory(disposedContextIds));

        await stream.ToListAsync();
        await stream.ToListAsync();

        Assert.That(queryContextIds.Distinct().Count(), Is.EqualTo(2));
        Assert.That(disposedContextIds.Distinct().Count(), Is.EqualTo(2));
    }

    [Test]
    public async Task From_ComposesWithMapFilterTakeAndForEachAsync()
    {
        var stream = EfStream.From(
            ctx => ctx.Set<TestEntity>().OrderBy(x => x.Id),
            createSeededFactory());

        var processed = new List<string>();
        await stream
            .Map(x => $"{x.Id}:{x.Name}")
            .Filter(x => x.Contains(':'))
            .Take(2)
            .ForEachAsync(processed.Add);

        Assert.That(processed, Is.EqualTo(new[] { "1:A", "2:B" }));
    }

    [Test]
    public async Task ToStream_Extension_UsesFactoryPathAndEmitsData()
    {
        Func<DbContext> factory = createSeededFactory();
        var stream = factory.ToStream(ctx => ctx.Set<TestEntity>().Where(x => x.IsActive).OrderBy(x => x.Id));

        var ids = await stream.Map(x => x.Id).ToListAsync();

        Assert.That(ids, Is.EqualTo(new[] { 1, 3 }));
    }

    [Test]
    public async Task FromStreamed_EmitsExpectedEntities()
    {
        var stream = EfStream.FromStreamed(
            ctx => ctx.Set<TestEntity>()
                .Where(x => x.IsActive)
                .OrderBy(x => x.Id),
            createSeededFactory(),
            name: "ActiveEntitiesStreamed");

        var result = await stream
            .Map(x => x.Name)
            .ToListAsync();

        Assert.That(result, Is.EqualTo(new[] { "A", "C" }));
    }

    [Test]
    public async Task FromStreamed_CancellationAfterFirstItem_ThrowsAndDisposesContext()
    {
        var disposedContextIds = new ConcurrentBag<int>();
        var stream = EfStream.FromStreamed(
            ctx => ctx.Set<TestEntity>().OrderBy(x => x.Id),
            createSeededFactory(disposedContextIds));

        using var cts = new CancellationTokenSource();
        var enumerator = stream.GetAsyncEnumerator(cts.Token);

        try
        {
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current.Id, Is.EqualTo(1));

            await cts.CancelAsync();

            Assert.ThrowsAsync<OperationCanceledException>(async () => await enumerator.MoveNextAsync().AsTask());
        }
        finally
        {
            await enumerator.DisposeAsync();
        }

        Assert.That(disposedContextIds, Has.Count.EqualTo(1));
    }

    [Test]
    public void FromStreamed_QueryBuilderFailure_PropagatesAndDisposesContext()
    {
        var disposedContextIds = new ConcurrentBag<int>();
        var stream = EfStream.FromStreamed<TestEntity>(
            _ => throw new InvalidOperationException("query failed"),
            createSeededFactory(disposedContextIds));

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
        Assert.That(exception!.Message, Is.EqualTo("query failed"));
        Assert.That(disposedContextIds, Has.Count.EqualTo(1));
    }

    [Test]
    public void FromStreamed_EnumerationFailure_PropagatesAndDisposesContext()
    {
        var disposedContextIds = new ConcurrentBag<int>();
        var entities = new[]
        {
            new TestEntity { Id = 1, IsActive = true, Name = "A" },
            new TestEntity { Id = 2, IsActive = true, Name = "B" }
        };

        var stream = EfStream.FromStreamed(
            _ => new InstrumentedAsyncQueryable<TestEntity>(entities, failOnMoveNextCall: 2),
            createSeededFactory(disposedContextIds));

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
        Assert.That(exception!.Message, Is.EqualTo("streamed query failed"));
        Assert.That(disposedContextIds, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task FromStreamed_CreatesDistinctContextPerSubscription()
    {
        var queryContextIds = new ConcurrentBag<int>();
        var disposedContextIds = new ConcurrentBag<int>();
        var stream = EfStream.FromStreamed(
            ctx =>
            {
                var typed = (TrackingDbContext)ctx;
                queryContextIds.Add(typed.InstanceId);
                return typed.Entities.OrderBy(x => x.Id);
            },
            createSeededFactory(disposedContextIds));

        await stream.ToListAsync();
        await stream.ToListAsync();

        Assert.That(queryContextIds.Distinct().Count(), Is.EqualTo(2));
        Assert.That(disposedContextIds.Distinct().Count(), Is.EqualTo(2));
    }

    [Test]
    public async Task BufferedAndStreamed_EmitSameValues_ForSameQuery()
    {
        Func<DbContext> factory = createSeededFactory();
        var buffered = EfStream.From(
            ctx => ctx.Set<TestEntity>().Where(x => x.IsActive).OrderBy(x => x.Id),
            factory);
        var streamed = EfStream.FromStreamed(
            ctx => ctx.Set<TestEntity>().Where(x => x.IsActive).OrderBy(x => x.Id),
            factory);

        var bufferedIds = await buffered.Map(x => x.Id).ToListAsync();
        var streamedIds = await streamed.Map(x => x.Id).ToListAsync();

        Assert.That(streamedIds, Is.EqualTo(bufferedIds));
    }

    [Test]
    public async Task Streamed_Take_StopsEnumerationEarlier_ThanBuffered()
    {
        var bufferedEnumeratedIds = new ConcurrentBag<int>();
        var streamedEnumeratedIds = new ConcurrentBag<int>();
        var entities = new[]
        {
            new TestEntity { Id = 1, IsActive = true, Name = "A" },
            new TestEntity { Id = 2, IsActive = true, Name = "B" },
            new TestEntity { Id = 3, IsActive = true, Name = "C" }
        };

        var buffered = EfStream.From(
            _ => new InstrumentedAsyncQueryable<TestEntity>(entities, entity => bufferedEnumeratedIds.Add(entity.Id)),
            createSeededFactory());
        var streamed = EfStream.FromStreamed(
            _ => new InstrumentedAsyncQueryable<TestEntity>(entities, entity => streamedEnumeratedIds.Add(entity.Id)),
            createSeededFactory());

        var bufferedResult = await buffered.Take(1).Map(x => x.Id).ToListAsync();
        var streamedResult = await streamed.Take(1).Map(x => x.Id).ToListAsync();

        Assert.That(bufferedResult, Is.EqualTo(new[] { 1 }));
        Assert.That(streamedResult, Is.EqualTo(new[] { 1 }));
        Assert.That(bufferedEnumeratedIds.OrderBy(x => x).ToArray(), Is.EqualTo(new[] { 1, 2, 3 }));
        Assert.That(streamedEnumeratedIds.OrderBy(x => x).ToArray(), Is.EqualTo(new[] { 1 }));
    }

    [Test]
    public async Task ToStreamed_Extension_UsesFactoryPathAndEmitsData()
    {
        Func<DbContext> factory = createSeededFactory();
        var stream = factory.ToStreamed(ctx => ctx.Set<TestEntity>().Where(x => x.IsActive).OrderBy(x => x.Id));

        var ids = await stream.Map(x => x.Id).ToListAsync();

        Assert.That(ids, Is.EqualTo(new[] { 1, 3 }));
    }

    static Func<DbContext> createSeededFactory(ConcurrentBag<int>? disposedContextIds = null)
    {
        var instanceCounter = 0;
        var databasePrefix = $"ef-stream-tests-{Guid.NewGuid():N}";

        return () =>
        {
            var instanceId = Interlocked.Increment(ref instanceCounter);
            var options = new DbContextOptionsBuilder<TrackingDbContext>()
                .UseInMemoryDatabase($"{databasePrefix}-{instanceId}")
                .Options;

            var context = new TrackingDbContext(
                options,
                instanceId,
                id => disposedContextIds?.Add(id));

            context.Database.EnsureCreated();
            context.Entities.AddRange(
                new TestEntity { Id = 1, IsActive = true, Name = "A" },
                new TestEntity { Id = 2, IsActive = false, Name = "B" },
                new TestEntity { Id = 3, IsActive = true, Name = "C" });
            context.SaveChanges();

            return context;
        };
    }
}
