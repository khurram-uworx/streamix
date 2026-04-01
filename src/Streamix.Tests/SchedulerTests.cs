using NUnit.Framework;
using Streamix.Abstractions;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Streamix.Tests;

[TestFixture]
public class SchedulerTests
{
    private class TrackingScheduler : TaskScheduler
    {
        public int ScheduledCount = 0;
        private readonly BlockingCollection<Task> _tasks = new();

        public TrackingScheduler()
        {
            var thread = new Thread(() =>
            {
                foreach (var task in _tasks.GetConsumingEnumerable())
                {
                    TryExecuteTask(task);
                }
            });
            thread.IsBackground = true;
            thread.Start();
        }

        protected override void QueueTask(Task task)
        {
            Interlocked.Increment(ref ScheduledCount);
            _tasks.Add(task);
        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

        protected override IEnumerable<Task> GetScheduledTasks() => _tasks.ToArray();
    }

    [Test]
    public async Task Stream_RunOn_Executes_Upstream_On_Scheduler()
    {
        var scheduler = new TrackingScheduler();
        var observedSchedulers = new ConcurrentBag<TaskScheduler>();

        async IAsyncEnumerable<int> GenerateItems()
        {
            observedSchedulers.Add(TaskScheduler.Current);
            yield return 1;
            observedSchedulers.Add(TaskScheduler.Current);
            yield return 2;
        }

        var stream = Stream.From(GenerateItems())
            .RunOn(scheduler);

        await foreach (var item in stream)
        {
            // consume
        }

        Assert.That(scheduler.ScheduledCount, Is.GreaterThan(0));
        Assert.That(observedSchedulers, Has.All.EqualTo(scheduler));
    }

    [Test]
    public async Task Single_RunOn_Executes_Upstream_On_Scheduler()
    {
        var scheduler = new TrackingScheduler();
        var observedSchedulers = new ConcurrentBag<TaskScheduler>();

        var single = Single.From(Task.Run(() => 1))
            .Map(x =>
            {
                observedSchedulers.Add(TaskScheduler.Current);
                return x;
            })
            .RunOn(scheduler);

        await single.ToTask();

        Assert.That(scheduler.ScheduledCount, Is.GreaterThan(0));
        Assert.That(observedSchedulers, Has.All.EqualTo(scheduler));
    }

    [Test]
    public async Task RunOn_Propagates_Cancellation()
    {
        var scheduler = new TrackingScheduler();
        var cts = new CancellationTokenSource();
        var tcs = new TaskCompletionSource<bool>();

        async IAsyncEnumerable<int> GenerateItems([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                await Task.Delay(2000, ct);
            }
            catch (OperationCanceledException)
            {
                tcs.SetResult(true);
                throw;
            }
            yield return 1;
        }

        var stream = Stream.From(GenerateItems())
            .RunOn(scheduler);

        var task = Task.Run(async () =>
        {
            await foreach (var item in stream.WithCancellation(cts.Token))
            {
            }
        });

        await Task.Delay(200);
        cts.Cancel();

        Assert.ThrowsAsync<TaskCanceledException>(async () => await task);
        Assert.That(await tcs.Task, Is.True);
    }
}
