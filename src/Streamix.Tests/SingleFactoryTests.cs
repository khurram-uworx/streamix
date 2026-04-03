using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

[TestFixture]
public class SingleFactoryTests
{
    [Test]
    public async Task From_FuncTask_IsLazy()
    {
        int count = 0;
        var single = Single.From(async () =>
        {
            count++;
            await Task.Yield();
            return 42;
        });

        Assert.That(count, Is.EqualTo(0));

        var result = await single.ToTask();
        Assert.That(result, Is.EqualTo(42));
        Assert.That(count, Is.EqualTo(1));

        var result2 = await single.ToTask();
        Assert.That(result2, Is.EqualTo(42));
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task From_FuncCTTask_RespectsCancellation()
    {
        var cts = new CancellationTokenSource();
        var single = Single.From(async ct =>
        {
            await Task.Delay(1000, ct);
            return 42;
        });

        cts.Cancel();
        Assert.ThrowsAsync<TaskCanceledException>(async () => await single.ToTask(cts.Token));
    }

    [Test]
    public void From_FuncTask_PropagatesException()
    {
        var single = Single.From<int>(async () =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Boom");
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await single.ToTask());
    }

    [Test]
    public async Task Defer_IsLazyAndInvokedPerSubscription()
    {
        int count = 0;
        var single = Single.Defer(() =>
        {
            count++;
            return Single.From(count);
        });

        Assert.That(count, Is.EqualTo(0));

        Assert.That(await single.ToTask(), Is.EqualTo(1));
        Assert.That(count, Is.EqualTo(1));

        Assert.That(await single.ToTask(), Is.EqualTo(2));
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public void Defer_FactoryException_Propagates()
    {
        var single = Single.Defer<int>(() =>
        {
            throw new InvalidOperationException("Factory Boom");
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await single.ToTask());
    }

    [Test]
    public async Task Defer_FuncCT_RespectsCancellation()
    {
        var single = Single.Defer(ct =>
        {
            if (ct.IsCancellationRequested) throw new OperationCanceledException(ct);
            return Single.From(42);
        });

        var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await single.ToTask(cts.Token));
    }
}
