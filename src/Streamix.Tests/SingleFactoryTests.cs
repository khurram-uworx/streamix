using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class SingleFactoryTests
{
    [Test]
    public async Task From_Value_Emits_Item()
    {
        var single = Single.From(42);
        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertValues(42)
            .AssertComplete();
    }

    [Test]
    public async Task Just_Value_Emits_Item()
    {
        var single = Single.Just(42);
        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertValues(42)
            .AssertComplete();
    }

    [Test]
    public async Task From_Task_Emits_Result()
    {
        var single = Single.From(Task.FromResult(42));
        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertValues(42)
            .AssertComplete();
    }

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

        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertValues(42)
            .AssertComplete();

        Assert.That(count, Is.EqualTo(1));

        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertValues(42)
            .AssertComplete();

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

        var subscribeTask = TestSubscriber<int>.SubscribeAsync(single, cts.Token);
        await Task.Delay(10);
        await cts.CancelAsync();

        var subscriber = await subscribeTask;
        subscriber.AssertValueCount(0);
        subscriber.AssertNotComplete();
    }

    [Test]
    public async Task From_FuncTask_PropagatesException()
    {
        var single = Single.From<int>(async () =>
        {
            await Task.Yield();
            throw new InvalidOperationException("Boom");
        });

        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertError<InvalidOperationException>(ex => Assert.That(ex.Message, Is.EqualTo("Boom")));
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

        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertValues(1)
            .AssertComplete();
        Assert.That(count, Is.EqualTo(1));

        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertValues(2)
            .AssertComplete();
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task Defer_FactoryException_Propagates()
    {
        var single = Single.Defer<int>(() =>
        {
            throw new InvalidOperationException("Factory Boom");
        });

        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertError<InvalidOperationException>(ex => Assert.That(ex.Message, Is.EqualTo("Factory Boom")));
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
        await cts.CancelAsync();

        (await TestSubscriber<int>.SubscribeAsync(single, cts.Token))
            .AssertValueCount(0)
            .AssertNotComplete();
    }
}
