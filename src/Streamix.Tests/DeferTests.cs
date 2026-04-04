using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class DeferTests
{
    [Test]
    public async Task Defer_Factory_Is_Not_Called_Until_Enumeration()
    {
        int factoryCalls = 0;
        var stream = Stream.Defer(() =>
        {
            factoryCalls++;
            return Stream.Range(1, 3);
        });

        Assert.That(factoryCalls, Is.EqualTo(0));

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertValues(1, 2, 3)
            .AssertComplete();

        Assert.That(factoryCalls, Is.EqualTo(1));
    }

    [Test]
    public async Task Defer_Factory_Is_Called_Once_Per_Subscriber()
    {
        int factoryCalls = 0;
        var stream = Stream.Defer(() =>
        {
            factoryCalls++;
            return Stream.From(factoryCalls);
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertValues(1)
            .AssertComplete();
        Assert.That(factoryCalls, Is.EqualTo(1));

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertValues(2)
            .AssertComplete();
        Assert.That(factoryCalls, Is.EqualTo(2));
    }

    [Test]
    public async Task Defer_Works_With_Retry_Later()
    {
        int factoryCalls = 0;
        var stream = Stream.Defer(() =>
        {
            factoryCalls++;
            if (factoryCalls == 1)
                return Stream.Error<int>(new System.Exception("Fail first time"));
            return Stream.From(factoryCalls);
        });

        (await TestSubscriber<int>.SubscribeAsync(stream.Retry(1)))
            .AssertValues(2)
            .AssertComplete();
        Assert.That(factoryCalls, Is.EqualTo(2));
    }

    [Test]
    public async Task Defer_Overload_Passes_CancellationToken()
    {
        CancellationToken capturedToken = default;
        var stream = Stream.Defer(ct =>
        {
            capturedToken = ct;
            return Stream.Range(1, 3);
        });

        using var cts = new CancellationTokenSource();
        await foreach (var item in stream.WithCancellation(cts.Token))
        {
            if (item == 1) break;
        }

        Assert.That(capturedToken, Is.EqualTo(cts.Token));
    }

    [Test]
    public async Task Defer_Factory_Exception_Propagates()
    {
        var stream = Stream.Defer<int>(() =>
        {
            throw new InvalidOperationException("Factory Boom");
        });

        (await TestSubscriber<int>.SubscribeAsync(stream))
            .AssertError<InvalidOperationException>(ex => Assert.That(ex.Message, Is.EqualTo("Factory Boom")));
    }

    [Test]
    public async Task Defer_Single_Factory_Exception_Propagates()
    {
        var single = Single.Defer<int>(() =>
        {
            throw new InvalidOperationException("Factory Boom");
        });

        (await TestSubscriber<int>.SubscribeAsync(single))
            .AssertError<InvalidOperationException>(ex => Assert.That(ex.Message, Is.EqualTo("Factory Boom")));
    }
}
