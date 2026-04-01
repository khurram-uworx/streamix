using NUnit.Framework;
using Streamix.Extensions;
using System.Reactive.Linq;

namespace Streamix.Tests.Extensions;

[TestFixture]
public class AsyncRxInteropTests
{
    [Test]
    public async Task ToAsyncObservable_EmitsAllItems()
    {
        var stream = Stream.Range(1, 5);
        var observable = stream.ToAsyncObservable();
        var results = new List<int>();

        await observable.ForEachAsync(x => results.Add(x));

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task ToAsyncObservable_Single_EmitsItem()
    {
        var single = Single.From(42);
        var observable = single.ToAsyncObservable();
        var results = new List<int>();

        await observable.ForEachAsync(x => results.Add(x));

        Assert.That(results, Is.EqualTo(new[] { 42 }));
    }

    [Test]
    public async Task ToStream_ConvertsBack()
    {
        var observable = AsyncObservable.Range(1, 5);
        var stream = observable.ToStream();
        var results = new List<int>();

        await stream.ForEachAsync(x => results.Add(x));

        Assert.That(results, Is.EqualTo(new[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public async Task ToSingle_ConvertsBack()
    {
        var observable = AsyncObservable.Return(42);
        var single = observable.ToSingle();

        var result = await single.ToTask();

        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task ToStream_HandlesError()
    {
        var exception = new Exception("Test error");
        var observable = AsyncObservable.Throw<int>(exception);
        var stream = observable.ToStream();

        Assert.ThrowsAsync<Exception>(async () => await stream.ForEachAsync(_ => { }));
    }

    [Test]
    public async Task ToAsyncObservable_HandlesError()
    {
        var exception = new Exception("Test error");
        var stream = Stream.Error<int>(exception);
        var observable = stream.ToAsyncObservable();

        var caught = false;
        try
        {
            await observable.ForEachAsync(_ => { });
        }
        catch (Exception ex) when (ex.Message == "Test error")
        {
            caught = true;
        }

        Assert.That(caught, Is.True);
    }

    [Test]
    public async Task ToAsyncObservable_RespectsSubscriptionCancellation()
    {
        var stream = Stream.Range(1, 100).Delay(TimeSpan.FromMilliseconds(50));
        var observable = stream.ToAsyncObservable();

        var results = new List<int>();
        var subscription = await observable.SubscribeAsync(async x =>
        {
            results.Add(x);
        });

        await Task.Delay(250);
        await subscription.DisposeAsync();

        var countAfterDispose = results.Count;
        await Task.Delay(200);

        Assert.That(results.Count, Is.EqualTo(countAfterDispose));
        Assert.That(results.Count, Is.InRange(3, 7));
    }
}
