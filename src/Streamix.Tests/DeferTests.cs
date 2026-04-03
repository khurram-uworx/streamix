using NUnit.Framework;
using Streamix.Abstractions;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

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

        await foreach (var _ in stream) { }

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

        var results1 = await stream.ToListAsync();
        Assert.That(results1, Is.EqualTo(new[] { 1 }));
        Assert.That(factoryCalls, Is.EqualTo(1));

        var results2 = await stream.ToListAsync();
        Assert.That(results2, Is.EqualTo(new[] { 2 }));
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

        var result = await stream.Retry(1).ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { 2 }));
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

        Assert.That(capturedToken.IsCancellationRequested, Is.False);
        Assert.That(capturedToken, Is.EqualTo(cts.Token));
    }

    [Test]
    public async Task Defer_Chains_With_Downstream_Operators()
    {
        var stream = Stream.Defer(() => Stream.Range(1, 5))
            .Filter(x => x % 2 == 0)
            .Map(x => x * 10);

        var result = await stream.ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { 20, 40 }));
    }
}
