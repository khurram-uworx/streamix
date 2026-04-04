using NUnit.Framework;
using Streamix;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Streamix.Tests;

[TestFixture]
public class SingleCardinalityTests
{
    [Test]
    public void SingleFromEnumerable_ThrowsOnMultipleItems()
    {
        async IAsyncEnumerable<int> MultipleItems()
        {
            yield return 1;
            yield return 2;
        }

        ISingle<int> single = Single.From(MultipleItems());

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in single)
            {
            }
        });
    }

    [Test]
    public void SingleMap_ThrowsOnMultipleItems()
    {
        async IAsyncEnumerable<int> MultipleItems()
        {
            yield return 1;
            yield return 2;
        }

        ISingle<int> single = Single.From(MultipleItems()).Map(x => x * 10);

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in single)
            {
            }
        });
    }

    [Test]
    public void ToTask_ThrowsOnMultipleItems()
    {
        async IAsyncEnumerable<int> MultipleItems()
        {
            yield return 1;
            yield return 2;
        }

        ISingle<int> single = Single.From(MultipleItems());
        Assert.ThrowsAsync<InvalidOperationException>(async () => await single.ToTask());
    }

    [Test]
    public void ForEachAsync_ThrowsOnMultipleItems()
    {
        async IAsyncEnumerable<int> MultipleItems()
        {
            yield return 1;
            yield return 2;
        }

        ISingle<int> single = Single.From(MultipleItems());
        Assert.ThrowsAsync<InvalidOperationException>(async () => await single.ForEachAsync(x => { }));
    }

    [Test]
    public void Retry_ThrowsOnMultipleItemsBeforeFailure()
    {
        int count = 0;
        async IAsyncEnumerable<int> Source()
        {
            count++;
            yield return 1;
            yield return 2;
        }

        ISingle<int> single = Single.From(Source()).Retry(1);
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
             await foreach (var item in single) { }
        });
    }
}
