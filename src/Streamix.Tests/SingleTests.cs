using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

[TestFixture]
public class SingleTests
{
    [Test]
    public async Task From_Value_Emits_Correct_Value()
    {
        ISingle<int> single = Single.From(10);
        int result = 0;
        await foreach (var item in single)
        {
            result = item;
        }
        Assert.That(result, Is.EqualTo(10));
    }

    [Test]
    public async Task Empty_Single_Is_Empty()
    {
        ISingle<int> single = Single.Empty<int>();
        int count = 0;
        await foreach (var _ in single)
        {
            count++;
        }
        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public void Error_Single_Propagates_Exception()
    {
        var exception = new InvalidOperationException("Test error");
        ISingle<int> single = Single.Error<int>(exception);

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in single) { }
        });
    }

    [Test]
    public async Task From_Task_Emits_Correct_Value()
    {
        ISingle<int> single = Single.From(Task.FromResult(20));
        int result = 0;
        await foreach (var item in single)
        {
            result = item;
        }
        Assert.That(result, Is.EqualTo(20));
    }

    [Test]
    public async Task ToTask_Returns_Value()
    {
        ISingle<int> single = Single.From(30);
        int result = await single.ToTask();
        Assert.That(result, Is.EqualTo(30));
    }

    [Test]
    public async Task ToTask_Returns_Default_For_Empty()
    {
        ISingle<int> single = Single.Empty<int>();
        int result = await single.ToTask();
        Assert.That(result, Is.EqualTo(0));
    }

    [Test]
    public async Task Map_Transforms_Value()
    {
        ISingle<int> single = Single.From(5).Map(x => x * 2);
        int result = await single.ToTask();
        Assert.That(result, Is.EqualTo(10));
    }

    [Test]
    public async Task FlatMap_Transforms_Value()
    {
        ISingle<int> single = Single.From(5).FlatMap(x => Single.From(x + 3));
        int result = await single.ToTask();
        Assert.That(result, Is.EqualTo(8));
    }

    [Test]
    public async Task FlatMapMany_Flattens_To_Stream()
    {
        ISingle<int> single = Single.From(3);
        IStream<int> stream = single.FlatMapMany(x => Stream.Range(1, x));
        var result = new List<int>();
        await foreach (var item in stream)
        {
            result.Add(item);
        }
        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task OnErrorResume_Handles_Error()
    {
        ISingle<int> single = Single.Error<int>(new Exception("Fail")).OnErrorResume(ex => Single.From(100));
        int result = await single.ToTask();
        Assert.That(result, Is.EqualTo(100));
    }

    [Test]
    public void Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        async IAsyncEnumerable<int> DelayedSource()
        {
            await Task.Delay(1000, cts.Token);
            yield return 1;
        }

        ISingle<int> single = Single.From(DelayedSource());
        cts.Cancel();

        Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await foreach (var item in single.WithCancellation(cts.Token))
            {
            }
        });
    }

    [Test]
    public async Task Single_To_Stream_Interoperability()
    {
        ISingle<int> single = Single.From(42);
        IStream<int> stream = Stream.From(single);
        var result = new List<int>();
        await foreach (var item in stream) result.Add(item);
        Assert.That(result, Is.EqualTo(new[] { 42 }));
    }
}
