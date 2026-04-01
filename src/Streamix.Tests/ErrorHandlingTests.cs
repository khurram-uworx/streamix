using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

[TestFixture]
public class ErrorHandlingTests
{
    #region Stream Tests

    [Test]
    public async Task Stream_OnErrorResume_Recovers_From_Error()
    {
        var exception = new InvalidOperationException("Initial failure");
        IStream<int> stream = Stream.Error<int>(exception)
            .OnErrorResume(ex =>
            {
                Assert.That(ex, Is.SameAs(exception));
                return Stream.Range(1, 3);
            });

        var result = new List<int>();
        await foreach (var item in stream)
        {
            result.Add(item);
        }

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public async Task Stream_OnErrorResume_MidStream_Recovers()
    {
        async IAsyncEnumerable<int> FailingSource()
        {
            yield return 1;
            yield return 2;
            throw new Exception("Boom");
        }

        IStream<int> stream = Stream.From(FailingSource())
            .OnErrorResume(ex => Stream.From(100));

        var result = new List<int>();
        await foreach (var item in stream)
        {
            result.Add(item);
        }

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 100 }));
    }

    [Test]
    public void Stream_OnErrorResume_Propagates_Recovery_Failure()
    {
        var recoveryException = new Exception("Recovery failed");
        IStream<int> stream = Stream.Error<int>(new Exception("Initial"))
            .OnErrorResume(ex => throw recoveryException);

        Assert.ThrowsAsync<Exception>(async () =>
        {
            await foreach (var _ in stream) { }
        }, "Recovery failed");
    }

    [Test]
    public async Task Stream_OnErrorReturn_Returns_Value()
    {
        IStream<int> stream = Stream.Error<int>(new Exception("Fail"))
            .OnErrorReturn(42);

        var result = new List<int>();
        await foreach (var item in stream) result.Add(item);

        Assert.That(result, Is.EqualTo(new[] { 42 }));
    }

    [Test]
    public void Stream_OnErrorMap_Transforms_Exception()
    {
        IStream<int> stream = Stream.Error<int>(new InvalidOperationException("Original"))
            .OnErrorMap(ex => new ArgumentException("Mapped", ex));

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in stream) { }
        });
        Assert.That(ex.Message, Is.EqualTo("Mapped"));
        Assert.That(ex.InnerException, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task Stream_ErrorOperators_NoOp_When_No_Error()
    {
        IStream<int> stream = Stream.Range(1, 3)
            .OnErrorResume(ex => Stream.From(10))
            .OnErrorReturn(20)
            .OnErrorMap(ex => new Exception("Should not happen"));

        var result = new List<int>();
        await foreach (var item in stream) result.Add(item);

        Assert.That(result, Is.EqualTo(new[] { 1, 2, 3 }));
    }

    [Test]
    public void Stream_OnErrorResume_Respects_Cancellation()
    {
        var cts = new CancellationTokenSource();
        IStream<int> stream = Stream.Error<int>(new Exception("Fail"))
            .OnErrorResume(ex => Stream.Range(1, 100));

        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var item in stream.WithCancellation(cts.Token))
            {
            }
        });
    }

    #endregion

    #region Single Tests

    [Test]
    public async Task Single_OnErrorResume_Recovers()
    {
        ISingle<int> single = Single.Error<int>(new Exception("Fail"))
            .OnErrorResume(ex => Single.From(42));

        int result = await single.ToTask();
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task Single_OnErrorReturn_Returns_Value()
    {
        ISingle<int> single = Single.Error<int>(new Exception("Fail"))
            .OnErrorReturn(42);

        int result = await single.ToTask();
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public void Single_OnErrorMap_Transforms_Exception()
    {
        ISingle<int> single = Single.Error<int>(new InvalidOperationException("Original"))
            .OnErrorMap(ex => new ArgumentException("Mapped", ex));

        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await single.ToTask();
        });
        Assert.That(ex.Message, Is.EqualTo("Mapped"));
    }

    [Test]
    public async Task Single_ErrorOperators_NoOp_When_No_Error()
    {
        ISingle<int> single = Single.From(1)
            .OnErrorResume(ex => Single.From(10))
            .OnErrorReturn(20)
            .OnErrorMap(ex => new Exception("Should not happen"));

        int result = await single.ToTask();
        Assert.That(result, Is.EqualTo(1));
    }

    #endregion
}
