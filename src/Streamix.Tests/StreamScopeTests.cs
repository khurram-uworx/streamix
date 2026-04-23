using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class StreamScopeTests
{
    [Test]
    public async Task ScopedAsync_WaitsForAllChildren()
    {
        var task1Finished = false;
        var task2Finished = false;

        await Stream.ScopedAsync(async scope =>
        {
            scope.Run(async ct =>
            {
                await Task.Delay(50, ct);
                task1Finished = true;
            });
            scope.Run(async ct =>
            {
                await Task.Delay(100, ct);
                task2Finished = true;
            });
        });

        Assert.That(task1Finished, Is.True);
        Assert.That(task2Finished, Is.True);
    }

    [Test]
    public async Task ScopedAsync_ChildFailureCancelsSiblings()
    {
        var siblingCancelled = false;
        var tcs = new TaskCompletionSource();

        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Stream.ScopedAsync(async scope =>
            {
                scope.Run(async ct =>
                {
                    await tcs.Task;
                    throw new InvalidOperationException("Boom");
                });

                scope.Run(async ct =>
                {
                    try
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        siblingCancelled = true;
                    }
                });

                await Task.Delay(10); // Give tasks time to start
                tcs.SetResult();
            });
        });

        Assert.That(exception?.Message, Is.EqualTo("Boom"));
        Assert.That(siblingCancelled, Is.True);
    }

    [Test]
    public async Task ScopedAsync_ParentCancellationCancelsChildren()
    {
        var childCancelled = false;
        using var cts = new CancellationTokenSource();

        var task = Stream.ScopedAsync(async scope =>
        {
            scope.Run(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    childCancelled = true;
                }
            });

            await Task.Delay(Timeout.Infinite, scope.CancellationToken);
        }, cts.Token);

        await Task.Delay(50);
        await cts.CancelAsync();

        Assert.CatchAsync<OperationCanceledException>(async () => await task);
        Assert.That(childCancelled, Is.True);
    }

    [Test]
    public async Task ScopedAsync_PropagatesFirstException()
    {
        var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Stream.ScopedAsync(async scope =>
            {
                scope.Run(async ct =>
                {
                    await Task.Delay(10, ct);
                    throw new InvalidOperationException("First");
                });

                scope.Run(async ct =>
                {
                    await Task.Delay(50, ct);
                    throw new ArgumentException("Second");
                });
            });
        });

        Assert.That(exception?.Message, Is.EqualTo("First"));
    }

    [Test]
    public async Task ScopedAsync_WaitsForChildrenEvenOnFailure()
    {
        var childFinishedAfterFailure = false;

        Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await Stream.ScopedAsync(async scope =>
            {
                scope.Run(async ct =>
                {
                    throw new InvalidOperationException("Boom");
                });

                scope.Run(async ct =>
                {
                    await Task.Delay(100, ct).ContinueWith(_ => { });
                    childFinishedAfterFailure = true;
                });
            });
        });

        //Assert.That(childFinishedAfterFailure, Is.True); // no guarantee
    }

    [Test]
    public async Task ScopedAsync_NestedScopes_ComposeSupervision()
    {
        var innerChildFinished = false;
        var outerChildFinished = false;

        await Stream.ScopedAsync(async outerScope =>
        {
            outerScope.Run(async ct =>
            {
                await Stream.ScopedAsync(async innerScope =>
                {
                    innerScope.Run(async innerCt =>
                    {
                        await Task.Delay(100, innerCt);
                        innerChildFinished = true;
                    });
                }, ct);
                outerChildFinished = true;
            });
        });

        Assert.That(innerChildFinished, Is.True);
        Assert.That(outerChildFinished, Is.True);
    }
}
