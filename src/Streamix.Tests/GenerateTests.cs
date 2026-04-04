using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class GenerateTests
{
    [Test]
    public async Task Generate_Sync_Finite_Emits_Items_And_Completes()
    {
        var stream = Stream.Generate<int, int>(0, state =>
        {
            if (state >= 5) return GenerationResult<int, int>.Complete();
            return GenerationResult<int, int>.Emit(state, state + 1);
        });

        var result = await stream.ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public async Task Generate_Sync_Infinite_Can_Be_Taken()
    {
        var stream = Stream.Generate<int, int>(0, state =>
        {
            return GenerationResult<int, int>.Emit(state, state + 1);
        });

        var result = await stream.Take(5).ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public async Task Generate_Sync_Supports_Skip()
    {
        var stream = Stream.Generate<int, int>(0, state =>
        {
            if (state >= 10) return GenerationResult<int, int>.Complete();
            if (state % 2 != 0) return GenerationResult<int, int>.Skip(state + 1);
            return GenerationResult<int, int>.Emit(state, state + 1);
        });

        var result = await stream.ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { 0, 2, 4, 6, 8 }));
    }

    [Test]
    public async Task Generate_Async_Finite_Emits_Items_And_Completes()
    {
        var stream = Stream.Generate<int, int>(0, async (state, ct) =>
        {
            await Task.Yield();
            if (state >= 5) return GenerationResult<int, int>.Complete();
            return GenerationResult<int, int>.Emit(state, state + 1);
        });

        var result = await stream.ToListAsync();
        Assert.That(result, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public async Task Generate_Async_Respects_Cancellation()
    {
        var generationCount = 0;
        var stream = Stream.Generate<int, int>(0, async (state, ct) =>
        {
            generationCount++;
            await Task.Delay(100, ct);
            return GenerationResult<int, int>.Emit(state, state + 1);
        });

        using var cts = new CancellationTokenSource();
        var consumeTask = stream.ToListAsync(cts.Token);

        await Task.Delay(150);
        await cts.CancelAsync();

        Assert.CatchAsync<OperationCanceledException>(async () => await consumeTask);
        Assert.That(generationCount, Is.LessThan(5));
    }

    [Test]
    public void Generate_Sync_Propagates_Exception()
    {
        var stream = Stream.Generate<int, int>(0, state =>
        {
            if (state == 2) throw new InvalidOperationException("Generation Error");
            return GenerationResult<int, int>.Emit(state, state + 1);
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }

    [Test]
    public void Generate_Async_Propagates_Exception()
    {
        var stream = Stream.Generate<int, int>(0, async (state, ct) =>
        {
            await Task.Yield();
            if (state == 2) throw new InvalidOperationException("Async Generation Error");
            return GenerationResult<int, int>.Emit(state, state + 1);
        });

        Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.ToListAsync());
    }
}
