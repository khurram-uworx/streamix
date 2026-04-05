using NUnit.Framework;

namespace Streamix.Tests;

/// <summary>
/// This test class ensures that the promised public API surface is available and compiles.
/// It uses reflection to verify members existence without executing them, as they are currently stubs.
/// </summary>
[TestFixture]
public class ApiContractTests
{
    [Test]
    public void Stream_Contract_Surface_Area_Exists()
    {
        var type = typeof(IStream<int>);

        // Factory methods on static facade
        Assert.That(typeof(Stream).GetMethod("Range"), Is.Not.Null);
        Assert.That(typeof(Stream).GetMethod("Empty"), Is.Not.Null);
        Assert.That(typeof(Stream).GetMethods().Any(m => m.Name == "From"), Is.True);
        Assert.That(typeof(Stream).GetMethod("FromEvent"), Is.Not.Null);
        Assert.That(typeof(Stream).GetMethod("Merge"), Is.Not.Null);
        Assert.That(typeof(Stream).GetMethods().Any(m => m.Name == "Zip"), Is.True);

        // Instance methods on IStream
        Assert.That(type.GetMethod("Map"), Is.Not.Null);
        Assert.That(type.GetMethod("Select"), Is.Not.Null);
        Assert.That(type.GetMethod("Filter"), Is.Not.Null);
        Assert.That(type.GetMethod("Where"), Is.Not.Null);
        Assert.That(type.GetMethods().Any(m => m.Name == "FlatMap"), Is.True);
        Assert.That(type.GetMethod("FlatMapMany"), Is.Not.Null);
        Assert.That(type.GetMethod("Take"), Is.Not.Null);
        Assert.That(type.GetMethod("Skip"), Is.Not.Null);
        Assert.That(type.GetMethod("Buffer"), Is.Not.Null);
        Assert.That(type.GetMethod("Window"), Is.Not.Null);
        Assert.That(type.GetMethod("Throttle"), Is.Not.Null);
        Assert.That(type.GetMethod("Delay"), Is.Not.Null);
        Assert.That(type.GetMethods().Any(m => m.Name == "Retry"), Is.True);
        Assert.That(type.GetMethod("Timeout"), Is.Not.Null);
        Assert.That(type.GetMethod("OnErrorResume"), Is.Not.Null);
        Assert.That(type.GetMethod("OnErrorReturn"), Is.Not.Null);
        Assert.That(type.GetMethod("OnErrorMap"), Is.Not.Null);
        Assert.That(type.GetMethod("Publish"), Is.Not.Null);
        Assert.That(type.GetMethod("RunOn"), Is.Not.Null);
        Assert.That(type.GetMethods().Any(m => m.Name == "ForEachAsync"), Is.True);

        // Terminal extensions (LINQ style)
        var extensionsType = typeof(TerminalExtensions);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "FirstAsync"), Is.True);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "FirstOrDefaultAsync"), Is.True);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "LastAsync"), Is.True);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "LastOrDefaultAsync"), Is.True);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "SingleAsync"), Is.True);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "SingleOrDefaultAsync"), Is.True);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "CountAsync"), Is.True);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "AnyAsync"), Is.True);
        Assert.That(extensionsType.GetMethods().Any(m => m.Name == "AllAsync"), Is.True);
    }

    [Test]
    public void Single_Contract_Surface_Area_Exists()
    {
        var type = typeof(ISingle<int>);

        // Factory methods on static facade
        Assert.That(typeof(Single).GetMethods().Any(m => m.Name == "From"), Is.True);

        // Instance methods on ISingle
        Assert.That(type.GetMethod("Map"), Is.Not.Null);
        Assert.That(type.GetMethod("Select"), Is.Not.Null);
        Assert.That(type.GetMethod("FlatMap"), Is.Not.Null);
        Assert.That(type.GetMethod("FlatMapMany"), Is.Not.Null);
        Assert.That(type.GetMethod("OnErrorResume"), Is.Not.Null);
        Assert.That(type.GetMethod("OnErrorReturn"), Is.Not.Null);
        Assert.That(type.GetMethod("OnErrorMap"), Is.Not.Null);
        Assert.That(type.GetMethod("RunOn"), Is.Not.Null);
        Assert.That(type.GetMethods().Any(m => m.Name == "ForEachAsync"), Is.True);
        Assert.That(type.GetMethods().Any(m => m.Name == "Retry"), Is.True);
        Assert.That(type.GetMethod("ToTask"), Is.Not.Null);
    }

    [Test]
    public void ConnectableStream_Contract_Surface_Area_Exists()
    {
        var type = typeof(IConnectableStream<int>);

        Assert.That(type.GetMethod("Connect"), Is.Not.Null);
        Assert.That(type.GetMethod("RefCount"), Is.Not.Null);
    }
}
