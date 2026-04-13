using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class DevxTests
{
    [Test]
    public void Stream_Named_SetsName()
    {
        var stream = Stream.Range(1, 5).Named("MyStream");
        Assert.That(stream.Name, Is.EqualTo("MyStream"));
    }

    [Test]
    public void Stream_Name_PropagatesThroughMap()
    {
        var stream = Stream.Range(1, 5).Named("MyStream").Map(x => x * 2);
        Assert.That(stream.Name, Is.EqualTo("MyStream"));
    }

    [Test]
    public void Stream_Name_PropagatesThroughFilter()
    {
        var stream = Stream.Range(1, 5).Named("MyStream").Filter(x => x % 2 == 0);
        Assert.That(stream.Name, Is.EqualTo("MyStream"));
    }

    [Test]
    public void Stream_Name_PropagatesThroughDoOnNext()
    {
        var stream = Stream.Range(1, 5).Named("MyStream").DoOnNext(x => { });
        Assert.That(stream.Name, Is.EqualTo("MyStream"));
    }

    [Test]
    public void Stream_Name_PropagatesThroughPublishAndRefCount()
    {
        var source = Stream.Range(1, 5).Named("MyStream");
        var published = source.Publish();
        Assert.That(published.Name, Is.EqualTo("MyStream"));

        var refCounted = published.RefCount();
        Assert.That(refCounted.Name, Is.EqualTo("MyStream"));
    }

    [Test]
    public void Single_Named_SetsName()
    {
        var single = Single.From(42).Named("MySingle");
        Assert.That(single.Name, Is.EqualTo("MySingle"));
    }

    [Test]
    public void Single_Name_PropagatesThroughMap()
    {
        var single = Single.From(42).Named("MySingle").Map(x => x * 2);
        Assert.That(single.Name, Is.EqualTo("MySingle"));
    }

    [Test]
    public void Single_Name_PropagatesThroughDoOnNext()
    {
        var single = Single.From(42).Named("MySingle").DoOnNext(x => { });
        Assert.That(single.Name, Is.EqualTo("MySingle"));
    }

    [Test]
    public void Stream_Named_CanBeOverridden()
    {
        var stream = Stream.Range(1, 5).Named("Initial").Named("Overridden");
        Assert.That(stream.Name, Is.EqualTo("Overridden"));
    }
}
