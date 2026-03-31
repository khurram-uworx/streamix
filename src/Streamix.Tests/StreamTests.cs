using NUnit.Framework;
using Streamix.Abstractions;

namespace Streamix.Tests;

[TestFixture]
public class StreamTests
{
    [Test]
    public async Task Empty_Stream_Is_Empty()
    {
        IStream<int> stream = Stream.Empty<int>();
        int count = 0;
        await foreach (var _ in stream)
        {
            count++;
        }
        Assert.That(count, Is.EqualTo(0));
    }
}
