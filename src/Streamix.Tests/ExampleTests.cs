using NUnit.Framework;

namespace Streamix.Tests;

[TestFixture]
public class ExampleTests
{
    [Test]
    public async Task Readme_Example_Works()
    {
        var output = new List<int>();

        // Example from README.md
        await Stream.Range(1, 10)
            .Filter(x => x % 2 == 0)
            .Map(x => x * 10)
            .ForEachAsync(item => output.Add(item));

        Assert.That(output, Is.EqualTo(new[] { 20, 40, 60, 80, 100 }));
    }
}
