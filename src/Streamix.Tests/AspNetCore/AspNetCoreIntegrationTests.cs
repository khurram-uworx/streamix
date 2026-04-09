using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using NUnit.Framework;
using Streamix.AspNetCore;

namespace Streamix.Tests.AspNetCore;

[TestFixture]
public class AspNetCoreIntegrationTests
{
    [Test]
    public async Task ToSseAsync_WritesServerSentEventsFormat()
    {
        var items = new[] { 1, 2, 3 };
        var stream = Stream.From(items);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        var app = builder.Build();

        app.MapGet("/stream", async (HttpResponse response, CancellationToken ct) =>
        {
            await stream.ToSseAsync(response, ct);
        });

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/stream", HttpCompletionOption.ResponseContentRead);

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(response.Content.Headers.ContentType?.MediaType, Is.EqualTo("text/event-stream"));

        var content = await response.Content.ReadAsStringAsync();
        Assert.That(content, Does.Contain("data: 1"));
        Assert.That(content, Does.Contain("data: 2"));
        Assert.That(content, Does.Contain("data: 3"));

        await app.StopAsync();
    }

    [Test]
    public async Task ToSseAsync_SerializesObjectsAsJson()
    {
        var items = new[] { new { Id = 1, Name = "A" }, new { Id = 2, Name = "B" } };
        var stream = Stream.From(items);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        var app = builder.Build();

        app.MapGet("/stream", async (HttpResponse response, CancellationToken ct) =>
        {
            await stream.ToSseAsync(response, ct);
        });

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/stream", HttpCompletionOption.ResponseContentRead);

        var content = await response.Content.ReadAsStringAsync();
        Assert.That(content, Does.Contain("\"Id\":1"));
        Assert.That(content, Does.Contain("\"Name\":\"A\""));

        await app.StopAsync();
    }

    [Test]
    public async Task StreamResult_ReturnsActionResult()
    {
        var items = new[] { "hello", "world" };
        var stream = Stream.From(items);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        var app = builder.Build();

        app.MapGet("/result", async (HttpContext ctx) =>
        {
            var result = new Streamix.AspNetCore.StreamResult<string>(stream);
            await result.ExecuteResultAsync(new Microsoft.AspNetCore.Mvc.ActionContext { HttpContext = ctx });
        });

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/result", HttpCompletionOption.ResponseContentRead);

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        var content = await response.Content.ReadAsStringAsync();
        Assert.That(content, Does.Contain("hello"));
        Assert.That(content, Does.Contain("world"));

        await app.StopAsync();
    }

    [Test]
    public async Task ToJsonResponseAsync_SerializesStreamAsJsonArray()
    {
        var items = new[] { 10, 20, 30 };
        var stream = Stream.From(items);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        var app = builder.Build();

        app.MapGet("/json", async (HttpResponse response, CancellationToken ct) =>
        {
            await stream.ToJsonResponseAsync(response, ct);
        });

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/json");

        Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        Assert.That(response.Content.Headers.ContentType?.MediaType, Is.EqualTo("application/json"));

        var content = await response.Content.ReadAsStringAsync();
        var array = JsonSerializer.Deserialize<int[]>(content);
        Assert.That(array, Is.EqualTo(new[] { 10, 20, 30 }));

        await app.StopAsync();
    }

    [Test]
    public async Task ToSseAsync_WritesMultipleEvents()
    {
        var stream = Stream.Range(1, 5);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        var app = builder.Build();

        app.MapGet("/range", async (HttpResponse response, CancellationToken ct) =>
        {
            await stream.ToSseAsync(response, ct);
        });

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/range", HttpCompletionOption.ResponseContentRead);
        var content = await response.Content.ReadAsStringAsync();

        var lines = content.Split(new[] { "\n\n" }, StringSplitOptions.RemoveEmptyEntries);
        Assert.That(lines.Length, Is.GreaterThanOrEqualTo(5));

        await app.StopAsync();
    }
}
