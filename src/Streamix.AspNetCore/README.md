# Streamix.AspNetCore

Seamless integration between Streamix reactive streams and ASP.NET Core, enabling effortless Server-Sent Events (SSE), WebSocket streaming, and HTTP response streaming with built-in backpressure and cancellation support.

## Quick Start

### Server-Sent Events (SSE)

```csharp
using Streamix.AspNetCore;

[ApiController]
[Route("api")]
public class PricesController : ControllerBase
{
    private readonly IPriceService priceService;

    [HttpGet("prices")]
    public IActionResult GetPrices()
    {
        var stream = priceService.GetPriceUpdates().Publish().RefCount();
        return new StreamResult<decimal>(stream);
    }
}
```

The `StreamResult<T>` handles:
- SSE headers and formatting
- Backpressure management (respects client slowness)
- Cancellation (closes cleanly when client disconnects)
- JSON serialization

### Minimal APIs

```csharp
app.MapGet("/prices", (IPriceService svc, HttpResponse res, CancellationToken ct) =>
    svc.GetPriceUpdates().ToSseAsync(res, ct)
);
```

### WebSocket Streaming

```csharp
[HttpGet("ws-prices")]
public async Task GetPricesWebSocket()
{
    if (HttpContext.WebSockets.IsWebSocketRequest)
    {
        using var ws = await HttpContext.WebSockets.AcceptWebSocketAsync();
        var stream = priceService.GetPriceUpdates();
        await stream.ToWebSocketAsync(ws, HttpContext.RequestAborted);
    }
    else
    {
        HttpContext.Response.StatusCode = 400;
    }
}
```

### JSON Response Streaming

```csharp
[HttpGet("orders/{userId}")]
public async Task GetOrders(int userId)
{
    var stream = orderService.GetOrders(userId);
    await stream.ToJsonResponseAsync(HttpContext.Response, HttpContext.RequestAborted);
}
```

## Extension Methods

- **`ToSseAsync(response, ct)`** - Stream items as Server-Sent Events
- **`ToWebSocketAsync(webSocket, ct)`** - Stream items to a WebSocket
- **`ToWebSocketAsync(webSocket, serializer, ct)`** - Stream with custom serialization
- **`ToJsonResponseAsync(response, ct)`** - Collect stream and write as JSON array

## StreamResult<T>

An `IActionResult` that automatically handles stream serialization as SSE. Supports custom content types and handlers for advanced scenarios.

```csharp
// Custom handler example
var customHandler = async (stream, response, ct) =>
{
    response.ContentType = "text/plain";
    await stream.ForEachAsync(async item =>
    {
        await response.WriteAsync($"{item}\n", ct);
        await response.Body.FlushAsync(ct);
    }, ct);
};

return new StreamResult<Order>(orders, "text/plain", customHandler);
```

## Features

✅ **Backpressure-aware** - Respects client-side flow control  
✅ **Cancellation support** - Cleans up gracefully on disconnect  
✅ **Hot stream compatible** - Works with `.Publish().RefCount()`  
✅ **Zero boilerplate** - One-line integration in controllers  
✅ **Custom serialization** - Override JSON serialization when needed  
✅ **Multiple formats** - SSE, WebSocket, JSON arrays

## When to Use

- **Real-time updates** (prices, notifications, metrics) → SSE or WebSocket
- **Large result sets** (reports, exports) → Streaming JSON
- **Server-sent events** (live feeds) → `ToSseAsync`
- **Bi-directional communication** → WebSocket
