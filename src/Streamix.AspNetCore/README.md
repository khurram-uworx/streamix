# Streamix.AspNetCore

ASP.NET Core streaming integration for Streamix.

This package connects Streamix pipelines to Server-Sent Events (SSE), WebSocket streaming, and HTTP response streaming with backpressure-aware and cancellation-friendly behavior.

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

## Extension Methods

- **`ToSseAsync(response, ct)`** - Stream items as Server-Sent Events
- **`ToWebSocketAsync(webSocket, ct)`** - Stream items to a WebSocket
- **`ToWebSocketAsync(webSocket, serializer, ct)`** - Stream with custom serialization
- **`ToJsonResponseAsync(response, ct)`** - Collect stream and write as JSON array

## Features

✅ **Backpressure-aware** - Respects client-side flow control  
✅ **Cancellation support** - Cleans up gracefully on disconnect  
✅ **Hot stream compatible** - Works with `.Publish().RefCount()`  
✅ **Zero boilerplate** - One-line integration in controllers  
✅ **Custom serialization** - Override JSON serialization when needed  
✅ **Multiple formats** - SSE, WebSocket, JSON arrays

## More Shapes

`StreamResult<T>` gives you an `IActionResult` wrapper for SSE endpoints, and the package also supports minimal APIs, direct WebSocket streaming, and JSON response streaming.

## When to Use

- **Real-time updates** (prices, notifications, metrics) → SSE or WebSocket
- **Large result sets** (reports, exports) → Streaming JSON
- **Server-sent events** (live feeds) → `ToSseAsync`
- **Bi-directional communication** → WebSocket

## Learn More

- Overview and package map: [README.md](https://github.com/khurram-uworx/streamix/blob/main/README.md)
- Developer guide: [GETTING-STARTED.md](https://github.com/khurram-uworx/streamix/blob/main/GETTING-STARTED.md)
- Architecture and design notes: [ARCHITECTURE.md](https://github.com/khurram-uworx/streamix/blob/main/ARCHITECTURE.md)
- Repository: [github.com/khurram-uworx/streamix](https://github.com/khurram-uworx/streamix)
