using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Streamix.AspNetCore;

/// <summary>
/// An IActionResult that streams items from an IStream&lt;T&gt; as Server-Sent Events (SSE).
/// This enables controllers and minimal APIs to directly return streams.
/// </summary>
/// <example>
/// <code>
/// [HttpGet("prices")]
/// public IActionResult GetPrices()
/// {
///     var priceStream = _priceService.GetPriceUpdates().Publish().RefCount();
///     return new StreamResult&lt;decimal&gt;(priceStream);
/// }
/// </code>
/// </example>
public class StreamResult<T> : IActionResult
{
    private readonly IStream<T> stream;
    private readonly string contentType;
    private readonly Func<IStream<T>, HttpResponse, CancellationToken, Task>? customHandler;

    /// <summary>
    /// Creates a StreamResult that streams items as Server-Sent Events.
    /// </summary>
    /// <param name="stream">The stream to serialize and send.</param>
    public StreamResult(IStream<T> stream)
        : this(stream, "text/event-stream", null)
    {
    }

    /// <summary>
    /// Creates a StreamResult with a custom content type and handler.
    /// </summary>
    /// <param name="stream">The stream to process.</param>
    /// <param name="contentType">The Content-Type header value.</param>
    /// <param name="customHandler">Optional custom handler function. If not provided, uses SSE format.</param>
    public StreamResult(
        IStream<T> stream,
        string contentType,
        Func<IStream<T>, HttpResponse, CancellationToken, Task>? customHandler = null)
    {
        this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
        this.contentType = contentType ?? throw new ArgumentNullException(nameof(contentType));
        this.customHandler = customHandler;
    }

    /// <summary>
    /// Executes the stream result by sending items to the response.
    /// </summary>
    /// <param name="context">The action context containing the HTTP response.</param>
    public async Task ExecuteResultAsync(ActionContext context)
    {
        var response = context.HttpContext.Response;
        response.ContentType = contentType;

        if (customHandler != null)
        {
            await customHandler(stream, response, context.HttpContext.RequestAborted);
        }
        else
        {
            await stream.ToSseAsync(response, context.HttpContext.RequestAborted);
        }
    }
}
