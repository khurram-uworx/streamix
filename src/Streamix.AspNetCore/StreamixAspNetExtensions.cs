using System.Net.WebSockets;
using System.Text.Json;
using Microsoft.AspNetCore.Http;

namespace Streamix.AspNetCore;

/// <summary>
/// Extension methods for Streamix streams to integrate with ASP.NET Core HTTP response handling,
/// Server-Sent Events (SSE), and WebSocket streaming.
/// </summary>
public static class StreamixAspNetExtensions
{
    /// <summary>
    /// Streams items to the HTTP response as Server-Sent Events (SSE).
    /// </summary>
    /// <param name="stream">The stream to serialize and send.</param>
    /// <param name="response">The HttpResponse to write to.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task ToSseAsync<T>(
        this IStream<T> stream,
        HttpResponse response,
        CancellationToken ct = default)
    {
        response.ContentType = "text/event-stream";
        response.Headers.Append("Cache-Control", "no-cache");
        response.Headers.Append("Connection", "keep-alive");

        await stream.ForEachAsync(async item =>
        {
            var json = JsonSerializer.Serialize(item);
            await response.WriteAsync($"data: {json}\n\n", ct);
            await response.Body.FlushAsync(ct);
        }, ct);
    }

    /// <summary>
    /// Streams items to a WebSocket connection with optional serialization.
    /// </summary>
    /// <param name="stream">The stream to serialize and send.</param>
    /// <param name="webSocket">The WebSocket to write to.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task ToWebSocketAsync<T>(
        this IStream<T> stream,
        WebSocket webSocket,
        CancellationToken ct = default)
    {
        await stream.ToWebSocketAsync(webSocket, SerializeToJsonBytes, ct);
    }

    /// <summary>
    /// Streams items to a WebSocket connection with custom serialization.
    /// </summary>
    /// <param name="stream">The stream to serialize and send.</param>
    /// <param name="webSocket">The WebSocket to write to.</param>
    /// <param name="serializer">Function to serialize each item to bytes.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task ToWebSocketAsync<T>(
        this IStream<T> stream,
        WebSocket webSocket,
        Func<T, byte[]> serializer,
        CancellationToken ct = default)
    {
        try
        {
            await stream.ForEachAsync(async item =>
            {
                var buffer = serializer(item);
                await webSocket.SendAsync(
                    new ArraySegment<byte>(buffer),
                    WebSocketMessageType.Text,
                    endOfMessage: true,
                    ct);
            }, ct);
        }
        finally
        {
            if (webSocket.State == WebSocketState.Open)
            {
                await webSocket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Stream completed",
                    CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// Converts a stream to a list asynchronously and writes it as JSON to the response.
    /// </summary>
    /// <param name="stream">The stream to collect and serialize.</param>
    /// <param name="response">The HttpResponse to write to.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task ToJsonResponseAsync<T>(
        this IStream<T> stream,
        HttpResponse response,
        CancellationToken ct = default)
    {
        var list = await stream.ToListAsync(ct);
        response.ContentType = "application/json";
        await response.WriteAsJsonAsync(list, cancellationToken: ct);
    }

    private static byte[] SerializeToJsonBytes<T>(T item)
    {
        var json = JsonSerializer.Serialize(item);
        return System.Text.Encoding.UTF8.GetBytes(json);
    }
}
