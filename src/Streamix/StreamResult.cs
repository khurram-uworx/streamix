namespace Streamix;

/// <summary>
/// Represents the result of executing a stream, including items, error, completion status, and duration.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public sealed record StreamResult<T>(List<T> Items, Exception? Error, bool Completed, TimeSpan Duration);
