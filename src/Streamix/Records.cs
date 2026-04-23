namespace Streamix;

/// <summary>
/// Represents a value that may or may not be present.
/// </summary>
/// <typeparam name="T">The type of the value.</typeparam>
public readonly record struct Option<T>
{
    /// <summary>
    /// Gets the value if it exists.
    /// </summary>
    public T? Value { get; }

    /// <summary>
    /// Gets a value indicating whether the option has a value.
    /// </summary>
    public bool HasValue { get; }

    private Option(T? value, bool hasValue)
    {
        Value = value;
        HasValue = hasValue;
    }

    /// <summary>
    /// Creates an option with a value.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <returns>An option with a value.</returns>
    public static Option<T> Some(T value) => new(value, true);

    /// <summary>
    /// Gets an option without a value.
    /// </summary>
    public static Option<T> None => new(default, false);

    /// <summary>
    /// Returns the value if it exists, or a default value.
    /// </summary>
    /// <param name="defaultValue">The default value to return if the option is empty.</param>
    /// <returns>The value if it exists, otherwise the default value.</returns>
    public T? GetValueOrDefault(T? defaultValue = default) => HasValue ? Value : defaultValue;

    /// <summary>
    /// Returns a string that represents the current option.
    /// </summary>
    public override string ToString() => HasValue ? $"Some({Value})" : "None";
}

/// <summary>
/// Represents the result of executing a stream, including items, error, completion status, and duration.
/// </summary>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public sealed record StreamResult<T>(List<T> Items, Exception? Error, bool Completed, TimeSpan Duration);

/// <summary>
/// Represents the result of a single generation step.
/// </summary>
/// <typeparam name="TState">The type of the state.</typeparam>
/// <typeparam name="T">The type of items in the stream.</typeparam>
public readonly record struct GenerationResult<TState, T>(TState NextState, T? Value, bool HasValue, bool IsComplete)
{
    /// <summary>
    /// Creates a result that emits a value and transitions to the next state.
    /// </summary>
    /// <param name="value">The value to emit.</param>
    /// <param name="nextState">The next state for generation.</param>
    /// <returns>A generation result that emits a value.</returns>
    public static GenerationResult<TState, T> Emit(T value, TState nextState) => new(nextState, value, true, false);

    /// <summary>
    /// Creates a result that skips emission and transitions to the next state.
    /// </summary>
    /// <param name="nextState">The next state for generation.</param>
    /// <returns>A generation result that skips emission.</returns>
    public static GenerationResult<TState, T> Skip(TState nextState) => new(nextState, default, false, false);

    /// <summary>
    /// Creates a result that signals the end of the stream.
    /// </summary>
    /// <returns>A generation result that signals completion.</returns>
    public static GenerationResult<TState, T> Complete() => new(default!, default, false, true);
}

/// <summary>
/// Represents a value that has been timestamped.
/// </summary>
/// <typeparam name="T">The type of the value.</typeparam>
public readonly record struct Timestamped<T>(T Value, DateTimeOffset Timestamp);

/// <summary>
/// Provides static methods for creating timestamped values.
/// </summary>
public static class Timestamped
{
    /// <summary>
    /// Creates a new timestamped value.
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="timestamp">The timestamp.</param>
    /// <returns>A new timestamped value.</returns>
    public static Timestamped<T> Create<T>(T value, DateTimeOffset timestamp) => new(value, timestamp);
}
