namespace Streamix;

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
