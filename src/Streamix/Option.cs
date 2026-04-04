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
