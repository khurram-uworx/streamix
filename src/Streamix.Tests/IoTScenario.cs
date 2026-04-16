using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Streamix.Tests.Implementations;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;

namespace Streamix.Tests;

record TemperatureReading(string SensorId, double Temperature, DateTime Timestamp);
record TemperatureUpdate(string Reason, double Temperature, DateTime Timestamp);
record IoTScenarioOptions(int SensorCount, TimeSpan SensorPeriod, TimeSpan Window, TimeSpan HeartbeatPeriod, TimeSpan RunDuration)
{
    public static IoTScenarioOptions Demo { get; } = new(
        SensorCount: 3,
        SensorPeriod: TimeSpan.FromSeconds(1),
        Window: TimeSpan.FromSeconds(10),
        HeartbeatPeriod: TimeSpan.FromSeconds(10),
        RunDuration: TimeSpan.FromSeconds(30));
}

[TestFixture]
public class IoTScenarios
{
    static TemperatureReading createReading(string sensorId, Random random)
        => new(sensorId, 20 + (random.NextDouble() * 10), DateTime.UtcNow);

    static void logUpdate(ILogger logger, TemperatureUpdate update)
        => logger.LogInformation("{Reason}: Max Temp {Temperature:F2}°C at {Timestamp:HH:mm:ss}", update.Reason, update.Temperature, update.Timestamp);

    static async IAsyncEnumerable<TemperatureReading> createIxSensor(
        string sensorId,
        int seed,
        IoTScenarioOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var random = new Random(seed);

        while (!cancellationToken.IsCancellationRequested)
        {
            yield return createReading(sensorId, random);
            await Task.Delay(options.SensorPeriod, cancellationToken);
        }
    }

    static IStream<TemperatureReading> createStreamixSensor(string sensorId, int seed, IoTScenarioOptions options)
    {
        var random = new Random(seed);

        return Stream.Interval(TimeSpan.Zero, options.SensorPeriod)
            .Map(_ => createReading(sensorId, random));
    }

    static IEnumerable<IObservable<TemperatureReading>> createRxSensors(IoTScenarioOptions options)
    {
        for (int i = 0; i < options.SensorCount; i++)
        {
            var sensorId = $"Sensor-{i + 1}";
            var random = new Random(1000 + i);

            yield return Observable.Interval(options.SensorPeriod)
                .Select(_ => createReading(sensorId, random));
        }
    }

    static IEnumerable<IAsyncObservable<TemperatureReading>> createAsyncRxSensors(IoTScenarioOptions options)
    {
        for (int i = 0; i < options.SensorCount; i++)
        {
            var sensorId = $"Sensor-{i + 1}";
            var random = new Random(1000 + i);

            yield return AsyncObservable.Interval(options.SensorPeriod)
                .Select(_ => createReading(sensorId, random));
        }
    }

    [Test]
    [Ignore("Long-running scenario demo")]
    public async Task RxWindow()
    {
        var logger = new NUnitLogger<IoTScenarios>();
        var options = IoTScenarioOptions.Demo;

        var allSensors = createRxSensors(options).Merge();

        var windowMax = allSensors
            .Window(options.Window, options.SensorPeriod)
            .SelectMany(window => window
                .ToList()
                .Select(readings => readings.Count == 0 ? double.NaN : readings.Max(item => item.Temperature)))
            .Publish()
            .RefCount();

        var changesOnly = windowMax
            .Where(max => !double.IsNaN(max))
            .DistinctUntilChanged()
            .Select(max => new TemperatureUpdate("change", max, DateTime.UtcNow));

        var heartbeat = Observable.Interval(options.HeartbeatPeriod)
            .WithLatestFrom(windowMax, (_, max) => new TemperatureUpdate("heartbeat", max, DateTime.UtcNow))
            .Where(update => !double.IsNaN(update.Temperature));

        using var subscription = changesOnly.Merge(heartbeat)
            .Subscribe(update => logUpdate(logger, update));

        await Task.Delay(options.RunDuration);
    }

    [Test]
    [Ignore("Long-running scenario demo")]
    public async Task AsyncRxWindow()
    {
        var logger = new NUnitLogger<IoTScenarios>();
        var options = IoTScenarioOptions.Demo;

        // Subscribe to all sensors
        var sensors = createAsyncRxSensors(options).ToList();
        var subscriptions = new List<IAsyncDisposable>();

        foreach (var sensor in sensors)
        {
            var subscription = await sensor.SubscribeAsync(
                async reading =>
                {
                    logUpdate(logger, new TemperatureUpdate("reading", reading.Temperature, DateTime.UtcNow));
                });
            subscriptions.Add(subscription);
        }

        try
        {
            await Task.Delay(options.RunDuration);
        }
        finally
        {
            foreach (var sub in subscriptions)
            {
                await sub.DisposeAsync();
            }
        }
    }

    static async IAsyncEnumerable<TemperatureUpdate> monitorSlidingMax(
        IAsyncEnumerable<TemperatureReading> readings,
        IoTScenarioOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var window = new List<TemperatureReading>();
        double? lastEmittedMax = null;
        var nextHeartbeatAt = DateTime.UtcNow + options.HeartbeatPeriod;

        await foreach (var reading in readings.WithCancellation(cancellationToken))
        {
            window.Add(reading);

            var cutoff = reading.Timestamp - options.Window;
            window.RemoveAll(item => item.Timestamp < cutoff);

            if (window.Count == 0)
            {
                continue;
            }

            var currentMax = window.Max(item => item.Temperature);

            if (lastEmittedMax is null || currentMax != lastEmittedMax.Value)
            {
                yield return new TemperatureUpdate("change", currentMax, reading.Timestamp);
                lastEmittedMax = currentMax;
            }

            while (lastEmittedMax is not null && reading.Timestamp >= nextHeartbeatAt)
            {
                yield return new TemperatureUpdate("heartbeat", lastEmittedMax.Value, reading.Timestamp);
                nextHeartbeatAt += options.HeartbeatPeriod;
            }
        }
    }

    [Test]
    [Ignore("Long-running scenario demo")]
    public async Task IxWindow()
    {
        var logger = new NUnitLogger<IoTScenarios>();
        var options = IoTScenarioOptions.Demo;

        var sensors = Enumerable.Range(0, options.SensorCount)
            .Select(index => createIxSensor($"Sensor-{index + 1}", 1000 + index, options));

        using var cts = new CancellationTokenSource(options.RunDuration);

        var updates = monitorSlidingMax(AsyncEnumerableEx.Merge(sensors), options, cts.Token);

        await foreach (var update in updates.WithCancellation(cts.Token))
        {
            logUpdate(logger, update);
        }
    }

    [Test]
    [Ignore("Long-running scenario demo")]
    public async Task StreamixWindow()
    {
        var logger = new NUnitLogger<IoTScenarios>();
        var options = IoTScenarioOptions.Demo;

        var sensors = Enumerable.Range(0, options.SensorCount)
            .Select(index => createStreamixSensor($"Sensor-{index + 1}", 1000 + index, options))
            .ToArray();

        using var cts = new CancellationTokenSource(options.RunDuration);

        var updates = Stream.Merge(sensors)
            .MapWithTimestamp(r => r.Timestamp)
            .WindowByTime(
                duration: options.Window,
                slide: options.SensorPeriod)
            .FlatMap(window => 
                Stream.From<double>((CancellationToken ct) => 
                {
                    var max = window.Select(ts => ts.Value.Temperature).MaxAsync(cancellationToken: ct);
                    return max;
                })
                .Map(max => new TemperatureUpdate("window", max, DateTime.UtcNow))
            );

        await updates.ForEachAsync(update => logUpdate(logger, update), cts.Token);
    }
}
