using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace TestDynamo.Tests.CSharp;

public class SuperBasicTestLogger(ITestOutputHelper output) : ILogger
{
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (IsEnabled(logLevel))
            output.WriteLine(formatter(state, exception));
    }

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
}