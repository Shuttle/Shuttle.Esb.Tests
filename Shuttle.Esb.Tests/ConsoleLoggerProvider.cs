using Microsoft.Extensions.Logging;

namespace Shuttle.Esb.Tests;

public class ConsoleLoggerProvider : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName)
    {
        return new ConsoleLogger();
    }

    public void Dispose()
    {
    }
}