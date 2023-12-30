using Microsoft.Extensions.Logging;
using System;

namespace Shuttle.Esb.Tests
{
    public class ConsoleLogger : ILogger
    {
        private static readonly object Lock = new object();

        public IDisposable BeginScope<TState>(TState state)
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            lock (Lock)
            {
                Console.WriteLine($"{DateTime.Now:O} : {formatter(state, exception)}");
            }
        }
    }
}