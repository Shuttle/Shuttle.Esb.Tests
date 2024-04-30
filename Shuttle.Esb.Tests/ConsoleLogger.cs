using Microsoft.Extensions.Logging;
using System;

namespace Shuttle.Esb.Tests
{
    public class ConsoleLogger : ILogger
    {
        private static readonly object Lock = new object();
        private DateTime _previousLogDateTime = DateTime.MinValue;

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
                var now = DateTime.Now;

                Console.WriteLine($"{now:HH:mm:ss.fffffff} / {(_previousLogDateTime > DateTime.MinValue ? $"{(now - _previousLogDateTime):fffffff}" : "0000000")} - {formatter(state, exception)}");

                _previousLogDateTime = now;
            }
        }
    }
}