using System;
using System.IO;
using Microsoft.Extensions.Logging;

namespace Shuttle.Esb.Tests
{
    public class FixtureFileLogger : ILogger, IDisposable
    {
        private static readonly object Lock = new object();
        private readonly StreamWriter _stream;

        public FixtureFileLogger(string name)
        {
            var folder = Path.Combine(AppContext.BaseDirectory, ".logs");
            var path = Path.Combine(folder, $"{name}--{DateTime.Now:yyy-MM-dd--HH-mm-ss.fff}.log");

            Console.WriteLine($"[FixtureFileLogger] : path = '{path}'");

            if (!Directory.Exists(folder))
            {
                Directory.CreateDirectory(folder);
            }

            _stream = new StreamWriter(path);
        }

        public void Dispose()
        {
            _stream?.Dispose();
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            lock (Lock)
            {
                _stream.WriteLine($"{DateTime.Now:O} - {formatter(state, exception)}");
                _stream.Flush();
            }
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull
        {
            return default!;
        }
    }
}