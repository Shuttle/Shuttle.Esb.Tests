using Microsoft.Extensions.Logging;

namespace Shuttle.Esb.Tests
{
    public class FixtureFileLoggerProvider : ILoggerProvider
    {
        private readonly FixtureFileLogger _logger;

        public FixtureFileLoggerProvider(string name)
        {
            _logger = new FixtureFileLogger(name);
        }

        public void Dispose()
        {
            _logger.Dispose();
        }

        public ILogger CreateLogger(string categoryName)
        {
            return _logger;
        }
    }
}