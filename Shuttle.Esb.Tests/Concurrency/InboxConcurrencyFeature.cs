using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests
{
    public class InboxConcurrencyFeature : IPipelineObserver<OnAfterGetMessage>
    {
        private readonly List<DateTime> _datesAfterGetMessage = new List<DateTime>();
        private readonly object _lock = new object();
        private readonly ILogger<InboxConcurrencyFeature> _logger;
        private DateTime _firstDateAfterGetMessage = DateTime.MinValue;

        public InboxConcurrencyFeature(ILogger<InboxConcurrencyFeature> logger, IPipelineFactory pipelineFactory)
        {
            Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory)).PipelineCreated += PipelineCreated;

            _logger = Guard.AgainstNull(logger, nameof(logger));
        }

        public int OnAfterGetMessageCount => _datesAfterGetMessage.Count;

        public void Execute(OnAfterGetMessage pipelineEvent)
        {
            lock (_lock)
            {
                var dateTime = DateTime.Now;

                if (_firstDateAfterGetMessage == DateTime.MinValue)
                {
                    _firstDateAfterGetMessage = DateTime.Now;

                    _logger.LogInformation("Offset date: {0:yyyy-MM-dd HH:mm:ss.fff}", _firstDateAfterGetMessage);
                }

                _datesAfterGetMessage.Add(dateTime);

                _logger.LogInformation("Dequeued date: {0:yyyy-MM-dd HH:mm:ss.fff}", dateTime);
            }
        }

        public async Task ExecuteAsync(OnAfterGetMessage pipelineEvent)
        {
            Execute(pipelineEvent);

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public bool AllMessagesReceivedWithinTimespan(int msToComplete)
        {
            return
                _datesAfterGetMessage.All(
                    dateTime => dateTime.Subtract(_firstDateAfterGetMessage) <=
                                TimeSpan.FromMilliseconds(msToComplete));
        }

        private void PipelineCreated(object sender, PipelineEventArgs e)
        {
            var fullName = e.Pipeline.GetType().FullName;

            if (fullName != null && !fullName.Equals(typeof(InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase))
            {
                return;
            }

            e.Pipeline.RegisterObserver(this);
        }
    }
}