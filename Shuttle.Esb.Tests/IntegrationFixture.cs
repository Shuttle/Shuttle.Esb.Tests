using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;
using Shuttle.Core.Transactions;

namespace Shuttle.Esb.Tests
{
    public class IntegrationFixture
    {
        private readonly List<string> _queueUris = new List<string>
        {
            "test-worker-work",
            "test-distributor-work",
            "test-distributor-control",
            "test-inbox-work",
            "test-inbox-deferred",
            "test-outbox-work",
            "test-error"
        };

        protected QueueService CreateQueueService(IServiceProvider serviceProvider)
        {
            return new QueueService(serviceProvider.GetRequiredService<IQueueFactoryService>(),
                serviceProvider.GetRequiredService<IUriResolver>());
        }

        protected async Task TryDropQueues(QueueService queueService, string queueUriFormat)
        {
            foreach (var queueUri in _queueUris)
            {
                if (!queueService.Contains(queueUri))
                {
                    continue;
                }

                await queueService.Get(string.Format(queueUriFormat, queueUri)).TryDrop().ConfigureAwait(false);
            }
        }
    }
}