using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Shuttle.Esb.Tests
{
    [NonParallelizable]
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

        protected IQueueService CreateQueueService(IServiceProvider serviceProvider)
        {
            return new QueueService(serviceProvider.GetRequiredService<IQueueFactoryService>(),
                serviceProvider.GetRequiredService<IUriResolver>()).WireQueueEvents();
        }

        protected async Task TryDropQueues(IQueueService queueService, string queueUriFormat)
        {
            foreach (var queueUri in _queueUris)
            {
                var uri = string.Format(queueUriFormat, queueUri);

                if (!queueService.Contains(uri))
                {
                    continue;
                }

                await queueService.Get(uri).TryDrop().ConfigureAwait(false);
            }
        }
    }
}