using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

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
            "test-error"
        };

        protected ServiceBusConfiguration DefaultConfiguration(int threadCount)
        {
            return new ServiceBusConfiguration
            {
                ScanForQueueFactories = true,
                Inbox = new InboxQueueConfiguration
                {
                    DurationToSleepWhenIdle = new[] { TimeSpan.FromMilliseconds(5) },
                    DurationToIgnoreOnFailure = new[] { TimeSpan.FromMilliseconds(5) },
                    ThreadCount = threadCount
                }
            };
        }

        protected QueueService CreateQueueService(IServiceProvider serviceProvider)
        {
            return new QueueService(serviceProvider.GetRequiredService<IQueueFactoryService>(),
                serviceProvider.GetRequiredService<IUriResolver>());
        }

        protected void AttemptDropQueues(QueueService queueService, string queueUriFormat)
        {
            foreach (var queueUri in _queueUris)
            {
                if (!queueService.Contains(queueUri))
                {
                    continue;
                }

                queueService.Get(string.Format(queueUriFormat, queueUri)).AttemptDrop();
            }
        }
    }
}