using System;
using System.Collections.Generic;
using log4net;
using NUnit.Framework;
using Shuttle.Core.Container;
using Shuttle.Core.Log4Net;
using Shuttle.Core.Logging;
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
            "test-error"
        };

        [OneTimeSetUp]
        protected void FixtureSetUp()
        {
            Log.Assign(new Log4NetLog(LogManager.GetLogger(typeof(IntegrationFixture))));
        }

        protected ServiceBusConfiguration DefaultConfiguration(bool isTransactional, int threadCount)
        {
            return new ServiceBusConfiguration
            {
                ScanForQueueFactories = true,
                TransactionScope = new TransactionScopeConfiguration
                {
                    Enabled = isTransactional
                },
                Inbox = new InboxQueueConfiguration
                {
                    DurationToSleepWhenIdle = new[] { TimeSpan.FromMilliseconds(5) },
                    DurationToIgnoreOnFailure = new[] { TimeSpan.FromMilliseconds(5) },
                    ThreadCount = threadCount
                }
            };
        }

        protected QueueManager CreateQueueManager(IComponentResolver resolver)
        {
            return (QueueManager)new QueueManager(resolver.Resolve<IUriResolver>()).Configure(resolver);
        }

        protected void AttemptDropQueues(QueueManager queueManager, string queueUriFormat)
        {
            foreach (var queueUri in _queueUris)
            {
                if (!queueManager.ContainsQueue(queueUri))
                {
                    continue;
                }

                queueManager.GetQueue(string.Format(queueUriFormat, queueUri)).AttemptDrop();
            }
        }
    }
}