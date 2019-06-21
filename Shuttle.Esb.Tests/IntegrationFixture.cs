using System;
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
        [OneTimeSetUp]
        protected void FixtureSetUp()
        {
            Log.Assign(new Log4NetLog(LogManager.GetLogger(typeof (IntegrationFixture))));
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
                    DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                    DurationToIgnoreOnFailure = new[] { TimeSpan.FromMilliseconds(5) },
                    ThreadCount = threadCount
                }
            };
        }

        protected IQueueManager CreateQueueManager(IComponentResolver resolver)
        {
            return new QueueManager(resolver.Resolve<IUriResolver>()).Configure(resolver);
        }

        protected void AttemptDropQueues(IQueueManager queueManager, string queueUriFormat)
        {
            queueManager.GetQueue(string.Format(queueUriFormat, "test-worker-work")).AttemptDrop();
            queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-work")).AttemptDrop();
            queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control")).AttemptDrop();
            queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work")).AttemptDrop();
            queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-deferred")).AttemptDrop();
            queueManager.GetQueue(string.Format(queueUriFormat, "test-error")).AttemptDrop();
        }
    }
}