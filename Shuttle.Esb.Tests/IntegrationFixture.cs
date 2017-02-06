using System;
using Castle.Windsor;
using log4net;
using Shuttle.Core.Castle;
using Shuttle.Core.Infrastructure;
using Shuttle.Core.Log4Net;

namespace Shuttle.Esb.Tests
{
    public class IntegrationFixture : Fixture
    {
        protected override void FixtureSetUp()
        {
            Log.Assign(new Log4NetLog(LogManager.GetLogger(typeof(IntegrationFixture))));
        }

        protected IComponentResolver GetComponentResolver(IServiceBusConfiguration configuration)
        {
            var container = new WindsorComponentContainer(new WindsorContainer());

            var configurator = new ServiceBusConfigurator(container);

            configurator.RegisterComponents(configuration);

            return container;
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
                    ThreadCount = threadCount
                },
            };

        }

         protected ServiceBusConfiguration DistributorConfiguration(bool isTransactional, int threadCount)
        {
            return DefaultConfiguration(isTransactional, threadCount, false);
        }

        private ServiceBusConfiguration DefaultConfiguration(bool isTransactional, int threadCount, bool hasControlInbox)
        {
            var configuration = new ServiceBusConfiguration
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
                },
            };

            if (hasControlInbox)
            {
            }

            return configuration;
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