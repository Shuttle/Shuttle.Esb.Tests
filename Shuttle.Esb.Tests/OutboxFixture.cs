using System;
using System.Collections.Generic;
using System.Threading;
using Moq;
using NUnit.Framework;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
    public abstract class OutboxFixture : IntegrationFixture
    {
        protected void TestOutboxSending(ComponentContainer container, string workQueueUriFormat, bool isTransactional)
        {
            TestOutboxSending(container, workQueueUriFormat, workQueueUriFormat, isTransactional);
        }

        protected void TestOutboxSending(ComponentContainer container, string workQueueUriFormat,
            string errorQueueUriFormat, bool isTransactional)
        {
            Guard.AgainstNull(container, "container");

            const int count = 100;
            const int threadCount = 3;

            var padlock = new object();
            var configuration = GetConfiguration(isTransactional, 1);

            var configurator = new ServiceBusConfigurator(container.Registry);

            var messageRouteProvider = new Mock<IMessageRouteProvider>();

            var receiverWorkQueueUri = string.Format(workQueueUriFormat, "test-receiver-work");

            messageRouteProvider.Setup(m => m.GetRouteUris(It.IsAny<string>())).Returns(new[] {receiverWorkQueueUri});

            container.Registry.Register(messageRouteProvider.Object);

            configurator.DontRegister<IMessageRouteProvider>();

            configurator.RegisterComponents(configuration);

            var queueManager = ConfigureQueueManager(container.Resolver);

            ConfigureQueues(queueManager, configuration, workQueueUriFormat, errorQueueUriFormat);

            var events = container.Resolver.Resolve<IServiceBusEvents>();

            Console.WriteLine("Sending {0} messages.", count);

            using (var bus = ServiceBus.Create(container.Resolver))
            {
                for (var i = 0; i < count; i++)
                {
                    bus.Send(new SimpleCommand());
                }

                var idleThreads = new List<int>();

                events.ThreadWaiting += (sender, args) =>
                {
                    if (!args.PipelineType.FullName.Equals(typeof (OutboxPipeline).FullName))
                    {
                        return;
                    }

                    lock (padlock)
                    {
                        if (idleThreads.Contains(Thread.CurrentThread.ManagedThreadId))
                        {
                            return;
                        }

                        idleThreads.Add(Thread.CurrentThread.ManagedThreadId);
                    }
                };

                bus.Start();

                while (idleThreads.Count < threadCount)
                {
                    Thread.Sleep(25);
                }
            }

            var receiverWorkQueue = queueManager.GetQueue(receiverWorkQueueUri);

            for (var i = 0; i < count; i++)
            {
                var receivedMessage = receiverWorkQueue.GetMessage();

                Assert.IsNotNull(receivedMessage);

                receiverWorkQueue.Acknowledge(receivedMessage.AcknowledgementToken);
            }

            receiverWorkQueue.AttemptDrop();

            var outboxWorkQueue = queueManager.GetQueue(string.Format(workQueueUriFormat, "test-outbox-work"));

            Assert.IsTrue(outboxWorkQueue.IsEmpty());

            outboxWorkQueue.AttemptDrop();

            queueManager.GetQueue(string.Format(errorQueueUriFormat, "test-error")).AttemptDrop();
        }

        private void ConfigureQueues(IQueueManager queueManager, IServiceBusConfiguration configuration,
            string workQueueUriFormat, string errorQueueUriFormat)
        {
            var outboxWorkQueue = queueManager.GetQueue(string.Format(workQueueUriFormat, "test-outbox-work"));
            var errorQueue = queueManager.GetQueue(string.Format(errorQueueUriFormat, "test-error"));

            configuration.Outbox.WorkQueue = outboxWorkQueue;
            configuration.Outbox.ErrorQueue = errorQueue;

            var receiverWorkQueue =
                queueManager.GetQueue(string.Format(workQueueUriFormat, "test-receiver-work"));

            outboxWorkQueue.AttemptDrop();
            receiverWorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            outboxWorkQueue.AttemptCreate();
            receiverWorkQueue.AttemptCreate();
            errorQueue.AttemptCreate();

            outboxWorkQueue.AttemptPurge();
            receiverWorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        private ServiceBusConfiguration GetConfiguration(bool isTransactional, int threadCount)
        {
            var configuration = new ServiceBusConfiguration
            {
                ScanForQueueFactories = true,
                TransactionScope = new TransactionScopeConfiguration
                {
                    Enabled = isTransactional
                },
                Outbox =
                    new OutboxQueueConfiguration
                    {
                        DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                        ThreadCount = threadCount
                    }
            };

            return configuration;
        }
    }
}