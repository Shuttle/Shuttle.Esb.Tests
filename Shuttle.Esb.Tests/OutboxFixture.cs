using System;
using System.Collections.Generic;
using System.Threading;
using Moq;
using NUnit.Framework;
using Shuttle.Core.Container;
using Shuttle.Core.Contract;
using Shuttle.Core.Transactions;

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
            var configuration = GetConfiguration(isTransactional, threadCount);

            var messageRouteProvider = new Mock<IMessageRouteProvider>();

            var receiverWorkQueueUri = string.Format(workQueueUriFormat, "test-receiver-work");

            messageRouteProvider.Setup(m => m.GetRouteUris(It.IsAny<string>())).Returns(new[] {receiverWorkQueueUri});

            container.Registry.RegisterInstance(messageRouteProvider.Object);

            ServiceBus.Register(container.Registry, configuration);

            var queueManager = CreateQueueManager(container.Resolver);

            ConfigureQueues(container.Resolver, configuration, workQueueUriFormat, errorQueueUriFormat);

            var events = container.Resolver.Resolve<IServiceBusEvents>();

            Console.WriteLine("Sending {0} messages.", count);

            using (var bus = ServiceBus.Create(container.Resolver).Start())
            {
                for (var i = 0; i < count; i++)
                {
                    bus.Send(new SimpleCommand());
                }

                var receiverWorkQueue = queueManager.GetQueue(receiverWorkQueueUri);
                var timedOut = false;
                var messageRetrieved = false;
                var timeout = DateTime.Now.AddSeconds(5);

                while (!messageRetrieved && !timedOut)
                {
                    var receivedMessage = receiverWorkQueue.GetMessage();

                    if (receivedMessage != null)
                    {
                        messageRetrieved = true;
                        receiverWorkQueue.Release(receivedMessage.AcknowledgementToken);
                    }
                    else
                    {
                        Thread.Sleep(25);

                        timedOut = timeout < DateTime.Now;
                    }
                }

                Assert.IsFalse(timedOut, "Timed out before any messages appeared in the receiver queue.");

                var idleThreads = new List<int>();

                events.ThreadWaiting += (sender, args) =>
                {
                    if (args.PipelineType.FullName != null &&
                        !args.PipelineType.FullName.Equals(typeof(OutboxPipeline).FullName))
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

                timedOut = false;
                timeout = DateTime.Now.AddSeconds(5);

                while (idleThreads.Count < threadCount && !timedOut)
                {
                    Thread.Sleep(25);

                    timedOut = timeout < DateTime.Now;
                }

                Assert.IsFalse(timedOut, "Timed out before processing {0} errors.  Waiting for {1} threads to be idle.", count, threadCount);

                for (var i = 0; i < count; i++)
                {
                    var receivedMessage = receiverWorkQueue.GetMessage();

                    Assert.IsNotNull(receivedMessage);

                    receiverWorkQueue.Acknowledge(receivedMessage.AcknowledgementToken);
                }
            }

            queueManager.GetQueue(receiverWorkQueueUri).AttemptDrop();

            var outboxWorkQueue = queueManager.GetQueue(string.Format(workQueueUriFormat, "test-outbox-work"));

            Assert.IsTrue(outboxWorkQueue.IsEmpty());

            outboxWorkQueue.AttemptDrop();

            queueManager.GetQueue(string.Format(errorQueueUriFormat, "test-error")).AttemptDrop();
        }

        private void ConfigureQueues(IComponentResolver resolver, IServiceBusConfiguration configuration, string workQueueUriFormat, string errorQueueUriFormat)
        {
            var queueManager = resolver.Resolve<IQueueManager>().Configure(resolver);
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