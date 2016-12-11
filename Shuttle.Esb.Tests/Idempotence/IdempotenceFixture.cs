using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
    public class IdempotenceFixture : IntegrationFixture
    {
        protected void TestIdempotenceProcessing(IIdempotenceService idempotenceService, string queueUriFormat,
            bool isTransactional, bool enqueueUniqueMessages)
        {
            const int threadCount = 1;
            const int messageCount = 5;

            var padlock = new object();
            var configuration = GetInboxConfiguration(queueUriFormat, threadCount, isTransactional);
            var container = new DefaultComponentContainer();

            var defaultConfigurator = new DefaultConfigurator(container);

            container.Register(typeof(IdempotenceCounter), typeof(IdempotenceCounter));
            container.Register<IMessageRouteProvider>(new IdempotenceMessageRouteProvider());
            container.Register(idempotenceService);
            container.Register<IMessageHandlerInvoker, IdempotenceMessageHandlerInvoker>();

            defaultConfigurator.DontRegister<IMessageRouteProvider>();
            defaultConfigurator.DontRegister<IIdempotenceService>();
            defaultConfigurator.DontRegister<IMessageHandlerInvoker>();

            defaultConfigurator.RegisterComponents(configuration);

            var transportMessageFactory = container.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolve<ISerializer>();
            var events = container.Resolve<IServiceBusEvents>();
            var messageHandlerInvoker = (IdempotenceMessageHandlerInvoker)container.Resolve<IMessageHandlerInvoker>();

            container.Resolve<IQueueManager>().ScanForQueueFactories();

            using (var bus = ServiceBus.Create(container))
            {
                if (enqueueUniqueMessages)
                {
                    for (var i = 0; i < messageCount; i++)
                    {
                        var message = transportMessageFactory.Create(new IdempotenceCommand(),
                            c => c.WithRecipient(configuration.Inbox.WorkQueue));

                        configuration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));
                    }
                }
                else
                {
                    var message = transportMessageFactory.Create(new IdempotenceCommand(),
                        c => c.WithRecipient(configuration.Inbox.WorkQueue));

                    for (var i = 0; i < messageCount; i++)
                    {
                        configuration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));
                    }
                }

                var idleThreads = new List<int>();

                events.ThreadWaiting += (sender, args) =>
                {
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
                    Thread.Sleep(5);
                }

                Assert.IsNull(configuration.Inbox.ErrorQueue.GetMessage());
                Assert.IsNull(configuration.Inbox.WorkQueue.GetMessage());

                Assert.AreEqual(enqueueUniqueMessages ? messageCount : 1, messageHandlerInvoker.ProcessedCount);
            }

            AttemptDropQueues(queueUriFormat);
        }

        private static ServiceBusConfiguration GetInboxConfiguration(string queueUriFormat, int threadCount, bool isTransactional)
        {
            using (var queueManager = GetQueueManager())
            {
                var configuration = DefaultConfiguration(isTransactional);

                var inboxWorkQueue =
                    queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work"));
                var errorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

                configuration.Inbox =
                    new InboxQueueConfiguration
                    {
                        WorkQueue = inboxWorkQueue,
                        ErrorQueue = errorQueue,
                        DurationToIgnoreOnFailure = new[] {TimeSpan.FromMilliseconds(5)},
                        DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                        ThreadCount = threadCount
                    };

                inboxWorkQueue.AttemptDrop();
                errorQueue.AttemptDrop();

                queueManager.CreatePhysicalQueues(configuration);

                inboxWorkQueue.AttemptPurge();
                errorQueue.AttemptPurge();

                return configuration;
            }
        }
    }
}