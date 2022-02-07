﻿using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using Shuttle.Core.Container;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class IdempotenceFixture : IntegrationFixture
    {
        protected void TestIdempotenceProcessing(ComponentContainer container, string queueUriFormat,
            bool isTransactional, bool enqueueUniqueMessages)
        {
            Guard.AgainstNull(container, "container");

            const int threadCount = 1;
            const int messageCount = 5;

            var padlock = new object();
            var configuration = DefaultConfiguration(isTransactional, threadCount);

            container.Registry.RegisterInstance<IMessageRouteProvider>(new IdempotenceMessageRouteProvider());
            container.Registry.Register<IMessageHandlerInvoker, IdempotenceMessageHandlerInvoker>();

            container.Registry.RegisterServiceBus(configuration);

            var queueManager = CreateQueueManager(container.Resolver);

            ConfigureQueues(container.Resolver, configuration, queueUriFormat);

            try
            {
                var transportMessageFactory = container.Resolver.Resolve<ITransportMessageFactory>();
                var serializer = container.Resolver.Resolve<ISerializer>();
                var events = container.Resolver.Resolve<IServiceBusEvents>();
                var messageHandlerInvoker =
                    (IdempotenceMessageHandlerInvoker) container.Resolver.Resolve<IMessageHandlerInvoker>();

                using (var bus = container.Resolver.Resolve<IServiceBus>())
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
                    var exception = false;

                    events.HandlerException += (sender, args) => { exception = true; };

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

                    while (!exception && idleThreads.Count < threadCount)
                    {
                        Thread.Sleep(5);
                    }

                    Assert.IsNull(configuration.Inbox.ErrorQueue.GetMessage());
                    Assert.IsNull(configuration.Inbox.WorkQueue.GetMessage());

                    Assert.AreEqual(enqueueUniqueMessages ? messageCount : 1, messageHandlerInvoker.ProcessedCount);

                }

                AttemptDropQueues(queueManager, queueUriFormat);
            }
            finally
            {
                queueManager.AttemptDispose();
            }
        }

        private void ConfigureQueues(IComponentResolver resolver, IServiceBusConfiguration configuration, string queueUriFormat)
        {
            var queueManager = resolver.Resolve<IQueueManager>().Configure(resolver);
            var inboxWorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox.WorkQueue = inboxWorkQueue;
            configuration.Inbox.ErrorQueue = errorQueue;

            inboxWorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            queueManager.CreatePhysicalQueues(configuration);

            inboxWorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }
    }
}