using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class IdempotenceFixture : IntegrationFixture
    {
        protected void TestIdempotenceProcessing(IServiceCollection services, string queueUriFormat,
            bool isTransactional, bool enqueueUniqueMessages)
        {
            Guard.AgainstNull(services, nameof(services));

            const int threadCount = 1;
            const int messageCount = 5;

            var padlock = new object();
            var configuration = AddServiceBus(services, threadCount, isTransactional);

            services.AddSingleton<IMessageRouteProvider>(new IdempotenceMessageRouteProvider());
            services.AddSingleton<IMessageHandlerInvoker, IdempotenceMessageHandlerInvoker>();

            var serviceProvider = services.BuildServiceProvider();

            var queueManager = CreateQueueService(serviceProvider);
            var handleMessageObserver = serviceProvider.GetRequiredService<IHandleMessageObserver>();

            ConfigureQueues(serviceProvider, configuration, queueUriFormat);

            try
            {
                var transportMessageFactory = serviceProvider.GetRequiredService<ITransportMessageFactory>();
                var serializer = serviceProvider.GetRequiredService<ISerializer>();
                var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

                var messageHandlerInvoker =
                    (IdempotenceMessageHandlerInvoker) serviceProvider.GetRequiredService<IMessageHandlerInvoker>();

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
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

                    handleMessageObserver.HandlerException += (sender, args) => { exception = true; };

                    threadActivity.ThreadWaiting += (sender, args) =>
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

        private void ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration configuration, string queueUriFormat)
        {
            var queueManager = serviceProvider.GetRequiredService<IQueueService>();
            var inboxWorkQueue = queueManager.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = queueManager.Get(string.Format(queueUriFormat, "test-error"));

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