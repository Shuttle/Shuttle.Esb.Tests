using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;
using Shuttle.Core.Transactions;

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

            var serviceBusOptions = AddServiceBus(services, threadCount, isTransactional, queueUriFormat);

            services.AddSingleton<IMessageRouteProvider>(new IdempotenceMessageRouteProvider());
            services.AddSingleton<IMessageHandlerInvoker, IdempotenceMessageHandlerInvoker>();

            var serviceProvider = services.BuildServiceProvider();

            var queueService = CreateQueueService(serviceProvider);
            var handleMessageObserver = serviceProvider.GetRequiredService<IHandleMessageObserver>();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            serviceBusConfiguration.Configure(serviceBusOptions);

            ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat);

            try
            {
                var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

                var messageHandlerInvoker =
                    (IdempotenceMessageHandlerInvoker)serviceProvider.GetRequiredService<IMessageHandlerInvoker>();

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    if (enqueueUniqueMessages)
                    {
                        for (var i = 0; i < messageCount; i++)
                        {
                            transportMessagePipeline.Execute(new IdempotenceCommand(), null, builder =>
                            {
                                builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                            });

                            serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                                transportMessagePipeline.State.GetTransportMessage(),
                                serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
                        }
                    }
                    else
                    {
                        transportMessagePipeline.Execute(new IdempotenceCommand(), null, builder =>
                        {
                            builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        });

                        var transportMessage = transportMessagePipeline.State.GetTransportMessage();
                        var messageStream = serializer.Serialize(transportMessage);
                        
                        for (var i = 0; i < messageCount; i++)
                        {
                            serviceBusConfiguration.Inbox.WorkQueue.Enqueue(transportMessage, messageStream);
                        }
                    }

                    var idleThreads = new List<int>();
                    var exception = false;

                    handleMessageObserver.HandlerException += (sender, args) =>
                    {
                        exception = true;
                    };

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

                    Assert.IsNull(serviceBusConfiguration.Inbox.ErrorQueue.GetMessage());
                    Assert.IsNull(serviceBusConfiguration.Inbox.WorkQueue.GetMessage());

                    Assert.AreEqual(enqueueUniqueMessages ? messageCount : 1, messageHandlerInvoker.ProcessedCount);
                }

                AttemptDropQueues(queueService, queueUriFormat);
            }
            finally
            {
                queueService.AttemptDispose();
            }
        }

        private void ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();
            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            inboxWorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            serviceBusConfiguration.CreatePhysicalQueues();

            inboxWorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        protected ServiceBusOptions AddServiceBus(IServiceCollection services, int threadCount, bool isTransactional, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            services.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            var serviceBusOptions = new ServiceBusOptions
            {
                Inbox = new InboxOptions
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                    DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    ThreadCount = threadCount
                }
            };

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
            });

            return serviceBusOptions;
        }
    }
}