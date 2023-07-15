using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
        protected async Task TestIdempotenceProcessing(IServiceCollection services, string queueUriFormat,
            bool isTransactional, bool enqueueUniqueMessages)
        {
            Guard.AgainstNull(services, nameof(services));

            const int threadCount = 1;
            const int messageCount = 5;

            var padlock = new object();

            AddServiceBus(services, threadCount, isTransactional, queueUriFormat);

            services.AddSingleton<IMessageRouteProvider>(new IdempotenceMessageRouteProvider());
            services.AddSingleton<IMessageHandlerInvoker, IdempotenceMessageHandlerInvoker>();

            var serviceProvider = services.BuildServiceProvider();

            var queueService = CreateQueueService(serviceProvider);
            var handleMessageObserver = serviceProvider.GetRequiredService<IHandleMessageObserver>();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat).ConfigureAwait(false);

            try
            {
                var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

                var messageHandlerInvoker =
                    (IdempotenceMessageHandlerInvoker)serviceProvider.GetRequiredService<IMessageHandlerInvoker>();

                var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
                
                await using (serviceBus.ConfigureAwait(false))
                {
                    if (enqueueUniqueMessages)
                    {
                        for (var i = 0; i < messageCount; i++)
                        {
                            await transportMessagePipeline.Execute(new IdempotenceCommand(), null, builder => { builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue); }).ConfigureAwait(false);

                            await serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                                transportMessagePipeline.State.GetTransportMessage(),
                                await serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        await transportMessagePipeline.Execute(new IdempotenceCommand(), null, builder => { builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue); }).ConfigureAwait(false);

                        var transportMessage = transportMessagePipeline.State.GetTransportMessage();
                        var messageStream = await serializer.Serialize(transportMessage).ConfigureAwait(false);
                        
                        for (var i = 0; i < messageCount; i++)
                        {
                            await serviceBusConfiguration.Inbox.WorkQueue.Enqueue(transportMessage, messageStream).ConfigureAwait(false);
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

                    await serviceBus.Start().ConfigureAwait(false);

                    while (!exception && idleThreads.Count < threadCount)
                    {
                        await Task.Delay(5).ConfigureAwait(false);
                    }

                    Assert.IsNull(serviceBusConfiguration.Inbox.ErrorQueue.GetMessage());
                    Assert.IsNull(serviceBusConfiguration.Inbox.WorkQueue.GetMessage());

                    Assert.AreEqual(enqueueUniqueMessages ? messageCount : 1, messageHandlerInvoker.ProcessedCount);
                }

                await TryDropQueues(queueService, queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                queueService.TryDispose();
            }
        }

        private async Task ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>().WireQueueCreated();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            await inboxWorkQueue.TryDrop().ConfigureAwait(false);
            await errorQueue.TryDrop().ConfigureAwait(false);

            await serviceBusConfiguration.CreatePhysicalQueues().ConfigureAwait(false);

            await inboxWorkQueue.TryPurge().ConfigureAwait(false);
            await errorQueue.TryPurge().ConfigureAwait(false);
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