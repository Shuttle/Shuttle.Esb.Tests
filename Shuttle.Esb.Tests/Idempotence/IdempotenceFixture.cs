using System;
using System.Collections.Generic;
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
        private async Task ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat, bool sync)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            if (sync)
            {
                inboxWorkQueue.TryDrop();
                errorQueue.TryDrop();
                serviceBusConfiguration.CreatePhysicalQueues();
                inboxWorkQueue.TryPurge();
                errorQueue.TryPurge();
            }
            else
            {
                await inboxWorkQueue.TryDropAsync().ConfigureAwait(false);
                await errorQueue.TryDropAsync().ConfigureAwait(false);

                await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);

                await inboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
                await errorQueue.TryPurgeAsync().ConfigureAwait(false);
            }
        }

        private ServiceBusOptions ConfigureServices(IServiceCollection services, string test, int threadCount, bool isTransactional, string queueUriFormat)
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

            services.ConfigureLogging(test);

            return serviceBusOptions;
        }

        protected void TestIdempotenceProcessing(IServiceCollection services, string queueUriFormat, bool isTransactional, bool enqueueUniqueMessages)
        {
            TestIdempotenceProcessingAsync(services, queueUriFormat, isTransactional, enqueueUniqueMessages, false).GetAwaiter().GetResult();
        }

        protected async Task TestIdempotenceProcessingAsync(IServiceCollection services, string queueUriFormat, bool isTransactional, bool enqueueUniqueMessages)
        {
            await TestIdempotenceProcessingAsync(services, queueUriFormat, isTransactional, enqueueUniqueMessages, false).ConfigureAwait(false);
        }

        private async Task TestIdempotenceProcessingAsync(IServiceCollection services, string queueUriFormat, bool isTransactional, bool enqueueUniqueMessages, bool sync)
        {
            Guard.AgainstNull(services, nameof(services));

            const int threadCount = 1;
            const int messageCount = 5;

            var padlock = new object();

            ConfigureServices(services, nameof(TestIdempotenceProcessingAsync), threadCount, isTransactional, queueUriFormat);

            services.AddSingleton<IMessageRouteProvider>(new IdempotenceMessageRouteProvider());
            services.AddSingleton<IMessageHandlerInvoker, IdempotenceMessageHandlerInvoker>();

            var serviceProvider = sync
                ? services.BuildServiceProvider().StartHostedServices()
                : await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

            var queueService = serviceProvider.CreateQueueService();
            var handleMessageObserver = serviceProvider.GetRequiredService<IHandleMessageObserver>();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, sync).ConfigureAwait(false);

            try
            {
                var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

                var messageHandlerInvoker =
                    (IdempotenceMessageHandlerInvoker)serviceProvider.GetRequiredService<IMessageHandlerInvoker>();

                var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

                try
                {
                    if (enqueueUniqueMessages)
                    {
                        for (var i = 0; i < messageCount; i++)
                        {
                            if (sync)
                            {
                                transportMessagePipeline.Execute(new IdempotenceCommand(), null, builder =>
                                {
                                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                                });

                                serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                                    transportMessagePipeline.State.GetTransportMessage(),
                                    serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
                            }
                            else
                            {
                                await transportMessagePipeline.ExecuteAsync(new IdempotenceCommand(), null, builder =>
                                {
                                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                                }).ConfigureAwait(false);

                                await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(
                                    transportMessagePipeline.State.GetTransportMessage(),
                                    await serializer.SerializeAsync(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                            }
                        }
                    }
                    else
                    {
                        if (sync)
                        {
                            transportMessagePipeline.Execute(new IdempotenceCommand(), null, builder =>
                            {
                                builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                            });
                        }
                        else
                        {
                            await transportMessagePipeline.ExecuteAsync(new IdempotenceCommand(), null, builder =>
                            {
                                builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                            }).ConfigureAwait(false);
                        }

                        var transportMessage = transportMessagePipeline.State.GetTransportMessage();

                        var messageStream = sync
                            ? serializer.Serialize(transportMessage)
                            : await serializer.SerializeAsync(transportMessage).ConfigureAwait(false);

                        for (var i = 0; i < messageCount; i++)
                        {
                            if (sync)
                            {
                                serviceBusConfiguration.Inbox.WorkQueue.Enqueue(transportMessage, messageStream);
                            }
                            else
                            {
                                await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(transportMessage, messageStream).ConfigureAwait(false);
                            }
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

                    if (sync)
                    {
                        serviceBus.Start();
                    }
                    else
                    {
                        await serviceBus.StartAsync().ConfigureAwait(false);
                    }

                    while (!exception && idleThreads.Count < threadCount)
                    {
                        await Task.Delay(5).ConfigureAwait(false);
                    }

                    Assert.IsNull(serviceBusConfiguration.Inbox.ErrorQueue.GetMessage());
                    Assert.IsNull(serviceBusConfiguration.Inbox.WorkQueue.GetMessage());

                    Assert.AreEqual(enqueueUniqueMessages ? messageCount : 1, messageHandlerInvoker.ProcessedCount);
                }
                finally
                {
                    if (sync)
                    {
                        serviceBus.Dispose();
                    }
                    else
                    {
                        await serviceBus.DisposeAsync();
                    }
                }

                await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                if (sync)
                {
                    queueService.TryDispose();
                }
                else
                {
                    await queueService.TryDisposeAsync().ConfigureAwait(false);
                }

                await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
            }
        }
    }
}