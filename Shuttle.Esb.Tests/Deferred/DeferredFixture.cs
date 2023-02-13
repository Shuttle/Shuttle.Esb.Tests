﻿using System;
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
    public class DeferredFixture : IntegrationFixture
    {
        protected async Task TestDeferredProcessing(IServiceCollection services, string queueUriFormat, bool isTransactional)
        {
            Guard.AgainstNull(services, nameof(services));

            const int deferredMessageCount = 10;
            const int millisecondsToDefer = 250;

            var module = new DeferredMessageModule(deferredMessageCount);

            services.AddSingleton(module);

            var serviceBusOptions = AddServiceBus(services, 1, isTransactional, queueUriFormat);

            var serviceProvider = services.BuildServiceProvider();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            var queueService = CreateQueueService(serviceProvider);

            serviceBusConfiguration.Configure(serviceBusOptions);

            await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat).ConfigureAwait(false);

            try
            {
                module.Assign(serviceProvider.GetRequiredService<IPipelineFactory>());

                using (serviceProvider.GetRequiredService<IServiceBus>().Start())
                {
                    var ignoreTillDate = DateTime.Now.AddSeconds(2);

                    for (var i = 0; i < deferredMessageCount; i++)
                    {
                        await EnqueueDeferredMessage(serviceBusConfiguration, transportMessagePipeline, serializer,
                            ignoreTillDate).ConfigureAwait(false);

                        ignoreTillDate = ignoreTillDate.AddMilliseconds(millisecondsToDefer);
                    }

                    // add the extra time else there is no time to process message being returned
                    var timeout =
                        ignoreTillDate.AddMilliseconds(deferredMessageCount * millisecondsToDefer +
                                                       millisecondsToDefer * 2);
                    var timedOut = false;

                    Console.WriteLine($"[start wait] : now = '{DateTime.Now}'");

                    // wait for the message to be returned from the deferred queue
                    while (!module.AllMessagesHandled()
                           &&
                           !timedOut)
                    {
                        Thread.Sleep(millisecondsToDefer);

                        timedOut = timeout < DateTime.Now;
                    }

                    Console.WriteLine(
                        $"[end wait] : now = '{DateTime.Now}' / timeout = '{timeout}' / timed out = '{timedOut}'");

                    Console.WriteLine(
                        $"{module.NumberOfDeferredMessagesReturned} of {deferredMessageCount} deferred messages returned to the inbox.");
                    Console.WriteLine(
                        $"{module.NumberOfMessagesHandled} of {deferredMessageCount} deferred messages handled.");

                    Assert.IsTrue(module.AllMessagesHandled(), "All the deferred messages were not handled.");

                    Assert.IsTrue(await serviceBusConfiguration.Inbox.ErrorQueue.IsEmpty().ConfigureAwait(false));
                    Assert.IsNull(await serviceBusConfiguration.Inbox.DeferredQueue.GetMessage().ConfigureAwait(false));
                    Assert.IsNull(await serviceBusConfiguration.Inbox.WorkQueue.GetMessage().ConfigureAwait(false));
                }

                await TryDropQueues(queueService, queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                queueService.TryDispose();
            }
        }

        private async Task EnqueueDeferredMessage(IServiceBusConfiguration serviceBusConfiguration,
            TransportMessagePipeline transportMessagePipeline, ISerializer serializer, DateTime ignoreTillDate)
        {
            var command = new SimpleCommand
            {
                Name = Guid.NewGuid().ToString()
            };

            await transportMessagePipeline.Execute(command, null, builder =>
            {
                builder.Defer(ignoreTillDate);
                builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
            }).ConfigureAwait(false);

            await serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                transportMessagePipeline.State.GetTransportMessage(),
                await serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);

            Console.WriteLine(
                $"[message enqueued] : name = '{command.Name}' / deferred till date = '{ignoreTillDate}'");
        }

        private async Task ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration,
            string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var inboxDeferredQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-deferred"));
            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            await inboxWorkQueue.TryDrop().ConfigureAwait(false);
            await inboxDeferredQueue.TryDrop().ConfigureAwait(false);
            await errorQueue.TryDrop().ConfigureAwait(false);

            await serviceBusConfiguration.CreatePhysicalQueues().ConfigureAwait(false);

            await inboxWorkQueue.TryPurge().ConfigureAwait(false);
            await inboxDeferredQueue.TryPurge().ConfigureAwait(false);
            await errorQueue.TryPurge().ConfigureAwait(false);
        }

        protected ServiceBusOptions AddServiceBus(IServiceCollection services, int threadCount, bool isTransactional,
            string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            services.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            var serviceBusOptions = GetServiceBusOptions(threadCount, queueUriFormat);

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
            });

            return serviceBusOptions;
        }

        private ServiceBusOptions GetServiceBusOptions(int threadCount, string queueUriFormat)
        {
            return new ServiceBusOptions
            {
                Inbox = new InboxOptions
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    DeferredQueueUri = string.Format(queueUriFormat, "test-inbox-deferred"),
                    ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                    DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    ThreadCount = threadCount
                }
            };
        }
    }
}