using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
        private async Task ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat, bool sync)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var inboxDeferredQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-deferred"));
            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            if (sync)
            {
                inboxWorkQueue.TryDrop();
                inboxDeferredQueue.TryDrop();
                errorQueue.TryDrop();

                serviceBusConfiguration.CreatePhysicalQueues();

                inboxWorkQueue.TryPurge();
                inboxDeferredQueue.TryPurge();
                errorQueue.TryPurge();
            }
            else
            {
                await inboxWorkQueue.TryDropAsync().ConfigureAwait(false);
                await inboxDeferredQueue.TryDropAsync().ConfigureAwait(false);
                await errorQueue.TryDropAsync().ConfigureAwait(false);

                await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);

                await inboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
                await inboxDeferredQueue.TryPurgeAsync().ConfigureAwait(false);
                await errorQueue.TryPurgeAsync().ConfigureAwait(false);
            }
        }

        private void ConfigureServices(IServiceCollection services, string test, int threadCount, bool isTransactional, string queueUriFormat)
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
                builder.SuppressHostedService = true;
            });

            services.ConfigureLogging(test);
        }

        private async Task EnqueueDeferredMessage(IServiceBusConfiguration serviceBusConfiguration,
            TransportMessagePipeline transportMessagePipeline, ISerializer serializer, DateTime ignoreTillDate, bool sync)
        {
            var command = new SimpleCommand
            {
                Name = Guid.NewGuid().ToString(),
                Context = "EnqueueDeferredMessage"
            };

            if (sync)
            {
                transportMessagePipeline.Execute(command, null, builder =>
                {
                    builder.Defer(ignoreTillDate);
                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                });

                serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                    transportMessagePipeline.State.GetTransportMessage(),
                    serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
            }
            else
            {
                await transportMessagePipeline.ExecuteAsync(command, null, builder =>
                {
                    builder.Defer(ignoreTillDate);
                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                }).ConfigureAwait(false);

                await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(
                    transportMessagePipeline.State.GetTransportMessage(),
                    await serializer.SerializeAsync(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
            }
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

        protected void TestDeferredProcessing(IServiceCollection services, string queueUriFormat, bool isTransactional)
        {
            TestDeferredProcessingAsync(services, queueUriFormat, isTransactional, true).GetAwaiter().GetResult();
        }

        protected async Task TestDeferredProcessingAsync(IServiceCollection services, string queueUriFormat, bool isTransactional)
        {
            await TestDeferredProcessingAsync(services, queueUriFormat, isTransactional, false).ConfigureAwait(false);
        }

        private async Task TestDeferredProcessingAsync(IServiceCollection services, string queueUriFormat, bool isTransactional, bool sync)
        {
            Guard.AgainstNull(services, nameof(services));

            const int deferredMessageCount = 10;
            const int millisecondsToDefer = 250;

            services.AddOptions<MessageCountOptions>().Configure(options =>
            {
                options.MessageCount = deferredMessageCount;
            });

            services.AddSingleton<DeferredMessageFeature>();

            ConfigureServices(services, nameof(TestDeferredProcessingAsync), 1, isTransactional, queueUriFormat);

            var serviceProvider = sync
                ? services.BuildServiceProvider().StartHostedServices()
                : await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

            serviceProvider.GetRequiredService<DeferredMessageFeature>();

            var logger = serviceProvider.GetLogger<DeferredFixture>();
            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();
            var feature = serviceProvider.GetRequiredService<DeferredMessageFeature>();

            var queueService = serviceProvider.CreateQueueService();

            await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, sync).ConfigureAwait(false);

            try
            {
                //var ignoreTillDate = DateTime.Now.AddSeconds(2);
                var ignoreTillDate = DateTime.Now.AddSeconds(30);

                for (var i = 0; i < deferredMessageCount; i++)
                {
                    await EnqueueDeferredMessage(serviceBusConfiguration, transportMessagePipeline, serializer, ignoreTillDate, sync).ConfigureAwait(false);

                    ignoreTillDate = ignoreTillDate.AddMilliseconds(millisecondsToDefer);
                }

                var timeout =
                    ignoreTillDate.AddMilliseconds(deferredMessageCount * millisecondsToDefer +
                                                   millisecondsToDefer * 2);
                var timedOut = false;

                var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

                try
                {
                    if (sync)
                    {
                        serviceBus.Start();
                    }
                    else
                    {
                        await serviceBus.StartAsync().ConfigureAwait(false);
                    }

                    logger.LogInformation($"[start wait] : now = '{DateTime.Now}'");

                    // wait for the message to be returned from the deferred queue
                    while (!feature.AllMessagesHandled()
                           &&
                           !timedOut)
                    {
                        await Task.Delay(millisecondsToDefer).ConfigureAwait(false);

                        timedOut = timeout < DateTime.Now;
                    }

                    logger.LogInformation(
                        $"[end wait] : now = '{DateTime.Now}' / timeout = '{timeout}' / timed out = '{timedOut}'");

                    logger.LogInformation(
                        $"{feature.NumberOfDeferredMessagesReturned} of {deferredMessageCount} deferred messages returned to the inbox.");
                    logger.LogInformation(
                        $"{feature.NumberOfMessagesHandled} of {deferredMessageCount} deferred messages handled.");

                    Assert.IsTrue(feature.AllMessagesHandled(), "All the deferred messages were not handled.");

                    if (sync)
                    {
                        Assert.That(serviceBusConfiguration.Inbox.ErrorQueue.IsEmpty(), Is.True);
                        Assert.That(serviceBusConfiguration.Inbox.DeferredQueue.GetMessage(), Is.Null);
                        Assert.That(serviceBusConfiguration.Inbox.WorkQueue.GetMessage(), Is.Null);
                    }
                    else
                    {
                        Assert.That(await serviceBusConfiguration.Inbox.ErrorQueue.IsEmptyAsync().ConfigureAwait(false), Is.True);
                        Assert.That(await serviceBusConfiguration.Inbox.DeferredQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);
                        Assert.That(await serviceBusConfiguration.Inbox.WorkQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);
                    }
                }
                finally
                {
                    if (sync)
                    {
                        serviceBus.Dispose();
                    }
                    else
                    {
                        await serviceBus.DisposeAsync().ConfigureAwait(false);
                    }
                }

                await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                queueService.TryDispose();

                await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
            }
        }
    }
}