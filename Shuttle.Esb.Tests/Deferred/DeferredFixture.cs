﻿using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.TransactionScope;

namespace Shuttle.Esb.Tests;

public class DeferredFixture : IntegrationFixture
{
    private async Task ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat)
    {
        var queueService = serviceProvider.GetRequiredService<IQueueService>();

        var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
        var inboxDeferredQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-deferred"));
        var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

        await inboxWorkQueue.TryDropAsync().ConfigureAwait(false);
        await inboxDeferredQueue.TryDropAsync().ConfigureAwait(false);
        await errorQueue.TryDropAsync().ConfigureAwait(false);

        await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);

        await inboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
        await inboxDeferredQueue.TryPurgeAsync().ConfigureAwait(false);
        await errorQueue.TryPurgeAsync().ConfigureAwait(false);
    }

    private void ConfigureServices(IServiceCollection services, string test, int threadCount, bool isTransactional, string queueUriFormat)
    {
        Guard.AgainstNull(services);

        services.AddTransactionScope(builder =>
        {
            builder.Options.Enabled = isTransactional;
        });

        services.AddServiceBus(builder =>
        {
            builder.Options = new()
            {
                Inbox = new()
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    DeferredQueueUri = string.Format(queueUriFormat, "test-inbox-deferred"),
                    ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                    DurationToSleepWhenIdle = new() { TimeSpan.FromMilliseconds(25) },
                    DurationToIgnoreOnFailure = new() { TimeSpan.FromMilliseconds(25) },
                    ThreadCount = threadCount,
                    DeferredMessageProcessorResetInterval = TimeSpan.FromMilliseconds(25),
                    DeferredMessageProcessorWaitInterval = TimeSpan.FromMilliseconds(25)
                }
            };
            builder.SuppressHostedService();
        });

        services.ConfigureLogging(test);
    }

    protected async Task TestDeferredProcessingAsync(IServiceCollection services, string queueUriFormat, bool isTransactional)
    {
        Guard.AgainstNull(services);

        const int deferredMessageCount = 10;
        const int millisecondsToDefer = 100;

        services.AddOptions<MessageCountOptions>().Configure(options =>
        {
            options.MessageCount = deferredMessageCount;
        });

        services.AddSingleton<DeferredMessageFeature>();

        ConfigureServices(services, nameof(TestDeferredProcessingAsync), 1, isTransactional, queueUriFormat);

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        serviceProvider.GetRequiredService<DeferredMessageFeature>();

        var logger = serviceProvider.GetLogger<DeferredFixture>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
        var deferredMessageProcessor = serviceProvider.GetRequiredService<IDeferredMessageProcessor>();
        var feature = serviceProvider.GetRequiredService<DeferredMessageFeature>();
        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
        var queueService = serviceProvider.CreateQueueService();

        await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat).ConfigureAwait(false);

        deferredMessageProcessor.DeferredMessageProcessingHalted += (_, args) =>
        {
            logger.LogDebug($"[DeferredMessageProcessingHalted] : restart date/time = '{args.RestartDateTime}'");
        };

        deferredMessageProcessor.DeferredMessageProcessingAdjusted += (_, args) =>
        {
            logger.LogDebug($"[DeferredMessageProcessingAdjusted] : next processing date/time = '{args.NextProcessingDateTime}'");
        };

        try
        {
            var ignoreTillDate = DateTime.UtcNow.AddSeconds(1);

            await serviceBus.StartAsync().ConfigureAwait(false);

            for (var i = 0; i < deferredMessageCount; i++)
            {
                var command = new SimpleCommand
                {
                    Name = Guid.NewGuid().ToString(),
                    Context = "EnqueueDeferredMessage"
                };

                var date = ignoreTillDate;

                await serviceBus.SendAsync(command, builder => builder.Defer(date).WithRecipient(serviceBusConfiguration.Inbox!.WorkQueue!)).ConfigureAwait(false);

                ignoreTillDate = ignoreTillDate.AddMilliseconds(millisecondsToDefer);
            }

            logger.LogInformation($"[start wait] : now = '{DateTime.Now}'");

            var timeout = ignoreTillDate.AddMilliseconds(deferredMessageCount * millisecondsToDefer + millisecondsToDefer * 2 + 3000);
            var timedOut = false;

            // wait for the message to be returned from the deferred queue
            while (!feature.AllMessagesHandled() && !timedOut)
            {
                await Task.Delay(millisecondsToDefer).ConfigureAwait(false);

                timedOut = timeout < DateTime.UtcNow;
            }

            logger.LogInformation($"[end wait] : now = '{DateTime.Now}' / timeout = '{timeout.ToLocalTime()}' / timed out = '{timedOut}'");
            logger.LogInformation($"{feature.NumberOfDeferredMessagesReturned} of {deferredMessageCount} deferred messages returned to the inbox.");
            logger.LogInformation($"{feature.NumberOfMessagesHandled} of {deferredMessageCount} deferred messages handled.");

            Assert.That(feature.AllMessagesHandled(), Is.True, "All the deferred messages were not handled.");

            await serviceBus.StopAsync().ConfigureAwait(false);

            Assert.That(await serviceBusConfiguration.Inbox!.ErrorQueue!.IsEmptyAsync().ConfigureAwait(false), Is.True);
            Assert.That(await serviceBusConfiguration.Inbox!.DeferredQueue!.GetMessageAsync().ConfigureAwait(false), Is.Null);
            Assert.That(await serviceBusConfiguration.Inbox!.WorkQueue!.GetMessageAsync().ConfigureAwait(false), Is.Null);
        }
        finally
        {
            await serviceBus.DisposeAsync().ConfigureAwait(false);
            await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
            await queueService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }
}