using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;
using Shuttle.Core.TransactionScope;

namespace Shuttle.Esb.Tests;

public class IdempotenceFixture : IntegrationFixture
{
    private async Task ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat)
    {
        var queueService = serviceProvider.GetRequiredService<IQueueService>();
        var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
        var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

        await inboxWorkQueue.TryDropAsync().ConfigureAwait(false);
        await errorQueue.TryDropAsync().ConfigureAwait(false);

        await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);

        await inboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
        await errorQueue.TryPurgeAsync().ConfigureAwait(false);
    }

    private void ConfigureServices(IServiceCollection services, string test, int threadCount, bool isTransactional, string queueUriFormat)
    {
        Guard.AgainstNull(services);

        services.AddTransactionScope(builder =>
        {
            builder.Options.Enabled = isTransactional;
        });

        var serviceBusOptions = new ServiceBusOptions
        {
            Inbox = new()
            {
                WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                DurationToSleepWhenIdle = new() { TimeSpan.FromMilliseconds(25) },
                DurationToIgnoreOnFailure = new() { TimeSpan.FromMilliseconds(25) },
                ThreadCount = threadCount
            }
        };

        services.AddServiceBus(builder =>
        {
            builder.Options = serviceBusOptions;
            builder.SuppressHostedService = true;
        });

        services.ConfigureLogging(test);
    }

    protected async Task TestIdempotenceProcessingAsync(IServiceCollection services, string queueUriFormat, bool isTransactional, bool enqueueUniqueMessages)
    {
        Guard.AgainstNull(services);

        const int threadCount = 1;
        const int messageCount = 5;

        var padlock = new object();

        ConfigureServices(services, nameof(TestIdempotenceProcessingAsync), threadCount, isTransactional, queueUriFormat);

        services.AddSingleton<IMessageRouteProvider>(new IdempotenceMessageRouteProvider());
        services.AddSingleton<IMessageHandlerInvoker, IdempotenceMessageHandlerInvoker>();

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var queueService = serviceProvider.CreateQueueService();
        var handleMessageObserver = serviceProvider.GetRequiredService<IHandleMessageObserver>();

        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

        await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat).ConfigureAwait(false);

        try
        {
            var inboxWorkQueue = Guard.AgainstNull(serviceBusConfiguration.Inbox?.WorkQueue);
            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

            var messageHandlerInvoker = (IdempotenceMessageHandlerInvoker)serviceProvider.GetRequiredService<IMessageHandlerInvoker>();

            if (enqueueUniqueMessages)
            {
                for (var i = 0; i < messageCount; i++)
                {
                    await transportMessagePipeline.ExecuteAsync(new IdempotenceCommand(), null, builder =>
                    {
                        builder.WithRecipient(inboxWorkQueue);
                    }).ConfigureAwait(false);

                    var transportMessage = Guard.AgainstNull(transportMessagePipeline.State.GetTransportMessage());

                    await inboxWorkQueue.EnqueueAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);
                }
            }
            else
            {
                await transportMessagePipeline.ExecuteAsync(new IdempotenceCommand(), null, builder =>
                {
                    builder.WithRecipient(inboxWorkQueue);
                }).ConfigureAwait(false);

                var transportMessage = Guard.AgainstNull(transportMessagePipeline.State.GetTransportMessage());

                var messageStream = await serializer.SerializeAsync(transportMessage).ConfigureAwait(false);

                for (var i = 0; i < messageCount; i++)
                {
                    await inboxWorkQueue.EnqueueAsync(transportMessage, messageStream).ConfigureAwait(false);
                }
            }

            var idleThreads = new List<int>();
            var exception = false;

            handleMessageObserver.HandlerException += (sender, _) =>
            {
                if (sender == null)
                    throw new ArgumentNullException(nameof(sender));
                exception = true;
            };

            threadActivity.ThreadWaiting += (_, _) =>
            {
                lock (padlock)
                {
                    if (idleThreads.Contains(Environment.CurrentManagedThreadId))
                    {
                        return;
                    }

                    idleThreads.Add(Environment.CurrentManagedThreadId);
                }
            };

            await serviceBus.StartAsync().ConfigureAwait(false);

            while (!exception && idleThreads.Count < threadCount)
            {
                await Task.Delay(5).ConfigureAwait(false);
            }

            await serviceBus.StopAsync().ConfigureAwait(false);

            Assert.That(await serviceBusConfiguration.Inbox!.ErrorQueue!.GetMessageAsync().ConfigureAwait(false), Is.Null);
            Assert.That(await serviceBusConfiguration.Inbox.WorkQueue!.GetMessageAsync().ConfigureAwait(false), Is.Null);

            Assert.That(messageHandlerInvoker.ProcessedCount, Is.EqualTo(enqueueUniqueMessages ? messageCount : 1));
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