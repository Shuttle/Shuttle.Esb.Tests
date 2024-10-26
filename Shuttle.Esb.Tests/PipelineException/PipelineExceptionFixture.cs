using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests;

public class PipelineExceptionFixture : IntegrationFixture
{
    protected async Task TestExceptionHandlingAsync(IServiceCollection services, string queueUriFormat)
    {
        var serviceBusOptions = new ServiceBusOptions
        {
            Inbox = new()
            {
                WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                DurationToSleepWhenIdle = new() { TimeSpan.FromMilliseconds(5) },
                DurationToIgnoreOnFailure = new() { TimeSpan.FromMilliseconds(5) },
                MaximumFailureCount = 100,
                ThreadCount = 1
            }
        };

        services.AddServiceBus(builder =>
        {
            builder.Options = serviceBusOptions;
            builder.SuppressHostedService = true;
        });

        services.ConfigureLogging(nameof(PipelineExceptionFixture));

        services.AddSingleton<ReceivePipelineExceptionFeature>();

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

        var inboxWorkQueue = serviceBusConfiguration.Inbox!.WorkQueue!;

        if (serviceBusConfiguration.Inbox!.WorkQueue is IDropQueue drop)
        {
            await drop.DropAsync().ConfigureAwait(false);
        }
        else
        {
            await inboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
        }

        await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);

        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
        var feature = serviceProvider.GetRequiredService<ReceivePipelineExceptionFeature>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();

        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

        try
        {
            await transportMessagePipeline.ExecuteAsync(new ReceivePipelineCommand(), null, builder =>
                {
                    builder.WithRecipient(inboxWorkQueue);
                }).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;
            
            await inboxWorkQueue.EnqueueAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);

            Assert.That(await inboxWorkQueue.IsEmptyAsync().ConfigureAwait(false), Is.False);

            await serviceBus.StartAsync().ConfigureAwait(false);

            while (feature.ShouldWait())
            {
                await Task.Delay(10).ConfigureAwait(false);
            }
        }
        finally
        {
            await serviceBus.TryDisposeAsync().ConfigureAwait(false);
        }

        await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
    }
}