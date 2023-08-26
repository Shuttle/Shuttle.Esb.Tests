using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class PipelineExceptionFixture : IntegrationFixture
    {
        protected async Task TestExceptionHandling(IServiceCollection services, string queueUriFormat)
        {
            var serviceBusOptions = new ServiceBusOptions
            {
                Inbox = new InboxOptions
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(5) },
                    DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(5) },
                    MaximumFailureCount = 100,
                    ThreadCount = 1
                }
            };

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
                builder.SuppressHostedService = true;
            });

            services.ConfigureLogging(nameof(TestExceptionHandling));

            services.AddSingleton<ReceivePipelineExceptionFeature>();

            var serviceProvider = await services.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);

            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

            try
            {
                await serviceBusConfiguration.Inbox.WorkQueue.TryDrop().ConfigureAwait(false);
                // if drop not supported, then purge
                await serviceBusConfiguration.Inbox.WorkQueue.TryPurge().ConfigureAwait(false);
            }
            catch 
            {
                // if dropped, purge will cause exception, ignore
            }

            await serviceBusConfiguration.CreatePhysicalQueues().ConfigureAwait(false);

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var feature = serviceProvider.GetRequiredService<ReceivePipelineExceptionFeature>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
            
            await using (serviceBus.ConfigureAwait(false))
            {
                await transportMessagePipeline.Execute(new ReceivePipelineCommand(), null, builder =>
                {
                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                }).ConfigureAwait(false);

                await serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                    transportMessagePipeline.State.GetTransportMessage(),
                    await serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);

                Assert.IsFalse(await serviceBusConfiguration.Inbox.WorkQueue.IsEmpty().ConfigureAwait(false));

                await serviceBus.Start().ConfigureAwait(false);

                while (feature.ShouldWait())
                {
                    await Task.Delay(10).ConfigureAwait(false);
                }
            }

            await serviceProvider.StopHostedServices().ConfigureAwait(false);
        }
    }
}