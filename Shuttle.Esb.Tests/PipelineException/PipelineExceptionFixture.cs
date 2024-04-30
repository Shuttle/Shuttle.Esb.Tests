using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class PipelineExceptionFixture : IntegrationFixture
    {
        protected void TestExceptionHandling(IServiceCollection services, string queueUriFormat)
        {
            TestExceptionHandlingAsync(services, queueUriFormat, true).GetAwaiter().GetResult();
        }

        protected async Task TestExceptionHandlingAsync(IServiceCollection services, string queueUriFormat)
        {
            await TestExceptionHandlingAsync(services, queueUriFormat, false).ConfigureAwait(false);
        }

        protected async Task TestExceptionHandlingAsync(IServiceCollection services, string queueUriFormat, bool sync)
        {
            var serviceBusOptions = new ServiceBusOptions
            {
                Asynchronous = !sync,
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

            var serviceProvider = sync
                ? services.BuildServiceProvider().StartHostedServices()
                : await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

            var drop = serviceBusConfiguration.Inbox.WorkQueue as IDropQueue;

            if (drop != null)
            {
                if (sync)
                {
                    drop.Drop();
                }
                else
                {
                    await drop.DropAsync().ConfigureAwait(false);
                }
            }
            else
            {
                if (sync)
                {
                    serviceBusConfiguration.Inbox.WorkQueue.TryPurge();
                }
                else
                {
                    await serviceBusConfiguration.Inbox.WorkQueue.TryPurgeAsync().ConfigureAwait(false);
                }
            }

            if (sync)
            {
                serviceBusConfiguration.CreatePhysicalQueues();
            }
            else
            {
                await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);
            }

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var feature = serviceProvider.GetRequiredService<ReceivePipelineExceptionFeature>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

            try
            {
                if (sync)
                {
                    transportMessagePipeline.Execute(new ReceivePipelineCommand(), null, builder =>
                    {
                        builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    });

                    serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                        transportMessagePipeline.State.GetTransportMessage(),
                        serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));

                    Assert.That(serviceBusConfiguration.Inbox.WorkQueue.IsEmpty(), Is.False);

                    serviceBus.Start();
                }
                else
                {
                    await transportMessagePipeline.ExecuteAsync(new ReceivePipelineCommand(), null, builder =>
                    {
                        builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    }).ConfigureAwait(false);

                    await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(
                        transportMessagePipeline.State.GetTransportMessage(),
                        await serializer.SerializeAsync(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);

                    Assert.That(await serviceBusConfiguration.Inbox.WorkQueue.IsEmptyAsync().ConfigureAwait(false), Is.False);

                    await serviceBus.StartAsync().ConfigureAwait(false);
                }

                while (feature.ShouldWait())
                {
                    await Task.Delay(10).ConfigureAwait(false);
                }
            }
            finally
            {
                if (sync)
                {
                    serviceBus.TryDispose();
                }
                else
                {
                    await serviceBus.TryDisposeAsync().ConfigureAwait(false);
                }
            }

            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }
}