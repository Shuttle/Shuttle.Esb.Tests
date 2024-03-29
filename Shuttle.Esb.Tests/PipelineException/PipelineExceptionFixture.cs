using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class PipelineExceptionFixture : IntegrationFixture
    {
        protected void TestExceptionHandling(IServiceCollection services, string queueUriFormat)
        {
            var serviceBusOptions = GetServiceBusOptions(1, queueUriFormat);

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
            });

            services.AddSingleton<ReceivePipelineExceptionModule>();

            var serviceProvider = services.BuildServiceProvider();

            serviceBusOptions.Inbox = new InboxOptions
            {
                WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(5) },
                DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(5) },
                MaximumFailureCount = 100,
                ThreadCount = 1
            };

            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

            serviceBusConfiguration.Configure(serviceBusOptions);

            try
            {
                serviceBusConfiguration.Inbox.WorkQueue.AttemptDrop();
                // if drop not supported, then purge
                serviceBusConfiguration.Inbox.WorkQueue.AttemptPurge();
            }
            catch 
            {
                // if dropped, purge will cause exception, ignore
            }

            serviceBusConfiguration.CreatePhysicalQueues();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var module = serviceProvider.GetRequiredService<ReceivePipelineExceptionModule>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
            {
                transportMessagePipeline.Execute(new ReceivePipelineCommand(), null, builder =>
                {
                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                });

                serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                    transportMessagePipeline.State.GetTransportMessage(),
                    serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));

                Assert.IsFalse(serviceBusConfiguration.Inbox.WorkQueue.IsEmpty());

                bus.Start();

                while (module.ShouldWait())
                {
                    Thread.Sleep(10);
                }
            }
        }

        private ServiceBusOptions GetServiceBusOptions(int threadCount, string queueUriFormat)
        {
            return new ServiceBusOptions
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
        }
    }
}