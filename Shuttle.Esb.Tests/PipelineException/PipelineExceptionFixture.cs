using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using NUnit.Framework;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class PipelineExceptionFixture : IntegrationFixture
    {
        protected void TestExceptionHandling(IServiceCollection services, string queueUriFormat)
        {
            var serviceBusOptions = DefaultServiceBusOptions(1);
            var serviceBusConfiguration = new ServiceBusConfiguration();

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
                builder.Configuration = serviceBusConfiguration;
            });

            var module = new ReceivePipelineExceptionModule(serviceBusConfiguration);

            services.AddSingleton(module.GetType(), module);

            var serviceProvider = services.BuildServiceProvider();

            module.Assign(serviceProvider.GetRequiredService<IPipelineFactory>());

            var queueManager = CreateQueueService(serviceProvider);

            var inboxWorkQueue = queueManager.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var inboxErrorQueue = queueManager.Get(string.Format(queueUriFormat, "test-error"));

            serviceBusConfiguration.Inbox =
                new InboxConfiguration
                {
                    WorkQueue = inboxWorkQueue,
                    ErrorQueue = inboxErrorQueue,
                };

            serviceBusOptions.Inbox = new InboxOptions
            {
                DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(5) },
                DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(5) },
                MaximumFailureCount = 100,
                ThreadCount = 1
            };

            inboxWorkQueue.Drop();
            inboxErrorQueue.Drop();

            queueManager.CreatePhysicalQueues(serviceBusConfiguration);

            var transportMessageFactory = serviceProvider.GetRequiredService<ITransportMessageFactory>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
            {
                var message = transportMessageFactory.Create(new ReceivePipelineCommand(),
                    c => c.WithRecipient(inboxWorkQueue));

                inboxWorkQueue.Enqueue(message, serializer.Serialize(message));

                Assert.IsFalse(inboxWorkQueue.IsEmpty());

                bus.Start();

                while (module.ShouldWait())
                {
                    Thread.Sleep(10);
                }
            }
        }
    }
}