using System;
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
            var configuration = DefaultConfiguration(1);

            services.AddServiceBus(builder =>
            {
                builder.Configure(configuration);
            });

            var module = new ReceivePipelineExceptionModule(configuration);

            services.AddSingleton(module.GetType(), module);

            var serviceProvider = services.BuildServiceProvider();

            module.Assign(serviceProvider.GetRequiredService<IPipelineFactory>());

            var queueManager = CreateQueueService(serviceProvider);

            var inboxWorkQueue = queueManager.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var inboxErrorQueue = queueManager.Get(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox =
                new InboxQueueConfiguration
                {
                    WorkQueue = inboxWorkQueue,
                    ErrorQueue = inboxErrorQueue,
                    DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                    DurationToIgnoreOnFailure = new[] {TimeSpan.FromMilliseconds(5)},
                    MaximumFailureCount = 100,
                    ThreadCount = 1
                };

            inboxWorkQueue.Drop();
            inboxErrorQueue.Drop();

            queueManager.CreatePhysicalQueues(configuration);

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