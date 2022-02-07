using System;
using System.Threading;
using NUnit.Framework;
using Shuttle.Core.Container;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class PipelineExceptionFixture : IntegrationFixture
    {
        protected void TestExceptionHandling(ComponentContainer container, string queueUriFormat)
        {
            var configuration = DefaultConfiguration(true, 1);

            container.Registry.RegisterServiceBus(configuration);

            var module = new ReceivePipelineExceptionModule(configuration);

            container.Registry.RegisterInstance(module.GetType(), module);

            module.Assign(container.Resolver.Resolve<IPipelineFactory>());

            var queueManager = CreateQueueManager(container.Resolver);

            var inboxWorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work"));
            var inboxErrorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

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

            var transportMessageFactory = container.Resolver.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolver.Resolve<ISerializer>();

            using (var bus = container.Resolver.Resolve<IServiceBus>())
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