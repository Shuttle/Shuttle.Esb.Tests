using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Transactions;

namespace Shuttle.Esb.Tests
{
    public class OutboxObserver : IPipelineObserver<OnAfterAcknowledgeMessage>
    {
        private readonly object _lock = new object();

        public int HandledMessageCount { get; private set; }

        public async Task Execute(OnAfterAcknowledgeMessage pipelineEvent)
        {
            lock (_lock)
            {
                HandledMessageCount++;
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    public abstract class OutboxFixture : IntegrationFixture
    {
        private async Task ConfigureQueues(IServiceProvider serviceProvider, string queueUriFormat,
            string errorQueueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();
            var outboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-outbox-work"));
            var errorQueue = queueService.Get(string.Format(errorQueueUriFormat, "test-error"));

            var receiverWorkQueue =
                queueService.Get(string.Format(queueUriFormat, "test-receiver-work"));

            await outboxWorkQueue.TryDrop().ConfigureAwait(false);
            await receiverWorkQueue.TryDrop().ConfigureAwait(false);
            await errorQueue.TryDrop().ConfigureAwait(false);

            await outboxWorkQueue.TryCreate().ConfigureAwait(false);
            await receiverWorkQueue.TryCreate().ConfigureAwait(false);
            await errorQueue.TryCreate().ConfigureAwait(false);

            await outboxWorkQueue.TryPurge().ConfigureAwait(false);
            await receiverWorkQueue.TryPurge().ConfigureAwait(false);
            await errorQueue.TryPurge().ConfigureAwait(false);
        }

        protected async Task TestOutboxSending(IServiceCollection services, string workQueueUriFormat, int threadCount,
            bool isTransactional)
        {
            await TestOutboxSending(services, workQueueUriFormat, workQueueUriFormat, threadCount, isTransactional).ConfigureAwait(false);
        }

        protected async Task TestOutboxSending(IServiceCollection services, string workQueueUriFormat,
            string errorQueueUriFormat, int threadCount, bool isTransactional)
        {
            Guard.AgainstNull(services, nameof(services));

            const int count = 100;

            if (threadCount < 1)
            {
                threadCount = 1;
            }

            services.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            services.AddServiceBus(builder =>
            {
                builder.Options = new ServiceBusOptions
                {
                    Outbox =
                        new OutboxOptions
                        {
                            WorkQueueUri = string.Format(workQueueUriFormat, "test-outbox-work"),
                            DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                            ThreadCount = threadCount
                        }
                };

                builder.SuppressHostedService = true;
            });

            var messageRouteProvider = new Mock<IMessageRouteProvider>();

            var receiverWorkQueueUri = string.Format(workQueueUriFormat, "test-receiver-work");

            messageRouteProvider.Setup(m => m.GetRouteUris(It.IsAny<string>())).Returns(new[] { receiverWorkQueueUri });

            services.AddSingleton(messageRouteProvider.Object);
            services.ConfigureLogging(nameof(TestOutboxSending));

            var serviceProvider = await services.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);

            var logger = serviceProvider.GetLogger<OutboxFixture>();
            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();

            var outboxObserver = new OutboxObserver();

            pipelineFactory.PipelineCreated += delegate(object sender, PipelineEventArgs args)
            {
                if (args.Pipeline.GetType() == typeof(OutboxPipeline))
                {
                    args.Pipeline.RegisterObserver(outboxObserver);
                }
            };

            var queueService = serviceProvider.CreateQueueService();

            await ConfigureQueues(serviceProvider, workQueueUriFormat, errorQueueUriFormat).ConfigureAwait(false);

            logger.LogInformation("Sending {0} messages.", count);

            await using (var serviceBus = await serviceProvider.GetRequiredService<IServiceBus>().Start().ConfigureAwait(false))
            {
                for (var i = 0; i < count; i++)
                {
                    await serviceBus.Send(new SimpleCommand { Context = "TestOutboxSending" }).ConfigureAwait(false);
                }

                var receiverWorkQueue = queueService.Get(receiverWorkQueueUri);
                var timedOut = false;
                var messageRetrieved = false;
                var timeout = DateTime.Now.AddSeconds(30);

                while (!messageRetrieved && !timedOut)
                {
                    var receivedMessage = await receiverWorkQueue.GetMessage().ConfigureAwait(false);

                    if (receivedMessage != null)
                    {
                        messageRetrieved = true;
                        await receiverWorkQueue.Release(receivedMessage.AcknowledgementToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await Task.Delay(25).ConfigureAwait(false);

                        timedOut = timeout < DateTime.Now;
                    }
                }

                Assert.IsFalse(timedOut, "Timed out before any messages appeared in the receiver queue.");

                timeout = DateTime.Now.AddSeconds(15);

                while (outboxObserver.HandledMessageCount < count && !timedOut)
                {
                    await Task.Delay(25).ConfigureAwait(false);

                    timedOut = timeout < DateTime.Now;
                }

                Assert.IsFalse(timedOut, "Timed out before processing {0} messages.", count);

                for (var i = 0; i < count; i++)
                {
                    var receivedMessage = await receiverWorkQueue.GetMessage().ConfigureAwait(false);

                    Assert.IsNotNull(receivedMessage);

                    await receiverWorkQueue.Acknowledge(receivedMessage.AcknowledgementToken).ConfigureAwait(false);
                }

                receiverWorkQueue.TryDispose();

                await receiverWorkQueue.TryDrop().ConfigureAwait(false);
            }

            await serviceProvider.StopHostedServices().ConfigureAwait(false);

            queueService = services.BuildServiceProvider().CreateQueueService();

            var outboxWorkQueue = queueService.Get(string.Format(workQueueUriFormat, "test-outbox-work"));

            Assert.IsTrue(await outboxWorkQueue.IsEmpty().ConfigureAwait(false));

            outboxWorkQueue.TryDispose();

            await outboxWorkQueue.TryDrop().ConfigureAwait(false);
            await queueService.Get(string.Format(errorQueueUriFormat, "test-error")).TryDrop().ConfigureAwait(false);
        }
    }
}