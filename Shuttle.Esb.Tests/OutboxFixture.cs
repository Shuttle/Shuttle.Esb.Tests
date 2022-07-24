using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Transactions;

namespace Shuttle.Esb.Tests
{
    public abstract class OutboxFixture : IntegrationFixture
    {
        protected void TestOutboxSending(IServiceCollection services, string workQueueUriFormat, int threadCount, bool isTransactional)
        {
            TestOutboxSending(services, workQueueUriFormat, workQueueUriFormat, threadCount, isTransactional);
        }

        protected void TestOutboxSending(IServiceCollection services, string workQueueUriFormat,
            string errorQueueUriFormat, int threadCount, bool isTransactional)
        {
            Guard.AgainstNull(services, nameof(services));

            const int count = 100;

            if (threadCount < 1)
            {
                threadCount = 1;
            }

            var padlock = new object();

            services.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            var serviceBusOptions = GetServiceBusOptions(threadCount);

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
            });

            var messageRouteProvider = new Mock<IMessageRouteProvider>();

            var receiverWorkQueueUri = string.Format(workQueueUriFormat, "test-receiver-work");

            messageRouteProvider.Setup(m => m.GetRouteUris(It.IsAny<string>())).Returns(new[] { receiverWorkQueueUri });

            services.AddSingleton(messageRouteProvider.Object);

            var serviceProvider = services.BuildServiceProvider();

            var queueService = CreateQueueService(serviceProvider);

            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

            ConfigureQueues(serviceProvider, workQueueUriFormat, errorQueueUriFormat);

            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

            Console.WriteLine("Sending {0} messages.", count);

            using (var bus = serviceProvider.GetRequiredService<IServiceBus>().Start())
            {
                for (var i = 0; i < count; i++)
                {
                    bus.Send(new SimpleCommand());
                }

                var receiverWorkQueue = queueService.Get(receiverWorkQueueUri);
                var timedOut = false;
                var messageRetrieved = false;
                var timeout = DateTime.Now.AddSeconds(30);

                while (!messageRetrieved && !timedOut)
                {
                    var receivedMessage = receiverWorkQueue.GetMessage();

                    if (receivedMessage != null)
                    {
                        messageRetrieved = true;
                        receiverWorkQueue.Release(receivedMessage.AcknowledgementToken);
                    }
                    else
                    {
                        Thread.Sleep(25);

                        timedOut = timeout < DateTime.Now;
                    }
                }

                Assert.IsFalse(timedOut, "Timed out before any messages appeared in the receiver queue.");

                var idleThreads = new List<int>();

                threadActivity.ThreadWaiting += (sender, args) =>
                {
                    if (args.Pipeline.GetType() != typeof(OutboxPipeline))
                    {
                        return;
                    }

                    lock (padlock)
                    {
                        if (idleThreads.Contains(Thread.CurrentThread.ManagedThreadId))
                        {
                            return;
                        }

                        idleThreads.Add(Thread.CurrentThread.ManagedThreadId);
                    }
                };

                timeout = DateTime.Now.AddSeconds(5);

                while (idleThreads.Count < threadCount && !timedOut)
                {
                    Thread.Sleep(25);

                    timedOut = timeout < DateTime.Now;
                }

                Assert.IsFalse(timedOut, "Timed out before processing {0} errors.  Waiting for {1} threads to be idle.",
                    count, threadCount);

                for (var i = 0; i < count; i++)
                {
                    var receivedMessage = receiverWorkQueue.GetMessage();

                    Assert.IsNotNull(receivedMessage);

                    receiverWorkQueue.Acknowledge(receivedMessage.AcknowledgementToken);
                }
            }

            queueService.Get(receiverWorkQueueUri).AttemptDrop();

            var outboxWorkQueue = queueService.Get(string.Format(workQueueUriFormat, "test-outbox-work"));

            Assert.IsTrue(outboxWorkQueue.IsEmpty());

            outboxWorkQueue.AttemptDrop();

            queueService.Get(string.Format(errorQueueUriFormat, "test-error")).AttemptDrop();
        }

        private void ConfigureQueues(IServiceProvider serviceProvider, string queueUriFormat, string errorQueueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();
            var outboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-outbox-work"));
            var errorQueue = queueService.Get(string.Format(errorQueueUriFormat, "test-error"));

            var receiverWorkQueue =
                queueService.Get(string.Format(queueUriFormat, "test-receiver-work"));

            outboxWorkQueue.AttemptDrop();
            receiverWorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            outboxWorkQueue.AttemptCreate();
            receiverWorkQueue.AttemptCreate();
            errorQueue.AttemptCreate();

            outboxWorkQueue.AttemptPurge();
            receiverWorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        private ServiceBusOptions GetServiceBusOptions(int threadCount)
        {
            return new ServiceBusOptions
            {
                Outbox =
                    new OutboxOptions
                    {
                        DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                        ThreadCount = threadCount
                    }
            };
        }
    }
}