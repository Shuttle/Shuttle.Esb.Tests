using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.Transactions;

namespace Shuttle.Esb.Tests
{
    public class BasicQueueFixture : IntegrationFixture
    {
        protected async Task TestSimpleEnqueueAndGetMessage(IServiceCollection services, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            ConfigureServices(services, nameof(TestSimpleEnqueueAndGetMessage), 1, false, queueUriFormat);

            var serviceProvider = services.BuildServiceProvider();
            var queueService = serviceProvider.CreateQueueService();
            var workQueue = await CreateWorkQueue(queueService, queueUriFormat).ConfigureAwait(false);

            try
            {
                var stream = new MemoryStream();

                stream.WriteByte(100);

                await workQueue.Enqueue(new TransportMessage
                {
                    MessageId = Guid.NewGuid()
                }, stream).ConfigureAwait(false);

                var receivedMessage = await workQueue.GetMessage().ConfigureAwait(false);

                Assert.IsNotNull(receivedMessage, "It appears as though the test transport message was not enqueued or was somehow removed before it could be dequeued.");
                Assert.AreEqual(100, receivedMessage.Stream.ReadByte());
                Assert.IsNull(await workQueue.GetMessage().ConfigureAwait(false));

                await workQueue.Acknowledge(receivedMessage.AcknowledgementToken).ConfigureAwait(false);

                Assert.IsNull(await workQueue.GetMessage().ConfigureAwait(false));

                await workQueue.TryDrop().ConfigureAwait(false);
            }
            finally
            {
                workQueue.TryDispose();
                queueService.TryDispose();
            }
        }

        protected async Task TestReleaseMessage(IServiceCollection services, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            ConfigureServices(services, nameof(TestReleaseMessage), 1, false, queueUriFormat);

            var serviceProvider = services.BuildServiceProvider();
            var queueService = serviceProvider.CreateQueueService();
            var workQueue = await CreateWorkQueue(queueService, queueUriFormat).ConfigureAwait(false);

            try
            {
                await workQueue.Enqueue(new TransportMessage { MessageId = Guid.NewGuid() }, new MemoryStream(Encoding.ASCII.GetBytes("message-body"))).ConfigureAwait(false);

                var receivedMessage = await workQueue.GetMessage().ConfigureAwait(false);

                Assert.IsNotNull(receivedMessage);
                Assert.IsNull(await workQueue.GetMessage().ConfigureAwait(false));

                await workQueue.Release(receivedMessage.AcknowledgementToken).ConfigureAwait(false);

                receivedMessage = await workQueue.GetMessage().ConfigureAwait(false);

                Assert.IsNotNull(receivedMessage);
                Assert.IsNull(await workQueue.GetMessage().ConfigureAwait(false));

                await workQueue.Acknowledge(receivedMessage.AcknowledgementToken).ConfigureAwait(false);

                Assert.IsNull(await workQueue.GetMessage().ConfigureAwait(false));

                await workQueue.TryDrop().ConfigureAwait(false);
            }
            finally
            {
                workQueue.TryDispose();
                queueService.TryDispose();
            }
        }

        protected async Task TestUnacknowledgedMessage(IServiceCollection services, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            ConfigureServices(services, nameof(TestUnacknowledgedMessage), 1, false, queueUriFormat);

            var queueService = services.BuildServiceProvider().CreateQueueService();
            var workQueue = await CreateWorkQueue(queueService, queueUriFormat).ConfigureAwait(false);

            await workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()
            }, new MemoryStream(Encoding.ASCII.GetBytes("message-body"))).ConfigureAwait(false);

            Assert.IsNotNull(await workQueue.GetMessage().ConfigureAwait(false));
            Assert.IsNull(await workQueue.GetMessage().ConfigureAwait(false));

            queueService.TryDispose();
            queueService = services.BuildServiceProvider().CreateQueueService();

            workQueue = await CreateWorkQueue(queueService, queueUriFormat, false).ConfigureAwait(false);

            var receivedMessage = await workQueue.GetMessage().ConfigureAwait(false);

            Assert.IsNotNull(receivedMessage);
            Assert.IsNull(await workQueue.GetMessage().ConfigureAwait(false));

            await workQueue.Acknowledge(receivedMessage.AcknowledgementToken).ConfigureAwait(false);
            workQueue.TryDispose();

            workQueue = await CreateWorkQueue(queueService, queueUriFormat, false).ConfigureAwait(false);

            Assert.IsNull(await workQueue.GetMessage().ConfigureAwait(false));

            await workQueue.TryDrop().ConfigureAwait(false);
            
            workQueue.TryDispose();
            queueService.TryDispose();
        }

        private async Task<IQueue> CreateWorkQueue(IQueueService queueService, string workQueueUriFormat, bool refresh = true)
        {
            Guard.AgainstNull(queueService, nameof(queueService));

            var workQueue = queueService.Get(string.Format(workQueueUriFormat, "test-work"));

            if (refresh)
            {
                await workQueue.TryDrop().ConfigureAwait(false);
                await workQueue.TryCreate().ConfigureAwait(false);
                await workQueue.TryPurge().ConfigureAwait(false);
            }

            return workQueue;
        }

        protected void ConfigureServices(IServiceCollection services, string test, int threadCount, bool isTransactional, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            services.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            services.AddServiceBus(builder =>
            {
                builder.Options = new ServiceBusOptions
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
            });

            services.ConfigureLogging(test);
        }
    }
}