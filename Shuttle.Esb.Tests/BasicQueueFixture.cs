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
        private void ConfigureServices(IServiceCollection services, string test, int threadCount, bool isTransactional, string queueUriFormat)
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

        private async Task<IQueue> CreateWorkQueueAsync(IQueueService queueService, string workQueueUriFormat, bool refresh, bool sync)
        {
            Guard.AgainstNull(queueService, nameof(queueService));

            var workQueue = queueService.Get(string.Format(workQueueUriFormat, "test-work"));

            if (refresh)
            {
                if (sync)
                {
                    workQueue.TryDrop();
                    workQueue.TryCreate();
                    workQueue.TryPurge();
                }
                else
                {
                    await workQueue.TryDropAsync().ConfigureAwait(false);
                    await workQueue.TryCreateAsync().ConfigureAwait(false);
                    await workQueue.TryPurgeAsync().ConfigureAwait(false);
                }
            }

            return workQueue;
        }

        protected void TestReleaseMessage(IServiceCollection services, string queueUriFormat)
        {
            TestReleaseMessageAsync(services, queueUriFormat, true).GetAwaiter().GetResult();
        }

        protected async Task TestReleaseMessageAsync(IServiceCollection services, string queueUriFormat)
        {
            await TestReleaseMessageAsync(services, queueUriFormat, false).ConfigureAwait(false);
        }

        private async Task TestReleaseMessageAsync(IServiceCollection services, string queueUriFormat, bool sync)
        {
            Guard.AgainstNull(services, nameof(services));

            ConfigureServices(services, nameof(TestReleaseMessageAsync), 1, false, queueUriFormat);

            var serviceProvider = services.BuildServiceProvider();
            var queueService = serviceProvider.CreateQueueService();
            var workQueue = sync
                ? CreateWorkQueueAsync(queueService, queueUriFormat, true, true).GetAwaiter().GetResult()
                : await CreateWorkQueueAsync(queueService, queueUriFormat, true, false).ConfigureAwait(false);

            try
            {
                if (sync)
                {
                    workQueue.Enqueue(new TransportMessage { MessageId = Guid.NewGuid() }, new MemoryStream(Encoding.ASCII.GetBytes("message-body")));

                    var receivedMessage = workQueue.GetMessage();

                    Assert.IsNotNull(receivedMessage);
                    Assert.IsNull(workQueue.GetMessage());

                    workQueue.Release(receivedMessage.AcknowledgementToken);

                    receivedMessage = workQueue.GetMessage();

                    Assert.IsNotNull(receivedMessage);
                    Assert.IsNull(workQueue.GetMessage());

                    workQueue.Acknowledge(receivedMessage.AcknowledgementToken);

                    Assert.IsNull(workQueue.GetMessage());

                    workQueue.TryDrop();
                }
                else
                {
                    await workQueue.EnqueueAsync(new TransportMessage { MessageId = Guid.NewGuid() }, new MemoryStream(Encoding.ASCII.GetBytes("message-body"))).ConfigureAwait(false);

                    var receivedMessage = await workQueue.GetMessageAsync().ConfigureAwait(false);

                    Assert.IsNotNull(receivedMessage);
                    Assert.IsNull(await workQueue.GetMessageAsync().ConfigureAwait(false));

                    await workQueue.ReleaseAsync(receivedMessage.AcknowledgementToken).ConfigureAwait(false);

                    receivedMessage = await workQueue.GetMessageAsync().ConfigureAwait(false);

                    Assert.IsNotNull(receivedMessage);
                    Assert.IsNull(await workQueue.GetMessageAsync().ConfigureAwait(false));

                    await workQueue.AcknowledgeAsync(receivedMessage.AcknowledgementToken).ConfigureAwait(false);

                    Assert.IsNull(await workQueue.GetMessageAsync().ConfigureAwait(false));

                    await workQueue.TryDropAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                if (sync)
                {
                    workQueue.TryDispose();
                    queueService.TryDispose();
                }
                else
                {
                    await workQueue.TryDisposeAsync();
                    await queueService.TryDisposeAsync();
                }
            }
        }

        protected void TestSimpleEnqueueAndGetMessage(IServiceCollection services, string queueUriFormat)
        {
            TestSimpleEnqueueAndGetMessageAsync(services, queueUriFormat, true).GetAwaiter().GetResult();
        }

        protected async Task TestSimpleEnqueueAndGetMessageAsync(IServiceCollection services, string queueUriFormat)
        {
            await TestSimpleEnqueueAndGetMessageAsync(services, queueUriFormat, false).ConfigureAwait(false);
        }

        private async Task TestSimpleEnqueueAndGetMessageAsync(IServiceCollection services, string queueUriFormat, bool sync)
        {
            Guard.AgainstNull(services, nameof(services));

            ConfigureServices(services, nameof(TestSimpleEnqueueAndGetMessageAsync), 1, false, queueUriFormat);

            var serviceProvider = services.BuildServiceProvider();
            var queueService = serviceProvider.CreateQueueService();
            var workQueue = sync
                ? CreateWorkQueueAsync(queueService, queueUriFormat, true, sync).GetAwaiter().GetResult()
                : await CreateWorkQueueAsync(queueService, queueUriFormat, true, sync).ConfigureAwait(false);

            try
            {
                var stream = new MemoryStream();

                stream.WriteByte(100);

                if (sync)
                {
                    workQueue.Enqueue(new TransportMessage
                    {
                        MessageId = Guid.NewGuid()
                    }, stream);
                }
                else
                {
                    await workQueue.EnqueueAsync(new TransportMessage
                    {
                        MessageId = Guid.NewGuid()
                    }, stream).ConfigureAwait(false);
                }

                var receivedMessage = sync
                    ? workQueue.GetMessage()
                    : await workQueue.GetMessageAsync().ConfigureAwait(false);

                Assert.That(receivedMessage, Is.Not.Null, "It appears as though the test transport message was not enqueued or was somehow removed before it could be dequeued.");
                Assert.That(receivedMessage.Stream.ReadByte(), Is.EqualTo(100));
                Assert.That(sync ? workQueue.GetMessage() : await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

                if (sync)
                {
                    workQueue.Acknowledge(receivedMessage.AcknowledgementToken);
                }
                else
                {
                    await workQueue.AcknowledgeAsync(receivedMessage.AcknowledgementToken).ConfigureAwait(false);
                }

                Assert.That(sync ? workQueue.GetMessage() : await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

                if (sync)
                {
                    workQueue.TryDrop();
                }
                else
                {
                    await workQueue.TryDropAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                if (sync)
                {
                    workQueue.TryDispose();
                    queueService.TryDispose();
                }
                else
                {
                    await workQueue.TryDisposeAsync();
                    await queueService.TryDisposeAsync();
                }
            }
        }

        protected void TestUnacknowledgedMessage(IServiceCollection services, string queueUriFormat)
        {
            TestUnacknowledgedMessageAsync(services, queueUriFormat, true).GetAwaiter().GetResult();
        }

        protected async Task TestUnacknowledgedMessageAsync(IServiceCollection services, string queueUriFormat)
        {
            await TestUnacknowledgedMessageAsync(services, queueUriFormat, false).ConfigureAwait(false);
        }

        private async Task TestUnacknowledgedMessageAsync(IServiceCollection services, string queueUriFormat, bool sync)
        {
            Guard.AgainstNull(services, nameof(services));

            ConfigureServices(services, nameof(TestUnacknowledgedMessageAsync), 1, false, queueUriFormat);

            var queueService = services.BuildServiceProvider().CreateQueueService();

            if (sync)
            {
                var workQueue = CreateWorkQueueAsync(queueService, queueUriFormat, true, true).GetAwaiter().GetResult();

                workQueue.Enqueue(new TransportMessage
                {
                    MessageId = Guid.NewGuid()
                }, new MemoryStream(Encoding.ASCII.GetBytes("message-body")));

                Assert.IsNotNull(workQueue.GetMessage());
                Assert.IsNull(workQueue.GetMessage());

                queueService.TryDispose();
                queueService = services.BuildServiceProvider().CreateQueueService();

                workQueue = CreateWorkQueueAsync(queueService, queueUriFormat, false, true).GetAwaiter().GetResult();

                var receivedMessage = workQueue.GetMessage();

                Assert.IsNotNull(receivedMessage);
                Assert.IsNull(workQueue.GetMessage());

                workQueue.Acknowledge(receivedMessage.AcknowledgementToken);
                workQueue.TryDispose();

                workQueue = CreateWorkQueueAsync(queueService, queueUriFormat, false, true).GetAwaiter().GetResult();

                Assert.IsNull(workQueue.GetMessage());

                workQueue.TryDrop();

                workQueue.TryDispose();
                queueService.TryDispose();
            }
            else
            {
                var workQueue = await CreateWorkQueueAsync(queueService, queueUriFormat, true, false).ConfigureAwait(false);

                await workQueue.EnqueueAsync(new TransportMessage
                {
                    MessageId = Guid.NewGuid()
                }, new MemoryStream(Encoding.ASCII.GetBytes("message-body"))).ConfigureAwait(false);

                Assert.IsNotNull(await workQueue.GetMessageAsync().ConfigureAwait(false));
                Assert.IsNull(await workQueue.GetMessageAsync().ConfigureAwait(false));

                await queueService.TryDisposeAsync().ConfigureAwait(false);
                queueService = services.BuildServiceProvider().CreateQueueService();

                workQueue = await CreateWorkQueueAsync(queueService, queueUriFormat, false, false).ConfigureAwait(false);

                var receivedMessage = await workQueue.GetMessageAsync().ConfigureAwait(false);

                Assert.IsNotNull(receivedMessage);
                Assert.IsNull(await workQueue.GetMessageAsync().ConfigureAwait(false));

                await workQueue.AcknowledgeAsync(receivedMessage.AcknowledgementToken).ConfigureAwait(false);
                await workQueue.TryDisposeAsync().ConfigureAwait(false);

                workQueue = await CreateWorkQueueAsync(queueService, queueUriFormat, false, false).ConfigureAwait(false);

                Assert.IsNull(await workQueue.GetMessageAsync().ConfigureAwait(false));

                await workQueue.TryDropAsync().ConfigureAwait(false);

                await workQueue.TryDisposeAsync().ConfigureAwait(false);
                await queueService.TryDisposeAsync().ConfigureAwait(false);
            }
        }
    }
}