using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.Transactions;

namespace Shuttle.Esb.Tests
{
    public class BasicQueueFixture : IntegrationFixture
    {
        protected void TestSimpleEnqueueAndGetMessage(IServiceCollection services, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            AddServiceBus(services, 1, false, queueUriFormat);

            var queueService = CreateQueueService(services.BuildServiceProvider());
            var workQueue = CreateWorkQueue(queueService, queueUriFormat);

            var stream = new MemoryStream();

            stream.WriteByte(100);

            workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()
            }, stream);

            var receivedMessage = workQueue.GetMessage();

            Assert.IsNotNull(receivedMessage, "It appears as though the test transport message was not enqueued or was somehow removed before it could be dequeued.");
            Assert.AreEqual(100, receivedMessage.Stream.ReadByte());
            Assert.IsNull(workQueue.GetMessage());

            workQueue.Acknowledge(receivedMessage.AcknowledgementToken);

            Assert.IsNull(workQueue.GetMessage());

            workQueue.AttemptDrop();
            workQueue.AttemptDispose();

            queueService.AttemptDispose();
        }

        protected void TestReleaseMessage(IServiceCollection services, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            AddServiceBus(services, 1, false, queueUriFormat);

            var queueService = CreateQueueService(services.BuildServiceProvider());
            var workQueue = CreateWorkQueue(queueService, queueUriFormat);

            workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()
            }, new MemoryStream(Encoding.ASCII.GetBytes("message-body")));

            var receivedMessage = workQueue.GetMessage();

            Assert.IsNotNull(receivedMessage);
            Assert.IsNull(workQueue.GetMessage());

            workQueue.Release(receivedMessage.AcknowledgementToken);

            receivedMessage = workQueue.GetMessage();

            Assert.IsNotNull(receivedMessage);
            Assert.IsNull(workQueue.GetMessage());

            workQueue.Acknowledge(receivedMessage.AcknowledgementToken);

            Assert.IsNull(workQueue.GetMessage());

            workQueue.AttemptDrop();
            workQueue.AttemptDispose();

            queueService.AttemptDispose();
        }

        protected void TestUnacknowledgedMessage(IServiceCollection services, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            AddServiceBus(services, 1, false, queueUriFormat);

            var queueService = CreateQueueService(services.BuildServiceProvider());
            var workQueue = CreateWorkQueue(queueService, queueUriFormat);

            workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()
            }, new MemoryStream(Encoding.ASCII.GetBytes("message-body")));

            Assert.IsNotNull(workQueue.GetMessage());
            Assert.IsNull(workQueue.GetMessage());

            queueService.AttemptDispose();
            queueService = CreateQueueService(services.BuildServiceProvider());

            workQueue = CreateWorkQueue(queueService, queueUriFormat, false);

            var receivedMessage = workQueue.GetMessage();

            Assert.IsNotNull(receivedMessage);
            Assert.IsNull(workQueue.GetMessage());

            workQueue.Acknowledge(receivedMessage.AcknowledgementToken);
            workQueue.AttemptDispose();

            workQueue = CreateWorkQueue(queueService, queueUriFormat, false);

            Assert.IsNull(workQueue.GetMessage());

            workQueue.AttemptDrop();
            workQueue.AttemptDispose();

            queueService.AttemptDispose();
        }

        private IQueue CreateWorkQueue(IQueueService queueService, string workQueueUriFormat, bool refresh = true)
        {
            Guard.AgainstNull(queueService, nameof(queueService));

            var workQueue = queueService.Get(string.Format(workQueueUriFormat, "test-work"));

            if (refresh)
            {
                workQueue.AttemptDrop();
                workQueue.AttemptCreate();
                workQueue.AttemptPurge();
            }

            return workQueue;
        }

        protected void AddServiceBus(IServiceCollection services, int threadCount, bool isTransactional, string queueUriFormat)
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
        }
    }
}