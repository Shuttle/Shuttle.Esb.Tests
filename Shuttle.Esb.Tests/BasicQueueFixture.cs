using System;
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
        protected void TestSimpleEnqueueAndGetMessage(IServiceCollection services, string workQueueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            AddServiceBus(services, 1, false, new ServiceBusConfiguration());

            var queueManager = CreateQueueService(services.BuildServiceProvider());
            var workQueue = CreateWorkQueue(queueManager, workQueueUriFormat);

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

            queueManager.AttemptDispose();
        }

        protected void TestReleaseMessage(IServiceCollection services, string workQueueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            AddServiceBus(services, 1, false, new ServiceBusConfiguration());

            var queueManager = CreateQueueService(services.BuildServiceProvider());
            var workQueue = CreateWorkQueue(queueManager, workQueueUriFormat);

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

            queueManager.AttemptDispose();
        }

        protected void TestUnacknowledgedMessage(IServiceCollection services, string workQueueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            AddServiceBus(services, 1, false, new ServiceBusConfiguration());

            var queueManager = CreateQueueService(services.BuildServiceProvider());
            var workQueue = CreateWorkQueue(queueManager, workQueueUriFormat);

            workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()
            }, new MemoryStream(Encoding.ASCII.GetBytes("message-body")));

            Assert.IsNotNull(workQueue.GetMessage());
            Assert.IsNull(workQueue.GetMessage());

            workQueue.AttemptDispose();

            workQueue = CreateWorkQueue(queueManager, workQueueUriFormat, false);

            var receivedMessage = workQueue.GetMessage();

            Assert.IsNotNull(receivedMessage);
            Assert.IsNull(workQueue.GetMessage());

            workQueue.Acknowledge(receivedMessage.AcknowledgementToken);
            workQueue.AttemptDispose();

            workQueue = CreateWorkQueue(queueManager, workQueueUriFormat, false);

            Assert.IsNull(workQueue.GetMessage());

            workQueue.AttemptDrop();
            workQueue.AttemptDispose();

            queueManager.AttemptDispose();
        }

        private IQueue CreateWorkQueue(IQueueService queueService, string workQueueUriFormat, bool refresh = true)
        {
            Guard.AgainstNull(queueService, nameof(queueService));

            var workQueue = queueService.Create(string.Format(workQueueUriFormat, "test-work"));

            if (refresh)
            {
                workQueue.AttemptDrop();
                workQueue.AttemptCreate();
                workQueue.AttemptPurge();
            }

            return workQueue;
        }
    }
}