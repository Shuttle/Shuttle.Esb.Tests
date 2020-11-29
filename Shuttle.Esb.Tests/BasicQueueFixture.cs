using System;
using System.IO;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;

namespace Shuttle.Esb.Tests
{
    public class BasicQueueFixture : IntegrationFixture
    {
        protected void TestSimpleEnqueueAndGetMessage(ComponentContainer container, string workQueueUriFormat)
        {
            Guard.AgainstNull(container, "container");

            Configure(container);

            var queueManager = CreateQueueManager(container.Resolver);
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

        private void Configure(ComponentContainer container)
        {
            ServiceBus.Register(container.Registry, DefaultConfiguration(true, 1));
        }

        protected void TestReleaseMessage(ComponentContainer container, string workQueueUriFormat)
        {
            Guard.AgainstNull(container, "container");

            Configure(container);

            var queueManager = CreateQueueManager(container.Resolver);
            var workQueue = CreateWorkQueue(queueManager, workQueueUriFormat);

            workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()

            }, new MemoryStream());

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

        protected void TestUnacknowledgedMessage(ComponentContainer container, string workQueueUriFormat)
        {
            Guard.AgainstNull(container, "container");

            Configure(container);

            var queueManager = CreateQueueManager(container.Resolver);
            var workQueue = CreateWorkQueue(queueManager, workQueueUriFormat);

            workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()
            }, new MemoryStream());

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

        private IQueue CreateWorkQueue(IQueueManager queueManager, string workQueueUriFormat)
        {
            return CreateWorkQueue(queueManager, workQueueUriFormat, true);
        }

        private IQueue CreateWorkQueue(IQueueManager queueManager, string workQueueUriFormat, bool refresh)
        {
            Guard.AgainstNull(queueManager, nameof(queueManager));

            var workQueue = queueManager.CreateQueue(string.Format(workQueueUriFormat, "test-work"));

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