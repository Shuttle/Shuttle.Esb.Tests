using System;
using System.IO;
using NUnit.Framework;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
    public class BasicQueueFixture : IntegrationFixture
    {
        protected void TestSimpleEnqueueAndGetMessage(IQueueManager queueManager, string workQueueUriFormat)
        {

            var stream = new MemoryStream();

            stream.WriteByte(100);

            workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()

            }, stream);

            var receivedMessage = workQueue.GetMessage();

            Assert.AreEqual(100, receivedMessage.Stream.ReadByte());
            Assert.IsNull(workQueue.GetMessage());

            workQueue.Acknowledge(receivedMessage.AcknowledgementToken);

            Assert.IsNull(workQueue.GetMessage());

            workQueue.AttemptDrop();
        }

        protected void TestReleaseMessage(IQueueManager queueManager, string workQueueUriFormat)
        {

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
        }

        protected void TestUnacknowledgedMessage(IQueueManager queueManager, string workQueueUriFormat)
        {

            workQueue.Enqueue(new TransportMessage
            {
                MessageId = Guid.NewGuid()

            }, new MemoryStream());

            Assert.IsNotNull(workQueue.GetMessage());
            Assert.IsNull(workQueue.GetMessage());

            workQueue.AttemptDispose();


            var receivedMessage = workQueue.GetMessage();

            Assert.IsNotNull(receivedMessage);
            Assert.IsNull(workQueue.GetMessage());

            workQueue.Acknowledge(receivedMessage.AcknowledgementToken);
            workQueue.AttemptDispose();


            Assert.IsNull(workQueue.GetMessage());

            workQueue.AttemptDrop();
        }

        {
        }

        {
            Guard.AgainstNull(queueManager, "queueManager");


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