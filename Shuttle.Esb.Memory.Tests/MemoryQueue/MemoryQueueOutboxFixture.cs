using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests
{
    public class MemoryQueueOutboxFixture : OutboxFixture
    {
        [TestCase(true)]
        [TestCase(false)]
        public void Should_be_able_to_use_outbox(bool isTransactionalEndpoint)
        {
            TestOutboxSending(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", 3, isTransactionalEndpoint);
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task Should_be_able_to_use_outbox_async(bool isTransactionalEndpoint)
        {
            await TestOutboxSendingAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", 3, isTransactionalEndpoint);
        }
    }
}