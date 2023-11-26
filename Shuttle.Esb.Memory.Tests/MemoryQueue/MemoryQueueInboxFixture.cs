using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests
{
    public class MemoryQueueInboxFixture : InboxFixture
    {
        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public void Should_be_able_handle_errors(bool hasErrorQueue, bool isTransactionalEndpoint)
        {
            TestInboxError(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", hasErrorQueue, isTransactionalEndpoint);
        }

        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public async Task Should_be_able_handle_errors_async(bool hasErrorQueue, bool isTransactionalEndpoint)
        {
            await TestInboxErrorAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", hasErrorQueue, isTransactionalEndpoint);
        }

        [TestCase(250, false)]
        [TestCase(250, true)]
        public void Should_be_able_to_process_messages_concurrently(int msToComplete, bool isTransactionalEndpoint)
        {
            TestInboxConcurrency(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", msToComplete, isTransactionalEndpoint);
        }

        [TestCase(250, false)]
        [TestCase(250, true)]
        public async Task Should_be_able_to_process_messages_concurrently_async(int msToComplete, bool isTransactionalEndpoint)
        {
            await TestInboxConcurrencyAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", msToComplete, isTransactionalEndpoint);
        }

        [TestCase(100, true)]
        [TestCase(100, false)]
        public void Should_be_able_to_process_queue_timeously(int count, bool isTransactionalEndpoint)
        {
            TestInboxThroughput(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", 1000, count, 5, isTransactionalEndpoint);
        }

        [TestCase(100, true)]
        [TestCase(100, false)]
        public async Task Should_be_able_to_process_queue_timeously_async(int count, bool isTransactionalEndpoint)
        {
            await TestInboxThroughputAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", 1000, count, 5, isTransactionalEndpoint);
        }

        [Test]
        public void Should_be_able_to_handle_a_deferred_message()
        {
            TestInboxDeferred(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}");
        }

        [Test]
        public async Task Should_be_able_to_handle_a_deferred_message_async()
        {
            await TestInboxDeferredAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}");
        }
    }
}