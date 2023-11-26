using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests
{
    public class MemoryQueueDeferredMessageFixture : DeferredFixture
    {
        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void Should_be_able_to_perform_full_processing(bool isTransactionalEndpoint)
        {
            TestDeferredProcessing(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", isTransactionalEndpoint);
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public async Task Should_be_able_to_perform_full_processing_async(bool isTransactionalEndpoint)
        {
            await TestDeferredProcessingAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}", isTransactionalEndpoint);
        }
    }
}