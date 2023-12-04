using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests
{
    public class TransientQueueDistributorFixture : DistributorFixture
    {
        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void Should_be_able_to_distribute_messages(bool isTransactionalEndpoint)
        {
            TestDistributor(TransientQueueConfiguration.GetServiceCollection(), TransientQueueConfiguration.GetServiceCollection(), @"transient-queue://./{0}", isTransactionalEndpoint);
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public async Task Should_be_able_to_distribute_messages_async(bool isTransactionalEndpoint)
        {
            await TestDistributorAsync(TransientQueueConfiguration.GetServiceCollection(), TransientQueueConfiguration.GetServiceCollection(), @"transient-queue://./{0}", isTransactionalEndpoint);
        }
    }
}