using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests;

public class TransientQueueDeferredMessageFixture : DeferredFixture
{
    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public async Task Should_be_able_to_perform_full_processing_async(bool isTransactionalEndpoint)
    {
        await TestDeferredProcessingAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", isTransactionalEndpoint);
    }
}