using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests;

public class TransientStreamInboxFixture : InboxFixture
{
    [TestCase(true, true)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(false, false)]
    public async Task Should_be_able_handle_errors_async(bool hasErrorQueue, bool isTransactionalEndpoint)
    {
        await TestInboxErrorAsync(TransientStreamConfiguration.GetServiceCollection(), "transient-stream://./{0}", hasErrorQueue, isTransactionalEndpoint);
    }

    [TestCase(100, true)]
    [TestCase(100, false)]
    public async Task Should_be_able_to_process_queue_timeously_async(int count, bool isTransactionalEndpoint)
    {
        await TestInboxThroughputAsync(TransientStreamConfiguration.GetServiceCollection(), "transient-stream://./{0}", 1000, count, 1, isTransactionalEndpoint);
    }
}