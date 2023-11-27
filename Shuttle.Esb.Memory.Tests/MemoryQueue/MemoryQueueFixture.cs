using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests
{
    [TestFixture]
    public class MemoryQueueFixture : BasicQueueFixture
    {
        [Test]
        public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
        {
            Assert.AreEqual($"memory-queue://{Environment.MachineName.ToLower()}/inputqueue", new MemoryQueue(new Uri("memory-queue://./inputqueue")).Uri.ToString());
        }

        [Test]
        public void Should_use_default_queue_name_for_empty_localpath()
        {
            Assert.AreEqual($"memory-queue://{Environment.MachineName.ToLower()}/default", new MemoryQueue(new Uri("memory-queue://.")).Uri.ToString());
        }

        [Test]
        public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_scheme()
        {
            Assert.Throws<InvalidSchemeException>(() => _ = new MemoryQueue(new Uri("sql://./inputqueue")));
        }

        [Test]
        public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_format()
        {
            Assert.Throws<UriFormatException>(() => _ = new MemoryQueue(new Uri("memory-queue://notthismachine")));
        }

        [Test]
        public void Should_be_able_to_perform_simple_enqueue_and_get_message()
        {
            TestSimpleEnqueueAndGetMessage(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}");
            TestSimpleEnqueueAndGetMessage(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}-transient");
        }

        [Test]
        public async Task Should_be_able_to_perform_simple_enqueue_and_get_message_async()
        {
            await TestSimpleEnqueueAndGetMessageAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}");
            await TestSimpleEnqueueAndGetMessageAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}-transient");
        }

        [Test]
        public void Should_be_able_to_release_a_message()
        {
            TestReleaseMessage(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}");
        }

        [Test]
        public async Task Should_be_able_to_release_a_message_async()
        {
            await TestReleaseMessageAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}");
        }

        [Test]
        public void Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed()
        {
            TestUnacknowledgedMessage(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}");
        }

        [Test]
        public async Task Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed_async()
        {
            await TestUnacknowledgedMessageAsync(MemoryQueueConfiguration.GetServiceCollection(), "memory-queue://./{0}");
        }
    }
}