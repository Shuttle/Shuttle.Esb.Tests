using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests
{
    [TestFixture]
    public class TransientQueueFixture : BasicQueueFixture
    {
        [Test]
        public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
        {
            Assert.AreEqual($"transient-queue://{Environment.MachineName.ToLower()}/inputqueue", new TransientQueue(new Uri("transient-queue://./inputqueue")).Uri.ToString());
        }

        [Test]
        public void Should_use_default_queue_name_for_empty_localpath()
        {
            Assert.AreEqual($"transient-queue://{Environment.MachineName.ToLower()}/default", new TransientQueue(new Uri("transient-queue://.")).Uri.ToString());
        }

        [Test]
        public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_scheme()
        {
            Assert.Throws<InvalidSchemeException>(() => _ = new TransientQueue(new Uri("sql://./inputqueue")));
        }

        [Test]
        public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_format()
        {
            Assert.Throws<UriFormatException>(() => _ = new TransientQueue(new Uri("transient-queue://notthismachine")));
        }

        [Test]
        public void Should_be_able_to_perform_simple_enqueue_and_get_message()
        {
            TestSimpleEnqueueAndGetMessage(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
            TestSimpleEnqueueAndGetMessage(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}-transient");
        }

        [Test]
        public async Task Should_be_able_to_perform_simple_enqueue_and_get_message_async()
        {
            await TestSimpleEnqueueAndGetMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
            await TestSimpleEnqueueAndGetMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}-transient");
        }

        [Test]
        public void Should_be_able_to_release_a_message()
        {
            TestReleaseMessage(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
        }

        [Test]
        public async Task Should_be_able_to_release_a_message_async()
        {
            await TestReleaseMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
        }

        [Test]
        public void Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed()
        {
            TestUnacknowledgedMessage(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
        }

        [Test]
        public async Task Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed_async()
        {
            await TestUnacknowledgedMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
        }
    }
}