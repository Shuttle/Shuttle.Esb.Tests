using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests;

[TestFixture]
public class TransientQueueFixture : BasicQueueFixture
{
    [Test]
    public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
    {
        Assert.That(new TransientQueue(new("transient-queue://./inputqueue"), CancellationToken.None).Uri.ToString(), Is.EqualTo($"transient-queue://{Environment.MachineName.ToLower()}/inputqueue"));
    }

    [Test]
    public void Should_use_default_queue_name_for_empty_localpath()
    {
        Assert.That(new TransientQueue(new("transient-queue://."), CancellationToken.None).Uri.ToString(), Is.EqualTo($"transient-queue://{Environment.MachineName.ToLower()}/default"));
    }

    [Test]
    public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_scheme()
    {
        Assert.Throws<InvalidSchemeException>(() => _ = new TransientQueue(new("sql://./inputqueue"), CancellationToken.None));
    }

    [Test]
    public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_format()
    {
        Assert.Throws<UriFormatException>(() => _ = new TransientQueue(new("transient-queue://notthismachine"), CancellationToken.None));
    }

    [Test]
    public async Task Should_be_able_to_perform_simple_enqueue_and_get_message_async()
    {
        await TestSimpleEnqueueAndGetMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
        await TestSimpleEnqueueAndGetMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}-transient");
    }

    [Test]
    public async Task Should_be_able_to_release_a_message_async()
    {
        await TestReleaseMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
    }

    [Test]
    public async Task Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed_async()
    {
        await TestUnacknowledgedMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
    }
}