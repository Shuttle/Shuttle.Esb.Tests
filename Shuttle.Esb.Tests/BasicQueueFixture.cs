using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.TransactionScope;

namespace Shuttle.Esb.Tests;

public class BasicQueueFixture : IntegrationFixture
{
    private void ConfigureServices(IServiceCollection services, string test, int threadCount, bool isTransactional, string queueUriFormat)
    {
        Guard.AgainstNull(services);

        services.AddTransactionScope(builder =>
        {
            builder.Options.Enabled = isTransactional;
        });

        services.AddServiceBus(builder =>
        {
            builder.Options = new()
            {
                Inbox = new()
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                    DurationToSleepWhenIdle = new() { TimeSpan.FromMilliseconds(25) },
                    DurationToIgnoreOnFailure = new() { TimeSpan.FromMilliseconds(25) },
                    ThreadCount = threadCount
                }
            };
        });

        services.ConfigureLogging(test);
    }

    private async Task<IQueue> CreateWorkQueueAsync(IQueueService queueService, string workQueueUriFormat, bool refresh)
    {
        var workQueue = Guard.AgainstNull(queueService).Get(string.Format(workQueueUriFormat, "test-work"));

        if (refresh)
        {
            await workQueue.TryDropAsync().ConfigureAwait(false);
            await workQueue.TryCreateAsync().ConfigureAwait(false);
            await workQueue.TryPurgeAsync().ConfigureAwait(false);
        }

        return workQueue;
    }

    protected void TestReleaseMessage(IServiceCollection services, string queueUriFormat)
    {
        TestReleaseMessageAsync(services, queueUriFormat, true).GetAwaiter().GetResult();
    }

    protected async Task TestReleaseMessageAsync(IServiceCollection services, string queueUriFormat)
    {
        await TestReleaseMessageAsync(services, queueUriFormat, false).ConfigureAwait(false);
    }

    private async Task TestReleaseMessageAsync(IServiceCollection services, string queueUriFormat, bool sync)
    {
        ConfigureServices(Guard.AgainstNull(services), nameof(TestReleaseMessageAsync), 1, false, queueUriFormat);

        var serviceProvider = services.BuildServiceProvider();
        var queueService = serviceProvider.CreateQueueService();
        var workQueue = sync
            ? CreateWorkQueueAsync(queueService, queueUriFormat, true).GetAwaiter().GetResult()
            : await CreateWorkQueueAsync(queueService, queueUriFormat, true).ConfigureAwait(false);

        try
        {
            await workQueue.EnqueueAsync(new() { MessageId = Guid.NewGuid() }, new MemoryStream(Encoding.ASCII.GetBytes("message-body"))).ConfigureAwait(false);

            var receivedMessage = await workQueue.GetMessageAsync().ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null);
            Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

            await workQueue.ReleaseAsync(receivedMessage!.AcknowledgementToken).ConfigureAwait(false);

            receivedMessage = await workQueue.GetMessageAsync().ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null);
            Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

            await workQueue.AcknowledgeAsync(receivedMessage!.AcknowledgementToken).ConfigureAwait(false);

            Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

            await workQueue.TryDropAsync().ConfigureAwait(false);
        }
        finally
        {
            await workQueue.TryDisposeAsync().ConfigureAwait(false);
            await queueService.TryDisposeAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestSimpleEnqueueAndGetMessageAsync(IServiceCollection services, string queueUriFormat)
    {
        ConfigureServices(Guard.AgainstNull(services), nameof(TestSimpleEnqueueAndGetMessageAsync), 1, false, queueUriFormat);

        var serviceProvider = services.BuildServiceProvider();
        var queueService = serviceProvider.CreateQueueService();
        var workQueue = await CreateWorkQueueAsync(queueService, queueUriFormat, true).ConfigureAwait(false);

        try
        {
            var stream = new MemoryStream();

            stream.WriteByte(100);

            await workQueue.EnqueueAsync(new()
                {
                    MessageId = Guid.NewGuid()
                }, stream).ConfigureAwait(false);

            var receivedMessage = await workQueue.GetMessageAsync().ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null, "It appears as though the test transport message was not enqueued or was somehow removed before it could be dequeued.");
            Assert.That(receivedMessage!.Stream.ReadByte(), Is.EqualTo(100));
            Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

            await workQueue.AcknowledgeAsync(receivedMessage.AcknowledgementToken).ConfigureAwait(false);

            Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

            await workQueue.TryDropAsync().ConfigureAwait(false);
        }
        finally
        {
            await workQueue.TryDisposeAsync().ConfigureAwait(false);
            await queueService.TryDisposeAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestUnacknowledgedMessageAsync(IServiceCollection services, string queueUriFormat)
    {
        ConfigureServices(Guard.AgainstNull(services), nameof(TestUnacknowledgedMessageAsync), 1, false, queueUriFormat);

        var queueService = services.BuildServiceProvider().CreateQueueService();

        var workQueue = await CreateWorkQueueAsync(queueService, queueUriFormat, true).ConfigureAwait(false);

        await workQueue.EnqueueAsync(new()
        {
            MessageId = Guid.NewGuid()
        }, new MemoryStream(Encoding.ASCII.GetBytes("message-body"))).ConfigureAwait(false);

        Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Not.Null);
        Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

        await queueService.TryDisposeAsync().ConfigureAwait(false);
        queueService = services.BuildServiceProvider().CreateQueueService();

        workQueue = await CreateWorkQueueAsync(queueService, queueUriFormat, false).ConfigureAwait(false);

        var receivedMessage = await workQueue.GetMessageAsync().ConfigureAwait(false);

        Assert.That(receivedMessage, Is.Not.Null);
        Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

        await workQueue.AcknowledgeAsync(receivedMessage!.AcknowledgementToken).ConfigureAwait(false);
        await workQueue.TryDisposeAsync().ConfigureAwait(false);

        workQueue = await CreateWorkQueueAsync(queueService, queueUriFormat, false).ConfigureAwait(false);

        Assert.That(await workQueue.GetMessageAsync().ConfigureAwait(false), Is.Null);

        await workQueue.TryDropAsync().ConfigureAwait(false);

        await workQueue.TryDisposeAsync().ConfigureAwait(false);
        await queueService.TryDisposeAsync().ConfigureAwait(false);
    }
}