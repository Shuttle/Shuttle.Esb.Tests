using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.TransactionScope;

namespace Shuttle.Esb.Tests;

public class OutboxObserver : IPipelineObserver<OnAfterAcknowledgeMessage>
{
    private readonly object _lock = new();

    public int HandledMessageCount { get; private set; }

    public async Task ExecuteAsync(IPipelineContext<OnAfterAcknowledgeMessage> pipelineContext)
    {
        lock (_lock)
        {
            HandledMessageCount++;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}

public abstract class OutboxFixture : IntegrationFixture
{
    private async Task ConfigureQueuesAsync(IServiceProvider serviceProvider, string queueUriFormat, string errorQueueUriFormat)
    {
        var queueService = serviceProvider.GetRequiredService<IQueueService>();
        var outboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-outbox-work"));

        Assert.That(outboxWorkQueue.IsStream, Is.False, "This test cannot be applied to streams.");

        var errorQueue = queueService.Get(string.Format(errorQueueUriFormat, "test-error"));

        var receiverWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-receiver-work"));

        await outboxWorkQueue.TryDropAsync().ConfigureAwait(false);
        await receiverWorkQueue.TryDropAsync().ConfigureAwait(false);
        await errorQueue.TryDropAsync().ConfigureAwait(false);

        await outboxWorkQueue.TryCreateAsync().ConfigureAwait(false);
        await receiverWorkQueue.TryCreateAsync().ConfigureAwait(false);
        await errorQueue.TryCreateAsync().ConfigureAwait(false);

        await outboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
        await receiverWorkQueue.TryPurgeAsync().ConfigureAwait(false);
        await errorQueue.TryPurgeAsync().ConfigureAwait(false);
    }

    protected async Task TestOutboxSendingAsync(IServiceCollection services, string workQueueUriFormat, int threadCount, bool isTransactional)
    {
        await TestOutboxSendingAsync(services, workQueueUriFormat, workQueueUriFormat, threadCount, isTransactional).ConfigureAwait(false);
    }

    protected async Task TestOutboxSendingAsync(IServiceCollection services, string workQueueUriFormat, string errorQueueUriFormat, int threadCount, bool isTransactional)
    {
        Guard.AgainstNull(services);

        const int count = 100;

        if (threadCount < 1)
        {
            threadCount = 1;
        }

        services.AddTransactionScope(builder =>
        {
            builder.Options.Enabled = isTransactional;
        });

        var workQueueUri = string.Format(workQueueUriFormat, "test-outbox-work");
        var receiverWorkQueueUri = string.Format(workQueueUriFormat, "test-receiver-work");
        var messageRouteProvider = new Mock<IMessageRouteProvider>();

        messageRouteProvider.Setup(m => m.GetRouteUris(It.IsAny<string>())).Returns(new[] { receiverWorkQueueUri });
        messageRouteProvider.Setup(m => m.GetRouteUrisAsync(It.IsAny<string>())).Returns(Task.FromResult<IEnumerable<string>>(new[] { receiverWorkQueueUri }));

        services.AddSingleton(messageRouteProvider.Object);

        services.AddServiceBus(builder =>
        {
            builder.Options = new()
            {
                Outbox =
                    new()
                    {
                        WorkQueueUri = workQueueUri,
                        DurationToSleepWhenIdle = new() { TimeSpan.FromMilliseconds(25) },
                        ThreadCount = threadCount
                    }
            };

            builder.SuppressHostedService = true;
        });

        services.ConfigureLogging(nameof(OutboxFixture));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var logger = serviceProvider.GetLogger<OutboxFixture>();
        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();

        var outboxObserver = new OutboxObserver();

        pipelineFactory.PipelineCreated += delegate(object? _, PipelineEventArgs args)
        {
            if (args.Pipeline.GetType() == typeof(OutboxPipeline))
            {
                args.Pipeline.RegisterObserver(outboxObserver);
            }
        };

        var queueService = serviceProvider.CreateQueueService();

        await ConfigureQueuesAsync(serviceProvider, workQueueUriFormat, errorQueueUriFormat).ConfigureAwait(false);

        logger.LogInformation("Sending {0} messages.", count);

        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

        try
        {
            await serviceBus.StartAsync().ConfigureAwait(false);

            var command = new SimpleCommand { Context = "TestOutboxSending" };

            for (var i = 0; i < count; i++)
            {
                await serviceBus.SendAsync(command).ConfigureAwait(false);
            }

            var receiverWorkQueue = queueService.Get(receiverWorkQueueUri);
            var timedOut = false;
            var timeout = DateTime.Now.AddSeconds(150);

            while (outboxObserver.HandledMessageCount < count && !timedOut)
            {
                await Task.Delay(25).ConfigureAwait(false);

                timedOut = timeout < DateTime.Now;
            }

            Assert.That(timedOut, Is.False, $"Timed out before processing {count} messages.");

            for (var i = 0; i < count; i++)
            {
                var receivedMessage = await receiverWorkQueue.GetMessageAsync().ConfigureAwait(false);

                Assert.That(receivedMessage, Is.Not.Null);

                await receiverWorkQueue.AcknowledgeAsync(receivedMessage!.AcknowledgementToken).ConfigureAwait(false);
            }

            await receiverWorkQueue.TryDisposeAsync().ConfigureAwait(false);
            await receiverWorkQueue.TryDropAsync().ConfigureAwait(false);
        }
        finally
        {
            await serviceBus.DisposeAsync().ConfigureAwait(false);
        }

        await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);

        queueService = services.BuildServiceProvider().CreateQueueService();

        var outboxWorkQueue = queueService.Get(workQueueUri);

        Assert.That(await outboxWorkQueue.IsEmptyAsync().ConfigureAwait(false), Is.True);

        await outboxWorkQueue.TryDisposeAsync().ConfigureAwait(false);

        var errorQueue = queueService.Get(string.Format(errorQueueUriFormat, "test-error"));

        await outboxWorkQueue.TryDropAsync().ConfigureAwait(false);
        await errorQueue.TryDropAsync().ConfigureAwait(false);
    }
}