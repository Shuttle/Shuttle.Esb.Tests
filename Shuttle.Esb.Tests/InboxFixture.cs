using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;
using Shuttle.Core.Threading;
using Shuttle.Core.TransactionScope;

namespace Shuttle.Esb.Tests;

public class ThroughputObserver : IPipelineObserver<OnAfterAcknowledgeMessage>
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

public class ProcessorThreadObserver : IPipelineObserver<OnStarted>
{
    private readonly ILogger<ProcessorThreadObserver> _logger;

    public ProcessorThreadObserver(ILogger<ProcessorThreadObserver> logger)
    {
        _logger = Guard.AgainstNull(logger);
    }

    public async Task ExecuteAsync(IPipelineContext<OnStarted> pipelineContext)
    {
        var executedThreads = new List<int>();

        foreach (var processorThread in pipelineContext.Pipeline.State.Get<IProcessorThreadPool>("InboxThreadPool")!.ProcessorThreads)
        {
            processorThread.ProcessorExecuting += (_, args) =>
            {
                if (executedThreads.Contains(args.ManagedThreadId))
                {
                    return;
                }

                _logger.LogInformation($"[executing] : thread id = {args.ManagedThreadId} / name = '{args.Name}'");

                executedThreads.Add(args.ManagedThreadId);
            };
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}

public class InboxMessagePipelineObserver : IPipelineObserver<OnPipelineException>
{
    private readonly ILogger<InboxFixture> _logger;

    public InboxMessagePipelineObserver(ILogger<InboxFixture> logger)
    {
        _logger = Guard.AgainstNull(logger);
    }

    public bool HasReceivedPipelineException { get; private set; }

    public async Task ExecuteAsync(IPipelineContext<OnPipelineException> pipelineContext)
    {
        HasReceivedPipelineException = true;

        _logger.LogInformation($"[OnPipelineException] : {nameof(HasReceivedPipelineException)} = 'true'");

        await Task.CompletedTask.ConfigureAwait(false);
    }
}

public abstract class InboxFixture : IntegrationFixture
{
    private async Task ConfigureQueuesAsync(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat, bool hasErrorQueue)
    {
        var queueService = serviceProvider.GetRequiredService<IQueueService>();

        var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
        var errorQueue = hasErrorQueue ? queueService.Get(string.Format(queueUriFormat, "test-error")) : null;

        await inboxWorkQueue.TryDropAsync().ConfigureAwait(false);
        await (errorQueue?.TryDropAsync() ?? ValueTask.FromResult(false)).ConfigureAwait(false);
        await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);
        await inboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
        await (errorQueue?.TryPurgeAsync() ?? ValueTask.FromResult(false)).ConfigureAwait(false);
    }

    private void ConfigureServices(IServiceCollection services, string test, bool hasErrorQueue, int threadCount, bool isTransactional, string queueUriFormat, TimeSpan durationToSleepWhenIdle)
    {
        Guard.AgainstNull(services);

        services.AddTransactionScope(builder =>
        {
            builder.Options.Enabled = isTransactional;
        });

        var serviceBusOptions = new ServiceBusOptions
        {
            Inbox = new()
            {
                WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                ErrorQueueUri = hasErrorQueue ? string.Format(queueUriFormat, "test-error") : string.Empty,
                DurationToSleepWhenIdle = new() { durationToSleepWhenIdle },
                DurationToIgnoreOnFailure = new() { TimeSpan.FromMilliseconds(25) },
                ThreadCount = threadCount,
                MaximumFailureCount = 0
            }
        };

        services.AddServiceBus(builder =>
        {
            builder.Options = serviceBusOptions;
            builder.SuppressHostedService();
        });

        services.ConfigureLogging(test);
    }

    // NOT APPLICABLE TO STREAMS
    protected async Task TestInboxConcurrencyAsync(IServiceCollection services, string queueUriFormat, int msToComplete, bool isTransactional)
    {
        const int threadCount = 3;

        var padlock = new object();

        ConfigureServices(services, nameof(TestInboxConcurrencyAsync), true, threadCount, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

        services.AddSingleton<InboxConcurrencyFeature>();

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();
        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var feature = serviceProvider.GetRequiredService<InboxConcurrencyFeature>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var queueService = serviceProvider.CreateQueueService();
        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

        logger.LogInformation($"[TestInboxConcurrency] : thread count = '{threadCount}'");

        try
        {
            await ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true).ConfigureAwait(false);

            Assert.That(serviceBusConfiguration.Inbox!.WorkQueue!.IsStream, Is.False, "This test cannot be applied to streams.");

            logger.LogInformation($"[TestInboxConcurrency] : enqueuing '{threadCount}' messages");

            for (var i = 0; i < threadCount; i++)
            {
                await transportMessagePipeline.ExecuteAsync(new ConcurrentCommand { MessageIndex = i }, null, builder =>
                {
                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                }).ConfigureAwait(false);

                var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

                await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);
            }

            var idlePipelines = new List<Guid>();

            threadActivity.ThreadWorking += (_, args) =>
            {
                logger.LogInformation($"[TestInboxConcurrency] : pipeline = '{args.Pipeline.GetType().FullName}' / pipeline id '{args.Pipeline.Id}' is performing work");
            };

            threadActivity.ThreadWaiting += (_, args) =>
            {
                lock (padlock)
                {
                    if (idlePipelines.Contains(args.Pipeline.Id))
                    {
                        return;
                    }

                    logger.LogInformation($"[TestInboxConcurrency] : pipeline = '{args.Pipeline.GetType().FullName}' / pipeline id '{args.Pipeline.Id}' is idle");

                    idlePipelines.Add(args.Pipeline.Id);
                }
            };

            logger.LogInformation("[TestInboxConcurrency] : starting service bus");

            await serviceBus.StartAsync().ConfigureAwait(false);

            var timeout = DateTimeOffset.Now.AddSeconds(5);
            var timedOut = false;

            logger.LogInformation($"[TestInboxConcurrency] : waiting till {timeout:O} for all pipelines to become idle");

            while (idlePipelines.Count < threadCount && !timedOut)
            {
                await Task.Delay(30).ConfigureAwait(false);
                timedOut = DateTimeOffset.Now >= timeout;
            }

            Assert.That(timedOut, Is.False, $"[TIMEOUT] : All pipelines did not become idle before {timeout:O} / idle threads = {idlePipelines.Count}");
        }
        finally
        {
            await serviceBus.DisposeAsync().ConfigureAwait(false);
            await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
            await queueService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }

        Assert.That(feature.OnAfterGetMessageCount, Is.EqualTo(threadCount), $"Got {feature.OnAfterGetMessageCount} messages but {threadCount} were sent.");
        Assert.That(feature.AllMessagesReceivedWithinTimespan(msToComplete), Is.True, $"All dequeued messages have to be within {msToComplete} ms of first get message.");
    }

    protected async Task TestInboxDeferredAsync(IServiceCollection services, string queueUriFormat, TimeSpan deferDuration = default)
    {
        var serviceBusOptions = new ServiceBusOptions
        {
            Inbox = new()
            {
                WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                DurationToSleepWhenIdle = new() { TimeSpan.FromMilliseconds(5) },
                DurationToIgnoreOnFailure = new() { TimeSpan.FromMilliseconds(5) },
                ThreadCount = 1
            }
        };

        services.AddSingleton<InboxDeferredFeature>();

        services.AddServiceBus(builder =>
        {
            builder.Options = serviceBusOptions;
            builder.SuppressHostedService();
        });

        services.ConfigureLogging(nameof(TestInboxDeferredAsync));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var logger = serviceProvider.GetLogger<InboxFixture>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

        var messageType = Guard.AgainstNullOrEmptyString(typeof(ReceivePipelineCommand).FullName);

        var queueService = serviceProvider.CreateQueueService();

        await ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true).ConfigureAwait(false);

        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

        try
        {
            var feature = serviceProvider.GetRequiredService<InboxDeferredFeature>();
            var deferDurationValue = deferDuration == default ? TimeSpan.FromMilliseconds(50) : deferDuration;

            await serviceBus.StartAsync().ConfigureAwait(false);

            var ignoreTillDate = DateTimeOffset.Now.Add(deferDurationValue);

            var transportMessage = await serviceBus.SendAsync(new ReceivePipelineCommand(),
                builder =>
                {
                    builder
                        .Defer(ignoreTillDate)
                        .WithRecipient(serviceBusConfiguration.Inbox!.WorkQueue!);
                }).ConfigureAwait(false);

            Assert.That(transportMessage, Is.Not.Null);

            logger.LogInformation($"[SENT (thread {Environment.CurrentManagedThreadId})] : message id = {transportMessage.MessageId} / deferred to = '{ignoreTillDate:O}'");

            var messageId = transportMessage.MessageId;

            var timeout = DateTimeOffset.Now.Add(deferDurationValue.Multiply(2));
            var timedOut = false;

            while (feature.TransportMessage == null && !timedOut)
            {
                await Task.Delay(5).ConfigureAwait(false);
                timedOut = DateTimeOffset.Now >= timeout;
            }

            Assert.That(timedOut, Is.False, "[TIMEOUT] : The deferred message was never received.");
            Assert.That(feature.TransportMessage, Is.Not.Null, "The InboxDeferredFeature.TransportMessage cannot be `null`.");
            Assert.That(feature.TransportMessage!.MessageId, Is.EqualTo(messageId), "The InboxDeferredFeature.TransportMessage.MessageId received is not the one sent.");
            Assert.That(feature.TransportMessage.MessageType, Is.EqualTo(messageType), "The InboxDeferredFeature.TransportMessage.MessageType is not the same as the one sent.");
        }
        finally
        {
            await serviceBus.TryDisposeAsync().ConfigureAwait(false);
            await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
            await queueService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestInboxErrorAsync(IServiceCollection services, string queueUriFormat, bool hasErrorQueue, bool isTransactional)
    {
        ConfigureServices(services, nameof(TestInboxErrorAsync), hasErrorQueue, 1, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var logger = serviceProvider.GetLogger<InboxFixture>();
        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var inboxMessagePipelineObserver = new InboxMessagePipelineObserver(logger);

        pipelineFactory.PipelineCreated += (_, args) =>
        {
            if (args.Pipeline.GetType() != typeof(InboxMessagePipeline))
            {
                return;
            }

            args.Pipeline.AddObserver(inboxMessagePipelineObserver);
        };

        var queueService = serviceProvider.CreateQueueService();
        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

        try
        {
            await ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, hasErrorQueue).ConfigureAwait(false);

            var workQueue = serviceBusConfiguration.Inbox!.WorkQueue!;

            await transportMessagePipeline.ExecuteAsync(new ErrorCommand(), null, builder =>
            {
                builder.WithRecipient(workQueue);
            }).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

            logger.LogInformation($"[enqueuing] : message id = '{transportMessage.MessageId}'");

            await workQueue.EnqueueAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);

            logger.LogInformation($"[enqueued] : message id = '{transportMessage.MessageId}'");

            await serviceBus.StartAsync().ConfigureAwait(false);

            var timeout = DateTimeOffset.Now.AddSeconds(150);
            var timedOut = false;

            while (!inboxMessagePipelineObserver.HasReceivedPipelineException &&
                   !timedOut)
            {
                await Task.Delay(25).ConfigureAwait(false);

                timedOut = DateTimeOffset.Now > timeout;
            }

            Assert.That(!timedOut, "Timed out before message was received.");

            await serviceBus.StopAsync().ConfigureAwait(false);

            if (hasErrorQueue)
            {
                Assert.That(await queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessageAsync().ConfigureAwait(false), Is.Null, "Should not have a message in queue 'test-inbox-work'.");
                Assert.That(await queueService.Get(string.Format(queueUriFormat, "test-error")).GetMessageAsync().ConfigureAwait(false), Is.Not.Null, "Should have a message in queue 'test-error'.");
            }
            else
            {
                Assert.That(await queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessageAsync().ConfigureAwait(false), Is.Not.Null, "Should have a message in queue 'test-inbox-work'.");
            }
        }
        finally
        {
            await serviceBus.DisposeAsync().ConfigureAwait(false);
            await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
            await queueService.DisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestInboxExpiryAsync(IServiceCollection services, string queueUriFormat, TimeSpan? expiryDuration = null)
    {
        expiryDuration ??= TimeSpan.FromMilliseconds(500);

        services
            .AddServiceBus(builder =>
            {
                builder.SuppressHostedService();
            })
            .ConfigureLogging(nameof(TestInboxExpiryAsync));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();

        var queueService = serviceProvider.CreateQueueService();

        try
        {
            var queue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));

            await queue.TryDropAsync().ConfigureAwait(false);
            await queue.TryCreateAsync().ConfigureAwait(false);
            await queue.TryPurgeAsync().ConfigureAwait(false);

            void Builder(TransportMessageBuilder builder)
            {
                builder.WillExpire(expiryDuration.Value);
                builder.WithRecipient(queue);
            }

            await transportMessagePipeline.ExecuteAsync(new ReceivePipelineCommand(), null, Builder).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

            await queue.EnqueueAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);

            Assert.That(transportMessage, Is.Not.Null, "TransportMessage may not be null.");
            Assert.That(transportMessage.HasExpired(), Is.False, "The message has already expired before being processed.");

            // wait until the message expires
            await Task.Delay(expiryDuration.Value.Add(TimeSpan.FromMilliseconds(50))).ConfigureAwait(false);

            Assert.That(await queue.GetMessageAsync().ConfigureAwait(false), Is.Null, "The message did not expire.  Call this test only if your queue actually supports message expiry internally.");

            await queue.TryDropAsync().ConfigureAwait(false);
        }
        finally
        {
            await queueService.DisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestInboxThroughputAsync(IServiceCollection services, string queueUriFormat, int timeoutMilliseconds, int messageCount, int threadCount, bool isTransactional)
    {
        if (threadCount < 1)
        {
            threadCount = 1;
        }

        ConfigureServices(Guard.AgainstNull(services), nameof(TestInboxThroughputAsync), true, threadCount, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

        services.AddSingleton<ProcessorThreadObserver, ProcessorThreadObserver>();

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();

        var throughputObserver = new ThroughputObserver();

        pipelineFactory.PipelineCreated += delegate(object? _, PipelineEventArgs args)
        {
            if (args.Pipeline.GetType() == typeof(InboxMessagePipeline))
            {
                args.Pipeline.AddObserver(throughputObserver);
            }

            if (args.Pipeline.GetType() == typeof(StartupPipeline))
            {
                args.Pipeline.AddObserver(serviceProvider.GetService<ProcessorThreadObserver>()!);
            }
        };

        var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var queueService = serviceProvider.CreateQueueService();
        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

        var sw = new Stopwatch();
        var timedOut = false;

        try
        {
            await ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true).ConfigureAwait(false);

            logger.LogInformation($"Sending {messageCount} messages to input queue '{serviceBusConfiguration.Inbox!.WorkQueue!.Uri}'.");

            sw.Start();

            for (var i = 0; i < messageCount; i++)
            {
                await transportMessagePipeline.ExecuteAsync(new SimpleCommand("command " + i) { Context = "TestInboxThroughput" }, null, builder =>
                {
                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                }).ConfigureAwait(false);

                var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

                await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);
            }

            sw.Stop();

            logger.LogInformation("Took {0} ms to send {1} messages.  Starting processing.", sw.ElapsedMilliseconds, messageCount);

            sw.Reset();

            await serviceBus.StartAsync().ConfigureAwait(false);

            logger.LogInformation($"[starting] : {DateTimeOffset.Now:HH:mm:ss.fff}");

            var timeout = DateTimeOffset.Now.AddSeconds(5);

            sw.Start();

            while (throughputObserver.HandledMessageCount < messageCount && !timedOut)
            {
                await Task.Delay(25).ConfigureAwait(false);

                timedOut = DateTimeOffset.Now > timeout;
            }

            sw.Stop();

            logger.LogInformation($"[stopped] : {DateTimeOffset.Now:HH:mm:ss.fff}");

            await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
        }
        finally
        {
            await serviceBus.DisposeAsync().ConfigureAwait(false);

            await queueService.Get(serviceBusConfiguration.Inbox!.WorkQueue!.Uri.ToString()).TryDisposeAsync().ConfigureAwait(false);
            await queueService.Get(serviceBusConfiguration.Inbox.ErrorQueue!.Uri.ToString()).TryDisposeAsync().ConfigureAwait(false);
            await queueService.TryDisposeAsync().ConfigureAwait(false);

            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }

        var ms = sw.ElapsedMilliseconds;

        if (!timedOut)
        {
            logger.LogInformation($"Processed {messageCount} messages in {ms} ms");

            Assert.That(ms < timeoutMilliseconds, Is.True, $"Should be able to process at least {messageCount} messages in {timeoutMilliseconds} ms but it took {ms} ms.");
        }
        else
        {
            Assert.Fail($"Timed out before processing {messageCount} messages.  Only processed {throughputObserver.HandledMessageCount} messages in {ms}.");
        }
    }
}