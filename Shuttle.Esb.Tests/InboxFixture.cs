using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Internal;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;
using Shuttle.Core.Threading;
using Shuttle.Core.Transactions;

namespace Shuttle.Esb.Tests
{
    public class ThroughputObserver : IPipelineObserver<OnAfterAcknowledgeMessage>
    {
        private readonly object _lock = new object();

        public int HandledMessageCount { get; private set; }

        public async Task Execute(OnAfterAcknowledgeMessage pipelineEvent)
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
            _logger = Guard.AgainstNull(logger, nameof(logger));
        }

        public async Task Execute(OnStarted pipelineEvent)
        {
            var executedThreads = new List<int>();

            foreach (var processorThread in pipelineEvent.Pipeline.State.Get<IProcessorThreadPool>("InboxThreadPool")
                         .ProcessorThreads)
            {
                processorThread.ProcessorExecuting += (sender, args) =>
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
            _logger = Guard.AgainstNull(logger, nameof(logger));
        }

        public bool HasReceivedPipelineException { get; private set; }

        public async Task Execute(OnPipelineException pipelineEvent)
        {
            HasReceivedPipelineException = true;

            _logger.LogInformation($"[OnPipelineException] : {nameof(HasReceivedPipelineException)} = 'true'");

            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    public abstract class InboxFixture : IntegrationFixture
    {
        protected ServiceBusOptions ConfigureServices(IServiceCollection services, string test, bool hasErrorQueue, int threadCount,
            bool isTransactional, string queueUriFormat, TimeSpan durationToSleepWhenIdle)
        {
            Guard.AgainstNull(services, nameof(services));

            services.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            var serviceBusOptions = new ServiceBusOptions
            {
                Inbox = new InboxOptions
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    ErrorQueueUri = hasErrorQueue ? string.Format(queueUriFormat, "test-error") : string.Empty,
                    DurationToSleepWhenIdle = new List<TimeSpan> { durationToSleepWhenIdle },
                    DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    ThreadCount = threadCount,
                    MaximumFailureCount = 0
                }
            };

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
                builder.SuppressHostedService = true;
            });

            services.ConfigureLogging(test);

            return serviceBusOptions;
        }

        private async Task ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat, bool hasErrorQueue)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = hasErrorQueue ? queueService.Get(string.Format(queueUriFormat, "test-error")) : null;

            await inboxWorkQueue.TryDrop().ConfigureAwait(false);
            await errorQueue.TryDrop().ConfigureAwait(false);

            await serviceBusConfiguration.CreatePhysicalQueues().ConfigureAwait(false);

            await inboxWorkQueue.TryPurge().ConfigureAwait(false);
            await errorQueue.TryPurge().ConfigureAwait(false);
        }

        // NOT APPLICABLE TO STREAMS
        protected async Task TestInboxConcurrency(IServiceCollection services, string queueUriFormat, int msToComplete,
            bool isTransactional)
        {
            const int threadCount = 3;

            var padlock = new object();

            ConfigureServices(services, nameof(TestInboxConcurrency), true, threadCount, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

            services.AddSingleton<InboxConcurrencyFeature>();

            var serviceProvider = await services.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);

            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();
            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();
            var feature = serviceProvider.GetRequiredService<InboxConcurrencyFeature>();

            var queueService = serviceProvider.CreateQueueService();

            try
            {
                await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, true).ConfigureAwait(false);

                Assert.That(serviceBusConfiguration.Inbox.WorkQueue.IsStream, Is.False, "This test cannot be applied to streams.");

                var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

                await using (serviceBus.ConfigureAwait(false))
                {
                    for (var i = 0; i < threadCount; i++)
                    {
                        await transportMessagePipeline.Execute(new ConcurrentCommand { MessageIndex = i }, null, builder =>
                        {
                            builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        }).ConfigureAwait(false);

                        await serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                            transportMessagePipeline.State.GetTransportMessage(),
                            await serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                    }

                    var idleThreads = new List<int>();

                    threadActivity.ThreadWaiting += (sender, args) =>
                    {
                        lock (padlock)
                        {
                            if (idleThreads.Contains(Thread.CurrentThread.ManagedThreadId))
                            {
                                return;
                            }

                            idleThreads.Add(Thread.CurrentThread.ManagedThreadId);
                        }
                    };

                    await serviceBus.Start().ConfigureAwait(false);

                    while (idleThreads.Count < threadCount)
                    {
                        await Task.Delay(30).ConfigureAwait(false);
                    }
                }

                await queueService.TryDropQueues(queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                queueService.TryDispose();

                await serviceProvider.StopHostedServices().ConfigureAwait(false);
            }

            Assert.AreEqual(threadCount, feature.OnAfterGetMessageCount,
                $"Got {feature.OnAfterGetMessageCount} messages but {threadCount} were sent.");

            Assert.IsTrue(feature.AllMessagesReceivedWithinTimespan(msToComplete),
                "All dequeued messages have to be within {0} ms of first get message.", msToComplete);
        }

        protected async Task TestInboxDeferred(IServiceCollection services, string queueUriFormat, TimeSpan deferDuration = default)
        {
            var serviceBusOptions = new ServiceBusOptions
            {
                Inbox = new InboxOptions
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                    DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(5) },
                    DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(5) },
                    ThreadCount = 1
                }
            };

            services.AddSingleton<InboxDeferredFeature>();

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
                builder.SuppressHostedService = true;
            });

            services.ConfigureLogging(nameof(TestInboxDeferred));

            var serviceProvider = await services.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);

            var logger = serviceProvider.GetLogger<InboxFixture>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

            var messageType = typeof(ReceivePipelineCommand).FullName ??
                              throw new InvalidOperationException(
                                  "Could not get the full type name of the ReceivePipelineCommand");

            var queueService = serviceProvider.CreateQueueService();

            await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, true).ConfigureAwait(false);

            try
            {
                var feature = serviceProvider.GetRequiredService<InboxDeferredFeature>();
                var deferDurationValue = deferDuration == default ? TimeSpan.FromMilliseconds(50) : deferDuration;

                await using (var serviceBus = await serviceProvider.GetRequiredService<IServiceBus>().Start().ConfigureAwait(false))
                {
                    var ignoreTillDate = DateTime.Now.Add(deferDurationValue);

                    var transportMessage = await serviceBus.Send(new ReceivePipelineCommand(),
                        builder =>
                        {
                            builder
                                .Defer(ignoreTillDate)
                                .WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        }).ConfigureAwait(false);

                    Assert.IsNotNull(transportMessage);
                    logger.LogInformation($"[SENT (thread {Thread.CurrentThread.ManagedThreadId})] : message id = {transportMessage.MessageId} / deferred to = '{ignoreTillDate:O}'");

                    var messageId = transportMessage.MessageId;

                    var timeout = DateTime.Now.Add(deferDurationValue.Multiply(2));
                    var timedOut = false;

                    while (feature.TransportMessage == null && !timedOut)
                    {
                        await Task.Delay(5).ConfigureAwait(false);
                        timedOut = DateTime.Now >= timeout;
                    }

                    Assert.That(timedOut, Is.False, "[TIMEOUT] : The deferred message was never received.");
                    Assert.IsNotNull(feature.TransportMessage, "The InboxDeferredFeature.TransportMessage cannot be `null`.");
                    Assert.That(feature.TransportMessage.MessageId, Is.EqualTo(messageId), "The InboxDeferredFeature.TransportMessage.MessageId received is not the one sent.");
                    Assert.That(feature.TransportMessage.MessageType, Is.EqualTo(messageType), "The InboxDeferredFeature.TransportMessage.MessageType is not the same as the one sent.");
                }

                await queueService.TryDropQueues(queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                queueService.TryDispose();

                await serviceProvider.StopHostedServices().ConfigureAwait(false);
            }
        }

        protected async Task TestInboxError(IServiceCollection services, string queueUriFormat, bool hasErrorQueue,
            bool isTransactional)
        {
            ConfigureServices(services, nameof(TestInboxError), hasErrorQueue, 1, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

            var serviceProvider = await services.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);

            var logger = serviceProvider.GetLogger<InboxFixture>();
            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();
            var inboxMessagePipelineObserver = new InboxMessagePipelineObserver(logger);

            pipelineFactory.PipelineCreated += (sender, args) =>
            {
                if (args.Pipeline.GetType() != typeof(InboxMessagePipeline))
                {
                    return;
                }

                args.Pipeline.RegisterObserver(inboxMessagePipelineObserver);
            };

            var queueService = serviceProvider.CreateQueueService();

            try
            {
                await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, hasErrorQueue).ConfigureAwait(false);

                var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

                await using (serviceBus.ConfigureAwait(false))
                {
                    await transportMessagePipeline.Execute(new ErrorCommand(), null, builder =>
                    {
                        builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    }).ConfigureAwait(false);

                    var transportMessage = transportMessagePipeline.State.GetTransportMessage();

                    logger.LogInformation($"[enqueuing] : message id = '{transportMessage.MessageId}'");

                    await serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                        transportMessage,
                        await serializer.Serialize(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);

                    logger.LogInformation($"[enqueued] : message id = '{transportMessage.MessageId}'");

                    await serviceBus.Start().ConfigureAwait(false);

                    var timeout = DateTime.Now.AddSeconds(15);
                    var timedOut = false;

                    while (!inboxMessagePipelineObserver.HasReceivedPipelineException &&
                           !timedOut)
                    {
                        await Task.Delay(25).ConfigureAwait(false);

                        timedOut = DateTime.Now > timeout;
                    }

                    try
                    {
                        Assert.That(!timedOut, "Timed out before message was received.");
                    }
                    finally
                    {
                        await serviceBus.Stop().ConfigureAwait(false);
                    }

                    if (hasErrorQueue)
                    {
                        Assert.Null(await queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessage().ConfigureAwait(false));
                        Assert.NotNull(await queueService.Get(string.Format(queueUriFormat, "test-error")).GetMessage().ConfigureAwait(false));
                    }
                    else
                    {
                        Assert.NotNull(await queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessage().ConfigureAwait(false));
                    }
                }

                await queueService.TryDropQueues(queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                await serviceProvider.StopHostedServices().ConfigureAwait(false);

                queueService.TryDispose();
            }
        }

        protected async Task TestInboxExpiry(IServiceCollection services, string queueUriFormat)
        {
            await TestInboxExpiry(services, queueUriFormat, TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        protected async Task TestInboxExpiry(IServiceCollection services, string queueUriFormat, TimeSpan expiryDuration)
        {
            services.AddServiceBus(builder =>
            {
                builder.SuppressHostedService = true;
            });

            var serviceProvider = await services.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);
            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            var queueService = serviceProvider.CreateQueueService();

            try
            {
                var queue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));

                await queue.TryDrop().ConfigureAwait(false);
                await queue.TryCreate().ConfigureAwait(false);

                await transportMessagePipeline.Execute(new ReceivePipelineCommand(), null, builder =>
                {
                    builder.WillExpire(DateTime.Now.AddMilliseconds(expiryDuration.TotalMilliseconds));
                    builder.WithRecipient(queue);
                }).ConfigureAwait(false);

                var transportMessage = transportMessagePipeline.State.GetTransportMessage();

                await queue.Enqueue(transportMessage,
                    await serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);

                Assert.IsNotNull(transportMessage, "TransportMessage is null.");
                Assert.IsFalse(transportMessage.HasExpired(),
                    "The message has already expired before being processed.");

                // wait until the message expires
                await Task.Delay(500).ConfigureAwait(false);

                Assert.IsNull(await queue.GetMessage().ConfigureAwait(false),
                    "The message did not expire.  Call this test only if your queue actually supports message expiry internally.");

                await queue.TryDrop().ConfigureAwait(false);
            }
            finally
            {
                queueService.TryDispose();

                await serviceProvider.StopHostedServices().ConfigureAwait(false);
            }
        }

        protected async Task TestInboxThroughput(IServiceCollection services, string queueUriFormat, int timeoutMilliseconds,
            int messageCount, int threadCount, bool isTransactional)
        {
            Guard.AgainstNull(services, nameof(services));

            if (threadCount < 1)
            {
                threadCount = 1;
            }

            ConfigureServices(services, nameof(TestInboxThroughput), true, threadCount, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

            services.AddSingleton<ProcessorThreadObserver, ProcessorThreadObserver>();

            var serviceProvider = await services.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();

            var throughputObserver = new ThroughputObserver();

            pipelineFactory.PipelineCreated += delegate(object sender, PipelineEventArgs args)
            {
                if (args.Pipeline.GetType() == typeof(InboxMessagePipeline))
                {
                    args.Pipeline.RegisterObserver(throughputObserver);
                }

                if (args.Pipeline.GetType() == typeof(StartupPipeline))
                {
                    args.Pipeline.RegisterObserver(serviceProvider.GetService<ProcessorThreadObserver>());
                }
            };

            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();
            var logger = serviceProvider.GetLogger<InboxFixture>();
            var queueService = serviceProvider.CreateQueueService();

            var sw = new Stopwatch();
            var timedOut = false;

            try
            {
                await ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, true).ConfigureAwait(false);

                logger.LogInformation(
                    $"Sending {messageCount} messages to input queue '{serviceBusConfiguration.Inbox.WorkQueue.Uri}'.");

                sw.Start();

                for (var i = 0; i < messageCount; i++)
                {
                    await transportMessagePipeline.Execute(new SimpleCommand("command " + i) { Context = "TestInboxThroughput" }, null, builder =>
                    {
                        builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    }).ConfigureAwait(false);

                    await serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                        transportMessagePipeline.State.GetTransportMessage(),
                        await serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                }

                sw.Stop();

                logger.LogInformation("Took {0} ms to send {1} messages.  Starting processing.", sw.ElapsedMilliseconds, messageCount);

                sw.Reset();

                await using (await serviceProvider.GetRequiredService<IServiceBus>().Start().ConfigureAwait(false))
                {
                    logger.LogInformation($"[starting] : {DateTime.Now:HH:mm:ss.fff}");

                    var timeout = DateTime.Now.AddSeconds(500);

                    sw.Start();

                    while (throughputObserver.HandledMessageCount < messageCount && !timedOut)
                    {
                        await Task.Delay(25).ConfigureAwait(false);

                        timedOut = DateTime.Now > timeout;
                    }

                    sw.Stop();

                    logger.LogInformation($"[stopped] : {DateTime.Now:HH:mm:ss.fff}");
                }

                await queueService.TryDropQueues(queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                queueService.Get(serviceBusConfiguration.Inbox.WorkQueue.Uri.ToString()).TryDispose();
                queueService.Get(serviceBusConfiguration.Inbox.ErrorQueue.Uri.ToString()).TryDispose();
                queueService.TryDispose();

                await serviceProvider.StopHostedServices().ConfigureAwait(false);
            }

            var ms = sw.ElapsedMilliseconds;

            if (!timedOut)
            {
                logger.LogInformation($"Processed {messageCount} messages in {ms} ms");

                Assert.IsTrue(ms < timeoutMilliseconds,
                    $"Should be able to process at least {messageCount} messages in {timeoutMilliseconds} ms but it took {ms} ms.");
            }
            else
            {
                Assert.Fail($"Timed out before processing {messageCount} messages.  Only processed {throughputObserver.HandledMessageCount} messages in {ms}.");
            }
        }
    }
}