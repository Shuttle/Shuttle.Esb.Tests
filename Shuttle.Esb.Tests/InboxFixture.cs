using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
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

namespace Shuttle.Esb.Tests
{
    public class ThroughputObserver : IPipelineObserver<OnAfterAcknowledgeMessage>
    {
        private readonly object _lock = new object();

        public int HandledMessageCount { get; private set; }

        public void Execute(OnAfterAcknowledgeMessage pipelineEvent)
        {
            lock (_lock)
            {
                HandledMessageCount++;
            }
        }

        public async Task ExecuteAsync(OnAfterAcknowledgeMessage pipelineEvent)
        {
            Execute(pipelineEvent);

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

        public void Execute(OnStarted pipelineEvent)
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
        }

        public async Task ExecuteAsync(OnStarted pipelineEvent)
        {
            Execute(pipelineEvent);

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

        public void Execute(OnPipelineException pipelineEvent)
        {
            HasReceivedPipelineException = true;

            _logger.LogInformation($"[OnPipelineException] : {nameof(HasReceivedPipelineException)} = 'true'");
        }

        public async Task ExecuteAsync(OnPipelineException pipelineEvent)
        {
            Execute(pipelineEvent);

            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    public abstract class InboxFixture : IntegrationFixture
    {
        private async Task ConfigureQueuesAsync(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration, string queueUriFormat, bool hasErrorQueue, bool sync)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = hasErrorQueue ? queueService.Get(string.Format(queueUriFormat, "test-error")) : null;

            if (sync)
            {
                errorQueue.TryDrop();
                inboxWorkQueue.TryDrop();
                serviceBusConfiguration.CreatePhysicalQueues();
                inboxWorkQueue.TryPurge();
                errorQueue.TryPurge();
            }
            else
            {
                await inboxWorkQueue.TryDropAsync().ConfigureAwait(false);
                await errorQueue.TryDropAsync().ConfigureAwait(false);
                await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);
                await inboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
                await errorQueue.TryPurgeAsync().ConfigureAwait(false);
            }
        }

        private ServiceBusOptions ConfigureServices(IServiceCollection services, string test, bool hasErrorQueue, int threadCount, bool isTransactional, string queueUriFormat, TimeSpan durationToSleepWhenIdle, bool sync)
        {
            Guard.AgainstNull(services, nameof(services));

            services.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            var serviceBusOptions = new ServiceBusOptions
            {
                Asynchronous = !sync,
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

        // NOT APPLICABLE TO STREAMS
        protected void TestInboxConcurrency(IServiceCollection services, string queueUriFormat, int msToComplete, bool isTransactional)
        {
            TestInboxConcurrencyAsync(services, queueUriFormat, msToComplete, isTransactional, true).GetAwaiter().GetResult();
        }

        protected async Task TestInboxConcurrencyAsync(IServiceCollection services, string queueUriFormat, int msToComplete, bool isTransactional)
        {
            await TestInboxConcurrencyAsync(services, queueUriFormat, msToComplete, isTransactional, false).ConfigureAwait(false);
        }

        private async Task TestInboxConcurrencyAsync(IServiceCollection services, string queueUriFormat, int msToComplete, bool isTransactional, bool sync)
        {
            const int threadCount = 3;

            var padlock = new object();

            ConfigureServices(services, nameof(TestInboxConcurrencyAsync), true, threadCount, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25), sync);

            services.AddSingleton<InboxConcurrencyFeature>();

            var serviceProvider = sync
                ? services.BuildServiceProvider().StartHostedServices()
                : await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

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
                if (sync)
                {
                    ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true, sync).GetAwaiter().GetResult();
                }
                else
                {
                    await ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true, sync).ConfigureAwait(false);
                }

                Assert.That(serviceBusConfiguration.Inbox.WorkQueue.IsStream, Is.False, "This test cannot be applied to streams.");

                logger.LogInformation($"[TestInboxConcurrency] : enqueuing '{threadCount}' messages");

                for (var i = 0; i < threadCount; i++)
                {
                    if (sync)
                    {
                        transportMessagePipeline.Execute(new ConcurrentCommand { MessageIndex = i }, null, builder =>
                        {
                            builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        });

                        serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                            transportMessagePipeline.State.GetTransportMessage(),
                            serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
                    }
                    else
                    {
                        await transportMessagePipeline.ExecuteAsync(new ConcurrentCommand { MessageIndex = i }, null, builder =>
                        {
                            builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        }).ConfigureAwait(false);

                        await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(
                            transportMessagePipeline.State.GetTransportMessage(),
                            await serializer.SerializeAsync(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                    }
                }

                var idlePipelines = new List<Guid>();

                threadActivity.ThreadWorking += (sender, args) =>
                {
                    logger.LogInformation($"[TestInboxConcurrency] : pipeline = '{args.Pipeline.GetType().FullName}' / pipeline id '{args.Pipeline.Id}' is performing work");
                };

                threadActivity.ThreadWaiting += (sender, args) =>
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

                logger.LogInformation($"[TestInboxConcurrency] : starting service bus");

                if (sync)
                {
                    serviceBus.Start();
                }
                else
                {
                    await serviceBus.StartAsync().ConfigureAwait(false);
                }

                var timeout = DateTime.Now.AddSeconds(5);
                var timedOut = false;

                logger.LogInformation($"[TestInboxConcurrency] : waiting till {timeout:O} for all pipelines to become idle");

                while (idlePipelines.Count < threadCount && !timedOut)
                {
                    await Task.Delay(30).ConfigureAwait(false);
                    timedOut = DateTime.Now >= timeout;
                }

                Assert.That(timedOut, Is.False, $"[TIMEOUT] : All pipelines did not become idle before {timeout:O} / idle threads = {idlePipelines.Count}");
            }
            finally
            {
                if (sync)
                {
                    serviceBus.Dispose();
                    queueService.TryDropQueues(queueUriFormat);
                    queueService.TryDispose();
                    serviceProvider.StopHostedServices();
                }
                else
                {
                    await serviceBus.DisposeAsync().ConfigureAwait(false);
                    await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
                    await queueService.TryDisposeAsync().ConfigureAwait(false);
                    await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
                }
            }

            Assert.AreEqual(threadCount, feature.OnAfterGetMessageCount,
                $"Got {feature.OnAfterGetMessageCount} messages but {threadCount} were sent.");

            Assert.IsTrue(feature.AllMessagesReceivedWithinTimespan(msToComplete),
                "All dequeued messages have to be within {0} ms of first get message.", msToComplete);
        }

        protected void TestInboxDeferred(IServiceCollection services, string queueUriFormat, TimeSpan deferDuration = default)
        {
            TestInboxDeferredAsync(services, queueUriFormat, deferDuration, false).GetAwaiter().GetResult();
        }

        protected async Task TestInboxDeferredAsync(IServiceCollection services, string queueUriFormat, TimeSpan deferDuration = default)
        {
            await TestInboxDeferredAsync(services, queueUriFormat, deferDuration, false).ConfigureAwait(false);
        }

        protected async Task TestInboxDeferredAsync(IServiceCollection services, string queueUriFormat, TimeSpan deferDuration, bool sync)
        {
            var serviceBusOptions = new ServiceBusOptions
            {
                Asynchronous = !sync,
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

            services.ConfigureLogging(nameof(TestInboxDeferredAsync));

            var serviceProvider = sync
                ? services.BuildServiceProvider().StartHostedServices()
                : await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

            var logger = serviceProvider.GetLogger<InboxFixture>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

            var messageType = typeof(ReceivePipelineCommand).FullName ??
                              throw new InvalidOperationException(
                                  "Could not get the full type name of the ReceivePipelineCommand");

            var queueService = serviceProvider.CreateQueueService();

            if (sync)
            {
                ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true, true).GetAwaiter().GetResult();
            }
            else
            {
                await ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true, false).ConfigureAwait(false);
            }

            var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

            try
            {
                var feature = serviceProvider.GetRequiredService<InboxDeferredFeature>();
                var deferDurationValue = deferDuration == default ? TimeSpan.FromMilliseconds(50) : deferDuration;

                if (sync)
                {
                    serviceBus.Start();
                }
                else
                {
                    await serviceBus.StartAsync().ConfigureAwait(false);
                }

                var ignoreTillDate = DateTime.Now.Add(deferDurationValue);

                var transportMessage = sync
                    ? serviceBus.Send(new ReceivePipelineCommand(),
                        builder =>
                        {
                            builder
                                .Defer(ignoreTillDate)
                                .WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        })
                    : await serviceBus.SendAsync(new ReceivePipelineCommand(),
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
            finally
            {
                if (sync)
                {
                    serviceBus.TryDispose();
                    queueService.TryDropQueues(queueUriFormat);
                    queueService.TryDispose();
                    serviceProvider.StopHostedServices();
                }
                else
                {
                    await serviceBus.TryDisposeAsync().ConfigureAwait(false);
                    await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
                    await queueService.TryDisposeAsync().ConfigureAwait(false);
                    await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
                }
            }
        }

        protected void TestInboxError(IServiceCollection services, string queueUriFormat, bool hasErrorQueue, bool isTransactional)
        {
            TestInboxErrorAsync(services, queueUriFormat, hasErrorQueue, isTransactional, true).GetAwaiter().GetResult();
        }

        protected async Task TestInboxErrorAsync(IServiceCollection services, string queueUriFormat, bool hasErrorQueue, bool isTransactional)
        {
            await TestInboxErrorAsync(services, queueUriFormat, hasErrorQueue, isTransactional, false).ConfigureAwait(false);
        }

        private async Task TestInboxErrorAsync(IServiceCollection services, string queueUriFormat, bool hasErrorQueue, bool isTransactional, bool sync)
        {
            ConfigureServices(services, nameof(TestInboxErrorAsync), hasErrorQueue, 1, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25), sync);

            var serviceProvider = sync
                ? services.BuildServiceProvider().StartHostedServices()
                : await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

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
            var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

            try
            {
                if (sync)
                {
                    ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, hasErrorQueue, true).GetAwaiter().GetResult();
                }
                else
                {
                    await ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, hasErrorQueue, false).ConfigureAwait(false);
                }

                if (sync)
                {
                    transportMessagePipeline.Execute(new ErrorCommand(), null, builder =>
                    {
                        builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    });
                }
                else
                {
                    await transportMessagePipeline.ExecuteAsync(new ErrorCommand(), null, builder =>
                    {
                        builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    }).ConfigureAwait(false);
                }

                var transportMessage = transportMessagePipeline.State.GetTransportMessage();

                logger.LogInformation($"[enqueuing] : message id = '{transportMessage.MessageId}'");

                if (sync)
                {
                    serviceBusConfiguration.Inbox.WorkQueue.Enqueue(transportMessage, serializer.Serialize(transportMessage));
                }
                else
                {
                    await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);
                }

                logger.LogInformation($"[enqueued] : message id = '{transportMessage.MessageId}'");

                if (sync)
                {
                    serviceBus.Start();
                }
                else
                {
                    await serviceBus.StartAsync().ConfigureAwait(false);
                }

                var timeout = DateTime.Now.AddSeconds(150);
                var timedOut = false;

                while (!inboxMessagePipelineObserver.HasReceivedPipelineException &&
                       !timedOut)
                {
                    await Task.Delay(25).ConfigureAwait(false);

                    timedOut = DateTime.Now > timeout;
                }

                Assert.That(!timedOut, "Timed out before message was received.");

                if (sync)
                {
                    serviceBus.Stop();

                    if (hasErrorQueue)
                    {
                        Assert.Null(queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessage(), "Should not have a message in queue 'test-inbox-work'.");
                        Assert.NotNull(queueService.Get(string.Format(queueUriFormat, "test-error")).GetMessage(), "Should have a message in queue 'test-error'.");
                    }
                    else
                    {
                        Assert.NotNull(queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessage(), "Should have a message in queue 'test-inbox-work'.");
                    }
                }
                else
                {
                    await serviceBus.StopAsync().ConfigureAwait(false);

                    if (hasErrorQueue)
                    {
                        Assert.Null(await queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessageAsync().ConfigureAwait(false), "Should not have a message in queue 'test-inbox-work'.");
                        Assert.NotNull(await queueService.Get(string.Format(queueUriFormat, "test-error")).GetMessageAsync().ConfigureAwait(false), "Should have a message in queue 'test-error'.");
                    }
                    else
                    {
                        Assert.NotNull(await queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessageAsync().ConfigureAwait(false), "Should have a message in queue 'test-inbox-work'.");
                    }
                }
            }
            finally
            {
                if (sync)
                {
                    serviceBus.Dispose();
                    queueService.TryDropQueues(queueUriFormat);
                    queueService.Dispose();
                    serviceProvider.StopHostedServices();
                }
                else
                {
                    await serviceBus.DisposeAsync().ConfigureAwait(false);
                    await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
                    await queueService.DisposeAsync().ConfigureAwait(false);
                    await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
                }
            }
        }

        protected void TestInboxExpiry(IServiceCollection services, string queueUriFormat, TimeSpan? expiryDuration = null)
        {
            TestInboxExpiryAsync(services, queueUriFormat, expiryDuration, true).GetAwaiter().GetResult();
        }

        protected async Task TestInboxExpiryAsync(IServiceCollection services, string queueUriFormat, TimeSpan? expiryDuration = null)
        {
            await TestInboxExpiryAsync(services, queueUriFormat, expiryDuration, false).ConfigureAwait(false);
        }

        private async Task TestInboxExpiryAsync(IServiceCollection services, string queueUriFormat, TimeSpan? expiryDuration, bool sync)
        {
            if (expiryDuration == null)
            {
                expiryDuration = TimeSpan.FromMilliseconds(500);
            }

            services
                .AddServiceBus(builder =>
                {
                    builder.SuppressHostedService = true;
                })
                .ConfigureLogging(nameof(TestInboxExpiryAsync));

            var serviceProvider = sync
                ? services.BuildServiceProvider().StartHostedServices()
                : await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            var queueService = serviceProvider.CreateQueueService();

            try
            {
                var queue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));

                if (sync)
                {
                    queue.TryDrop();
                    queue.TryCreate();
                    queue.TryPurge();
                }
                else
                {
                    await queue.TryDropAsync().ConfigureAwait(false);
                    await queue.TryCreateAsync().ConfigureAwait(false);
                    await queue.TryPurgeAsync().ConfigureAwait(false);
                }

                void Builder(TransportMessageBuilder builder)
                {
                    builder.WillExpire(expiryDuration.Value);
                    builder.WithRecipient(queue);
                }

                if (sync)
                {
                    transportMessagePipeline.Execute(new ReceivePipelineCommand(), null, Builder);
                }
                else
                {
                    await transportMessagePipeline.ExecuteAsync(new ReceivePipelineCommand(), null, Builder).ConfigureAwait(false);
                }

                var transportMessage = transportMessagePipeline.State.GetTransportMessage();

                if (sync)
                {
                    queue.Enqueue(transportMessage, serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
                }
                else
                {
                    await queue.EnqueueAsync(transportMessage, await serializer.SerializeAsync(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                }

                Assert.IsNotNull(transportMessage, "TransportMessage may not be null.");
                Assert.IsFalse(transportMessage.HasExpired(), "The message has already expired before being processed.");

                // wait until the message expires
                await Task.Delay(expiryDuration.Value.Add(TimeSpan.FromMilliseconds(50))).ConfigureAwait(false);

                Assert.IsNull(sync ? queue.GetMessage() : await queue.GetMessageAsync().ConfigureAwait(false), "The message did not expire.  Call this test only if your queue actually supports message expiry internally.");

                if (sync)
                {
                    queue.TryDrop();
                }
                else
                {
                    await queue.TryDropAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                if (sync)
                {
                    queueService.Dispose();
                    serviceProvider.StopHostedServices();
                }
                else
                {
                    await queueService.DisposeAsync().ConfigureAwait(false);
                    await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
                }
            }
        }

        protected void TestInboxThroughput(IServiceCollection services, string queueUriFormat, int timeoutMilliseconds, int messageCount, int threadCount, bool isTransactional)
        {
            TestInboxThroughputAsync(services, queueUriFormat, timeoutMilliseconds, messageCount, threadCount, isTransactional, true).GetAwaiter().GetResult();
        }

        protected async Task TestInboxThroughputAsync(IServiceCollection services, string queueUriFormat, int timeoutMilliseconds, int messageCount, int threadCount, bool isTransactional)
        {
            await TestInboxThroughputAsync(services, queueUriFormat, timeoutMilliseconds, messageCount, threadCount, isTransactional, false).ConfigureAwait(false);
        }

        private async Task TestInboxThroughputAsync(IServiceCollection services, string queueUriFormat, int timeoutMilliseconds, int messageCount, int threadCount, bool isTransactional, bool sync)
        {
            Guard.AgainstNull(services, nameof(services));

            if (threadCount < 1)
            {
                threadCount = 1;
            }

            ConfigureServices(services, nameof(TestInboxThroughputAsync), true, threadCount, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25), sync);

            services.AddSingleton<ProcessorThreadObserver, ProcessorThreadObserver>();

            var serviceProvider = sync
                ? services.BuildServiceProvider().StartHostedServices()
                : await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

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
            var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();

            var sw = new Stopwatch();
            var timedOut = false;

            try
            {
                if (sync)
                {
                    ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true, true).GetAwaiter().GetResult();
                }
                else
                {
                    await ConfigureQueuesAsync(serviceProvider, serviceBusConfiguration, queueUriFormat, true, false).ConfigureAwait(false);
                }

                logger.LogInformation($"Sending {messageCount} messages to input queue '{serviceBusConfiguration.Inbox.WorkQueue.Uri}'.");

                sw.Start();

                for (var i = 0; i < messageCount; i++)
                {
                    if (sync)
                    {
                        transportMessagePipeline.Execute(new SimpleCommand("command " + i) { Context = "TestInboxThroughput" }, null, builder =>
                        {
                            builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        });

                        serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                            transportMessagePipeline.State.GetTransportMessage(),
                            serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
                    }
                    else
                    {
                        await transportMessagePipeline.ExecuteAsync(new SimpleCommand("command " + i) { Context = "TestInboxThroughput" }, null, builder =>
                        {
                            builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        }).ConfigureAwait(false);

                        await serviceBusConfiguration.Inbox.WorkQueue.EnqueueAsync(
                            transportMessagePipeline.State.GetTransportMessage(),
                            await serializer.SerializeAsync(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                    }
                }

                sw.Stop();

                logger.LogInformation("Took {0} ms to send {1} messages.  Starting processing.", sw.ElapsedMilliseconds, messageCount);

                sw.Reset();

                if (sync)
                {
                    serviceBus.Start();
                }
                else
                {
                    await serviceBus.StartAsync().ConfigureAwait(false);
                }

                logger.LogInformation($"[starting] : {DateTime.Now:HH:mm:ss.fff}");

                var timeout = DateTime.Now.AddSeconds(5);

                sw.Start();

                while (throughputObserver.HandledMessageCount < messageCount && !timedOut)
                {
                    await Task.Delay(25).ConfigureAwait(false);

                    timedOut = DateTime.Now > timeout;
                }

                sw.Stop();

                logger.LogInformation($"[stopped] : {DateTime.Now:HH:mm:ss.fff}");

                await queueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                if (sync)
                {
                    serviceBus.Dispose();

                    queueService.Get(serviceBusConfiguration.Inbox.WorkQueue.Uri.ToString()).TryDispose();
                    queueService.Get(serviceBusConfiguration.Inbox.ErrorQueue.Uri.ToString()).TryDispose();
                    queueService.TryDispose();

                    serviceProvider.StopHostedServices();
                }
                else
                {
                    await serviceBus.DisposeAsync().ConfigureAwait(false);

                    await queueService.Get(serviceBusConfiguration.Inbox.WorkQueue.Uri.ToString()).TryDisposeAsync();
                    await queueService.Get(serviceBusConfiguration.Inbox.ErrorQueue.Uri.ToString()).TryDisposeAsync();
                    await queueService.TryDisposeAsync();

                    await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
                }
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