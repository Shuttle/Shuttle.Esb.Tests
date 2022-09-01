using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
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

        public void Execute(OnAfterAcknowledgeMessage pipelineEvent)
        {
            lock (_lock)
            {
                HandledMessageCount++;
            }
        }
    }

    public class ProcessorThreadObserver : IPipelineObserver<OnStarted>
    {
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

                    Console.WriteLine($"[executing] : thread id = {args.ManagedThreadId} / name = '{args.Name}'");

                    executedThreads.Add(args.ManagedThreadId);
                };
            }
        }
    }

    public class InboxMessagePipelineObserver : IPipelineObserver<OnPipelineException>
    {
        public bool HasReceivedPipelineException { get; private set; }

        public void Execute(OnPipelineException pipelineEvent)
        {
            HasReceivedPipelineException = true;
        }
    }

    public abstract class InboxFixture : IntegrationFixture
    {
        protected void TestInboxThroughput(IServiceCollection services, string queueUriFormat, int timeoutMilliseconds,
            int messageCount, int threadCount, bool isTransactional)
        {
            Guard.AgainstNull(services, nameof(services));

            if (threadCount < 1)
            {
                threadCount = 1;
            }

            var serviceBusOptions = AddServiceBus(services, true, threadCount, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

            services.AddSingleton<ProcessorThreadObserver, ProcessorThreadObserver>();

            var serviceProvider = services.BuildServiceProvider();

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
            var queueService = CreateQueueService(serviceProvider);

            var sw = new Stopwatch();
            var timedOut = false;

            try
            {
                serviceBusConfiguration.Configure(serviceBusOptions);

                ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, true);

                Console.WriteLine(
                    $"Sending {messageCount} messages to input queue '{serviceBusConfiguration.Inbox.WorkQueue.Uri}'.");

                sw.Start();

                for (var i = 0; i < messageCount; i++)
                {
                    transportMessagePipeline.Execute(new SimpleCommand("command " + i), null, builder =>
                    {
                        builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    });

                    serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                        transportMessagePipeline.State.GetTransportMessage(),
                        serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
                }

                sw.Stop();
                
                Console.WriteLine("Took {0} ms to send {1} messages.  Starting processing.", sw.ElapsedMilliseconds, messageCount);

                sw.Reset();

                using (serviceProvider.GetRequiredService<IServiceBus>().Start())
                {
                    Console.WriteLine($"[starting] : {DateTime.Now:HH:mm:ss.fff}");

                    var timeout = DateTime.Now.AddSeconds(500);

                    sw.Start();

                    while (throughputObserver.HandledMessageCount < messageCount && !timedOut)
                    {
                        Thread.Sleep(25);

                        timedOut = DateTime.Now > timeout;
                    }

                    sw.Stop();

                    Console.WriteLine($"[stopped] : {DateTime.Now:HH:mm:ss.fff}");
                }

                AttemptDropQueues(queueService, queueUriFormat);
            }
            finally
            {
                queueService.Get(serviceBusConfiguration.Inbox.WorkQueue.Uri.ToString()).AttemptDispose();
                queueService.Get(serviceBusConfiguration.Inbox.ErrorQueue.Uri.ToString()).AttemptDispose();
                queueService.AttemptDispose();
            }

            var ms = sw.ElapsedMilliseconds;

            if (!timedOut)
            {
                Console.WriteLine($"Processed {messageCount} messages in {ms} ms");

                Assert.IsTrue(ms < timeoutMilliseconds,
                    $"Should be able to process at least {messageCount} messages in {timeoutMilliseconds} ms but it took {ms} ms.");
            }
            else
            {
                Assert.Fail($"Timed out before processing {messageCount} messages.  Only processed {throughputObserver.HandledMessageCount} messages in {ms}.");
            }
        }

        protected void TestInboxError(IServiceCollection services, string queueUriFormat, bool hasErrorQueue,
            bool isTransactional)
        {
            var serviceBusOptions = AddServiceBus(services, hasErrorQueue, 1, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

            serviceBusOptions.Inbox.MaximumFailureCount = 0;

            var serviceProvider = services.BuildServiceProvider();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();
            var inboxMessagePipelineObserver = new InboxMessagePipelineObserver();

            pipelineFactory.PipelineCreated += (sender, args) =>
            {
                if (args.Pipeline.GetType() != typeof(InboxMessagePipeline))
                {
                    return;
                }

                args.Pipeline.RegisterObserver(inboxMessagePipelineObserver);
            };

            var queueService = CreateQueueService(serviceProvider);

            try
            {
                serviceBusConfiguration.Configure(serviceBusOptions);

                ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, hasErrorQueue);

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    transportMessagePipeline.Execute(new ErrorCommand(), null, builder =>
                    {
                        builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    });

                    serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                        transportMessagePipeline.State.GetTransportMessage(),
                        serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));

                    bus.Start();

                    var timeout = DateTime.Now.AddSeconds(15);
                    var timedOut = false;

                    while (!inboxMessagePipelineObserver.HasReceivedPipelineException &&
                           !timedOut)
                    {
                        Thread.Sleep(25);

                        timedOut = DateTime.Now > timeout;
                    }

                    try
                    {
                        Assert.That(!timedOut, "Timed out before message was received.");
                    }
                    finally
                    {
                        bus.Stop();
                    }

                    if (hasErrorQueue)
                    {
                        Assert.Null(queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessage());
                        Assert.NotNull(queueService.Get(string.Format(queueUriFormat, "test-error")).GetMessage());
                    }
                    else
                    {
                        Assert.NotNull(queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessage());
                    }
                }

                AttemptDropQueues(queueService, queueUriFormat);
            }
            finally
            {
                queueService.AttemptDispose();
            }
        }

        private void ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration serviceBusConfiguration,
            string queueUriFormat, bool hasErrorQueue)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = hasErrorQueue ? queueService.Get(string.Format(queueUriFormat, "test-error")) : null;

            inboxWorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            serviceBusConfiguration.CreatePhysicalQueues();

            inboxWorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        protected void TestInboxConcurrency(IServiceCollection services, string queueUriFormat, int msToComplete,
            bool isTransactional)
        {
            const int threadCount = 1;

            var padlock = new object();

            var serviceBusOptions = AddServiceBus(services, true, threadCount, isTransactional, queueUriFormat, TimeSpan.FromMilliseconds(25));

            var module = new InboxConcurrencyModule();

            services.AddSingleton(module);

            var serviceProvider = services.BuildServiceProvider();

            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();
            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            module.Assign(serviceProvider.GetRequiredService<IPipelineFactory>());

            var queueService = CreateQueueService(serviceProvider);

            try
            {
                serviceBusConfiguration.Configure(serviceBusOptions);

                ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, true);

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    for (var i = 0; i < threadCount; i++)
                    {
                        transportMessagePipeline.Execute(new ConcurrentCommand
                        {
                            MessageIndex = i
                        }, null, builder =>
                        {
                            builder.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        });

                        serviceBusConfiguration.Inbox.WorkQueue.Enqueue(
                            transportMessagePipeline.State.GetTransportMessage(),
                            serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
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

                    bus.Start();

                    while (idleThreads.Count < threadCount)
                    {
                        Thread.Sleep(30);
                    }
                }

                AttemptDropQueues(queueService, queueUriFormat);
            }
            finally
            {
                queueService.AttemptDispose();
            }

            Assert.AreEqual(threadCount, module.OnAfterGetMessageCount,
                $"Got {module.OnAfterGetMessageCount} messages but {threadCount} were sent.");

            Assert.IsTrue(module.AllMessagesReceivedWithinTimespan(msToComplete),
                "All dequeued messages have to be within {0} ms of first get message.", msToComplete);
        }

        protected void TestInboxDeferred(IServiceCollection services, string queueUriFormat)
        {
            var serviceBusOptions = new ServiceBusOptions
            {
                Inbox = new InboxOptions
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                    DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    ThreadCount = 1
                }
            };

            services.AddSingleton<InboxDeferredModule>();

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
            });

            var serviceProvider = services.BuildServiceProvider();

            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

            var messageType = typeof(ReceivePipelineCommand).FullName ??
                              throw new InvalidOperationException(
                                  "Could not get the full type name of the ReceivePipelineCommand");

            var queueService = CreateQueueService(serviceProvider);

            serviceBusConfiguration.Configure(serviceBusOptions);

            ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, true);

            try
            {
                var module = serviceProvider.GetRequiredService<InboxDeferredModule>();

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>().Start())
                {
                    var transportMessage = bus.Send(new ReceivePipelineCommand(),
                        builder =>
                        {
                            builder
                                .Defer(DateTime.Now.AddMilliseconds(500))
                                .WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                        });

                    var timeout = DateTime.Now.AddMilliseconds(1000);

                    Assert.IsNotNull(transportMessage);

                    var messageId = transportMessage.MessageId;

                    while (module.TransportMessage == null && DateTime.Now < timeout)
                    {
                        Thread.Sleep(5);
                    }

                    Assert.IsNotNull(module.TransportMessage);
                    Assert.True(messageId.Equals(module.TransportMessage.MessageId));
                    Assert.True(messageType.Equals(module.TransportMessage.MessageType,
                        StringComparison.OrdinalIgnoreCase));
                }

                AttemptDropQueues(queueService, queueUriFormat);
            }
            finally
            {
                queueService.AttemptDispose();
            }
        }

        protected void TestInboxExpiry(IServiceCollection services, string queueUriFormat)
        {
            TestInboxExpiry(services, queueUriFormat, TimeSpan.FromMilliseconds(500));
        }

        protected void TestInboxExpiry(IServiceCollection services, string queueUriFormat, TimeSpan expiryDuration)
        {
            services.AddServiceBus();

            var serviceProvider = services.BuildServiceProvider();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            var queueService = CreateQueueService(serviceProvider);

            try
            {
                var queue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));

                queue.AttemptDrop();
                queue.AttemptCreate();

                transportMessagePipeline.Execute(new ReceivePipelineCommand(), null, builder =>
                {
                    builder.WillExpire(DateTime.Now.AddMilliseconds(expiryDuration.TotalMilliseconds));
                    builder.WithRecipient(queue);
                });

                var transportMessage = transportMessagePipeline.State.GetTransportMessage();

                queue.Enqueue(transportMessage,
                    serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));

                Assert.IsNotNull(transportMessage, "TransportMessage is null.");
                Assert.IsFalse(transportMessage.HasExpired(),
                    "The message has already expired before being processed.");

                // wait until the message expires
                Thread.Sleep(expiryDuration.Add(TimeSpan.FromMilliseconds(100)));

                Assert.IsNull(queue.GetMessage(),
                    "The message did not expire.  Call this test only if your queue actually supports message expiry internally.");

                queue.AttemptDrop();
            }
            finally
            {
                queueService.AttemptDispose();
            }
        }

        protected ServiceBusOptions AddServiceBus(IServiceCollection services, bool hasErrorQueue, int threadCount,
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
                    ThreadCount = threadCount
                }
            };

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
            });

            return serviceBusOptions;
        }
    }
}