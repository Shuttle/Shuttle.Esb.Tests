using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;
using Shuttle.Core.Threading;
using Shuttle.Core.Transactions;

namespace Shuttle.Esb.Tests
{
    internal class ProcessorThreadObserver : IPipelineObserver<OnStarted>
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

            var padlock = new object();

            var serviceBusOptions = AddServiceBus(services, true, threadCount, isTransactional, queueUriFormat);

            services.AddSingleton<ProcessorThreadObserver, ProcessorThreadObserver>();

            var serviceProvider = services.BuildServiceProvider();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();

            pipelineFactory.PipelineCreated += delegate(object sender, PipelineEventArgs args)
            {
                if (args.Pipeline.GetType() == typeof(InboxMessagePipeline))
                {
                    args.Pipeline.GetStage("Read").BeforeEvent<OnGetMessage>().Register<OnStartRead>();

                    // create a new instance since it stores state
                    args.Pipeline.RegisterObserver(new ThroughputObserver());
                }

                if (args.Pipeline.GetType() == typeof(StartupPipeline))
                {
                    args.Pipeline.RegisterObserver(serviceProvider.GetService<ProcessorThreadObserver>());
                }
            };

            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            var sw = new Stopwatch();

            var queueService = CreateQueueService(serviceProvider);

            try
            {
                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    serviceBusConfiguration.Configure(serviceBusOptions);

                    ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, true);

                    Console.WriteLine("Sending {0} messages to input queue '{1}'.", messageCount,
                        serviceBusConfiguration.Inbox.WorkQueue.Uri);

                    for (var i = 0; i < 5; i++)
                    {
                        transportMessagePipeline.Execute(new SimpleCommand("warmup"), null, builder =>
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
                        Thread.Sleep(25);
                    }

                    bus.Stop();

                    serviceBusConfiguration.Configure(serviceBusOptions);

                    ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat, true);

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

                    Console.WriteLine("Took {0} ms to send {1} messages.  Starting processing.", sw.ElapsedMilliseconds,
                        messageCount);

                    idleThreads.Clear();
                    bus.Start();

                    sw.Reset();
                    sw.Start();

                    while (idleThreads.Count < threadCount)
                    {
                        Thread.Sleep(25);
                    }

                    sw.Stop();
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

            Console.WriteLine("Processed {0} messages in {1} ms", messageCount, ms);

            Assert.IsTrue(ms < timeoutMilliseconds,
                "Should be able to process at least {0} messages in {1} ms but it ook {2} ms.",
                messageCount, timeoutMilliseconds, ms);
        }

        protected void TestInboxError(IServiceCollection services, string queueUriFormat, bool hasErrorQueue,
            bool isTransactional)
        {
            var padlock = new object();
            var serviceBusOptions = AddServiceBus(services, hasErrorQueue, 1, isTransactional, queueUriFormat);

            serviceBusOptions.Inbox.MaximumFailureCount = 0;

            var serviceProvider = services.BuildServiceProvider();

            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();
            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

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

                    var threadWaitingCount = 0;

                    threadActivity.ThreadWaiting += (sender, args) =>
                    {
                        lock (padlock)
                        {
                            threadWaitingCount++;
                        }
                    };

                    bus.Start();

                    if (hasErrorQueue)
                    {
                        while (threadWaitingCount < 1)
                        {
                            Thread.Sleep(5);
                        }

                        bus.Stop();

                        Assert.Null(queueService.Get(string.Format(queueUriFormat, "test-inbox-work")).GetMessage());
                        Assert.NotNull(queueService.Get(string.Format(queueUriFormat, "test-error")).GetMessage());
                    }
                    else
                    {
                        while (threadWaitingCount < 1)
                        {
                            Thread.Sleep(5);
                        }

                        bus.Stop();

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

            var serviceBusOptions = AddServiceBus(services, true, threadCount, isTransactional, queueUriFormat);

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
            var serviceBusOptions = GetServiceBusOptions(true, 1, queueUriFormat);

            var module = new InboxDeferredModule();

            services.AddSingleton(module);

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
                module.Assign(serviceProvider.GetRequiredService<IPipelineFactory>());

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>().Start())
                {
                    var transportMessage = bus.Send(new ReceivePipelineCommand(),
                        c => c.Defer(DateTime.Now.AddMilliseconds(500))
                            .WithRecipient(serviceBusConfiguration.Inbox.WorkQueue));

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
            GetServiceBusOptions(true, 1, queueUriFormat);

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

                queue.Enqueue(transportMessage, serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));

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

        private ServiceBusOptions GetServiceBusOptions(bool hasErrorQueue, int threadCount, string queueUriFormat)
        {
            return new ServiceBusOptions
            {
                Inbox = new InboxOptions
                {
                    WorkQueueUri = string.Format(queueUriFormat, "test-inbox-work"),
                    ErrorQueueUri = hasErrorQueue ? string.Format(queueUriFormat, "test-error") : string.Empty,
                    DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                    ThreadCount = threadCount
                }
            };
        }

        protected ServiceBusOptions AddServiceBus(IServiceCollection services, bool hasErrorQueue, int threadCount,
            bool isTransactional, string queueUriFormat)
        {
            Guard.AgainstNull(services, nameof(services));

            services.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            var serviceBusOptions = GetServiceBusOptions(hasErrorQueue, threadCount, queueUriFormat);

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
            });

            return serviceBusOptions;
        }
    }
}