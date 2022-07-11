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

namespace Shuttle.Esb.Tests
{
    public abstract class InboxFixture : IntegrationFixture
    {
        protected void TestInboxThroughput(IServiceCollection services, string queueUriFormat, int timeoutMilliseconds,
            int count, bool isTransactional)
        {
            Guard.AgainstNull(services, nameof(services));

            const int threadCount = 15;
            var padlock = new object();

            var serviceBusConfiguration = new ServiceBusConfiguration();

            AddServiceBus(services, threadCount, isTransactional, serviceBusConfiguration);

            var serviceProvider = services.BuildServiceProvider();

            var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();

            pipelineFactory.PipelineCreated += delegate(object sender, PipelineEventArgs args)
            {
                if (!(args.Pipeline.GetType().FullName ?? string.Empty).Equals(typeof(InboxMessagePipeline).FullName,
                        StringComparison.InvariantCultureIgnoreCase))
                {
                    return;
                }

                args.Pipeline.GetStage("Read").BeforeEvent<OnGetMessage>().Register<OnStartRead>();

                // create a new instance since it stores state
                args.Pipeline.RegisterObserver(new ThroughputObserver());
            };

            var transportMessageFactory = serviceProvider.GetRequiredService<ITransportMessageFactory>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();
            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

            var sw = new Stopwatch();

            var queueService = CreateQueueService(serviceProvider);

            try
            {
                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat);

                    Console.WriteLine("Sending {0} messages to input queue '{1}'.", count,
                        serviceBusConfiguration.Inbox.WorkQueue.Uri);

                    for (var i = 0; i < 5; i++)
                    {
                        var warmup = transportMessageFactory.Create(new SimpleCommand("warmup"),
                            c => c.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue));

                        serviceBusConfiguration.Inbox.WorkQueue.Enqueue(warmup, serializer.Serialize(warmup));
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

                    ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat);

                    sw.Start();

                    for (var i = 0; i < count; i++)
                    {
                        var message = transportMessageFactory.Create(new SimpleCommand("command " + i),
                            c => c.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue));

                        serviceBusConfiguration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));
                    }

                    sw.Stop();

                    Console.WriteLine("Took {0} ms to send {1} messages.  Starting processing.", sw.ElapsedMilliseconds,
                        count);

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

            Console.WriteLine("Processed {0} messages in {1} ms", count, ms);

            Assert.IsTrue(ms < timeoutMilliseconds,
                "Should be able to process at least {0} messages in {1} ms but it ook {2} ms.",
                count, timeoutMilliseconds, ms);
        }

        protected void TestInboxError(IServiceCollection services, string queueUriFormat, bool isTransactional)
        {
            var padlock = new object();
            var serviceBusConfiguration = new ServiceBusConfiguration();
            var serviceBusOptions = AddServiceBus(services, 1, isTransactional, serviceBusConfiguration);

            serviceBusOptions.Inbox.MaximumFailureCount = 0;

            var serviceProvider = services.BuildServiceProvider();

            var transportMessageFactory = serviceProvider.GetRequiredService<ITransportMessageFactory>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();
            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

            var queueManager = CreateQueueService(serviceProvider);

            try
            {
                ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat);

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    var message = transportMessageFactory.Create(new ErrorCommand(),
                        c => c.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue));

                    serviceBusConfiguration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));

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

                    while (idleThreads.Count < 1)
                    {
                        Thread.Sleep(5);
                    }

                    Assert.Null(serviceBusConfiguration.Inbox.WorkQueue.GetMessage());
                    Assert.NotNull(serviceBusConfiguration.Inbox.ErrorQueue.GetMessage());
                }

                AttemptDropQueues(queueManager, queueUriFormat);
            }
            finally
            {
                queueManager.AttemptDispose();
            }
        }

        private void ConfigureQueues(IServiceProvider serviceProvider, ServiceBusConfiguration configuration, string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox = new InboxConfiguration
            {
                WorkQueue = inboxWorkQueue,
                ErrorQueue = errorQueue
            };

            inboxWorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            queueService.CreatePhysicalQueues(configuration);

            inboxWorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        protected void TestInboxConcurrency(IServiceCollection services, string workQueueUriFormat, int msToComplete,
            bool isTransactional)
        {
            const int threadCount = 1;

            var padlock = new object();

            var serviceBusConfiguration = new ServiceBusConfiguration();

            AddServiceBus(services, threadCount, isTransactional, serviceBusConfiguration);

            var module = new InboxConcurrencyModule();

            services.AddSingleton(module);

            var serviceProvider = services.BuildServiceProvider();

            var transportMessageFactory = serviceProvider.GetRequiredService<ITransportMessageFactory>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();
            var threadActivity = serviceProvider.GetRequiredService<IPipelineThreadActivity>();

            module.Assign(serviceProvider.GetRequiredService<IPipelineFactory>());

            var queueManager = CreateQueueService(serviceProvider);

            try
            {
                ConfigureQueues(serviceProvider, serviceBusConfiguration, workQueueUriFormat);

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    for (var i = 0; i < threadCount; i++)
                    {
                        var message = transportMessageFactory.Create(new ConcurrentCommand
                        {
                            MessageIndex = i
                        }, c => c.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue));

                        serviceBusConfiguration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));
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

                AttemptDropQueues(queueManager, workQueueUriFormat);
            }
            finally
            {
                queueManager.AttemptDispose();
            }

            Assert.AreEqual(threadCount, module.OnAfterGetMessageCount,
                $"Got {module.OnAfterGetMessageCount} messages but {threadCount} were sent.");

            Assert.IsTrue(module.AllMessagesReceivedWithinTimespan(msToComplete),
                "All dequeued messages have to be within {0} ms of first get message.", msToComplete);
        }

        protected void TestInboxDeferred(IServiceCollection services, string queueUriFormat)
        {
            var serviceBusOptions = DefaultServiceBusOptions(1);
            var serviceBusConfiguration = new ServiceBusConfiguration();

            var module = new InboxDeferredModule();

            services.AddSingleton(module);

            services.AddServiceBus(builder =>
            {
                builder.Options = serviceBusOptions;
                builder.Configuration = serviceBusConfiguration;
            });

            var serviceProvider = services.BuildServiceProvider();

            var messageType = typeof(ReceivePipelineCommand).FullName ??
                              throw new InvalidOperationException(
                                  "Could not get the full type name of the ReceivePipelineCommand");

            var queueManager = CreateQueueService(serviceProvider);

            ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat);

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
                    Assert.True(messageType.Equals(module.TransportMessage.MessageType, StringComparison.OrdinalIgnoreCase));
                }

                AttemptDropQueues(queueManager, queueUriFormat);
            }
            finally
            {
                queueManager.AttemptDispose();
            }
        }

        protected void TestInboxExpiry(IServiceCollection services, string queueUriFormat)
        {
            DefaultServiceBusOptions(1);

            var serviceBusConfiguration = new ServiceBusConfiguration();

            services.AddServiceBus(builder =>
            {
                builder.Configuration = serviceBusConfiguration;
            });

            var serviceProvider = services.BuildServiceProvider();

            var transportMessageFactory = serviceProvider.GetRequiredService<ITransportMessageFactory>();
            var serializer = serviceProvider.GetRequiredService<ISerializer>();

            var queueManager = CreateQueueService(serviceProvider);

            try
            {
                ConfigureQueues(serviceProvider, serviceBusConfiguration, queueUriFormat);

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    bus.Start();

                    var transportMessage = transportMessageFactory.Create(new ReceivePipelineCommand(), c =>
                    {
                        c.WillExpire(DateTime.Now.AddMilliseconds(500));
                        c.WithRecipient(serviceBusConfiguration.Inbox.WorkQueue);
                    });

                    serviceBusConfiguration.Inbox.WorkQueue.Enqueue(transportMessage,
                        serializer.Serialize(transportMessage));

                    Assert.IsNotNull(transportMessage, "TransportMessage is null.");
                    Assert.IsFalse(transportMessage.HasExpired(),
                        "The message has already expired before being processed.");

                    // wait until the message expires
                    Thread.Sleep(550);

                    Assert.IsNull(serviceBusConfiguration.Inbox.WorkQueue.GetMessage(),
                        "The message did not expire.  Call this test only if your queue actually supports message expiry internally.");
                }

                AttemptDropQueues(queueManager, queueUriFormat);
            }
            finally
            {
                queueManager.AttemptDispose();
            }
        }
    }
}