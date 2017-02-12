using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using NUnit.Framework;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
    public abstract class InboxFixture : IntegrationFixture
    {
        protected void TestInboxThroughput(ComponentContainer container, string queueUriFormat, int timeoutMilliseconds,
            int count, bool isTransactional)
        {
            Guard.AgainstNull(container, "container");

            const int threadCount = 15;
            var padlock = new object();
            var configuration = DefaultConfiguration(isTransactional, threadCount);

            new ServiceBusConfigurator(container.Registry).RegisterComponents(configuration);

            var transportMessageFactory = container.Resolver.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolver.Resolve<ISerializer>();
            var events = container.Resolver.Resolve<IServiceBusEvents>();

            var sw = new Stopwatch();

            var queueManager = ConfigureQueueManager(container.Resolver);

            ConfigureQueues(queueManager, configuration, queueUriFormat);

            using (var bus = ServiceBus.Create(container.Resolver))
            {
                Console.WriteLine("Sending {0} messages to input queue '{1}'.", count, configuration.Inbox.WorkQueue.Uri);

                for (var i = 0; i < 5; i++)
                {
                    var warmup = transportMessageFactory.Create(new SimpleCommand("warmup"),
                        c => c.WithRecipient(configuration.Inbox.WorkQueue));

                    configuration.Inbox.WorkQueue.Enqueue(warmup, serializer.Serialize(warmup));
                }

                var idleThreads = new List<int>();

                events.ThreadWaiting += (sender, args) =>
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

                sw.Start();

                for (var i = 0; i < count; i++)
                {
                    var message = transportMessageFactory.Create(new SimpleCommand("command " + i),
                        c => c.WithRecipient(configuration.Inbox.WorkQueue));

                    configuration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));
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

            AttemptDropQueues(queueManager, queueUriFormat);

            var ms = sw.ElapsedMilliseconds;

            Console.WriteLine("Processed {0} messages in {1} ms", count, ms);

            Assert.IsTrue(ms < timeoutMilliseconds,
                "Should be able to process at least {0} messages in {1} ms but it ook {2} ms.",
                count, timeoutMilliseconds, ms);
        }

        protected void TestInboxError(ComponentContainer container, string queueUriFormat, bool isTransactional)
        {
            var padlock = new object();
            var configuration = DefaultConfiguration(isTransactional, 1);

            configuration.Inbox.MaximumFailureCount = 0;

            new ServiceBusConfigurator(container.Registry).RegisterComponents(configuration);

            var transportMessageFactory = container.Resolver.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolver.Resolve<ISerializer>();
            var events = container.Resolver.Resolve<IServiceBusEvents>();

            var queueManager = ConfigureQueueManager(container.Resolver);

            ConfigureQueues(queueManager, configuration, queueUriFormat);

            using (var bus = ServiceBus.Create(container.Resolver))
            {
                var message = transportMessageFactory.Create(new ErrorCommand(),
                    c => c.WithRecipient(configuration.Inbox.WorkQueue));

                configuration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));

                var idleThreads = new List<int>();

                events.ThreadWaiting += (sender, args) =>
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

                Assert.Null(configuration.Inbox.WorkQueue.GetMessage());
                Assert.NotNull(configuration.Inbox.ErrorQueue.GetMessage());
            }

            AttemptDropQueues(queueManager, queueUriFormat);
        }

        private void ConfigureQueues(IQueueManager queueManager, IServiceBusConfiguration configuration,
            string queueUriFormat)
        {
            var inboxWorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work"));
            var errorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox.WorkQueue = inboxWorkQueue;
            configuration.Inbox.ErrorQueue = errorQueue;

            inboxWorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            queueManager.CreatePhysicalQueues(configuration);

            inboxWorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        protected void TestInboxConcurrency(ComponentContainer container, string workQueueUriFormat, int msToComplete,
            bool isTransactional)
        {
            const int threadCount = 1;

            var padlock = new object();
            var configuration = DefaultConfiguration(isTransactional, threadCount);

            var configurator = new ServiceBusConfigurator(container.Registry);

            configurator.DontRegister<InboxConcurrencyModule>();

            configurator.RegisterComponents(configuration);

            var module = new InboxConcurrencyModule();

            container.Registry.Register(module);

            var transportMessageFactory = container.Resolver.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolver.Resolve<ISerializer>();
            var events = container.Resolver.Resolve<IServiceBusEvents>();

            module.Assign(container.Resolver.Resolve<IPipelineFactory>());

            var queueManager = ConfigureQueueManager(container.Resolver);

            ConfigureQueues(queueManager, configuration, workQueueUriFormat);

            using (var bus = ServiceBus.Create(container.Resolver))
            {
                for (var i = 0; i < threadCount; i++)
                {
                    var message = transportMessageFactory.Create(new ConcurrentCommand
                    {
                        MessageIndex = i
                    }, c => c.WithRecipient(configuration.Inbox.WorkQueue));

                    configuration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));
                }

                var idleThreads = new List<int>();

                events.ThreadWaiting += (sender, args) =>
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

            Assert.AreEqual(threadCount, module.OnAfterGetMessageCount,
                string.Format("Got {0} messages but {1} were sent.", module.OnAfterGetMessageCount, threadCount));

            Assert.IsTrue(module.AllMessagesReceivedWithinTimespan(msToComplete),
                "All dequeued messages have to be within {0} ms of first get message.", msToComplete);
        }

        protected void TestInboxDeferred(ComponentContainer container, string queueUriFormat)
        {
            var configuration = DefaultConfiguration(false, 1);

            var configurator = new ServiceBusConfigurator(container.Registry);

            configurator.DontRegister<InboxDeferredModule>();

            configurator.RegisterComponents(configuration);

            var module = new InboxDeferredModule();

            container.Registry.Register(module);

            module.Assign(container.Resolver.Resolve<IPipelineFactory>());

            var messageType = typeof (ReceivePipelineCommand).FullName;

            var queueManager = ConfigureQueueManager(container.Resolver);

            ConfigureQueues(queueManager, configuration, queueUriFormat);

            using (var bus = ServiceBus.Create(container.Resolver))
            {
                bus.Start();

                var transportMessage = bus.Send(new ReceivePipelineCommand(),
                    c => c.Defer(DateTime.Now.AddMilliseconds(500))
                        .WithRecipient(configuration.Inbox.WorkQueue));

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

        protected void TestInboxExpiry(ComponentContainer container, string queueUriFormat)
        {
            var configuration = DefaultConfiguration(false, 1);

            new ServiceBusConfigurator(container.Registry).RegisterComponents(configuration);

            var transportMessageFactory = container.Resolver.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolver.Resolve<ISerializer>();

            var queueManager = ConfigureQueueManager(container.Resolver);

            ConfigureQueues(queueManager, configuration, queueUriFormat);

            using (var bus = ServiceBus.Create(container.Resolver))
            {
                bus.Start();

                var transportMessage = transportMessageFactory.Create(new ReceivePipelineCommand(), c =>
                {
                    c.WillExpire(DateTime.Now.AddMilliseconds(500));
                    c.WithRecipient(configuration.Inbox.WorkQueue);
                });

                configuration.Inbox.WorkQueue.Enqueue(transportMessage, serializer.Serialize(transportMessage));

                Assert.IsNotNull(transportMessage, "TransportMessage is null.");
                Assert.IsFalse(transportMessage.HasExpired(), "The message has already expired before being processed.");

                // wait until the message expires
                Thread.Sleep(550);

                Assert.IsNull(configuration.Inbox.WorkQueue.GetMessage(),
                    "The message did not expire.  Call this test only if your queue actully supports message expiry internally.");
            }

            AttemptDropQueues(queueManager, queueUriFormat);
        }
    }
}