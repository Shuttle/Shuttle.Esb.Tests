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
        protected void TestInboxThroughput(string queueUriFormat, int timeoutMilliseconds, int count, bool isTransactional)
        {
            const int threadCount = 15;
            var padlock = new object();
            var configuration = GetConfiguration(queueUriFormat, threadCount, isTransactional);

            var container = GetComponentContainer(configuration);

            var transportMessageFactory = container.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolve<ISerializer>();
            var events = container.Resolve<IServiceBusEvents>();

            Console.WriteLine("Sending {0} messages to input queue '{1}'.", count, configuration.Inbox.WorkQueue.Uri);

            var sw = new Stopwatch();

            using (var bus = ServiceBus.Create(container))
            {
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

            AttemptDropQueues(queueUriFormat);

            var ms = sw.ElapsedMilliseconds;

            Console.WriteLine("Processed {0} messages in {1} ms", count, ms);

            Assert.IsTrue(ms < timeoutMilliseconds,
                "Should be able to process at least {0} messages in {1} ms but it ook {2} ms.",
                count, timeoutMilliseconds, ms);
        }

        protected void TestInboxError(string queueUriFormat, bool isTransactional)
        {
            var padlock = new object();
            var configuration = GetConfiguration(queueUriFormat, 1, isTransactional);

            var container = GetComponentContainer(configuration);

            var transportMessageFactory = container.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolve<ISerializer>();
            var events = container.Resolve<IServiceBusEvents>();

            using (var bus = ServiceBus.Create(container))
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

            AttemptDropQueues(queueUriFormat);
        }

        private static ServiceBusConfiguration GetConfiguration(string queueUriFormat, int threadCount, bool isTransactional)
        {
            using (var queueManager = GetQueueManager())
            {
                var configuration = DefaultConfiguration(isTransactional);

                var inboxWorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work"));
                var errorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

                configuration.Inbox =
                    new InboxQueueConfiguration
                    {
                        WorkQueue = inboxWorkQueue,
                        ErrorQueue = errorQueue,
                        DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                        DurationToIgnoreOnFailure = new[] {TimeSpan.FromMilliseconds(5)},
                        ThreadCount = threadCount
                    };

                inboxWorkQueue.AttemptDrop();
                errorQueue.AttemptDrop();

                queueManager.CreatePhysicalQueues(configuration);

                inboxWorkQueue.AttemptPurge();
                errorQueue.AttemptPurge();

                return configuration;
            }
        }

        protected void TestInboxConcurrency(string workQueueUriFormat, int msToComplete, bool isTransactional)
        {
            const int threadCount = 1;

            var padlock = new object();
            var configuration = GetConfiguration(workQueueUriFormat, threadCount, isTransactional);
            var module = new InboxConcurrencyModule();

            var container = new DefaultComponentContainer();

            var configurator = new DefaultConfigurator(container);

            configurator.DontRegister<InboxConcurrencyModule>();

            configurator.RegisterComponents(configuration);

            var transportMessageFactory = container.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolve<ISerializer>();
            var events = container.Resolve<IServiceBusEvents>();

            container.Register(module.GetType(), module);

            using (var bus = ServiceBus.Create(container))
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

            AttemptDropQueues(workQueueUriFormat);

            Assert.AreEqual(threadCount, module.OnAfterGetMessageCount,
                string.Format("Got {0} messages but {1} were sent.", module.OnAfterGetMessageCount, threadCount));

            Assert.IsTrue(module.AllMessagesReceivedWithinTimespan(msToComplete),
                "All dequeued messages have to be within {0} ms of first get message.", msToComplete);
        }

        protected void TestInboxDeferred(string queueUriFormat)
        {
            var configuration = GetConfiguration(queueUriFormat, 1, false);

            var container = new DefaultComponentContainer();

            var configurator = new DefaultConfigurator(container);

            configurator.DontRegister<InboxDeferredModule>();

            configurator.RegisterComponents(configuration);

            var pipelineFactory = container.Resolve<IPipelineFactory>();

            var module = new InboxDeferredModule(pipelineFactory);

            container.Register(typeof(InboxDeferredModule), module);

            var messageType = typeof(ReceivePipelineCommand).FullName;

            using (var bus = ServiceBus.Create(container))
            {
                bus.Start();

                var transportMessage = bus.Send(new ReceivePipelineCommand(), c => c.Defer(DateTime.Now.AddMilliseconds(500))
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
        }

        protected void TestInboxExpiry(string queueUriFormat)
        {
            var configuration = GetConfiguration(queueUriFormat, 1, false);

            var container = GetComponentContainer(configuration);

            var transportMessageFactory = container.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolve<ISerializer>();

            using (var bus = ServiceBus.Create(container))
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

                Assert.IsNull(configuration.Inbox.WorkQueue.GetMessage(), "The message did not expire.  Call this test only if your queue actully supports message expiry internally.");
            }
        }
    }
}