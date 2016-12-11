using System;
using System.Threading;
using NUnit.Framework;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
    public class DistributorFixture : IntegrationFixture
    {
        private class WorkerModule : IPipelineObserver<OnAfterHandleMessage>
        {
            private readonly ILog _log;
            private readonly int _messageCount;
            private readonly object padlock = new object();
            private int _messagesHandled;

            public WorkerModule(int messageCount)
            {
                _messageCount = messageCount;

                _log = Log.For(this);
            }

            public WorkerModule(IPipelineFactory pipelineFactory)
            {
                pipelineFactory.PipelineCreated += PipelineCreated;
            }

            public void Execute(OnAfterHandleMessage pipelineEvent1)
            {
                _log.Information("[OnAfterHandleMessage]");

                lock (padlock)
                {
                    _messagesHandled++;
                }
            }

            private void PipelineCreated(object sender, PipelineEventArgs e)
            {
                if (!e.Pipeline.GetType()
                    .FullName.Equals(typeof (InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase)
                    &&
                    !e.Pipeline.GetType()
                        .FullName.Equals(typeof (DeferredMessagePipeline).FullName,
                            StringComparison.InvariantCultureIgnoreCase))
                {
                    return;
                }

                e.Pipeline.RegisterObserver(this);
            }

            public bool AllMessagesHandled()
            {
                return _messagesHandled == _messageCount;
            }
        }

        private readonly ILog _log;

        public DistributorFixture()
        {
            _log = Log.For(this);
        }

        protected void TestDistributor(string queueUriFormat, bool isTransactional)
        {
            const int messageCount = 12;

            var module = new WorkerModule(messageCount);

            var distributorConfiguration = GetDistributorConfiguration(queueUriFormat, isTransactional);
            var distributorContainer = GetComponentContainer(distributorConfiguration);

            var transportMessageFactory = distributorContainer.Resolve<ITransportMessageFactory>();
            var serializer = distributorContainer.Resolve<ISerializer>();

            var workerContainer = GetComponentContainer(GetWorkerConfiguration(queueUriFormat, isTransactional));

            workerContainer.Register<WorkerModule, WorkerModule>();

            using (var distributorBus = ServiceBus.Create(distributorContainer))
            using (var workerBus = ServiceBus.Create(workerContainer))
            {
                for (var i = 0; i < messageCount; i++)
                {
                    var command = new SimpleCommand
                    {
                        Name = Guid.NewGuid().ToString()
                    };

                    var workQueue = distributorConfiguration.Inbox.WorkQueue;
                    var message = transportMessageFactory.Create(command, c => c.WithRecipient(workQueue));

                    workQueue.Enqueue(message, serializer.Serialize(message));
                }

                distributorBus.Start();
                workerBus.Start();

                var timeout = DateTime.Now.AddSeconds(15);
                var timedOut = false;

                _log.Information(string.Format("[start wait] : now = '{0}'", DateTime.Now));

                while (!module.AllMessagesHandled() && !timedOut)
                {
                    Thread.Sleep(50);

                    timedOut = timeout < DateTime.Now;
                }

                _log.Information(string.Format("[end wait] : now = '{0}' / timeout = '{1}' / timed out = '{2}'",
                    DateTime.Now, timeout, timedOut));

                Assert.IsTrue(module.AllMessagesHandled(), "Not all messages were handled.");
            }

            AttemptDropQueues(queueUriFormat);
        }

        private static ServiceBusConfiguration GetDistributorConfiguration(string queueUriFormat, bool isTransactional)
        {
            using (var queueManager = GetQueueManager())
            {
                var configuration = DefaultConfiguration(isTransactional);

                var errorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

                configuration.Inbox =
                    new InboxQueueConfiguration
                    {
                        WorkQueue =
                            queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-work")),
                        ErrorQueue = errorQueue,
                        DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                        ThreadCount = 1,
                        Distribute = true,
                        DistributeSendCount = 3
                    };

                configuration.ControlInbox = new ControlInboxQueueConfiguration
                {
                    WorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control")),
                    ErrorQueue = errorQueue,
                    DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                    ThreadCount = 1
                };

                configuration.Inbox.WorkQueue.AttemptDrop();
                configuration.ControlInbox.WorkQueue.AttemptDrop();
                errorQueue.AttemptDrop();

                queueManager.CreatePhysicalQueues(configuration);

                configuration.Inbox.WorkQueue.AttemptPurge();
                configuration.ControlInbox.WorkQueue.AttemptPurge();
                errorQueue.AttemptPurge();

                return configuration;
            }
        }

        private static ServiceBusConfiguration GetWorkerConfiguration(string queueUriFormat, bool isTransactional)
        {
            using (var queueManager = GetQueueManager())
            {
                var configuration = DefaultConfiguration(isTransactional);

                configuration.Inbox =
                    new InboxQueueConfiguration
                    {
                        WorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-worker-work")),
                        ErrorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error")),
                        DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                        ThreadCount = 1
                    };

                configuration.Worker =
                    new WorkerConfiguration
                    {
                        DistributorControlInboxWorkQueue =
                            queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control")),
                        ThreadAvailableNotificationIntervalSeconds = 30
                    };

                configuration.Inbox.WorkQueue.AttemptDrop();

                queueManager.CreatePhysicalQueues(configuration);

                configuration.Inbox.WorkQueue.AttemptPurge();

                return configuration;
            }
        }
    }
}