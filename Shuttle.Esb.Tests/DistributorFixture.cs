using System;
using System.Threading;
using Castle.Windsor;
using NUnit.Framework;
using Shuttle.Core.Castle;
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

            public WorkerModule(IPipelineFactory pipelineFactory, int messageCount)
            {
                Guard.AgainstNull(pipelineFactory, "pipelineFactory");

                pipelineFactory.PipelineCreated += PipelineCreated;

                _messageCount = messageCount;

                _log = Log.For(this);
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
                    .FullName.Equals(typeof(InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase)
                    &&
                    !e.Pipeline.GetType()
                        .FullName.Equals(typeof(DeferredMessagePipeline).FullName,
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

            var distributorConfiguration = DefaultConfiguration(isTransactional, 1);
            var distributorContainer = GetComponentResolver(distributorConfiguration);

            var queueManager = distributorContainer.Resolve<IQueueManager>();

            ConfigureDistributorQueues(queueManager, distributorConfiguration, queueUriFormat);

            var transportMessageFactory = distributorContainer.Resolve<ITransportMessageFactory>();
            var serializer = distributorContainer.Resolve<ISerializer>();

            var workerContainer = new WindsorComponentContainer(new WindsorContainer());

            var workerConfigurator = new ServiceBusConfigurator(workerContainer);

            workerConfigurator.DontRegister<WorkerModule>();

            var workerConfiguration = DefaultConfiguration(isTransactional, 1);
            workerConfigurator.RegisterComponents(DefaultConfiguration(isTransactional, 1));

            ConfigureWorkerQueues(workerContainer.Resolve<IQueueManager>(), workerConfiguration, queueUriFormat);

            var module = new WorkerModule(workerContainer.Resolve<IPipelineFactory>(), messageCount);

            workerContainer.Register<WorkerModule>(module);

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

                var timeout = DateTime.Now.AddSeconds(150);
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

            AttemptDropQueues(queueManager, queueUriFormat);
        }

        private void ConfigureDistributorQueues(IQueueManager queueManager, IServiceBusConfiguration configuration, string queueUriFormat)
        {
            var errorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox.WorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-work"));
            configuration.Inbox.ErrorQueue = errorQueue;

            configuration.ControlInbox.WorkQueue =
                queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control"));
            configuration.ControlInbox.ErrorQueue = errorQueue;

            configuration.Inbox.WorkQueue.AttemptDrop();
            configuration.ControlInbox.WorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            queueManager.CreatePhysicalQueues(configuration);

            configuration.Inbox.WorkQueue.AttemptPurge();
            configuration.ControlInbox.WorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        private void ConfigureWorkerQueues(IQueueManager queueManager, IServiceBusConfiguration configuration,
            string queueUriFormat)
        {
            configuration.Inbox.WorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-worker-work"));
            configuration.Inbox.ErrorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));
            configuration.Worker.DistributorControlInboxWorkQueue =
                queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control"));

            configuration.Inbox.WorkQueue.AttemptDrop();

            queueManager.CreatePhysicalQueues(configuration);

            configuration.Inbox.WorkQueue.AttemptPurge();
        }

        private IServiceBusConfiguration DistrobutorConfiguration(bool isTransactional, int threadCount)
        {
            var configuration = DefaultConfiguration(isTransactional, threadCount);

            configuration.ControlInbox = new ControlInboxQueueConfiguration
            {
                DurationToSleepWhenIdle = new[] { TimeSpan.FromMilliseconds(5) },
                ThreadCount = 1
            };

            return configuration;
        }
    }
}

