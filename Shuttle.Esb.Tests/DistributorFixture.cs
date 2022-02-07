﻿using System;
using System.Threading;
using NUnit.Framework;
using Shuttle.Core.Container;
using Shuttle.Core.Contract;
using Shuttle.Core.Logging;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class DistributorFixture : IntegrationFixture
    {
        private readonly ILog _log;

        public DistributorFixture()
        {
            _log = Log.For(this);
        }

        protected void TestDistributor(ComponentContainer distributorContainer, ComponentContainer workerContainer,
            string queueUriFormat, bool isTransactional, int timeoutSeconds = 5)
        {
            Guard.AgainstNull(distributorContainer, "distributorContainer");
            Guard.AgainstNull(workerContainer, "workerContainer");

            const int messageCount = 12;

            var distributorConfiguration = DefaultConfiguration(isTransactional, 1);

            distributorContainer.Registry.RegisterServiceBus(distributorConfiguration);

            var queueManager = CreateQueueManager(distributorContainer.Resolver);

            try
            {
                ConfigureDistributorQueues(distributorContainer.Resolver, distributorConfiguration, queueUriFormat);

                var transportMessageFactory = distributorContainer.Resolver.Resolve<ITransportMessageFactory>();
                var serializer = distributorContainer.Resolver.Resolve<ISerializer>();

                var module = new WorkerModule(messageCount);

                workerContainer.Registry.RegisterInstance(module);

                var workerConfiguration = DefaultConfiguration(isTransactional, 1);

                workerContainer.Registry.RegisterServiceBus(workerConfiguration);

                var workerQueueManager = workerContainer.Resolver.Resolve<IQueueManager>();

                workerQueueManager.Configure(workerContainer.Resolver);
                ConfigureWorkerQueues(workerContainer.Resolver, workerConfiguration, queueUriFormat);

                module.Assign(workerContainer.Resolver.Resolve<IPipelineFactory>());

                using (var distributorBus = distributorContainer.Resolver.Resolve<IServiceBus>())
                using (var workerBus = workerContainer.Resolver.Resolve<IServiceBus>())
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

                    var timeout = DateTime.Now.AddSeconds(timeoutSeconds < 5 ? 5 : timeoutSeconds);
                    var timedOut = false;

                    _log.Information($"[start wait] : now = '{DateTime.Now}'");

                    while (!module.AllMessagesHandled() && !timedOut)
                    {
                        Thread.Sleep(50);

                        timedOut = timeout < DateTime.Now;
                    }

                    _log.Information(
                        $"[end wait] : now = '{DateTime.Now}' / timeout = '{timeout}' / timed out = '{timedOut}'");

                    Assert.IsTrue(module.AllMessagesHandled(), "Not all messages were handled.");
                }

                AttemptDropQueues(queueManager, queueUriFormat);
            }
            finally
            {
                queueManager.AttemptDispose();
            }
        }

        private void ConfigureDistributorQueues(IComponentResolver resolver, IServiceBusConfiguration configuration,  string queueUriFormat)
        {
            var queueManager = resolver.Resolve<IQueueManager>().Configure(resolver);

            var errorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox.WorkQueue =
                queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-work"));
            configuration.Inbox.ErrorQueue = errorQueue;
            configuration.Inbox.Distribute = true;

            configuration.ControlInbox = new ControlInboxQueueConfiguration
            {
                WorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control")),
                ErrorQueue = errorQueue,
                DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                DurationToIgnoreOnFailure = new[] {TimeSpan.FromMilliseconds(5)},
                ThreadCount = 1
            };

            configuration.Inbox.WorkQueue.AttemptDrop();
            configuration.ControlInbox.WorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            queueManager.CreatePhysicalQueues(configuration);

            configuration.Inbox.WorkQueue.AttemptPurge();
            configuration.ControlInbox.WorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        private void ConfigureWorkerQueues(IComponentResolver resolver, IServiceBusConfiguration configuration, string queueUriFormat)
        {
            var queueManager = resolver.Resolve<IQueueManager>().Configure(resolver);

            configuration.Inbox.WorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-worker-work"));
            configuration.Inbox.ErrorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

            configuration.Worker = new WorkerConfiguration
            {
                DistributorControlInboxWorkQueue =
                    queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control"))
            };

            configuration.Inbox.WorkQueue.AttemptDrop();

            queueManager.CreatePhysicalQueues(configuration);

            configuration.Inbox.WorkQueue.AttemptPurge();
        }

        public class WorkerModule : IPipelineObserver<OnAfterHandleMessage>
        {
            private readonly ILog _log;
            private readonly int _messageCount;
            private readonly object _lock = new object();
            private int _messagesHandled;

            public WorkerModule(int messageCount)
            {
                _messageCount = messageCount;

                _log = Log.For(this);
            }

            public void Execute(OnAfterHandleMessage pipelineEvent1)
            {
                _log.Information("[OnAfterHandleMessage]");

                lock (_lock)
                {
                    _messagesHandled++;
                }
            }

            private void PipelineCreated(object sender, PipelineEventArgs e)
            {
                var fullName = e.Pipeline.GetType().FullName;

                if (fullName != null &&
                    !fullName.Equals(typeof(InboxMessagePipeline).FullName,
                        StringComparison.InvariantCultureIgnoreCase) && !fullName.Equals(
                        typeof(DeferredMessagePipeline).FullName,
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

            public void Assign(IPipelineFactory pipelineFactory)
            {
                Guard.AgainstNull(pipelineFactory, "pipelineFactory");

                pipelineFactory.PipelineCreated += PipelineCreated;
            }
        }
    }
}