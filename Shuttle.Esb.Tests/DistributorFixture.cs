using System;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;

namespace Shuttle.Esb.Tests
{
    public class DistributorFixture : IntegrationFixture
    {
        protected void TestDistributor(IServiceCollection distributorServices, IServiceCollection workerServices,
            string queueUriFormat, bool isTransactional, int timeoutSeconds = 5)
        {
            Guard.AgainstNull(distributorServices, nameof(distributorServices));
            Guard.AgainstNull(workerServices, nameof(workerServices));

            const int messageCount = 12;

            var distributorConfiguration = DefaultConfiguration(1);

            distributorServices.AddServiceBus(builder =>
            {
                builder.Configure(distributorConfiguration);
            });

            var distributorServiceProvider = distributorServices.BuildServiceProvider();

            var queueService = CreateQueueService(distributorServiceProvider);

            try
            {
                ConfigureDistributorQueues(distributorServiceProvider, distributorConfiguration, queueUriFormat);

                var transportMessageFactory = distributorServiceProvider.GetRequiredService<ITransportMessageFactory>();
                var serializer = distributorServiceProvider.GetRequiredService<ISerializer>();

                var module = new WorkerModule(messageCount);

                workerServices.AddSingleton(module);

                var workerConfiguration = DefaultConfiguration(1);

                workerServices.AddServiceBus(builder =>
                {
                    builder.Configure(workerConfiguration);
                });

                var workerServiceProvider = workerServices.BuildServiceProvider();

                var workerQueueService = workerServiceProvider.GetRequiredService<IQueueService>();

                ConfigureWorkerQueues(workerServiceProvider, workerConfiguration, queueUriFormat);

                module.Assign(workerServiceProvider.GetRequiredService<IPipelineFactory>());

                using (var distributorBus = distributorServiceProvider.GetRequiredService<IServiceBus>())
                using (var workerBus = workerServiceProvider.GetRequiredService<IServiceBus>())
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

                    Console.WriteLine($"[start wait] : now = '{DateTime.Now}'");

                    while (!module.AllMessagesHandled() && !timedOut)
                    {
                        Thread.Sleep(50);

                        timedOut = timeout < DateTime.Now;
                    }

                    Console.WriteLine(
                        $"[end wait] : now = '{DateTime.Now}' / timeout = '{timeout}' / timed out = '{timedOut}'");

                    Assert.IsTrue(module.AllMessagesHandled(), "Not all messages were handled.");
                }

                AttemptDropQueues(queueService, queueUriFormat);
            }
            finally
            {
                queueService.AttemptDispose();
            }
        }

        private void ConfigureDistributorQueues(IServiceProvider serviceProvider, ServiceBusConfiguration configuration,  string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox.WorkQueue =
                queueService.Get(string.Format(queueUriFormat, "test-distributor-work"));
            configuration.Inbox.ErrorQueue = errorQueue;
            configuration.Inbox.Distribute = true;

            configuration.ControlInbox = new ControlInboxQueueConfiguration
            {
                WorkQueue = queueService.Get(string.Format(queueUriFormat, "test-distributor-control")),
                ErrorQueue = errorQueue,
                DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
                DurationToIgnoreOnFailure = new[] {TimeSpan.FromMilliseconds(5)},
                ThreadCount = 1
            };

            configuration.Inbox.WorkQueue.AttemptDrop();
            configuration.ControlInbox.WorkQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            queueService.CreatePhysicalQueues(configuration);

            configuration.Inbox.WorkQueue.AttemptPurge();
            configuration.ControlInbox.WorkQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }

        private void ConfigureWorkerQueues(IServiceProvider serviceProvider, ServiceBusConfiguration configuration, string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            configuration.Inbox.WorkQueue = queueService.Get(string.Format(queueUriFormat, "test-worker-work"));
            configuration.Inbox.ErrorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            configuration.Worker = new WorkerConfiguration
            {
                DistributorControlInboxWorkQueue =
                    queueService.Get(string.Format(queueUriFormat, "test-distributor-control"))
            };

            configuration.Inbox.WorkQueue.AttemptDrop();

            queueService.CreatePhysicalQueues(configuration);

            configuration.Inbox.WorkQueue.AttemptPurge();
        }

        public class WorkerModule : IPipelineObserver<OnAfterHandleMessage>
        {
            private readonly int _messageCount;
            private readonly object _lock = new object();
            private int _messagesHandled;

            public WorkerModule(int messageCount)
            {
                _messageCount = messageCount;
            }

            public void Execute(OnAfterHandleMessage pipelineEvent1)
            {
                Console.WriteLine("[OnAfterHandleMessage]");

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