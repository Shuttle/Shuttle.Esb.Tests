using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;
using Shuttle.Core.Transactions;

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

            distributorServices.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            distributorServices.AddServiceBus(builder =>
            {
                builder.Options = new ServiceBusOptions
                {
                    Inbox = new InboxOptions
                    {
                        Distribute = true,
                        WorkQueueUri = string.Format(queueUriFormat, "test-distributor-work"),
                        ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                        DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                        DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                        ThreadCount = 1
                    },
                    ControlInbox = new ControlInboxOptions
                    {
                        WorkQueueUri = string.Format(queueUriFormat, "test-distributor-control"),
                        DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                        DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                        ThreadCount = 1
                    }
                };
            });

            var distributorServiceProvider = distributorServices.BuildServiceProvider();

            var distributorServiceBusConfiguration =
                distributorServiceProvider.GetRequiredService<IServiceBusConfiguration>();
            var pipelineFactory = distributorServiceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serializer = distributorServiceProvider.GetRequiredService<ISerializer>();

            var distributorQueueService = CreateQueueService(distributorServiceProvider);

            var module = new WorkerModule(messageCount);

            workerServices.AddSingleton(module);

            workerServices.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            workerServices.AddServiceBus(builder =>
            {
                builder.Options = new ServiceBusOptions
                {
                    Inbox = new InboxOptions
                    {
                        WorkQueueUri = string.Format(queueUriFormat, "test-worker-work"),
                        ErrorQueueUri = string.Format(queueUriFormat, "test-error"),
                        DurationToSleepWhenIdle = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                        DurationToIgnoreOnFailure = new List<TimeSpan> { TimeSpan.FromMilliseconds(25) },
                        ThreadCount = 1
                    },
                    Worker = new WorkerOptions
                    {
                        DistributorControlInboxWorkQueueUri = string.Format(queueUriFormat, "test-distributor-control")
                    }
                };
            });

            var workerServiceProvider = workerServices.BuildServiceProvider();
            var workerServiceBusConfiguration = workerServiceProvider.GetRequiredService<IServiceBusConfiguration>();

            try
            {
                ConfigureDistributorQueues(distributorServiceBusConfiguration);

                ConfigureWorkerQueues(workerServiceBusConfiguration);

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

                        var workQueue = distributorServiceBusConfiguration.Inbox.WorkQueue;

                        transportMessagePipeline.Execute(command, null, builder =>
                        {
                            builder.WithRecipient(workQueue);
                        });

                        workQueue.Enqueue(
                            transportMessagePipeline.State.GetTransportMessage(),
                            serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
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

                AttemptDropQueues(distributorQueueService, queueUriFormat);
            }
            finally
            {
                distributorQueueService.AttemptDispose();
            }
        }

        private void ConfigureDistributorQueues(IServiceBusConfiguration serviceBusConfiguration)
        {
            serviceBusConfiguration.Inbox.WorkQueue.AttemptDrop();
            serviceBusConfiguration.Inbox.ErrorQueue.AttemptDrop();
            serviceBusConfiguration.ControlInbox.WorkQueue.AttemptDrop();

            serviceBusConfiguration.CreatePhysicalQueues();

            serviceBusConfiguration.Inbox.WorkQueue.AttemptPurge();
            serviceBusConfiguration.ControlInbox.WorkQueue.AttemptPurge();
            serviceBusConfiguration.Inbox.ErrorQueue.AttemptPurge();
        }

        private void ConfigureWorkerQueues(IServiceBusConfiguration serviceBusConfiguration)
        {
            serviceBusConfiguration.Inbox.WorkQueue.AttemptDrop();

            serviceBusConfiguration.CreatePhysicalQueues();

            serviceBusConfiguration.Inbox.WorkQueue.AttemptPurge();
        }

        public class WorkerModule : IPipelineObserver<OnAfterHandleMessage>
        {
            private readonly object _lock = new object();
            private readonly int _messageCount;
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
                Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory));

                pipelineFactory.PipelineCreated += PipelineCreated;
            }
        }
    }
}