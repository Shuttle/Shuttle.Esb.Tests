using System;
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

            var distributorServiceBusOptions = DefaultServiceBusOptions(1);
            var distributorServiceBusConfiguration = new ServiceBusConfiguration();

            distributorServices.AddTransactionScope(options =>
            {
                if (!isTransactional)
                {
                    options.Disable();
                }
            });

            distributorServices.AddServiceBus(builder =>
            {
                builder.Options = distributorServiceBusOptions;
                builder.Configuration = distributorServiceBusConfiguration;
            });

            var distributorServiceProvider = distributorServices.BuildServiceProvider();

            var distributorQueueService = CreateQueueService(distributorServiceProvider);

            try
            {
                ConfigureDistributorQueues(distributorServiceProvider, distributorServiceBusOptions,
                    distributorServiceBusConfiguration, queueUriFormat);

                var transportMessageFactory = distributorServiceProvider.GetRequiredService<ITransportMessageFactory>();
                var serializer = distributorServiceProvider.GetRequiredService<ISerializer>();

                var module = new WorkerModule(messageCount);

                workerServices.AddSingleton(module);

                var workerServiceBusOptions = DefaultServiceBusOptions(1);
                var workerServiceBusConfiguration = new ServiceBusConfiguration();

                workerServices.AddTransactionScope(options =>
                {
                    if (!isTransactional)
                    {
                        options.Disable();
                    }
                });

                workerServices.AddServiceBus(builder =>
                {
                    builder.Options = workerServiceBusOptions;
                    builder.Configuration = workerServiceBusConfiguration;
                });

                var workerServiceProvider = workerServices.BuildServiceProvider();

                ConfigureWorkerQueues(workerServiceProvider, workerServiceBusConfiguration, queueUriFormat);

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

                AttemptDropQueues(distributorQueueService, queueUriFormat);
            }
            finally
            {
                distributorQueueService.AttemptDispose();
            }
        }

        private void ConfigureDistributorQueues(IServiceProvider serviceProvider, ServiceBusOptions serviceBusOptions,
            ServiceBusConfiguration configuration, string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox = new InboxConfiguration
            {
                WorkQueue = queueService.Get(string.Format(queueUriFormat, "test-distributor-work")),
                ErrorQueue = errorQueue
            };

            serviceBusOptions.Inbox.Distribute = true;

            configuration.ControlInbox = new ControlInboxConfiguration
            {
                WorkQueue = queueService.Get(string.Format(queueUriFormat, "test-distributor-control")),
                ErrorQueue = errorQueue,
                DurationToSleepWhenIdle = new[] { TimeSpan.FromMilliseconds(5) },
                DurationToIgnoreOnFailure = new[] { TimeSpan.FromMilliseconds(5) },
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

        private void ConfigureWorkerQueues(IServiceProvider serviceProvider, ServiceBusConfiguration configuration,
            string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            configuration.Inbox = new InboxConfiguration
            {
                WorkQueue = queueService.Get(string.Format(queueUriFormat, "test-worker-work")),
                ErrorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"))
            };

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