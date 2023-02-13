using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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
        protected async Task TestDistributor(IServiceCollection distributorServices, IServiceCollection workerServices,
            string queueUriFormat, bool isTransactional, int timeoutSeconds = 5)
        {
            Guard.AgainstNull(distributorServices, nameof(distributorServices));
            Guard.AgainstNull(workerServices, nameof(workerServices));

            const int messageCount = 12;

            distributorServices.AddTransactionScope(builder =>
            {
                builder.Options.Enabled = isTransactional;
            });

            var distributorServiceBusOptions = new ServiceBusOptions
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

            distributorServices.AddServiceBus(builder =>
            {
                builder.Options = distributorServiceBusOptions;
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

            var workerServiceBusOptions = new ServiceBusOptions
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

            workerServices.AddServiceBus(builder =>
            {
                builder.Options = workerServiceBusOptions;
            });

            var workerServiceProvider = workerServices.BuildServiceProvider();
            var workerServiceBusConfiguration = workerServiceProvider.GetRequiredService<IServiceBusConfiguration>();

            try
            {
                distributorServiceBusConfiguration.Configure(distributorServiceBusOptions);
                workerServiceBusConfiguration.Configure(workerServiceBusOptions);

                await ConfigureDistributorQueues(distributorServiceBusConfiguration).ConfigureAwait(false);
                ConfigureWorkerQueues(workerServiceBusConfiguration);

                module.Assign(workerServiceProvider.GetRequiredService<IPipelineFactory>());

                var distributorBus = distributorServiceProvider.GetRequiredService<IServiceBus>();
                var workerBus = workerServiceProvider.GetRequiredService<IServiceBus>();

                await using (distributorBus.ConfigureAwait(false))
                await using (workerBus.ConfigureAwait(false))
                {
                    for (var i = 0; i < messageCount; i++)
                    {
                        var command = new SimpleCommand
                        {
                            Name = Guid.NewGuid().ToString()
                        };

                        var workQueue = distributorServiceBusConfiguration.Inbox.WorkQueue;

                        await transportMessagePipeline.Execute(command, null, builder =>
                        {
                            builder.WithRecipient(workQueue);
                        }).ConfigureAwait(false);

                        await workQueue.Enqueue(transportMessagePipeline.State.GetTransportMessage(), await serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                    }

                    await distributorBus.Start();
                    await workerBus.Start();

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

                await TryDropQueues(distributorQueueService, queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                distributorQueueService.TryDispose();
            }
        }

        private async Task ConfigureDistributorQueues(IServiceBusConfiguration serviceBusConfiguration)
        {
            await serviceBusConfiguration.Inbox.WorkQueue.TryDrop();
            await serviceBusConfiguration.Inbox.ErrorQueue.TryDrop();
            await serviceBusConfiguration.ControlInbox.WorkQueue.TryDrop();

            await serviceBusConfiguration.CreatePhysicalQueues();

            await serviceBusConfiguration.Inbox.WorkQueue.TryPurge();
            await serviceBusConfiguration.ControlInbox.WorkQueue.TryPurge();
            await serviceBusConfiguration.Inbox.ErrorQueue.TryPurge();
        }

        private void ConfigureWorkerQueues(IServiceBusConfiguration serviceBusConfiguration)
        {
            serviceBusConfiguration.Inbox.WorkQueue.TryDrop();

            serviceBusConfiguration.CreatePhysicalQueues();

            serviceBusConfiguration.Inbox.WorkQueue.TryPurge();
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

            public async Task Execute(OnAfterHandleMessage pipelineEvent1)
            {
                Console.WriteLine("[OnAfterHandleMessage]");

                lock (_lock)
                {
                    _messagesHandled++;
                }

                await Task.CompletedTask.ConfigureAwait(false);
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