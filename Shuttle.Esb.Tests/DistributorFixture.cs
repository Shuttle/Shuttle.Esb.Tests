using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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
        private async Task ConfigureDistributorQueuesAsync(IServiceBusConfiguration serviceBusConfiguration, bool sync)
        {
            if (sync)
            {
                serviceBusConfiguration.Inbox.WorkQueue.TryDrop();
                serviceBusConfiguration.Inbox.ErrorQueue.TryDrop();
                serviceBusConfiguration.ControlInbox.WorkQueue.TryDrop();
                serviceBusConfiguration.CreatePhysicalQueues();
                serviceBusConfiguration.Inbox.WorkQueue.TryPurge();
                serviceBusConfiguration.ControlInbox.WorkQueue.TryPurge();
                serviceBusConfiguration.Inbox.ErrorQueue.TryPurge();
            }
            else
            {
                await serviceBusConfiguration.Inbox.WorkQueue.TryDropAsync().ConfigureAwait(false);
                await serviceBusConfiguration.Inbox.ErrorQueue.TryDropAsync().ConfigureAwait(false);
                await serviceBusConfiguration.ControlInbox.WorkQueue.TryDropAsync().ConfigureAwait(false);

                await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);

                await serviceBusConfiguration.Inbox.WorkQueue.TryPurgeAsync().ConfigureAwait(false);
                await serviceBusConfiguration.ControlInbox.WorkQueue.TryPurgeAsync().ConfigureAwait(false);
                await serviceBusConfiguration.Inbox.ErrorQueue.TryPurgeAsync().ConfigureAwait(false);
            }
        }

        private async Task ConfigureWorkerQueuesAsync(IServiceBusConfiguration serviceBusConfiguration, bool sync)
        {
            if (sync)
            {
                serviceBusConfiguration.Inbox.WorkQueue.TryDrop();

                serviceBusConfiguration.CreatePhysicalQueues();

                serviceBusConfiguration.Inbox.WorkQueue.TryPurge();
            }
            else
            {
                await serviceBusConfiguration.Inbox.WorkQueue.TryDropAsync().ConfigureAwait(false);

                await serviceBusConfiguration.CreatePhysicalQueuesAsync().ConfigureAwait(false);

                await serviceBusConfiguration.Inbox.WorkQueue.TryPurgeAsync().ConfigureAwait(false);
            }
        }

        protected void TestDistributor(IServiceCollection distributorServices, IServiceCollection workerServices, string queueUriFormat, bool isTransactional, int timeoutSeconds = 5)
        {
            TestDistributorAsync(distributorServices, workerServices, queueUriFormat, isTransactional, timeoutSeconds, true).GetAwaiter().GetResult();
        }

        protected async Task TestDistributorAsync(IServiceCollection distributorServices, IServiceCollection workerServices, string queueUriFormat, bool isTransactional, int timeoutSeconds = 5)
        {
            await TestDistributorAsync(distributorServices,  workerServices, queueUriFormat, isTransactional, timeoutSeconds, false).ConfigureAwait(false);
        }

        private async Task TestDistributorAsync(IServiceCollection distributorServices, IServiceCollection workerServices, string queueUriFormat, bool isTransactional, int timeoutSeconds, bool sync)
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
                builder.SuppressHostedService = true;
            });

            distributorServices.ConfigureLogging($"{nameof(TestDistributorAsync)}-distributor");

            var distributorServiceProvider = sync
                ? distributorServices.BuildServiceProvider().StartHostedServices()
                : await distributorServices.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);
            var logger = distributorServiceProvider.GetLogger<DistributorFixture>();

            var distributorServiceBusConfiguration =
                distributorServiceProvider.GetRequiredService<IServiceBusConfiguration>();
            var pipelineFactory = distributorServiceProvider.GetRequiredService<IPipelineFactory>();
            var transportMessagePipeline = pipelineFactory.GetPipeline<TransportMessagePipeline>();
            var serializer = distributorServiceProvider.GetRequiredService<ISerializer>();

            var distributorQueueService = distributorServiceProvider.CreateQueueService();

            workerServices.AddOptions<MessageCountOptions>().Configure(options =>
            {
                options.MessageCount = messageCount;
            });

            workerServices.AddSingleton<WorkerFeature>();

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
                builder.SuppressHostedService = true;
            });

            workerServices.ConfigureLogging($"{nameof(TestDistributorAsync)}-worker");

            var workerServiceProvider = sync 
                ? workerServices.BuildServiceProvider().StartHostedServices() 
                : await workerServices.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);
            var workerServiceBusConfiguration = workerServiceProvider.GetRequiredService<IServiceBusConfiguration>();

            var feature = workerServiceProvider.GetRequiredService<WorkerFeature>();

            try
            {
                if (sync)
                {
                    ConfigureDistributorQueuesAsync(distributorServiceBusConfiguration, true).GetAwaiter().GetResult();
                    ConfigureWorkerQueuesAsync(workerServiceBusConfiguration, true).GetAwaiter().GetResult();
                }
                else
                {
                    await ConfigureDistributorQueuesAsync(distributorServiceBusConfiguration, false).ConfigureAwait(false);
                    await ConfigureWorkerQueuesAsync(workerServiceBusConfiguration, false).ConfigureAwait(false);
                }

                var distributorBus = distributorServiceProvider.GetRequiredService<IServiceBus>();
                var workerBus = workerServiceProvider.GetRequiredService<IServiceBus>();

                var workQueue = distributorServiceBusConfiguration.Inbox.WorkQueue;

                for (var i = 0; i < messageCount; i++)
                {
                    var command = new SimpleCommand
                    {
                        Name = Guid.NewGuid().ToString(),
                        Context = "TestDistributor"
                    };

                    if (sync)
                    {
                        transportMessagePipeline.Execute(command, null, builder =>
                        {
                            builder.WithRecipient(workQueue);
                        });

                        workQueue.Enqueue(transportMessagePipeline.State.GetTransportMessage(), serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()));
                    }
                    else
                    {
                        await transportMessagePipeline.ExecuteAsync(command, null, builder =>
                        {
                            builder.WithRecipient(workQueue);
                        }).ConfigureAwait(false);

                        await workQueue.EnqueueAsync(transportMessagePipeline.State.GetTransportMessage(), await serializer.SerializeAsync(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                    }
                }

                try
                {
                    if (sync)
                    {
                        distributorBus.Start();
                        workerBus.Start();
                    }
                    else
                    {
                        await distributorBus.StartAsync().ConfigureAwait(false);
                        await workerBus.StartAsync().ConfigureAwait(false);
                    }

                    var timeout = DateTime.Now.AddSeconds(timeoutSeconds < 5 ? 5 : timeoutSeconds);
                    var timedOut = false;

                    logger.LogInformation($"[start wait] : now = '{DateTime.Now}'");

                    while (!feature.AllMessagesHandled() && !timedOut)
                    {
                        await Task.Delay(50).ConfigureAwait(false);

                        timedOut = timeout < DateTime.Now;
                    }

                    logger.LogInformation(
                        $"[end wait] : now = '{DateTime.Now}' / timeout = '{timeout}' / timed out = '{timedOut}'");

                    Assert.IsTrue(feature.AllMessagesHandled(), "Not all messages were handled.");
                }
                finally
                {
                    if (sync)
                    {
                        distributorBus.TryDispose();
                        workerBus.TryDispose();
                    }
                    else
                    {
                        await distributorBus.TryDisposeAsync().ConfigureAwait(false);
                        await workerBus.TryDisposeAsync().ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                if (sync)
                {
                    distributorQueueService.TryDropQueues(queueUriFormat);
                    distributorQueueService.TryDispose();
                    workerServiceProvider.StopHostedServices();
                    distributorServiceProvider.StopHostedServices();
                }
                else
                {
                    await distributorQueueService.TryDropQueuesAsync(queueUriFormat).ConfigureAwait(false);
                    await distributorQueueService.TryDisposeAsync().ConfigureAwait(false);
                    await workerServiceProvider.StopHostedServicesAsync().ConfigureAwait(false);
                    await distributorServiceProvider.StopHostedServicesAsync().ConfigureAwait(false);
                }
            }
        }

        public class WorkerFeature : IPipelineObserver<OnAfterHandleMessage>
        {
            private readonly object _lock = new object();
            private readonly ILogger<WorkerFeature> _logger;
            private readonly int _messageCount;
            private int _messagesHandled;

            public WorkerFeature(ILogger<WorkerFeature> logger, IOptions<MessageCountOptions> options, IPipelineFactory pipelineFactory)
            {
                Guard.AgainstNull(options, nameof(options));
                Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory)).PipelineCreated += PipelineCreated;

                _logger = Guard.AgainstNull(logger, nameof(logger));
                _messageCount = Guard.AgainstNull(options.Value, nameof(options.Value)).MessageCount;
            }

            public void Execute(OnAfterHandleMessage pipelineEvent)
            {
                _logger.LogInformation("[OnAfterHandleMessage]");

                lock (_lock)
                {
                    _messagesHandled++;
                }
            }

            public async Task ExecuteAsync(OnAfterHandleMessage pipelineEvent)
            {
                Execute(pipelineEvent);

                await Task.CompletedTask.ConfigureAwait(false);
            }

            public bool AllMessagesHandled()
            {
                return _messagesHandled == _messageCount;
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
        }
    }
}