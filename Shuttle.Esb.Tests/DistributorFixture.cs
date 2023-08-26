﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using NUnit.Framework.Internal;
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
                builder.SuppressHostedService = true;
            });

            distributorServices.ConfigureLogging($"{nameof(TestDistributor)}-distributor");

            var distributorServiceProvider = await distributorServices.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);
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

            workerServices.ConfigureLogging($"{nameof(TestDistributor)}-worker");

            var workerServiceProvider = await workerServices.BuildServiceProvider().StartHostedServices().ConfigureAwait(false);
            var workerServiceBusConfiguration = workerServiceProvider.GetRequiredService<IServiceBusConfiguration>();

            var feature = workerServiceProvider.GetRequiredService<WorkerFeature>();

            try
            {
                await ConfigureDistributorQueues(distributorServiceBusConfiguration).ConfigureAwait(false);
                await ConfigureWorkerQueues(workerServiceBusConfiguration).ConfigureAwait(false);

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
                    
                    await transportMessagePipeline.Execute(command, null, builder =>
                    {
                        builder.WithRecipient(workQueue);
                    }).ConfigureAwait(false);

                    await workQueue.Enqueue(transportMessagePipeline.State.GetTransportMessage(), await serializer.Serialize(transportMessagePipeline.State.GetTransportMessage()).ConfigureAwait(false)).ConfigureAwait(false);
                }

                await using (await distributorBus.Start().ConfigureAwait(false))
                await using (await workerBus.Start().ConfigureAwait(false))
                {
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

                await distributorQueueService.TryDropQueues(queueUriFormat).ConfigureAwait(false);
            }
            finally
            {
                distributorQueueService.TryDispose();

                await workerServiceProvider.StopHostedServices().ConfigureAwait(false);
                await distributorServiceProvider.StopHostedServices().ConfigureAwait(false);
            }
        }

        private async Task ConfigureDistributorQueues(IServiceBusConfiguration serviceBusConfiguration)
        {
            await serviceBusConfiguration.Inbox.WorkQueue.TryDrop().ConfigureAwait(false);
            await serviceBusConfiguration.Inbox.ErrorQueue.TryDrop().ConfigureAwait(false);
            await serviceBusConfiguration.ControlInbox.WorkQueue.TryDrop().ConfigureAwait(false);

            await serviceBusConfiguration.CreatePhysicalQueues().ConfigureAwait(false);

            await serviceBusConfiguration.Inbox.WorkQueue.TryPurge().ConfigureAwait(false);
            await serviceBusConfiguration.ControlInbox.WorkQueue.TryPurge().ConfigureAwait(false);
            await serviceBusConfiguration.Inbox.ErrorQueue.TryPurge().ConfigureAwait(false);
        }

        private async Task ConfigureWorkerQueues(IServiceBusConfiguration serviceBusConfiguration)
        {
            await serviceBusConfiguration.Inbox.WorkQueue.TryDrop().ConfigureAwait(false);

            await serviceBusConfiguration.CreatePhysicalQueues().ConfigureAwait(false);

            await serviceBusConfiguration.Inbox.WorkQueue.TryPurge().ConfigureAwait(false);
        }

        public class WorkerFeature : 
            IPipelineObserver<OnAfterHandleMessage>
        {
            private readonly ILogger<WorkerFeature> _logger;
            private readonly object _lock = new object();
            private readonly int _messageCount;
            private int _messagesHandled;

            public WorkerFeature(ILogger<WorkerFeature> logger, IOptions<MessageCountOptions> options, IPipelineFactory pipelineFactory)
            {
                Guard.AgainstNull(options, nameof(options));
                Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory)).PipelineCreated += PipelineCreated;

                _logger = Guard.AgainstNull(logger, nameof(logger));
                _messageCount = Guard.AgainstNull(options.Value, nameof(options.Value)).MessageCount;
            }

            public async Task Execute(OnAfterHandleMessage pipelineEvent1)
            {
                _logger.LogInformation("[OnAfterHandleMessage]");

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
        }
    }
}