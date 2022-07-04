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
    public class DeferredFixture : IntegrationFixture
    {
        protected void TestDeferredProcessing(IServiceCollection services, string queueUriFormat, bool isTransactional)
        {
            Guard.AgainstNull(services, nameof(services));

            const int deferredMessageCount = 10;
            const int millisecondsToDefer = 500;

            var module = new DeferredMessageModule(deferredMessageCount);

            services.AddSingleton(module);

            var configuration = AddServiceBus(services, 1, isTransactional);

            var serviceProvider = services.BuildServiceProvider();

            var queueManager = CreateQueueService(serviceProvider);

            ConfigureQueues(serviceProvider, configuration, queueUriFormat);

            try
            {
                module.Assign(serviceProvider.GetRequiredService<IPipelineFactory>());

                using (var bus = serviceProvider.GetRequiredService<IServiceBus>())
                {
                    bus.Start();

                    var ignoreTillDate = DateTime.Now.AddSeconds(5);

                    for (var i = 0; i < deferredMessageCount; i++)
                    {
                        EnqueueDeferredMessage(configuration, serviceProvider.GetRequiredService<ITransportMessageFactory>(),
                            serviceProvider.GetRequiredService<ISerializer>(), ignoreTillDate);

                        ignoreTillDate = ignoreTillDate.AddMilliseconds(millisecondsToDefer);
                    }

                    // add the extra time else there is no time to process message being returned
                    var timeout = ignoreTillDate.AddSeconds(15);
                    var timedOut = false;

                    Console.Write($"[start wait] : now = '{DateTime.Now}'");

                    // wait for the message to be returned from the deferred queue
                    while (!module.AllMessagesHandled()
                           &&
                           !timedOut)
                    {
                        Thread.Sleep(millisecondsToDefer);

                        timedOut = timeout < DateTime.Now;
                    }

                    Console.Write(
                        $"[end wait] : now = '{DateTime.Now}' / timeout = '{timeout}' / timed out = '{timedOut}'");

                    Console.Write(
                        $"{module.NumberOfDeferredMessagesReturned} of {deferredMessageCount} deferred messages returned to the inbox.");
                    Console.Write(
                        $"{module.NumberOfMessagesHandled} of {deferredMessageCount} deferred messages handled.");

                    Assert.IsTrue(module.AllMessagesHandled(), "All the deferred messages were not handled.");

                    Assert.IsTrue(configuration.Inbox.ErrorQueue.IsEmpty());
                    Assert.IsNull(configuration.Inbox.DeferredQueue.GetMessage());
                    Assert.IsNull(configuration.Inbox.WorkQueue.GetMessage());
                }

                AttemptDropQueues(queueManager, queueUriFormat);
            }
            finally
            {
                queueManager.AttemptDispose();
            }
        }

        private void EnqueueDeferredMessage(IServiceBusConfiguration configuration,
            ITransportMessageFactory transportMessageFactory, ISerializer serializer, DateTime ignoreTillDate)
        {
            var command = new SimpleCommand
            {
                Name = Guid.NewGuid().ToString()
            };

            var message = transportMessageFactory.Create(command, c => c
                .Defer(ignoreTillDate)
                .WithRecipient(configuration.Inbox.WorkQueue), null);

            configuration.Inbox.WorkQueue.Enqueue(message, serializer.Serialize(message));

            Console.WriteLine(
                $"[message enqueued] : name = '{command.Name}' / deferred till date = '{message.IgnoreTillDate}'");
        }

        private void ConfigureQueues(IServiceProvider serviceProvider, IServiceBusConfiguration configuration, string queueUriFormat)
        {
            var queueService = serviceProvider.GetRequiredService<IQueueService>();

            var inboxWorkQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-work"));
            var inboxDeferredQueue = queueService.Get(string.Format(queueUriFormat, "test-inbox-deferred"));
            var errorQueue = queueService.Get(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox.WorkQueue = inboxWorkQueue;
            configuration.Inbox.DeferredQueue = inboxDeferredQueue;
            configuration.Inbox.ErrorQueue = errorQueue;

            inboxWorkQueue.AttemptDrop();
            inboxDeferredQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            queueService.CreatePhysicalQueues(configuration);

            inboxWorkQueue.AttemptPurge();
            inboxDeferredQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }
    }
}