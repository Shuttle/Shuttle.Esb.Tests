using System;
using System.Threading;
using NUnit.Framework;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
    public class DeferredFixture : IntegrationFixture
    {
        private readonly ILog _log;

        public DeferredFixture()
        {
            _log = Log.For(this);
        }

        protected void TestDeferredProcessing(ComponentContainer container, string queueUriFormat, bool isTransactional)
        {
            Guard.AgainstNull(container, "container");

            const int deferredMessageCount = 10;
            const int millisecondsToDefer = 500;

	        var module = new DeferredMessageModule(deferredMessageCount);

	        container.Registry.Register(module);

			var configuration = DefaultConfiguration(isTransactional, 1);

            ServiceBus.Register(container.Registry, configuration);

            var queueManager = ConfigureQueueManager(container.Resolver);

            ConfigureQueues(queueManager, configuration, queueUriFormat);

			module.Assign(container.Resolver.Resolve<IPipelineFactory>());

			using (var bus = ServiceBus.Create(container.Resolver))
            {
                bus.Start();

                var ignoreTillDate = DateTime.Now.AddSeconds(5);

                for (var i = 0; i < deferredMessageCount; i++)
                {
                    EnqueueDeferredMessage(configuration, container.Resolver.Resolve<ITransportMessageFactory>(),
                        container.Resolver.Resolve<ISerializer>(), ignoreTillDate);

                    ignoreTillDate = ignoreTillDate.AddMilliseconds(millisecondsToDefer);
                }

                // add the extra time else there is no time to process message being returned
                var timeout = ignoreTillDate.AddSeconds(15);
                var timedOut = false;

                _log.Information(string.Format("[start wait] : now = '{0}'", DateTime.Now));

                // wait for the message to be returned from the deferred queue
                while (!module.AllMessagesHandled()
                       &&
                       !timedOut)
                {
                    Thread.Sleep(millisecondsToDefer);

                    timedOut = timeout < DateTime.Now;
                }

                _log.Information(string.Format("[end wait] : now = '{0}' / timeout = '{1}' / timed out = '{2}'",
                    DateTime.Now,
                    timeout, timedOut));

                _log.Information(string.Format("{0} of {1} deferred messages returned to the inbox.",
                    module.NumberOfDeferredMessagesReturned, deferredMessageCount));
                _log.Information(string.Format("{0} of {1} deferred messages handled.", module.NumberOfMessagesHandled,
                    deferredMessageCount));

                Assert.IsTrue(module.AllMessagesHandled(), "All the deferred messages were not handled.");

                Assert.IsTrue(configuration.Inbox.ErrorQueue.IsEmpty());
                Assert.IsNull(configuration.Inbox.DeferredQueue.GetMessage());
                Assert.IsNull(configuration.Inbox.WorkQueue.GetMessage());
            }

            AttemptDropQueues(queueManager, queueUriFormat);
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

            _log.Information(string.Format("[message enqueued] : name = '{0}' / deferred till date = '{1}'",
                command.Name,
                message.IgnoreTillDate));
        }

        private void ConfigureQueues(IQueueManager queueManager, IServiceBusConfiguration configuration,
            string queueUriFormat)
        {
            var inboxWorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work"));
            var inboxDeferredQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-deferred"));
            var errorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

            configuration.Inbox.WorkQueue = inboxWorkQueue;
            configuration.Inbox.DeferredQueue = inboxDeferredQueue;
            configuration.Inbox.ErrorQueue = errorQueue;

            inboxWorkQueue.AttemptDrop();
            inboxDeferredQueue.AttemptDrop();
            errorQueue.AttemptDrop();

            queueManager.CreatePhysicalQueues(configuration);

            inboxWorkQueue.AttemptPurge();
            inboxDeferredQueue.AttemptPurge();
            errorQueue.AttemptPurge();
        }
    }
}