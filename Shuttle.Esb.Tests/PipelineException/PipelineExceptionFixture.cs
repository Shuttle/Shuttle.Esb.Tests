using System;
using System.Threading;
using Castle.Windsor;
using NUnit.Framework;
using Shuttle.Core.Castle;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
	public class PipelineExceptionFixture : IntegrationFixture
	{
		protected void TestExceptionHandling(string queueUriFormat)
		{
			var configuration = DefaultConfiguration(true, 1);

            var container = new WindsorComponentContainer(new WindsorContainer());

            var configurator = new ServiceBusConfigurator(container);

		    configurator.DontRegister<ReceivePipelineExceptionModule>();

            configurator.RegisterComponents(configuration);

            var module = new ReceivePipelineExceptionModule(configuration);

            container.Register(module.GetType(), module);

            var queueManager = container.Resolve<IQueueManager>();
		    IQueue inboxWorkQueue;

		        inboxWorkQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work"));
		        var inboxErrorQueue = queueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

		        configuration.Inbox =
		            new InboxQueueConfiguration
		            {
		                WorkQueue = inboxWorkQueue,
		                ErrorQueue = inboxErrorQueue,
		                DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
		                DurationToIgnoreOnFailure = new[] {TimeSpan.FromMilliseconds(5)},
		                MaximumFailureCount = 100,
		                ThreadCount = 1
		            };


		        inboxWorkQueue.Drop();
		        inboxErrorQueue.Drop();

		        queueManager.CreatePhysicalQueues(configuration);
		
            var transportMessageFactory = container.Resolve<ITransportMessageFactory>();
            var serializer = container.Resolve<ISerializer>();

            using (var bus = ServiceBus.Create(container))
			{
				var message = transportMessageFactory.Create(new ReceivePipelineCommand(), c => c.WithRecipient(inboxWorkQueue));

				inboxWorkQueue.Enqueue(message, serializer.Serialize(message));

				Assert.IsFalse(inboxWorkQueue.IsEmpty());

				bus.Start();

				while (module.ShouldWait())
				{
					Thread.Sleep(10);
				}
			}
		}
	}
}