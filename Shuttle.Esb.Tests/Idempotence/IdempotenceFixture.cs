using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;

namespace Shuttle.Esb.Tests
{
	public class IdempotenceFixture : IntegrationFixture
	{
		protected void TestIdempotenceProcessing(IIdempotenceService idempotenceService, string queueUriFormat,
			bool isTransactional, bool enqueueUniqueMessages)
		{
			const int threadCount = 1;
			const int messageCount = 5;

			var configuration = GetInboxConfiguration(idempotenceService, queueUriFormat, threadCount, isTransactional);
			var padlock = new object();

			using (var bus = new ServiceBus(configuration))
			{
				if (enqueueUniqueMessages)
				{
					for (var i = 0; i < messageCount; i++)
					{
						var message = bus.CreateTransportMessage(new IdempotenceCommand(),
							c => c.WithRecipient(configuration.Inbox.WorkQueue));

						configuration.Inbox.WorkQueue.Enqueue(message, configuration.Serializer.Serialize(message));
					}
				}
				else
				{
					var message = bus.CreateTransportMessage(new IdempotenceCommand(),
						c => c.WithRecipient(configuration.Inbox.WorkQueue));

					for (var i = 0; i < messageCount; i++)
					{
						configuration.Inbox.WorkQueue.Enqueue(message, configuration.Serializer.Serialize(message));
					}
				}

				var idleThreads = new List<int>();

				bus.Events.ThreadWaiting += (sender, args) =>
				{
					lock (padlock)
					{
						if (idleThreads.Contains(Thread.CurrentThread.ManagedThreadId))
						{
							return;
						}

						idleThreads.Add(Thread.CurrentThread.ManagedThreadId);
					}
				};

				bus.Start();

				while (idleThreads.Count < threadCount)
				{
					Thread.Sleep(5);
				}

				Assert.IsNull(configuration.Inbox.ErrorQueue.GetMessage());
				Assert.IsNull(configuration.Inbox.WorkQueue.GetMessage());

				if (enqueueUniqueMessages)
				{
					Assert.AreEqual(messageCount,
						((IdempotenceMessageHandlerFactory) bus.Configuration.MessageHandlerFactory).ProcessedCount);
				}
				else
				{
					Assert.AreEqual(1, ((IdempotenceMessageHandlerFactory) bus.Configuration.MessageHandlerFactory).ProcessedCount);
				}
			}

			AttemptDropQueues(queueUriFormat);
		}

		private static ServiceBusConfiguration GetInboxConfiguration(IIdempotenceService idempotenceService,
			string queueUriFormat, int threadCount, bool isTransactional)
		{
			var configuration = DefaultConfiguration(isTransactional);

			configuration.MessageRouteProvider = new IdempotenceMessageRouteProvider();
			configuration.MessageHandlerFactory = new IdempotenceMessageHandlerFactory();
			configuration.IdempotenceService = idempotenceService;

			var inboxWorkQueue =
				configuration.QueueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work"));
			var errorQueue = configuration.QueueManager.GetQueue(string.Format(queueUriFormat, "test-error"));

			configuration.Inbox =
				new InboxQueueConfiguration
				{
					WorkQueue = inboxWorkQueue,
					ErrorQueue = errorQueue,
					DurationToIgnoreOnFailure = new[] {TimeSpan.FromMilliseconds(5)},
					DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
					ThreadCount = threadCount
				};

			inboxWorkQueue.AttemptDrop();
			errorQueue.AttemptDrop();

			configuration.QueueManager.CreatePhysicalQueues(configuration);

			inboxWorkQueue.AttemptPurge();
			errorQueue.AttemptPurge();

			return configuration;
		}
	}
}