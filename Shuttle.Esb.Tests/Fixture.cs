using System;
using System.Transactions;
using NUnit.Framework;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
	[TestFixture]
	public abstract class Fixture
	{
		[SetUp]
		public void SetUpTest()
		{
			Serializer = new DefaultSerializer();

			TestSetUp();
		}

		[TearDown]
		protected virtual void TearDownTest()
		{
		}

		public ISerializer Serializer { get; private set; }

		public ServiceBusConfiguration CreateMemoryConfiguration()
		{
			MemoryQueue.Clear();

		    return new ServiceBusConfiguration
			{
				Inbox =
					new InboxQueueConfiguration
					{
						WorkQueue = CreateMemoryInboxWork(),
						ErrorQueue = CreateMemoryInboxError(),
						DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
						ThreadCount = 1
					},
				ControlInbox =
					new ControlInboxQueueConfiguration
					{
						WorkQueue = CreateMemoryControlInboxWork(),
						ErrorQueue = CreateMemoryControlInboxError(),
						DurationToSleepWhenIdle = new[] {TimeSpan.FromMilliseconds(5)},
						ThreadCount = 1
					}
			};
		}

		public IQueue CreateMemoryInboxWork()
		{
			return MemoryQueue.From("memory://./inbox_work");
		}

		public IQueue CreateMemoryInboxError()
		{
			return MemoryQueue.From("memory://./inbox_error");
		}

		public IQueue CreateMemoryControlInboxWork()
		{
			return MemoryQueue.From("memory://./control-inbox-work");
		}

		public IQueue CreateMemoryControlInboxError()
		{
			return MemoryQueue.From("memory://./control-inbox-error");
		}

		public IQueue CreateMemoryOutboxWork()
		{
			return MemoryQueue.From("memory://./outbox_work");
		}

		public IQueue CreateMemoryOutboxError()
		{
			return MemoryQueue.From("memory://./outbox_error");
		}

		[OneTimeSetUp]
		protected virtual void FixtureSetUp()
		{
		}

		protected virtual void TestSetUp()
		{
		}

		[OneTimeTearDown]
		protected virtual void FixtureTearDown()
		{
		}
	}
}