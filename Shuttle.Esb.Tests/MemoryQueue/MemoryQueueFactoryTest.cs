using System;
using NUnit.Framework;

namespace Shuttle.Esb.Tests
{
	[TestFixture]
	public class MemoryQueueFactoryTest
	{
		private static IQueueFactory SUT()
		{
			return new MemoryQueueFactory();
		}

		[Test]
		public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
		{
			Assert.NotNull(SUT().Create(new Uri("memory://./inputqueue")));
		}
	}
}