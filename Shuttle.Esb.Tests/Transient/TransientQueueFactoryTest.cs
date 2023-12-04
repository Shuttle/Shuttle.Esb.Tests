using System;
using NUnit.Framework;

namespace Shuttle.Esb.Tests
{
	[TestFixture]
	public class TransientQueueFactoryTest
	{
		[Test]
		public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
		{
			Assert.NotNull(new TransientQueueFactory().Create(new Uri("transient-queue://./inputqueue")));
		}
	}
}