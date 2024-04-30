using System;
using NUnit.Framework;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.Tests
{
	[TestFixture]
	public class TransientQueueFactoryTest
	{
		[Test]
		public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
		{
			Assert.NotNull(new TransientQueueFactory(new DefaultCancellationTokenSource()).Create(new Uri("transient-queue://./inputqueue")));
		}
	}
}