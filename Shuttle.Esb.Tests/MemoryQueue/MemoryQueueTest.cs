using System;
using NUnit.Framework;

namespace Shuttle.Esb.Tests
{
	[TestFixture]
	public class MemoryQueueTest
	{
		[Test]
		public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
		{
			Assert.AreEqual(string.Format("memory://{0}/inputqueue", Environment.MachineName.ToLower()),
				new MemoryQueue(new Uri("memory://./inputqueue")).Uri.ToString());
		}

		[Test]
		public void Should_use_default_queue_name_for_empty_localpath()
		{
			Assert.AreEqual(string.Format("memory://{0}/default", Environment.MachineName.ToLower()),
				new MemoryQueue(new Uri("memory://.")).Uri.ToString());
		}

		[Test]
		public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_scheme()
		{
			Assert.Throws<InvalidSchemeException>(() => new MemoryQueue(new Uri("sql://./inputqueue")));
		}

		[Test]
		public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_format()
		{
			Assert.Throws<UriFormatException>(() => new MemoryQueue(new Uri("memory://notthismachinee")));
		}
	}
}