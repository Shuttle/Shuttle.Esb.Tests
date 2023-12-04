using System;
using NUnit.Framework;

namespace Shuttle.Esb.Tests
{
    [TestFixture]
    public class TransientStreamFactoryTest
    {
        [Test]
        public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
        {
            Assert.NotNull(new TransientStreamFactory().Create(new Uri("transient-stream://./input")));
        }
    }
}