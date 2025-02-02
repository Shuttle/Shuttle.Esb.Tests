using NUnit.Framework;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.Tests;

[TestFixture]
public class TransientStreamFactoryTest
{
    [Test]
    public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
    {
        Assert.That(new TransientStreamFactory(new DefaultCancellationTokenSource()).Create(new("transient-stream://./input")), Is.Not.Null);
    }
}