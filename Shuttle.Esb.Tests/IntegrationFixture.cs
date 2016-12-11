using log4net;
using Shuttle.Core.Infrastructure;
using Shuttle.Core.Log4Net;

namespace Shuttle.Esb.Tests
{
	public class IntegrationFixture : Fixture
	{
		protected override void FixtureSetUp()
		{
			Log.Assign(new Log4NetLog(LogManager.GetLogger(typeof (IntegrationFixture))));
		}

		protected static ServiceBusConfiguration DefaultConfiguration(bool isTransactional)
		{
			var configuration = new ServiceBusConfiguration
			{
				TransactionScope = new TransactionScopeConfiguration
				{
					Enabled = isTransactional
				}
			};

			return configuration;
		}

		protected void AttemptDropQueues(string queueUriFormat)
		{
			using (var queueManager = GetQueueManager())
			{
				queueManager.GetQueue(string.Format(queueUriFormat, "test-worker-work")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-work")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-deferred")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-error")).AttemptDrop();
			}
		}

	    protected DefaultComponentContainer GetComponentContainer(ServiceBusConfiguration configuration)
	    {
            var container = new DefaultComponentContainer();

            var defaultConfigurator = new DefaultConfigurator(container);

            defaultConfigurator.RegisterComponents(configuration);

            container.Resolve<IQueueManager>().ScanForQueueFactories();

	        return container;
	    }

	    protected static QueueManager GetQueueManager()
	    {
	        var queueManager = new QueueManager(new DefaultUriResolver());

            queueManager.ScanForQueueFactories();

	        return queueManager;
	    }
	}
}