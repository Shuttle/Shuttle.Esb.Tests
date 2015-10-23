using Shuttle.Core.Infrastructure;
using Shuttle.Core.Infrastructure.Log4Net;
using Shuttle.ESB.Core;
using log4net;

namespace Shuttle.ESB.Tests
{
	public class IntegrationFixture : Fixture
	{
		protected IServiceBus GetServiceBus()
		{
			return ServiceBus
				.Create()
				.Start();
		}

		protected override void FixtureSetUp()
		{
			Log.Assign(new Log4NetLog(LogManager.GetLogger(typeof(IntegrationFixture))));
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

			configuration.QueueManager.ScanForQueueFactories();

			return configuration;
		}

		protected void AttemptDropQueues(string queueUriFormat)
		{
			using (var queueManager = new QueueManager())
			{
				queueManager.ScanForQueueFactories();

				queueManager.GetQueue(string.Format(queueUriFormat, "test-worker-work")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-work")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-distributor-control")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-work")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-inbox-deferred")).AttemptDrop();
				queueManager.GetQueue(string.Format(queueUriFormat, "test-error")).AttemptDrop();
			}
		}
	}
}