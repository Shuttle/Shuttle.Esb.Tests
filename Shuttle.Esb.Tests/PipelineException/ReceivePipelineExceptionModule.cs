using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;

namespace Shuttle.Esb.Tests
{
	public class ReceivePipelineExceptionModule :
		IPipelineObserver<OnGetMessage>,
		IPipelineObserver<OnAfterGetMessage>,
		IPipelineObserver<OnDeserializeTransportMessage>,
		IPipelineObserver<OnAfterDeserializeTransportMessage>
	{
	    private readonly IServiceBusConfiguration _serviceBusConfiguration;
	    private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1,1);

		private readonly List<ExceptionAssertion> _assertions = new List<ExceptionAssertion>();
		private string _assertionName;
		private volatile bool _failed;
		private int _pipelineCount;

		public ReceivePipelineExceptionModule(IServiceBusConfiguration serviceBusConfiguration, IPipelineFactory pipelineFactory)
		{
		    Guard.AgainstNull(serviceBusConfiguration, nameof(serviceBusConfiguration));
		    Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory));

		    pipelineFactory.PipelineCreated += PipelineCreated;
		    pipelineFactory.PipelineReleased += PipelineReleased;
		    pipelineFactory.PipelineObtained += PipelineObtained;

			_serviceBusConfiguration = serviceBusConfiguration;

            AddAssertion("OnGetMessage");
            AddAssertion("OnAfterGetMessage");
            AddAssertion("OnDeserializeTransportMessage");
            AddAssertion("OnAfterDeserializeTransportMessage");
		}

		private void PipelineObtained(object sender, PipelineEventArgs e)
		{
			_pipelineCount += 1;
			_assertionName = string.Empty;

			Console.WriteLine($"[pipeline obtained] : count = {_pipelineCount}");
		}

		private void PipelineCreated(object sender, PipelineEventArgs e)
		{
		    var fullName = e.Pipeline.GetType().FullName;

		    if (fullName != null && !fullName.Equals(typeof (InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase))
			{
				return;
			}

			e.Pipeline.RegisterObserver(this);
		}

		private void ThrowException(string name)
		{
			lock (Lock)
			{
				_assertionName = name;

				var assertion = GetAssertion(_assertionName);

				if (assertion.HasRun)
				{
					return;
				}

				throw new AssertionException($"Testing assertion for '{name}'.");
			}
		}

		private void AddAssertion(string name)
		{
			lock (Lock)
			{
				_assertions.Add(new ExceptionAssertion(name));

				Console.WriteLine($"[added] : assertion = '{name}'.");
			}
		}

		private async void PipelineReleased(object sender, PipelineEventArgs e)
		{
			if (string.IsNullOrEmpty(_assertionName))
			{
				return;
			}

			await Lock.WaitAsync().ConfigureAwait(false);

			try
			{
				var assertion = GetAssertion(_assertionName);

				if (assertion == null || assertion.HasRun)
				{
					return;
				}

				Console.WriteLine($"[invoking] : assertion = '{assertion.Name}'.");

				try
				{
					// IsEmpty does not work for prefetch queues
					var receivedMessage = await _serviceBusConfiguration.Inbox.WorkQueue.GetMessage().ConfigureAwait(false);

					Assert.IsNotNull(receivedMessage);

					await _serviceBusConfiguration.Inbox.WorkQueue.Release(receivedMessage.AcknowledgementToken).ConfigureAwait(false);
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.AllMessages());

					_failed = true;
				}

				assertion.MarkAsRun();

				Console.WriteLine($"[invoke complete] : assertion = '{assertion.Name}'.");
			}
            finally
			{
				Lock.Release();
			}
		}

		private ExceptionAssertion GetAssertion(string name)
		{
			return _assertions.Find(item => item.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase));
		}

		public bool ShouldWait()
		{
			lock (Lock)
			{
				return !_failed && _assertions.Find(item => !item.HasRun) != null;
			}
		}

		public async Task Execute(OnGetMessage pipelineEvent1)
		{
			ThrowException("OnGetMessage");

			await Task.CompletedTask.ConfigureAwait(false);
		}

		public async Task Execute(OnAfterGetMessage pipelineEvent)
		{
			ThrowException("OnAfterGetMessage");

			await Task.CompletedTask.ConfigureAwait(false);
		}

        public async Task Execute(OnDeserializeTransportMessage pipelineEvent1)
		{
			ThrowException("OnDeserializeTransportMessage");

			await Task.CompletedTask.ConfigureAwait(false);
		}

        public async Task Execute(OnAfterDeserializeTransportMessage pipelineEvent1)
		{
			ThrowException("OnAfterDeserializeTransportMessage");

			await Task.CompletedTask.ConfigureAwait(false);
		}
    }
}