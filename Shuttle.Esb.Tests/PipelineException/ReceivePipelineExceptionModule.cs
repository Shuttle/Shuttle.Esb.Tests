using System;
using System.Collections.Generic;
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
	    private readonly IServiceBusConfiguration _configuration;
	    private static readonly object Lock = new object();

		private readonly List<ExceptionAssertion> _assertions = new List<ExceptionAssertion>();
		private string _assertionName;
		private volatile bool _failed;
		private int _pipelineCount;

		public ReceivePipelineExceptionModule(IServiceBusConfiguration configuration)
		{
		    Guard.AgainstNull(configuration, nameof(configuration));

            _configuration = configuration;

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

		private void PipelineReleased(object sender, PipelineEventArgs e)
		{
			if (string.IsNullOrEmpty(_assertionName))
			{
				return;
			}

			lock (Lock)
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
					var receivedMessage = _configuration.Inbox.WorkQueue.GetMessage();

					Assert.IsNotNull(receivedMessage);

                    _configuration.Inbox.WorkQueue.Release(receivedMessage.AcknowledgementToken);
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.AllMessages());

					_failed = true;
				}

				assertion.MarkAsRun();

				Console.WriteLine($"[invoke complete] : assertion = '{assertion.Name}'.");
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

		public void Execute(OnGetMessage pipelineEvent1)
		{
			ThrowException("OnGetMessage");
		}

		public void Execute(OnAfterGetMessage pipelineEvent)
		{
			ThrowException("OnAfterGetMessage");
		}

		public void Execute(OnDeserializeTransportMessage pipelineEvent1)
		{
			ThrowException("OnDeserializeTransportMessage");
		}

		public void Execute(OnAfterDeserializeTransportMessage pipelineEvent1)
		{
			ThrowException("OnAfterDeserializeTransportMessage");
		}

	    public void Assign(IPipelineFactory pipelineFactory)
	    {
            Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory));

            pipelineFactory.PipelineCreated += PipelineCreated;
            pipelineFactory.PipelineReleased += PipelineReleased;
            pipelineFactory.PipelineObtained += PipelineObtained;
        }
    }
}