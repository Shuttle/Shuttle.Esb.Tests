using System;
using System.Threading.Tasks;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests
{
	public class DeferredMessageFeature :
		IPipelineFeature,
		IPipelineObserver<OnAfterHandleMessage>,
		IPipelineObserver<OnAfterProcessDeferredMessage>
	{
		private readonly object _lock = new object();
	    private readonly int _deferredMessageCount;

	    public DeferredMessageFeature(int deferredMessageCount)
		{
            _deferredMessageCount = deferredMessageCount;
		}

		public int NumberOfDeferredMessagesReturned { get; private set; }
		public int NumberOfMessagesHandled { get; private set; }

		private void PipelineCreated(object sender, PipelineEventArgs e)
		{
		    if (e.Pipeline.GetType() != typeof (InboxMessagePipeline) && e.Pipeline.GetType() != typeof (DeferredMessagePipeline))
			{
				return;
			}

			e.Pipeline.RegisterObserver(this);
		}

		public async Task Execute(OnAfterHandleMessage pipelineEvent)
		{
			Console.WriteLine("[OnAfterHandleMessage]");

			lock (_lock)
			{
				NumberOfMessagesHandled++;
			}

			await Task.CompletedTask.ConfigureAwait(false);
		}

		public async Task Execute(OnAfterProcessDeferredMessage pipelineEvent)
		{
			Console.WriteLine(
				$"[OnAfterProcessDeferredMessage] : deferred message returned = '{pipelineEvent.Pipeline.State.GetDeferredMessageReturned()}'");

			if (pipelineEvent.Pipeline.State.GetDeferredMessageReturned())
			{
				lock (_lock)
				{
					NumberOfDeferredMessagesReturned++;
				}
			}

			await Task.CompletedTask.ConfigureAwait(false);
		}

		public bool AllMessagesHandled()
		{
			return NumberOfMessagesHandled == _deferredMessageCount;
		}

		public void Assign(IPipelineFactory pipelineFactory)
		{
			Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory));

			pipelineFactory.PipelineCreated += PipelineCreated;
		}
	}
}