using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests
{
	public class DeferredMessageFeature :
		IPipelineObserver<OnAfterHandleMessage>,
		IPipelineObserver<OnAfterProcessDeferredMessage>
	{
		private readonly object _lock = new object();
	    private readonly int _deferredMessageCount;

	    public DeferredMessageFeature(IOptions<MessageCountOptions> options, IPipelineFactory pipelineFactory)
		{
			Guard.AgainstNull(options, nameof(options));
            Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory)).PipelineCreated += PipelineCreated;

            _deferredMessageCount = Guard.AgainstNull(options.Value, nameof(options.Value)).MessageCount; 
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
	}
}