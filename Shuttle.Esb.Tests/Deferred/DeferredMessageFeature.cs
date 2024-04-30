using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests
{
	public class DeferredMessageFeature :
		IPipelineObserver<OnAfterHandleMessage>,
		IPipelineObserver<OnAfterProcessDeferredMessage>
	{
		private readonly ILogger<DeferredMessageFeature> _logger;
		private readonly object _lock = new object();
	    private readonly int _deferredMessageCount;

	    public DeferredMessageFeature(IOptions<MessageCountOptions> options, ILogger<DeferredMessageFeature> logger, IPipelineFactory pipelineFactory)
		{
			Guard.AgainstNull(options, nameof(options));
			Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory)).PipelineCreated += PipelineCreated;

			_logger = Guard.AgainstNull(logger, nameof(logger));
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

		public void Execute(OnAfterHandleMessage pipelineEvent)
		{
			_logger.LogInformation("[OnAfterHandleMessage]");

			lock (_lock)
			{
				NumberOfMessagesHandled++;
			}
		}

        public async Task ExecuteAsync(OnAfterHandleMessage pipelineEvent)
		{
			Execute(pipelineEvent);

			await Task.CompletedTask.ConfigureAwait(false);
		}

		public void Execute(OnAfterProcessDeferredMessage pipelineEvent)
		{
			_logger.LogInformation($"[OnAfterProcessDeferredMessage] : deferred message returned = '{pipelineEvent.Pipeline.State.GetDeferredMessageReturned()}'");

			if (!pipelineEvent.Pipeline.State.GetDeferredMessageReturned())
			{
				return;
			}

			lock (_lock)
			{
				NumberOfDeferredMessagesReturned++;
			}
		}

        public async Task ExecuteAsync(OnAfterProcessDeferredMessage pipelineEvent)
		{
			Execute(pipelineEvent);

			await Task.CompletedTask.ConfigureAwait(false);
		}

		public bool AllMessagesHandled()
		{
			return NumberOfMessagesHandled == _deferredMessageCount;
		}
	}
}