using System;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
	public class InboxDeferredModule :
		IPipelineModule,
		IPipelineObserver<OnAfterDeserializeTransportMessage>
	{
		public TransportMessage TransportMessage { get; private set; }

		private void PipelineCreated(object sender, PipelineEventArgs e)
		{
			if (
				!e.Pipeline.GetType()
					.FullName.Equals(typeof (InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase))
			{
				return;
			}

			e.Pipeline.RegisterObserver(this);
		}

		public void Execute(OnAfterDeserializeTransportMessage pipelineEvent)
		{
			TransportMessage = pipelineEvent.Pipeline.State.GetTransportMessage();
		}

	    public void Start(IPipelineFactory pipelineFactory)
	    {
	        pipelineFactory.PipelineCreated += PipelineCreated;
        }
	}
}