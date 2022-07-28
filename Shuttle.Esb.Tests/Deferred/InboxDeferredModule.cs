using System;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests
{
    public class InboxDeferredModule :
        IPipelineObserver<OnAfterDeserializeTransportMessage>
    {
        public TransportMessage TransportMessage { get; private set; }

        public void Execute(OnAfterDeserializeTransportMessage pipelineEvent)
        {
            TransportMessage = pipelineEvent.Pipeline.State.GetTransportMessage();
        }

        private void PipelineCreated(object sender, PipelineEventArgs e)
        {
            if (e.Pipeline.GetType() != typeof(InboxMessagePipeline))
            {
                return;
            }

            e.Pipeline.RegisterObserver(this);
        }

        public InboxDeferredModule(IPipelineFactory pipelineFactory)
        {
            Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory));

            pipelineFactory.PipelineCreated += PipelineCreated;
        }
    }
}