using System;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
    public class InboxDeferredModule :
        IPipelineObserver<OnAfterDeserializeTransportMessage>
    {
        public InboxDeferredModule(IPipelineFactory pipelineFactory)
        {
            pipelineFactory.PipelineCreated += PipelineCreated;
        }

        public TransportMessage TransportMessage { get; private set; }

        public void Execute(OnAfterDeserializeTransportMessage pipelineEvent)
        {
            TransportMessage = pipelineEvent.Pipeline.State.GetTransportMessage();
        }

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
    }
}