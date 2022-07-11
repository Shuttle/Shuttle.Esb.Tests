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
            var fullName = e.Pipeline.GetType().FullName;

            if (fullName != null && !fullName.Equals(typeof(InboxMessagePipeline).FullName,
                    StringComparison.InvariantCultureIgnoreCase))
            {
                return;
            }

            e.Pipeline.RegisterObserver(this);
        }

        public void Assign(IPipelineFactory pipelineFactory)
        {
            Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory));

            pipelineFactory.PipelineCreated += PipelineCreated;
        }
    }
}