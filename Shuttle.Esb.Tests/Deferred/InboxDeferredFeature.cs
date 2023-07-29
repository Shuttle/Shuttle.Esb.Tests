using System.Threading.Tasks;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests
{
    public class InboxDeferredFeature :
        IPipelineObserver<OnAfterDeserializeTransportMessage>
    {
        public TransportMessage TransportMessage { get; private set; }

        public async Task Execute(OnAfterDeserializeTransportMessage pipelineEvent)
        {
            TransportMessage = pipelineEvent.Pipeline.State.GetTransportMessage();

            await Task.CompletedTask.ConfigureAwait(false);
        }

        private void PipelineCreated(object sender, PipelineEventArgs e)
        {
            if (e.Pipeline.GetType() != typeof(InboxMessagePipeline))
            {
                return;
            }

            e.Pipeline.RegisterObserver(this);
        }

        public InboxDeferredFeature(IPipelineFactory pipelineFactory)
        {
            Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory));

            pipelineFactory.PipelineCreated += PipelineCreated;
        }
    }
}