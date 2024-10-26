using System.Threading.Tasks;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests;

public class InboxDeferredFeature :
    IPipelineObserver<OnAfterDeserializeTransportMessage>
{
    public InboxDeferredFeature(IPipelineFactory pipelineFactory)
    {
        Guard.AgainstNull(pipelineFactory).PipelineCreated += PipelineCreated;
    }

    public TransportMessage? TransportMessage { get; private set; }

    public async Task ExecuteAsync(IPipelineContext<OnAfterDeserializeTransportMessage> pipelineContext)
    {
        TransportMessage = Guard.AgainstNull(Guard.AgainstNull(pipelineContext).Pipeline.State.GetTransportMessage());

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private void PipelineCreated(object? sender, PipelineEventArgs e)
    {
        if (e.Pipeline.GetType() != typeof(InboxMessagePipeline))
        {
            return;
        }

        e.Pipeline.RegisterObserver(this);
    }
}