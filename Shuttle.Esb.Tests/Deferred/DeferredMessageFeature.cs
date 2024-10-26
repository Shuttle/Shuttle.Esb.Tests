using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests;

public class DeferredMessageFeature :
    IPipelineObserver<OnAfterHandleMessage>,
    IPipelineObserver<OnAfterProcessDeferredMessage>
{
    private readonly int _deferredMessageCount;
    private readonly object _lock = new();
    private readonly ILogger<DeferredMessageFeature> _logger;

    public DeferredMessageFeature(IOptions<MessageCountOptions> options, ILogger<DeferredMessageFeature> logger, IPipelineFactory pipelineFactory)
    {
        Guard.AgainstNull(pipelineFactory).PipelineCreated += PipelineCreated;

        _logger = Guard.AgainstNull(logger);
        _deferredMessageCount = Guard.AgainstNull(Guard.AgainstNull(options).Value).MessageCount;
    }

    public int NumberOfDeferredMessagesReturned { get; private set; }
    public int NumberOfMessagesHandled { get; private set; }

    public async Task ExecuteAsync(IPipelineContext<OnAfterHandleMessage> pipelineContext)
    {
        _logger.LogInformation("[OnAfterHandleMessage]");

        lock (_lock)
        {
            NumberOfMessagesHandled++;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<OnAfterProcessDeferredMessage> pipelineContext)
    {
        _logger.LogInformation($"[OnAfterProcessDeferredMessage] : deferred message returned = '{pipelineContext.Pipeline.State.GetDeferredMessageReturned()}'");

        if (!pipelineContext.Pipeline.State.GetDeferredMessageReturned())
        {
            return;
        }

        lock (_lock)
        {
            NumberOfDeferredMessagesReturned++;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public bool AllMessagesHandled()
    {
        return NumberOfMessagesHandled == _deferredMessageCount;
    }

    private void PipelineCreated(object? sender, PipelineEventArgs e)
    {
        if (e.Pipeline.GetType() != typeof(InboxMessagePipeline) && e.Pipeline.GetType() != typeof(DeferredMessagePipeline))
        {
            return;
        }

        e.Pipeline.RegisterObserver(this);
    }
}