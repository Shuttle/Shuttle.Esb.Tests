using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests;

public class InboxConcurrencyFeature : IPipelineObserver<OnAfterGetMessage>
{
    private readonly List<DateTimeOffset> _datesAfterGetMessage = [];
    private readonly object _lock = new();
    private readonly ILogger<InboxConcurrencyFeature> _logger;
    private DateTimeOffset _firstDateAfterGetMessage = DateTimeOffset.MinValue;

    public InboxConcurrencyFeature(ILogger<InboxConcurrencyFeature> logger, IPipelineFactory pipelineFactory)
    {
        Guard.AgainstNull(pipelineFactory).PipelineCreated += PipelineCreated;

        _logger = Guard.AgainstNull(logger);
    }

    public int OnAfterGetMessageCount => _datesAfterGetMessage.Count;

    public async Task ExecuteAsync(IPipelineContext<OnAfterGetMessage> pipelineContext)
    {
        lock (_lock)
        {
            var dateTime = DateTimeOffset.Now;

            if (_firstDateAfterGetMessage == DateTimeOffset.MinValue)
            {
                _firstDateAfterGetMessage = DateTimeOffset.Now;

                _logger.LogInformation("Offset date: {0:yyyy-MM-dd HH:mm:ss.fff}", _firstDateAfterGetMessage);
            }

            _datesAfterGetMessage.Add(dateTime);

            _logger.LogInformation("Dequeued date: {0:yyyy-MM-dd HH:mm:ss.fff}", dateTime);
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public bool AllMessagesReceivedWithinTimespan(int msToComplete)
    {
        return
            _datesAfterGetMessage.All(
                dateTime => dateTime.Subtract(_firstDateAfterGetMessage) <=
                            TimeSpan.FromMilliseconds(msToComplete));
    }

    private void PipelineCreated(object? sender, PipelineEventArgs e)
    {
        var fullName = e.Pipeline.GetType().FullName;

        if (fullName != null && !fullName.Equals(typeof(InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase))
        {
            return;
        }

        e.Pipeline.AddObserver(this);
    }
}