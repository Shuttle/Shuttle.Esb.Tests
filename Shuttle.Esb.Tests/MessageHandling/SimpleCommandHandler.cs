using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests;

public class SimpleCommandHandler : IMessageHandler<SimpleCommand>
{
    private readonly ILogger<SimpleCommandHandler> _logger;

    public SimpleCommandHandler(ILogger<SimpleCommandHandler> logger)
    {
        _logger = Guard.AgainstNull(logger);
    }

    public async Task ProcessMessageAsync(IHandlerContext<SimpleCommand> context)
    {
        _logger.LogInformation($"[SimpleCommandHandler:SimpleCommand (thread {Environment.CurrentManagedThreadId})] : name = '{context.Message.Name}' / context = '{context.Message.Context}'");

        await Task.CompletedTask.ConfigureAwait(false);
    }
}