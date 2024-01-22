using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
	public class SimpleCommandHandler : 
		IMessageHandler<SimpleCommand>,
		IAsyncMessageHandler<SimpleCommand>
	{
		private readonly ILogger<SimpleCommandHandler> _logger;

		public SimpleCommandHandler(ILogger<SimpleCommandHandler> logger)
		{
			_logger = Guard.AgainstNull(logger, nameof(logger));
		}

		public void ProcessMessage(IHandlerContext<SimpleCommand> context)
		{
			_logger.LogInformation($"[SimpleCommandHandler:SimpleCommand (thread {Thread.CurrentThread.ManagedThreadId})] : name = '{context.Message.Name}' / context = '{context.Message.Context}'");
		}

		public async Task ProcessMessageAsync(IHandlerContext<SimpleCommand> context)
		{
			ProcessMessage(context);

			await Task.CompletedTask.ConfigureAwait(false);
        }
    }
}