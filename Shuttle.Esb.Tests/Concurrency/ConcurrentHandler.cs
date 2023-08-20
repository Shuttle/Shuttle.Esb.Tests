using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
	public class ConcurrentHandler : IMessageHandler<ConcurrentCommand>
	{
		private readonly ILogger<ConcurrentHandler> _logger;

		public ConcurrentHandler(ILogger<ConcurrentHandler> logger)
		{
			_logger = Guard.AgainstNull(logger, nameof(logger));
		}

		public async Task ProcessMessage(IHandlerContext<ConcurrentCommand> context)
		{
			_logger.LogInformation($"[ConcurrentHandler:ConcurrentCommand] : index = {context.Message.MessageIndex}");

			await Task.Delay(500).ConfigureAwait(false);
		}
	}
}