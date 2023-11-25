using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public class ConcurrentHandler :
        IMessageHandler<ConcurrentCommand>,
        IAsyncMessageHandler<ConcurrentCommand>
    {
        private readonly ILogger<ConcurrentHandler> _logger;

        public ConcurrentHandler(ILogger<ConcurrentHandler> logger)
        {
            _logger = Guard.AgainstNull(logger, nameof(logger));
        }

        public async Task ProcessMessageAsync(IHandlerContext<ConcurrentCommand> context)
        {
            await ProcessMessageAsync(context, false).ConfigureAwait(false);
        }

        public void ProcessMessage(IHandlerContext<ConcurrentCommand> context)
        {
            ProcessMessageAsync(context, true).GetAwaiter().GetResult();
        }

        private async Task ProcessMessageAsync(IHandlerContext<ConcurrentCommand> context, bool sync)
        {
            _logger.LogInformation($"[ConcurrentHandler:ConcurrentCommand] : index = {context.Message.MessageIndex}");

            if (sync)
            {
                Task.Delay(500).GetAwaiter().GetResult();
                ;
            }
            else
            {
                await Task.Delay(500).ConfigureAwait(false);
            }
        }
    }
}