using System;
using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class ErrorCommandHandler : 
		IMessageHandler<ErrorCommand>,
		IAsyncMessageHandler<ErrorCommand>
	{
		public async Task ProcessMessageAsync(IHandlerContext<ErrorCommand> context)
		{
			ProcessMessage(context);

			await Task.CompletedTask.ConfigureAwait(false);
		}

		public void ProcessMessage(IHandlerContext<ErrorCommand> context)
		{
			throw new ApplicationException("[testing exception handling]");
        }
	}
}