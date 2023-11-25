using System;
using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class ErrorCommandHandler : 
		IMessageHandler<ErrorCommand>,
		IAsyncMessageHandler<ErrorCommand>
	{
		public Task ProcessMessageAsync(IHandlerContext<ErrorCommand> context)
		{
			ProcessMessage(context);

			return Task.CompletedTask;
		}

		public void ProcessMessage(IHandlerContext<ErrorCommand> context)
		{
			throw new ApplicationException("[testing exception handling]");
        }
	}
}