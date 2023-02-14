using System;
using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class ErrorCommandHandler : IMessageHandler<ErrorCommand>
	{
		public Task ProcessMessage(IHandlerContext<ErrorCommand> context)
		{
			throw new ApplicationException("[testing exception handling]");
		}
	}
}