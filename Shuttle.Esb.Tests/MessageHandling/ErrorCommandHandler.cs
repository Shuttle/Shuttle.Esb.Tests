using System;

namespace Shuttle.Esb.Tests
{
	public class ErrorCommandHandler : IMessageHandler<ErrorCommand>
	{
		public void ProcessMessage(IHandlerContext<ErrorCommand> context)
		{
			throw new ApplicationException("[testing expection handling]");
		}

		public bool IsReusable
		{
			get { return true; }
		}
	}
}