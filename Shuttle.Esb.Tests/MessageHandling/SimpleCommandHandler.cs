using System;

namespace Shuttle.Esb.Tests
{
	public class SimpleCommandHandler : IMessageHandler<SimpleCommand>
	{
		public void ProcessMessage(IHandlerContext<SimpleCommand> context)
		{
			Console.WriteLine($"[executed] : name = '{context.Message.Name}'");
		}
	}
}