using System;
using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class SimpleCommandHandler : IMessageHandler<SimpleCommand>
	{
		public Task ProcessMessage(IHandlerContext<SimpleCommand> context)
		{
			Console.WriteLine($"[SimpleCommandHandler:SimpleCommand] : name = '{context.Message.Name}'");

			return Task.CompletedTask;
		}
	}
}