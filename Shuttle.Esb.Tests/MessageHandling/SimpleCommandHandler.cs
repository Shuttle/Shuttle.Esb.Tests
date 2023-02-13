using System;
using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class SimpleCommandHandler : IMessageHandler<SimpleCommand>
	{
		public Task ProcessMessage(IHandlerContext<SimpleCommand> context)
		{
			Console.WriteLine($"[executed] : name = '{context.Message.Name}'");

			return Task.CompletedTask;
		}
	}
}