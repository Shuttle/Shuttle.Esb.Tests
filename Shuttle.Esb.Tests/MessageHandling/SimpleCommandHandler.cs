using System;
using System.Threading;
using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class SimpleCommandHandler : IMessageHandler<SimpleCommand>
	{
		public Task ProcessMessage(IHandlerContext<SimpleCommand> context)
		{
			Console.WriteLine($"[SimpleCommandHandler:SimpleCommand (thread {Thread.CurrentThread.ManagedThreadId})] : name = '{context.Message.Name}' / context = '{context.Message.Context}'");

			return Task.CompletedTask;
		}
	}
}