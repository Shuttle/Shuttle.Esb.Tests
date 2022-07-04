using System;
using System.Threading;

namespace Shuttle.Esb.Tests
{
	public class ConcurrentHandler : IMessageHandler<ConcurrentCommand>
	{
		public void ProcessMessage(IHandlerContext<ConcurrentCommand> context)
		{
			Console.WriteLine($"[processing message] : index = {context.Message.MessageIndex}");

			Thread.Sleep(500);
		}
	}
}