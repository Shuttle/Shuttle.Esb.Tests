using System;
using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class ConcurrentHandler : IMessageHandler<ConcurrentCommand>
	{
		public async Task ProcessMessage(IHandlerContext<ConcurrentCommand> context)
		{
			Console.WriteLine($"[processing message] : index = {context.Message.MessageIndex}");

			await Task.Delay(500).ConfigureAwait(false);
		}
	}
}