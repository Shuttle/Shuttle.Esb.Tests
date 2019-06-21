using System.Threading;
using Shuttle.Core.Logging;

namespace Shuttle.Esb.Tests
{
	public class ConcurrentHandler : IMessageHandler<ConcurrentCommand>
	{
		public void ProcessMessage(IHandlerContext<ConcurrentCommand> context)
		{
			Log.Debug($"[processing message] : index = {context.Message.MessageIndex}");

			Thread.Sleep(500);
		}
	}
}