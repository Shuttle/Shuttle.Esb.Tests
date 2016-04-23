using System.Threading;
using Shuttle.Core.Infrastructure;

namespace Shuttle.Esb.Tests
{
	public class ConcurrentHandler : IMessageHandler<ConcurrentCommand>
	{
		public void ProcessMessage(IHandlerContext<ConcurrentCommand> context)
		{
			Log.Debug(string.Format("[processing message] : index = {0}", context.Message.MessageIndex));

			Thread.Sleep(500);
		}

		public bool IsReusable
		{
			get { return true; }
		}
	}
}