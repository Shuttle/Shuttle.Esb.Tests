using Shuttle.Core.Logging;

namespace Shuttle.Esb.Tests
{
	public class SimpleCommandHandler : IMessageHandler<SimpleCommand>
	{
		private readonly ILog _log;

		public SimpleCommandHandler()
		{
			_log = Log.For(this);
		}

		public void ProcessMessage(IHandlerContext<SimpleCommand> context)
		{
			_log.Trace($"[executed] : name = '{context.Message.Name}'");
		}
	}
}