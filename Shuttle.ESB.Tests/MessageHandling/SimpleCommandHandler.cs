using Shuttle.Core.Infrastructure;
using Shuttle.ESB.Core;

namespace Shuttle.ESB.Tests
{
	public class SimpleCommandHandler : IMessageHandler<SimpleCommand>
	{
		private readonly ILog _log ;

		public SimpleCommandHandler()
		{
			_log = Log.For(this);
		}

		public void ProcessMessage(IHandlerContext<SimpleCommand> context)
		{
			_log.Trace(string.Format("[executed] : name = '{0}'", context.Message.Name));
		}

		public bool IsReusable
		{
			get { return true; }
		}
	}
}