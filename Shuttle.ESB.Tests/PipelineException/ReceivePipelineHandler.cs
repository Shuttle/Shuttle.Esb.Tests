using Shuttle.ESB.Core;

namespace Shuttle.ESB.Tests
{
	public class ReceivePipelineHandler : IMessageHandler<ReceivePipelineCommand>
	{
		public void ProcessMessage(IHandlerContext<ReceivePipelineCommand> context)
		{
		}

		public bool IsReusable
		{
			get { return true; }
		}
	}
}