namespace Shuttle.Esb.Tests
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