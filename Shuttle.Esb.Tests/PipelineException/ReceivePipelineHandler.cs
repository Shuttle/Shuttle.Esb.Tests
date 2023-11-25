using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class ReceivePipelineHandler : IMessageHandler<ReceivePipelineCommand>
	{
		public Task ProcessMessageAsync(IHandlerContext<ReceivePipelineCommand> context)
		{
			return Task.CompletedTask;
		}

		public void ProcessMessage(IHandlerContext<ReceivePipelineCommand> context)
		{
		}
	}
}