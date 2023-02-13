using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class ReceivePipelineHandler : IMessageHandler<ReceivePipelineCommand>
	{
		public Task ProcessMessage(IHandlerContext<ReceivePipelineCommand> context)
		{
			return Task.CompletedTask;
		}
	}
}