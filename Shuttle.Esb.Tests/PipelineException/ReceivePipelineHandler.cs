using System.Threading.Tasks;

namespace Shuttle.Esb.Tests
{
	public class ReceivePipelineHandler : IMessageHandler<ReceivePipelineCommand>
	{
		public async Task ProcessMessageAsync(IHandlerContext<ReceivePipelineCommand> context)
		{
			await Task.CompletedTask.ConfigureAwait(false);
		}

		public void ProcessMessage(IHandlerContext<ReceivePipelineCommand> context)
		{
		}
	}
}