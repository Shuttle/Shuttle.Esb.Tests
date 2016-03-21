using Shuttle.Esb;

namespace Shuttle.Esb.Tests
{
	public class IdempotenceHandler : IMessageHandler<IdempotenceCommand>
	{
		private readonly IdempotenceCounter _counter;

		public IdempotenceHandler(IdempotenceCounter counter)
		{
			_counter = counter;
		}

		public void ProcessMessage(IHandlerContext<IdempotenceCommand> context)
		{
			context.Send(new NoHandlerCommand());

			_counter.Processed();
		}

		public int ProcessedCount
		{
			get { return _counter.ProcessedCount; }
		}

		public bool IsReusable {
			get { return true; }
		}
	}
}