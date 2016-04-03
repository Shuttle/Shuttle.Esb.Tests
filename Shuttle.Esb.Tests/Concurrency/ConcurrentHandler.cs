using Shuttle.Core.Infrastructure;
using Shuttle.Esb;

namespace Shuttle.Esb.Tests
{
    public class ConcurrentHandler : IMessageHandler<ConcurrentCommand>
    {
        public void ProcessMessage(IHandlerContext<ConcurrentCommand> context)
        {
            Log.Debug(string.Format("[processing message] : index = {0}", context.Message.MessageIndex));

            System.Threading.Thread.Sleep(500);
        }

        public bool IsReusable
        {
            get { return true; }
        }
    }
}