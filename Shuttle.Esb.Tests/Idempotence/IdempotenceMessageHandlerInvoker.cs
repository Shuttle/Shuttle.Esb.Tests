using System;
using System.Threading.Tasks;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    internal class IdempotenceMessageHandlerInvoker : IMessageHandlerInvoker
    {
        private readonly IdempotenceCounter _counter = new IdempotenceCounter();
        private readonly Type _type = typeof (IdempotenceCommand);

        public int ProcessedCount => _counter.ProcessedCount;

        public MessageHandlerInvokeResult Invoke(OnHandleMessage pipelineEvent)
        {
            var state = Guard.AgainstNull(pipelineEvent, nameof(pipelineEvent)).Pipeline.State;
            var message = Guard.AgainstNull(state.GetMessage(), StateKeys.Message);

            if (message.GetType() != _type)
            {
                throw new Exception("Can only handle type of 'IdempotenceCommand'.");
            }

            _counter.Processed();

            return MessageHandlerInvokeResult.InvokedHandler(null);
        }

        public async Task<MessageHandlerInvokeResult> InvokeAsync(OnHandleMessage pipelineEvent)
        {
            return await Task.FromResult(Invoke(pipelineEvent)).ConfigureAwait(false);
        }
    }
}