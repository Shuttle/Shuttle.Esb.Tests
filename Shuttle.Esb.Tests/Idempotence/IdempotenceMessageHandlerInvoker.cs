using System;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests
{
    internal class IdempotenceMessageHandlerInvoker : IMessageHandlerInvoker
    {
        private readonly IdempotenceCounter _counter = new IdempotenceCounter();
        private readonly Type _type = typeof (IdempotenceCommand);

        public int ProcessedCount => _counter.ProcessedCount;

        public MessageHandlerInvokeResult Invoke(IPipelineEvent pipelineEvent)
        {
            Guard.AgainstNull(pipelineEvent, "pipelineEvent");

            var state = pipelineEvent.Pipeline.State;
            var message = state.GetMessage();

            if (message.GetType() != _type)
            {
                throw new Exception("Can only handle type of 'IdempotenceCommand'.");
            }

            _counter.Processed();

            return MessageHandlerInvokeResult.InvokedHandler(null);
        }
    }
}