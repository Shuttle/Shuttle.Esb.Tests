using System;
using Shuttle.Core.Pipelines;
using Shuttle.Core.PipelineTransaction;

namespace Shuttle.Esb.Tests
{
    public class OnStartRead : PipelineEvent
    {
    }

    public class ThroughputObserver : 
        IPipelineObserver<OnStartRead>,
        IPipelineObserver<OnGetMessage>,
        IPipelineObserver<OnAfterGetMessage>,
        IPipelineObserver<OnDeserializeTransportMessage>,
        IPipelineObserver<OnAfterDeserializeTransportMessage>,
        IPipelineObserver<OnDecompressMessage>,
        IPipelineObserver<OnAfterDecompressMessage>,
        IPipelineObserver<OnDecryptMessage>,
        IPipelineObserver<OnAfterDecryptMessage>,
        IPipelineObserver<OnDeserializeMessage>,
        IPipelineObserver<OnAfterDeserializeMessage>,
        IPipelineObserver<OnStartTransactionScope>,
        IPipelineObserver<OnAssessMessageHandling>,
        IPipelineObserver<OnAfterAssessMessageHandling>,
        IPipelineObserver<OnProcessIdempotenceMessage>,
        IPipelineObserver<OnHandleMessage>,
        IPipelineObserver<OnAfterHandleMessage>,
        IPipelineObserver<OnCompleteTransactionScope>,
        IPipelineObserver<OnSendDeferred>,
        IPipelineObserver<OnAfterSendDeferred>,
        IPipelineObserver<OnAcknowledgeMessage>,
        IPipelineObserver<OnAfterAcknowledgeMessage>
    {
        private readonly int _maximumDuration = 5;
        private readonly bool _logDetails = false;
        private DateTime _startDate;
        private DateTime _offsetDate;
        private DateTime _handleStartDate;
        private DateTime _readStartDate;

        public void Execute(OnStartRead pipelineEvent)
        {
            _startDate = DateTime.Now;
            _readStartDate = _startDate;

            if (_logDetails)
            {
                Console.WriteLine($"[start] : started = '{_offsetDate.Date:HH:mm:ss.fff}'");
            }
        }

        public void Execute(OnGetMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        private void Start(IPipelineEvent pipelineEvent)
        {
            _offsetDate = DateTime.Now;

            if (_logDetails)
            {
                Console.WriteLine($"[{pipelineEvent.Name}] : started = '{_offsetDate.Date:HH:mm:ss.fff}'");
            }
        }

        public void Execute(OnAfterGetMessage pipelineEvent)
        {
            Stop(pipelineEvent);
        }

        private void Stop(IPipelineEvent pipelineEvent)
        {
            var duration = (int)(DateTime.Now - _offsetDate).TotalMilliseconds;

            if (_logDetails)
            {
                Console.WriteLine($"[{pipelineEvent.Name}] : stopped = '{_offsetDate.Date:HH:mm:ss.fff}' / duration = '{duration}'");
            }

            if (duration > _maximumDuration)
            {
                Console.WriteLine($"[maximum duration exceeded] : took {duration} ms to get to '{pipelineEvent.Name}'");
            }
        }

        public void Execute(OnDeserializeTransportMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnAfterDeserializeTransportMessage pipelineEvent)
        {
            Stop(pipelineEvent);
        }

        public void Execute(OnDecompressMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnAfterDecompressMessage pipelineEvent)
        {
            Stop(pipelineEvent);
        }

        public void Execute(OnDecryptMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnAfterDecryptMessage pipelineEvent)
        {
            Stop(pipelineEvent);
        }

        public void Execute(OnDeserializeMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnAfterDeserializeMessage pipelineEvent)
        {
            Stop(pipelineEvent);

            _handleStartDate = DateTime.Now;
        }

        public void Execute(OnStartTransactionScope pipelineEvent)
        {
            if (_logDetails)
            {
                Console.WriteLine(
                    $"[read-done] : '{_offsetDate.Date:HH:mm:ss.fff}' /  duration = {(int) (DateTime.Now - _readStartDate).TotalMilliseconds} ms");
            }

            Start(pipelineEvent);
        }

        public void Execute(OnAssessMessageHandling pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnAfterAssessMessageHandling pipelineEvent)
        {
            Stop(pipelineEvent);
        }

        public void Execute(OnProcessIdempotenceMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnHandleMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnAfterHandleMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnCompleteTransactionScope pipelineEvent)
        {
            if (_logDetails)
            {
                Console.WriteLine($"[handle-done] : '{_offsetDate.Date:HH:mm:ss.fff}' / duration {(int)(DateTime.Now - _handleStartDate).TotalMilliseconds} ms");
                Console.WriteLine($"[end] : '{_offsetDate.Date:HH:mm:ss.fff}'");
            }
        }

        public void Execute(OnSendDeferred pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnAfterSendDeferred pipelineEvent)
        {
            Stop(pipelineEvent);
        }

        public void Execute(OnAcknowledgeMessage pipelineEvent)
        {
            Start(pipelineEvent);
        }

        public void Execute(OnAfterAcknowledgeMessage pipelineEvent)
        {
            Stop(pipelineEvent);
        }
    }
}