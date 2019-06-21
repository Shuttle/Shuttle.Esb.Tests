using System;
using System.Collections.Generic;
using System.Linq;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Esb.Tests
{
    public class InboxConcurrencyModule :
        IPipelineObserver<OnAfterGetMessage>
    {
        private readonly List<DateTime> _datesAfterGetMessage = new List<DateTime>();
        private readonly object _padlock = new object();
        private DateTime _firstDateAfterGetMessage = DateTime.MinValue;

        public int OnAfterGetMessageCount => _datesAfterGetMessage.Count;

        public void Execute(OnAfterGetMessage pipelineEvent)
        {
            lock (_padlock)
            {
                var dateTime = DateTime.Now;

                if (_firstDateAfterGetMessage == DateTime.MinValue)
                {
                    _firstDateAfterGetMessage = DateTime.Now;

                    Console.WriteLine("Offset date: {0:yyyy-MM-dd HH:mm:ss.fff}", _firstDateAfterGetMessage);
                }

                _datesAfterGetMessage.Add(dateTime);

                Console.WriteLine("Dequeued date: {0:yyyy-MM-dd HH:mm:ss.fff}", dateTime);
            }
        }

        private void PipelineCreated(object sender, PipelineEventArgs e)
        {
            var fullName = e.Pipeline.GetType().FullName;

            if (fullName != null && !fullName.Equals(typeof(InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase))
            {
                return;
            }

            e.Pipeline.RegisterObserver(this);
        }

        public bool AllMessagesReceivedWithinTimespan(int msToComplete)
        {
            return
                _datesAfterGetMessage.All(
                    dateTime => dateTime.Subtract(_firstDateAfterGetMessage) <=
                                TimeSpan.FromMilliseconds(msToComplete));
        }

        public void Assign(IPipelineFactory pipelineFactory)
        {
            Guard.AgainstNull(pipelineFactory, "pipelineFactory");

            pipelineFactory.PipelineCreated += PipelineCreated;
        }
    }
}