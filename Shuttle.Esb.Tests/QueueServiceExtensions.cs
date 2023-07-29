using System;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public static class QueueServiceExtensions
    {
        public static IQueueService WireQueueEvents(this IQueueService queueService)
        {
            Guard.AgainstNull(queueService, nameof(queueService));

            queueService.QueueCreated += (sender, args) =>
            {
                args.Queue.MessageAcknowledged += QueueOnMessageAcknowledged;
                args.Queue.MessageEnqueued += QueueOnMessageEnqueued;
                args.Queue.MessageReceived += QueueOnMessageReceived;
                args.Queue.MessageReleased += QueueOnMessageReleased;
                args.Queue.OperationCompleted += QueueOnOperationCompleted;
            };

            return queueService;
        }

        private static void QueueOnOperationCompleted(object sender, OperationCompletedEventArgs e)
        {
            var queue = (IQueue)sender;

            Console.WriteLine($"[{queue.Uri.Uri.Scheme}.OperationCompleted] : queue = '{queue.Uri.QueueName}' / operation name = '{e.Name}'");
        }

        private static void QueueOnMessageReleased(object sender, MessageReleasedEventArgs e)
        {
            var queue = (IQueue)sender;

            Console.WriteLine($"[{queue.Uri.Uri.Scheme}.MessageReleased] : queue = '{queue.Uri.QueueName}'");
        }

        private static void QueueOnMessageReceived(object sender, MessageReceivedEventArgs e)
        {
            var queue = (IQueue)sender;

            Console.WriteLine($"[{queue.Uri.Uri.Scheme}.MessageReceived] : queue = '{queue.Uri.QueueName}'");
        }

        private static void QueueOnMessageEnqueued(object sender, MessageEnqueuedEventArgs e)
        {
            var queue = (IQueue)sender;

            Console.WriteLine($"[{queue.Uri.Uri.Scheme}.MessageEnqueued] : queue = '{queue.Uri.QueueName}' / type = '{e.TransportMessage.MessageType}'");
        }

        private static void QueueOnMessageAcknowledged(object sender, MessageAcknowledgedEventArgs e)
        {
            var queue = (IQueue)sender;

            Console.WriteLine($"[{queue.Uri.Uri.Scheme}.MessageAcknowledged] : queue = '{queue.Uri.QueueName}'");
        }

    }
}