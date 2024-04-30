using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public static class QueueServiceExtensions
    {
        private static readonly List<string> QueueUris = new List<string>
        {
            "test-worker-work",
            "test-distributor-work",
            "test-distributor-control",
            "test-inbox-work",
            "test-inbox-deferred",
            "test-outbox-work",
            "test-error"
        };

        public static void TryDropQueues(this IQueueService queueService, string queueUriFormat)
        {
            TryDropQueuesAsync(queueService, queueUriFormat).GetAwaiter().GetResult();
        }

        public static async Task TryDropQueuesAsync(this IQueueService queueService, string queueUriFormat)
        {
            await TryDropQueuesAsync(queueService, queueUriFormat, false).ConfigureAwait(false);
        }

        private static async Task TryDropQueuesAsync(IQueueService queueService, string queueUriFormat, bool sync)
        {
            Guard.AgainstNull(queueService, nameof(queueService));
            Guard.AgainstNullOrEmptyString(queueUriFormat, nameof(queueUriFormat));

            foreach (var queueUri in QueueUris)
            {
                var uri = string.Format(queueUriFormat, queueUri);

                if (!queueService.Contains(uri))
                {
                    continue;
                }

                await queueService.Get(uri).TryDropAsync().ConfigureAwait(false);
            }
        }

        public static IQueueService WireQueueEvents(this IQueueService queueService, ILogger logger)
        {
            Guard.AgainstNull(queueService, nameof(queueService));

            queueService.QueueCreated += (sender, args) =>
            {
                args.Queue.MessageAcknowledged += (o, e) =>
                {
                    var queue = (IQueue)o;

                    logger.LogInformation($"[{queue.Uri.Uri.Scheme}.MessageAcknowledged] : queue = '{queue.Uri.QueueName}'");
                };

                args.Queue.MessageEnqueued += (o, e) =>
                {
                    var queue = (IQueue)o;

                    logger.LogInformation($"[{queue.Uri.Uri.Scheme}.MessageEnqueued] : queue = '{queue.Uri.QueueName}' / type = '{e.TransportMessage.MessageType}'");
                };

                args.Queue.MessageReceived += (o, e) =>
                {
                    var queue = (IQueue)o;

                    logger.LogInformation($"[{queue.Uri.Uri.Scheme}.MessageReceived] : queue = '{queue.Uri.QueueName}'");
                };

                args.Queue.MessageReleased += (o, e) =>
                {
                    var queue = (IQueue)o;

                    logger.LogInformation($"[{queue.Uri.Uri.Scheme}.MessageReleased] : queue = '{queue.Uri.QueueName}'");
                };

                args.Queue.Operation += (o, e) =>
                {
                    var queue = (IQueue)o;

                    logger.LogInformation($"[{queue.Uri.Uri.Scheme}.Operation] : queue = '{queue.Uri.QueueName}' / operation name = '{e.Name}'");
                };

                logger.LogInformation($"[QueueCreated] : queue = '{args.Queue.Uri.QueueName}'");
            };

            queueService.QueueDisposing += (sender, args) =>
            {
                logger.LogInformation($"[QueueDisposing] : queue = '{args.Queue.Uri.QueueName}'");
            };

            queueService.QueueDisposed += (sender, args) =>
            {
                logger.LogInformation($"[QueueDisposed] : queue = '{args.Queue.Uri.QueueName}'");
            };

            return queueService;
        }
    }
}