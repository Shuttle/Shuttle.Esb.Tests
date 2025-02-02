using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests;

public static class QueueServiceExtensions
{
    private static readonly List<string> QueueUris =
    [
        "test-worker-work",
        "test-distributor-work",
        "test-distributor-control",
        "test-inbox-work",
        "test-inbox-deferred",
        "test-outbox-work",
        "test-error"
    ];

    public static async Task TryDropQueuesAsync(this IQueueService queueService, string queueUriFormat)
    {
        Guard.AgainstNull(queueService);
        Guard.AgainstNullOrEmptyString(queueUriFormat);

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
        Guard.AgainstNull(queueService);

        queueService.QueueCreated += (_, args) =>
        {
            args.Queue.MessageAcknowledged += (o, _) =>
            {
                var queue = (IQueue)o!;

                logger.LogInformation($"[{queue.Uri.Uri.Scheme}.MessageAcknowledged] : queue = '{queue.Uri.QueueName}'");
            };

            args.Queue.MessageEnqueued += (o, e) =>
            {
                var queue = (IQueue)o!;

                logger.LogInformation($"[{queue.Uri.Uri.Scheme}.MessageEnqueued] : queue = '{queue.Uri.QueueName}' / type = '{e.TransportMessage.MessageType}'");
            };

            args.Queue.MessageReceived += (o, _) =>
            {
                var queue = (IQueue)o!;

                logger.LogInformation($"[{queue.Uri.Uri.Scheme}.MessageReceived] : queue = '{queue.Uri.QueueName}'");
            };

            args.Queue.MessageReleased += (o, _) =>
            {
                var queue = (IQueue)o!;

                logger.LogInformation($"[{queue.Uri.Uri.Scheme}.MessageReleased] : queue = '{queue.Uri.QueueName}'");
            };

            args.Queue.Operation += (o, e) =>
            {
                var queue = (IQueue)o!;

                logger.LogInformation($"[{queue.Uri.Uri.Scheme}.Operation] : queue = '{queue.Uri.QueueName}' / operation name = '{e.Name}'");
            };

            logger.LogInformation($"[QueueCreated] : queue = '{args.Queue.Uri.QueueName}'");
        };

        queueService.QueueDisposing += (_, args) =>
        {
            logger.LogInformation($"[QueueDisposing] : queue = '{args.Queue.Uri.QueueName}'");
        };

        queueService.QueueDisposed += (_, args) =>
        {
            logger.LogInformation($"[QueueDisposed] : queue = '{args.Queue.Uri.QueueName}'");
        };

        return queueService;
    }
}