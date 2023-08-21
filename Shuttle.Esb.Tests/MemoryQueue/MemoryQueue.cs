using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.Tests
{
    public class MemoryQueue : IQueue, ICreateQueue, IPurgeQueue
    {
        internal const string Scheme = "memory";

        private static readonly object Lock = new object();

        private static readonly Dictionary<string, Dictionary<int, MemoryQueueItem>> _queues =
            new Dictionary<string, Dictionary<int, MemoryQueueItem>>();

        private readonly List<int> _unacknowledgedMessageIds = new List<int>();
        private int _itemId;

        public MemoryQueue(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            if (!uri.Scheme.Equals(Scheme, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new InvalidSchemeException(Scheme, uri.ToString());
            }

            var builder = new UriBuilder(uri);

            if (uri.Host.Equals("."))
            {
                builder.Host = Environment.MachineName.ToLower();
            }

            if (uri.LocalPath == "/")
            {
                builder.Path = "/default";
            }

            Uri = new QueueUri(builder.Uri);

            if (Uri.Uri.Host != Environment.MachineName.ToLower())
            {
                throw new UriFormatException(string.Format(Resources.UriFormatException,
                    $"memory://{{.|{Environment.MachineName.ToLower()}}}/{{name}}", uri));
            }

            Create().GetAwaiter().GetResult();
        }

        public async Task Create()
        {
            OperationStarting.Invoke(this, new OperationEventArgs("Create"));

            if (!_queues.ContainsKey(Uri.ToString()))
            {
                _queues.Add(Uri.ToString(), new Dictionary<int, MemoryQueueItem>());
            }

            OperationCompleted.Invoke(this, new OperationEventArgs("Create"));

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public async Task Purge()
        {
            OperationStarting.Invoke(this, new OperationEventArgs("Purge"));

            lock (Lock)
            {
                _queues[Uri.ToString()].Clear();
            }

            OperationCompleted.Invoke(this, new OperationEventArgs("Purge"));

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public QueueUri Uri { get; }
        public bool IsStream => false;

        public async ValueTask<bool> IsEmpty()
        {
            bool result;

            lock (Lock)
            {
                result = _queues[Uri.ToString()].Count == 0;
            }

            return await new ValueTask<bool>(result).ConfigureAwait(false);
        }

        public async Task Enqueue(TransportMessage transportMessage, Stream stream)
        {
            lock (Lock)
            {
                _itemId++;

                _queues[Uri.ToString()].Add(_itemId, new MemoryQueueItem(_itemId, transportMessage.MessageId, stream.Copy()));
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public async Task<ReceivedMessage> GetMessage()
        {
            ReceivedMessage result = null;

            lock (Lock)
            {
                var queue = _queues[Uri.ToString()];

                var index = 0;

                while (index < queue.Count)
                {
                    var pair = queue.ElementAt(index);

                    if (!_unacknowledgedMessageIds.Contains(pair.Value.ItemId))
                    {
                        _unacknowledgedMessageIds.Add(pair.Value.ItemId);

                        result = new ReceivedMessage(pair.Value.Stream, pair.Value.ItemId);

                        break;
                    }

                    index++;
                }
            }

            return await Task.FromResult(result).ConfigureAwait(false);
        }

        public async Task Acknowledge(object acknowledgementToken)
        {
            var itemId = (int)acknowledgementToken;

            lock (Lock)
            {
                var queue = _queues[Uri.ToString()];

                if (!queue.ContainsKey(itemId) || !_unacknowledgedMessageIds.Contains(itemId))
                {
                    return;
                }

                if (queue.ContainsKey(itemId))
                {
                    queue.Remove(itemId);
                }

                _unacknowledgedMessageIds.Remove(itemId);
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public async Task Release(object acknowledgementToken)
        {
            var itemId = (int)acknowledgementToken;

            lock (Lock)
            {
                var queue = _queues[Uri.ToString()];

                if (!queue.ContainsKey(itemId) || !_unacknowledgedMessageIds.Contains(itemId))
                {
                    return;
                }

                if (queue.ContainsKey(itemId))
                {
                    var message = queue[itemId];

                    queue.Remove(itemId);

                    queue.Add(itemId, message);
                }

                _unacknowledgedMessageIds.Remove(itemId);
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public event EventHandler<MessageEnqueuedEventArgs> MessageEnqueued = delegate
        {
        };

        public event EventHandler<MessageAcknowledgedEventArgs> MessageAcknowledged = delegate
        {
        };

        public event EventHandler<MessageReleasedEventArgs> MessageReleased = delegate
        {
        };

        public event EventHandler<MessageReceivedEventArgs> MessageReceived = delegate
        {
        };

        public event EventHandler<OperationEventArgs> OperationStarting = delegate
        {
        };

        public event EventHandler<OperationEventArgs> OperationCompleted = delegate
        {
        };
    }

    internal class MemoryQueueItem
    {
        public MemoryQueueItem(int index, Guid messageId, Stream stream)
        {
            ItemId = index;
            MessageId = messageId;
            Stream = stream;
        }

        public int ItemId { get; }
        public Guid MessageId { get; }
        public Stream Stream { get; }
    }
}