using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.Tests
{
    public class TransientStream : IQueue, ICreateQueue, IPurgeQueue
    {
        private readonly CancellationToken _cancellationToken;
        internal const string Scheme = "transient-stream";

        private static readonly object Lock = new object();
        private static readonly Dictionary<string, Dictionary<int, TransientMessage>> Queues = new Dictionary<string, Dictionary<int, TransientMessage>>();
        private static int _itemId;

        private readonly List<int> _unacknowledgedMessageIds = new List<int>();

        public TransientStream(Uri uri, CancellationToken cancellationToken)
        {
            Guard.AgainstNull(uri, nameof(uri));

            if (!uri.Scheme.Equals(Scheme, StringComparison.InvariantCultureIgnoreCase))
            {
                throw new InvalidSchemeException(Scheme, uri.ToString());
            }

            _cancellationToken = cancellationToken;

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

            Create();
        }

        public void Create()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[create/cancelled]"));
                return;
            }

            Operation?.Invoke(this, new OperationEventArgs("[create/starting]"));

            if (!Queues.ContainsKey(Uri.ToString()))
            {
                Queues.Add(Uri.ToString(), new Dictionary<int, TransientMessage>());
            }

            Operation?.Invoke(this, new OperationEventArgs("[create/completed]"));
        }

        public async Task CreateAsync()
        {
            Create();

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public void Purge()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[purge/cancelled]"));
                return;
            }

            Operation?.Invoke(this, new OperationEventArgs("[purge/starting]"));

            lock (Lock)
            {
                Queues[Uri.ToString()].Clear();
            }

            Operation?.Invoke(this, new OperationEventArgs("[purge/completed]"));
        }

        public async Task PurgeAsync()
        {
            Purge();

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public QueueUri Uri { get; }
        public bool IsStream => true;

        public bool IsEmpty()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[is-empty/cancelled]"));
                return true;
            }

            lock (Lock)
            {
                return Queues[Uri.ToString()].Count == 0;
            }
        }

        public async ValueTask<bool> IsEmptyAsync()
        {
            return await new ValueTask<bool>(IsEmpty()).ConfigureAwait(false);
        }

        public void Enqueue(TransportMessage transportMessage, Stream stream)
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[enqueue/cancelled]"));
                return;
            }

            lock (Lock)
            {
                _itemId++;

                Queues[Uri.ToString()].Add(_itemId, new TransientMessage(_itemId, transportMessage, stream.Copy()));
            }

            MessageEnqueued?.Invoke(this, new MessageEnqueuedEventArgs(transportMessage, stream));
        }

        public async Task EnqueueAsync(TransportMessage transportMessage, Stream stream)
        {
            Enqueue(transportMessage, stream);

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public ReceivedMessage GetMessage()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[get-message/cancelled]"));
                return null;
            }

            ReceivedMessage result = null;

            lock (Lock)
            {
                var queue = Queues[Uri.ToString()];

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

            if (result != null)
            {
                MessageReceived?.Invoke(this, new MessageReceivedEventArgs(result));
            }

            return result;
        }

        public async Task<ReceivedMessage> GetMessageAsync()
        {
            return await Task.FromResult(GetMessage()).ConfigureAwait(false);
        }

        public void Acknowledge(object acknowledgementToken)
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[acknowledge/cancelled]"));
                return;
            }

            var itemId = (int)acknowledgementToken;

            lock (Lock)
            {
                var queue = Queues[Uri.ToString()];

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

            MessageAcknowledged?.Invoke(this, new MessageAcknowledgedEventArgs(acknowledgementToken));
        }

        public async Task AcknowledgeAsync(object acknowledgementToken)
        {
            Acknowledge(acknowledgementToken);

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public void Release(object acknowledgementToken)
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[release/cancelled]"));
                return;
            }

            var itemId = (int)acknowledgementToken;

            lock (Lock)
            {
                var queue = Queues[Uri.ToString()];

                if (!queue.ContainsKey(itemId) || !_unacknowledgedMessageIds.Contains(itemId))
                {
                    return;
                }

                _unacknowledgedMessageIds.Remove(itemId);
            }

            MessageReleased?.Invoke(this, new MessageReleasedEventArgs(acknowledgementToken));
        }

        public async Task ReleaseAsync(object acknowledgementToken)
        {
            Release(acknowledgementToken);

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public event EventHandler<MessageEnqueuedEventArgs> MessageEnqueued;
        public event EventHandler<MessageAcknowledgedEventArgs> MessageAcknowledged;
        public event EventHandler<MessageReleasedEventArgs> MessageReleased;
        public event EventHandler<MessageReceivedEventArgs> MessageReceived;
        public event EventHandler<OperationEventArgs> Operation;
    }
}