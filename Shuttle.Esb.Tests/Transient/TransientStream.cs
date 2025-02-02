using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.Tests;

public class TransientStream : IQueue, ICreateQueue, IPurgeQueue
{
    internal const string Scheme = "transient-stream";

    private static readonly SemaphoreSlim Lock = new(1, 1);
    private static readonly Dictionary<string, Dictionary<int, TransientMessage>> Queues = new();
    private static int _itemId;
    private readonly CancellationToken _cancellationToken;

    private readonly List<int> _unacknowledgedMessageIds = new();

    public TransientStream(Uri uri, CancellationToken cancellationToken)
    {
        Guard.AgainstNull(uri);

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

        Uri = new(builder.Uri);

        if (Uri.Uri.Host != Environment.MachineName.ToLower())
        {
            throw new UriFormatException(string.Format(Resources.UriFormatException,
                $"memory://{{.|{Environment.MachineName.ToLower()}}}/{{name}}", uri));
        }

        CreateAsync().GetAwaiter().GetResult();
    }

    public async Task CreateAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[create/cancelled]"));
            return;
        }

        Operation?.Invoke(this, new("[create/starting]"));

        await Lock.WaitAsync(_cancellationToken).ConfigureAwait(false);

        try
        {
            if (!Queues.ContainsKey(Uri.ToString()))
            {
                Queues.Add(Uri.ToString(), new());
            }
        }
        finally
        {
            Lock.Release();
        }

        Operation?.Invoke(this, new("[create/completed]"));

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task PurgeAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[purge/cancelled]"));
            return;
        }

        Operation?.Invoke(this, new("[purge/starting]"));

        await Lock.WaitAsync(_cancellationToken).ConfigureAwait(false);

        try
        {
            Queues[Uri.ToString()].Clear();
        }
        finally
        {
            Lock.Release();
        }

        Operation?.Invoke(this, new("[purge/completed]"));
    }

    public QueueUri Uri { get; }
    public bool IsStream => true;

    public async ValueTask<bool> IsEmptyAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[is-empty/cancelled]"));
            return true;
        }

        await Lock.WaitAsync(_cancellationToken).ConfigureAwait(false);

        try
        {
            return Queues[Uri.ToString()].Count == 0;
        }
        finally
        {
            Lock.Release();
        }
    }

    public async Task EnqueueAsync(TransportMessage transportMessage, Stream stream)
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[enqueue/cancelled]"));
            return;
        }

        await Lock.WaitAsync(_cancellationToken).ConfigureAwait(false);

        try
        {
            _itemId++;

            Queues[Uri.ToString()].Add(_itemId, new(_itemId, transportMessage, await stream.CopyAsync().ConfigureAwait(false)));
        }
        finally
        {
            Lock.Release();
        }

        MessageEnqueued?.Invoke(this, new(transportMessage, stream));

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task<ReceivedMessage?> GetMessageAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[get-message/cancelled]"));
            return null;
        }

        ReceivedMessage? result = null;

        await Lock.WaitAsync(_cancellationToken).ConfigureAwait(false);

        try
        {
            var queue = Queues[Uri.ToString()];

            var index = 0;

            while (index < queue.Count)
            {
                var pair = queue.ElementAt(index);

                if (!_unacknowledgedMessageIds.Contains(pair.Value.ItemId))
                {
                    _unacknowledgedMessageIds.Add(pair.Value.ItemId);

                    result = new(pair.Value.Stream, pair.Value.ItemId);

                    break;
                }

                index++;
            }
        }
        finally
        {
            Lock.Release();
        }

        if (result != null)
        {
            MessageReceived?.Invoke(this, new(result));
        }

        return result;
    }

    public async Task AcknowledgeAsync(object acknowledgementToken)
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[acknowledge/cancelled]"));
            return;
        }

        var itemId = (int)acknowledgementToken;

        await Lock.WaitAsync(_cancellationToken).ConfigureAwait(false);

        try
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
        finally
        {
            Lock.Release();
        }

        MessageAcknowledged?.Invoke(this, new(acknowledgementToken));

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ReleaseAsync(object acknowledgementToken)
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[release/cancelled]"));
            return;
        }

        var itemId = (int)acknowledgementToken;

        await Lock.WaitAsync(_cancellationToken).ConfigureAwait(false);

        try
        {
            var queue = Queues[Uri.ToString()];

            if (!queue.ContainsKey(itemId) || !_unacknowledgedMessageIds.Contains(itemId))
            {
                return;
            }

            _unacknowledgedMessageIds.Remove(itemId);
        }
        finally
        {
            Lock.Release();
        }

        MessageReleased?.Invoke(this, new(acknowledgementToken));

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public event EventHandler<MessageEnqueuedEventArgs>? MessageEnqueued;
    public event EventHandler<MessageAcknowledgedEventArgs>? MessageAcknowledged;
    public event EventHandler<MessageReleasedEventArgs>? MessageReleased;
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;
    public event EventHandler<OperationEventArgs>? Operation;
}