using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.Tests
{
	public class MemoryQueue : IQueue, ICreateQueue, IPurgeQueue
	{
		internal const string Scheme = "memory";

		private static readonly object Padlock = new object();

		private static Dictionary<string, Dictionary<int, MemoryQueueItem>> _queues =
			new Dictionary<string, Dictionary<int, MemoryQueueItem>>();

		private readonly List<int> _unacknowledgedMessageIds = new List<int>();
		private int _itemId;

		public MemoryQueue(Uri uri)
		{
			Guard.AgainstNull(uri, "uri");

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

			Uri = builder.Uri;

			if (Uri.Host != Environment.MachineName.ToLower())
			{
				throw new UriFormatException(string.Format(Resources.UriFormatException,
				    $"memory://{{.|{Environment.MachineName.ToLower()}}}/{{name}}", uri));
			}

			Create();
		}

		public Uri Uri { get; }
		public bool IsStream => false;

		public bool IsEmpty()
		{
			lock (Padlock)
			{
				return _queues[Uri.ToString()].Count == 0;
			}
		}

		public void Enqueue(TransportMessage transportMessage, Stream stream)
		{
			lock (Padlock)
			{
				_itemId++;

				_queues[Uri.ToString()].Add(_itemId, new MemoryQueueItem(_itemId, transportMessage.MessageId, stream.Copy()));
			}
		}

		public ReceivedMessage GetMessage()
		{
			lock (Padlock)
			{
				var queue = _queues[Uri.ToString()];

				var index = 0;

				while (index < queue.Count)
				{
					var pair = queue.ElementAt(index);

					if (!_unacknowledgedMessageIds.Contains(pair.Value.ItemId))
					{
						_unacknowledgedMessageIds.Add(pair.Value.ItemId);

						return new ReceivedMessage(pair.Value.Stream, pair.Value.ItemId);
					}

					index++;
				}

				return null;
			}
		}

		public void Acknowledge(object acknowledgementToken)
		{
			var itemId = (int) acknowledgementToken;

			lock (Padlock)
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
		}

		public void Release(object acknowledgementToken)
		{
			var itemId = (int) acknowledgementToken;

			lock (Padlock)
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
		}

		public bool IsLocal => true;

	    public static IQueue From(string uri)
		{
			return new MemoryQueue(new Uri(uri));
		}

		public static void Clear()
		{
			_queues = new Dictionary<string, Dictionary<int, MemoryQueueItem>>();
		}

		public void Create()
		{
			if (!_queues.ContainsKey(Uri.ToString()))
			{
				_queues.Add(Uri.ToString(), new Dictionary<int, MemoryQueueItem>());
			}
		}

		public void Purge()
		{
			lock (Padlock)
			{
				_queues[Uri.ToString()].Clear();
			}
		}
	}

	internal class MemoryQueueItem
	{
		public int ItemId { get; }
		public Guid MessageId { get; }
		public Stream Stream { get; }

		public MemoryQueueItem(int index, Guid messageId, Stream stream)
		{
			ItemId = index;
			MessageId = messageId;
			Stream = stream;
		}
	}
}