using System.Collections.Generic;
using Shuttle.ESB.Core;

namespace Shuttle.ESB.Tests
{
	public class IdempotenceMessageRouteProvider : IMessageRouteProvider
	{
		public IEnumerable<string> GetRouteUris(string messageType)
		{
			return new List<string> { "memory://./idempotence-inbox-work" };
		}

		public void Add(IMessageRoute messageRoute)
		{
			throw new System.NotImplementedException();
		}

		public IMessageRoute Find(string uri)
		{
			throw new System.NotImplementedException();
		}

	    public bool Any()
	    {
	        throw new System.NotImplementedException();
	    }
	}
}