using System;
using System.Collections.Generic;

namespace Shuttle.Esb.Tests
{
    public class IdempotenceMessageRouteProvider : IMessageRouteProvider
    {
        public IEnumerable<string> GetRouteUris(string messageType)
        {
            return new List<string> {"memory://./idempotence-inbox-work"};
        }

        public void Add(IMessageRoute messageRoute)
        {
            throw new NotImplementedException();
        }

        public IMessageRoute Find(string uri)
        {
            throw new NotImplementedException();
        }

        public bool Any()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IMessageRoute> MessageRoutes
        {
            get { return new List<IMessageRoute>(); }
        }
    }
}