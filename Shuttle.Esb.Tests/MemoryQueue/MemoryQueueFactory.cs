using System;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public class MemoryQueueFactory : IQueueFactory
    {
        public string Scheme => MemoryQueue.Scheme;

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            return new MemoryQueue(uri);
        }
    }
}