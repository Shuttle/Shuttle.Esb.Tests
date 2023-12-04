using System;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public class TransientQueueFactory : IQueueFactory
    {
        public string Scheme => TransientQueue.Scheme;

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            return new TransientQueue(uri);
        }
    }
}