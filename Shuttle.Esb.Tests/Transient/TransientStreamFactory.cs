using System;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public class TransientStreamFactory : IQueueFactory
    {
        public string Scheme => TransientStream.Scheme;

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            return new TransientStream(uri);
        }
    }
}