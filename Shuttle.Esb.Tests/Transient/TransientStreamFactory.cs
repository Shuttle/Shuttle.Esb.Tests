using System;
using Shuttle.Core.Contract;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.Tests
{
    public class TransientStreamFactory : IQueueFactory
    {
        public string Scheme => TransientStream.Scheme;

        private readonly ICancellationTokenSource _cancellationTokenSource;

        public TransientStreamFactory(ICancellationTokenSource cancellationTokenSource)
        {
            _cancellationTokenSource = Guard.AgainstNull(cancellationTokenSource, nameof(cancellationTokenSource));
        }

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, nameof(uri));

            return new TransientStream(uri, _cancellationTokenSource.Get().Token);
        }
    }
}