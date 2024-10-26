using System;
using Shuttle.Core.Contract;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.Tests;

public class TransientQueueFactory : IQueueFactory
{
    private readonly ICancellationTokenSource _cancellationTokenSource;

    public TransientQueueFactory(ICancellationTokenSource cancellationTokenSource)
    {
        _cancellationTokenSource = Guard.AgainstNull(cancellationTokenSource);
    }

    public string Scheme => TransientQueue.Scheme;

    public IQueue Create(Uri uri)
    {
        return new TransientQueue(Guard.AgainstNull(uri), _cancellationTokenSource.Get().Token);
    }
}