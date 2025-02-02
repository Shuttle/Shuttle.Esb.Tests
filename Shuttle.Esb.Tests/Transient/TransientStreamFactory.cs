using System;
using Shuttle.Core.Contract;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.Tests;

public class TransientStreamFactory : IQueueFactory
{
    private readonly ICancellationTokenSource _cancellationTokenSource;

    public TransientStreamFactory(ICancellationTokenSource cancellationTokenSource)
    {
        _cancellationTokenSource = Guard.AgainstNull(cancellationTokenSource);
    }

    public string Scheme => TransientStream.Scheme;

    public IQueue Create(Uri uri)
    {
        return new TransientStream(Guard.AgainstNull(uri), _cancellationTokenSource.Get().Token);
    }
}