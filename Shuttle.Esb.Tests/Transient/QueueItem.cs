using System.IO;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests;

internal class TransientMessage
{
    public TransientMessage(int index, TransportMessage transportMessage, Stream stream)
    {
        ItemId = index;
        TransportMessage = Guard.AgainstNull(transportMessage);
        Stream = Guard.AgainstNull(stream);
    }

    public int ItemId { get; }
    public Stream Stream { get; }
    public TransportMessage TransportMessage { get; }
}