using System.IO;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    internal class TransientMessage
    {
        public TransientMessage(int index, TransportMessage transportMessage, Stream stream)
        {
            ItemId = index;
            TransportMessage = Guard.AgainstNull(transportMessage, nameof(transportMessage));
            Stream = Guard.AgainstNull(stream, nameof(stream));
        }

        public int ItemId { get; }
        public TransportMessage TransportMessage { get; }
        public Stream Stream { get; }
    }
}