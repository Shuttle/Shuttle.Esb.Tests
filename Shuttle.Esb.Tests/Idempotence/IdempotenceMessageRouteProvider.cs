using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Shuttle.Esb.Tests;

public class IdempotenceMessageRouteProvider : IMessageRouteProvider
{
    public async Task<IEnumerable<string>> GetRouteUrisAsync(string messageType)
    {
        return await Task.FromResult(GetRouteUris(messageType)).ConfigureAwait(false);
    }

    public IEnumerable<string> GetRouteUris(string messageType)
    {
        return new List<string> { "memory://./idempotence-inbox-work" };
    }

    public void Add(IMessageRoute messageRoute)
    {
    }

    public IEnumerable<IMessageRoute> MessageRoutes => Enumerable.Empty<IMessageRoute>();
}