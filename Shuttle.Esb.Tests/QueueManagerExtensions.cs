using Shuttle.Core.Container;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public static class QueueManagerExtensions
    {
        public static IQueueManager Configure(this IQueueManager queueManager, IComponentResolver resolver)
        {
            Guard.AgainstNull(queueManager, nameof(queueManager));
            Guard.AgainstNull(resolver, nameof(resolver));

            foreach (var queueFactory in resolver.ResolveAll<IQueueFactory>())
            {
                queueManager.RegisterQueueFactory(queueFactory);
            }

            return queueManager;
        }
    }
}