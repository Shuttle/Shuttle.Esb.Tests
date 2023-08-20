using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public static class ServiceProviderExtensions
    {
        public static async Task<IServiceProvider> StartHostedServices(this IServiceProvider serviceProvider)
        {
            Guard.AgainstNull(serviceProvider, nameof(serviceProvider));

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(CancellationToken.None).ConfigureAwait(false);
            }

            return serviceProvider;
        }

        public static async Task<IServiceProvider> StopHostedServices(this IServiceProvider serviceProvider)
        {
            Guard.AgainstNull(serviceProvider, nameof(serviceProvider));

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                await hostedService.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }

            return serviceProvider;
        }

        public static IQueueService CreateQueueService(this IServiceProvider serviceProvider)
        {
            Guard.AgainstNull(serviceProvider, nameof(serviceProvider));

            return new QueueService(serviceProvider.GetRequiredService<IQueueFactoryService>(),
                serviceProvider.GetRequiredService<IUriResolver>()).WireQueueEvents(serviceProvider.GetLogger<QueueService>());
        }

        public static ILogger<T> GetLogger<T>(this IServiceProvider serviceProvider)
        {
            return (ILogger<T>)Guard.AgainstNull(serviceProvider, nameof(serviceProvider)).GetRequiredService<ILoggerFactory>().CreateLogger(typeof(T));
        }
    }
}