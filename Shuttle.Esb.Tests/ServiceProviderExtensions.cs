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

            var logger = serviceProvider.GetLogger();

            logger.LogInformation($"[StartHostedServices]");

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                logger.LogInformation($"[HostedService-starting] : {hostedService.GetType().Name}");

                await hostedService.StartAsync(CancellationToken.None).ConfigureAwait(false);

                logger.LogInformation($"[HostedService-started] : {hostedService.GetType().Name}");
            }

            return serviceProvider;
        }

        public static async Task<IServiceProvider> StopHostedServices(this IServiceProvider serviceProvider)
        {
            Guard.AgainstNull(serviceProvider, nameof(serviceProvider));

            var logger = serviceProvider.GetLogger();

            logger.LogInformation($"[StopHostedServices]");
            
            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                logger.LogInformation($"[HostedService-stopping] : {hostedService.GetType().Name}");

                await hostedService.StopAsync(CancellationToken.None).ConfigureAwait(false);

                logger.LogInformation($"[HostedService-stopped] : {hostedService.GetType().Name}");
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
            return Guard.AgainstNull(serviceProvider, nameof(serviceProvider)).GetRequiredService<ILoggerFactory>().CreateLogger<T>();
        }

        public static ILogger GetLogger(this IServiceProvider serviceProvider)
        {
            return Guard.AgainstNull(serviceProvider, nameof(serviceProvider)).GetRequiredService<ILoggerFactory>().CreateLogger("Fixture");
        }
    }
}