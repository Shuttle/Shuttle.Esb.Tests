using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
    }
}