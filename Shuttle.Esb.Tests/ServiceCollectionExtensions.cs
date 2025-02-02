using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;
using Shuttle.Esb.Logging;

namespace Shuttle.Esb.Tests;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddTransientQueues(this IServiceCollection services)
    {
        Guard.AgainstNull(services);

        services.TryAddSingleton<IQueueFactory, TransientQueueFactory>();

        return services;
    }

    public static IServiceCollection AddTransientStreams(this IServiceCollection services)
    {
        Guard.AgainstNull(services);

        services.TryAddSingleton<IQueueFactory, TransientStreamFactory>();

        return services;
    }

    public static IServiceCollection ConfigureLogging(this IServiceCollection services, string test)
    {
        Guard.AgainstNull(services);

        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider>(new FixtureFileLoggerProvider(Guard.AgainstNullOrEmptyString(test))));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, ConsoleLoggerProvider>());

        services.AddServiceBusLogging(builder =>
        {
            builder.Options.QueueEvents = true;
            builder.Options.TransportMessageDeferred = true;
            builder.Options.Threading = true;
        });

        services.AddLogging(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        return services;
    }
}