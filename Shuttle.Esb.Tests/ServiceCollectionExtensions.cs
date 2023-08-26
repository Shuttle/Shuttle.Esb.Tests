using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Esb.Logging;

namespace Shuttle.Esb.Tests
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection ConfigureLogging(this IServiceCollection services, string test)
        {
            Guard.AgainstNull(services, nameof(services));

            services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider>(new FixtureFileLoggerProvider(Guard.AgainstNullOrEmptyString(test, nameof(test)))));

            services.AddServiceBusLogging(builder =>
            {
                builder.Options.AddPipelineEventType<OnAbortPipeline>();
                builder.Options.AddPipelineEventType<OnPipelineException>();
                builder.Options.AddPipelineEventType<OnAfterGetMessage>();
                builder.Options.AddPipelineEventType<OnAfterDeserializeTransportMessage>();
                builder.Options.AddPipelineEventType<OnHandleMessage>();
                builder.Options.AddPipelineEventType<OnAfterHandleMessage>();
                builder.Options.QueueEvents = true;
                builder.Options.TransportMessageDeferred = true;
            });

            services.AddLogging(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Trace);
                builder.AddConsole();
            });

            return services;
        }
    }
}