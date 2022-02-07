using System;
using Shuttle.Core.Container;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests
{
    public class ComponentContainer
    {
        private readonly Func<IComponentResolver> _resolverFactory;
        private IComponentResolver _resolver;

        public ComponentContainer(IComponentRegistry registry, Func<IComponentResolver> resolverFactory)
        {
            Guard.AgainstNull(registry, "registry");
            Guard.AgainstNull(resolverFactory, "resolverFactory");

            Registry = registry;

            _resolverFactory = resolverFactory;
        }

        public IComponentRegistry Registry { get; }

        public IComponentResolver Resolver => _resolver ?? (_resolver = _resolverFactory.Invoke());
    }
}