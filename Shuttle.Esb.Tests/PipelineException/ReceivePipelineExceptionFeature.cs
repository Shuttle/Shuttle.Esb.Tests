using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;

namespace Shuttle.Esb.Tests
{
    public class ReceivePipelineExceptionFeature :
        IPipelineObserver<OnGetMessage>,
        IPipelineObserver<OnAfterGetMessage>,
        IPipelineObserver<OnDeserializeTransportMessage>,
        IPipelineObserver<OnAfterDeserializeTransportMessage>
    {
        private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1, 1);

        private readonly List<ExceptionAssertion> _assertions = new List<ExceptionAssertion>();
        private readonly ILogger<ReceivePipelineExceptionFeature> _logger;
        private readonly IServiceBusConfiguration _serviceBusConfiguration;
        private string _assertionName;
        private volatile bool _failed;
        private int _pipelineCount;

        public ReceivePipelineExceptionFeature(ILogger<ReceivePipelineExceptionFeature> logger, IServiceBusConfiguration serviceBusConfiguration, IPipelineFactory pipelineFactory)
        {
            Guard.AgainstNull(serviceBusConfiguration, nameof(serviceBusConfiguration));
            Guard.AgainstNull(pipelineFactory, nameof(pipelineFactory));

            pipelineFactory.PipelineCreated += PipelineCreated;
            pipelineFactory.PipelineReleased += PipelineReleased;
            pipelineFactory.PipelineObtained += PipelineObtained;

            _logger = Guard.AgainstNull(logger, nameof(logger));
            _serviceBusConfiguration = serviceBusConfiguration;

            AddAssertion("OnGetMessage");
            AddAssertion("OnAfterGetMessage");
            AddAssertion("OnDeserializeTransportMessage");
            AddAssertion("OnAfterDeserializeTransportMessage");
        }

        public void Execute(OnAfterDeserializeTransportMessage pipelineEvent)
        {
            ThrowException("OnAfterDeserializeTransportMessage");
        }

        public async Task ExecuteAsync(OnAfterDeserializeTransportMessage pipelineEvent1)
        {
            ThrowException("OnAfterDeserializeTransportMessage");

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public void Execute(OnAfterGetMessage pipelineEvent)
        {
            ThrowException("OnAfterGetMessage");
        }

        public async Task ExecuteAsync(OnAfterGetMessage pipelineEvent)
        {
            ThrowException("OnAfterGetMessage");

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public void Execute(OnDeserializeTransportMessage pipelineEvent)
        {
            ThrowException("OnDeserializeTransportMessage");
        }

        public async Task ExecuteAsync(OnDeserializeTransportMessage pipelineEvent1)
        {
            ThrowException("OnDeserializeTransportMessage");

            await Task.CompletedTask.ConfigureAwait(false);
        }

        public void Execute(OnGetMessage pipelineEvent)
        {
            ThrowException("OnGetMessage");
        }

        public async Task ExecuteAsync(OnGetMessage pipelineEvent1)
        {
            ThrowException("OnGetMessage");

            await Task.CompletedTask.ConfigureAwait(false);
        }

        private void AddAssertion(string name)
        {
            Lock.Wait();

            try
            {
                _assertions.Add(new ExceptionAssertion(name));

                _logger.LogInformation($"[ReceivePipelineExceptionModule:Added] : assertion = '{name}'.");
            }
            finally
            {
                Lock.Release();
            }
        }

        private ExceptionAssertion GetAssertion(string name)
        {
            return _assertions.Find(item => item.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase));
        }

        private void PipelineCreated(object sender, PipelineEventArgs e)
        {
            var fullName = e.Pipeline.GetType().FullName;

            if (fullName != null && !fullName.Equals(typeof(InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase))
            {
                return;
            }

            e.Pipeline.RegisterObserver(this);
        }

        private void PipelineObtained(object sender, PipelineEventArgs e)
        {
            _pipelineCount += 1;
            _assertionName = string.Empty;

            _logger.LogInformation($"[ReceivePipelineExceptionModule:PipelineObtained] : count = {_pipelineCount}");
        }

        private void PipelineReleased(object sender, PipelineEventArgs e)
        {
            if (string.IsNullOrEmpty(_assertionName))
            {
                return;
            }

            Lock.Wait();

            try
            {
                var assertion = GetAssertion(_assertionName);

                if (assertion == null || assertion.HasRun)
                {
                    return;
                }

                _logger.LogInformation($"[ReceivePipelineExceptionModule:Invoking] : assertion = '{assertion.Name}'.");

                try
                {
                    // IsEmpty does not work for prefetch queues
                    var receivedMessage = _serviceBusConfiguration.Inbox.WorkQueue.GetMessage();

                    Assert.IsNotNull(receivedMessage);

                    _serviceBusConfiguration.Inbox.WorkQueue.Release(receivedMessage.AcknowledgementToken);
                }
                catch (Exception ex)
                {
                    _logger.LogInformation(ex.AllMessages());

                    _failed = true;
                }

                assertion.MarkAsRun();

                _logger.LogInformation($"[ReceivePipelineExceptionModule:Invoked] : assertion = '{assertion.Name}'.");
            }
            finally
            {
                Lock.Release();
            }
        }

        public bool ShouldWait()
        {
            Lock.Wait();

            try
            {
                return !_failed && _assertions.Find(item => !item.HasRun) != null;
            }
            finally
            {
                Lock.Release();
            }
        }

        private void ThrowException(string name)
        {
            Lock.Wait();

            try
            {
                _assertionName = name;

                var assertion = GetAssertion(_assertionName);

                if (assertion.HasRun)
                {
                    return;
                }

                throw new AssertionException($"Testing assertion for '{name}'.");
            }
            finally
            {
                Lock.Release();
            }
        }
    }
}