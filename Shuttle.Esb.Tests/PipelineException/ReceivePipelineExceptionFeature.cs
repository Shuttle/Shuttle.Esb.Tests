using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;

namespace Shuttle.Esb.Tests;

public class ReceivePipelineExceptionFeature :
    IPipelineObserver<OnGetMessage>,
    IPipelineObserver<OnAfterGetMessage>,
    IPipelineObserver<OnDeserializeTransportMessage>,
    IPipelineObserver<OnAfterDeserializeTransportMessage>
{
    private static readonly SemaphoreSlim Lock = new(1, 1);

    private readonly List<ExceptionAssertion> _assertions = new();
    private readonly ILogger<ReceivePipelineExceptionFeature> _logger;
    private readonly IServiceBusConfiguration _serviceBusConfiguration;
    private string _assertionName = string.Empty;
    private volatile bool _failed;
    private int _pipelineCount;

    public ReceivePipelineExceptionFeature(ILogger<ReceivePipelineExceptionFeature> logger, IServiceBusConfiguration serviceBusConfiguration, IPipelineFactory pipelineFactory)
    {
        Guard.AgainstNull(pipelineFactory);

        pipelineFactory.PipelineCreated += PipelineCreated;
        pipelineFactory.PipelineReleased += PipelineReleased;
        pipelineFactory.PipelineObtained += PipelineObtained;

        _logger = Guard.AgainstNull(logger);
        _serviceBusConfiguration = Guard.AgainstNull(serviceBusConfiguration);

        AddAssertion("OnGetMessage");
        AddAssertion("OnAfterGetMessage");
        AddAssertion("OnDeserializeTransportMessage");
        AddAssertion("OnAfterDeserializeTransportMessage");
    }

    public async Task ExecuteAsync(IPipelineContext<OnAfterDeserializeTransportMessage> pipelineContext)
    {
        ThrowException("OnAfterDeserializeTransportMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<OnAfterGetMessage> pipelineContext)
    {
        ThrowException("OnAfterGetMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<OnDeserializeTransportMessage> pipelineContext)
    {
        ThrowException("OnDeserializeTransportMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<OnGetMessage> pipelineContext)
    {
        ThrowException("OnGetMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private void AddAssertion(string name)
    {
        Lock.Wait();

        try
        {
            _assertions.Add(new(name));

            _logger.LogInformation($"[ReceivePipelineExceptionModule:Added] : assertion = '{name}'.");
        }
        finally
        {
            Lock.Release();
        }
    }

    private ExceptionAssertion? GetAssertion(string name)
    {
        return _assertions.Find(item => item.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase));
    }

    private void PipelineCreated(object? sender, PipelineEventArgs e)
    {
        var fullName = e.Pipeline.GetType().FullName;

        if (fullName != null && !fullName.Equals(typeof(InboxMessagePipeline).FullName, StringComparison.InvariantCultureIgnoreCase))
        {
            return;
        }

        e.Pipeline.AddObserver(this);
    }

    private void PipelineObtained(object? sender, PipelineEventArgs e)
    {
        _pipelineCount += 1;
        _assertionName = string.Empty;

        _logger.LogInformation($"[ReceivePipelineExceptionModule:PipelineObtained] : count = {_pipelineCount}");
    }

    private void PipelineReleased(object? sender, PipelineEventArgs e)
    {
        if (string.IsNullOrEmpty(_assertionName))
        {
            return;
        }

        Lock.Wait();

        try
        {
            var assertion = Guard.AgainstNull(GetAssertion(_assertionName));

            if (assertion.HasRun)
            {
                return;
            }

            _logger.LogInformation($"[ReceivePipelineExceptionModule:Invoking] : assertion = '{assertion.Name}'.");

            try
            {
                // IsEmpty does not work for prefetch queues
                var receivedMessage = _serviceBusConfiguration.Inbox!.WorkQueue!.GetMessageAsync().GetAwaiter().GetResult();

                Assert.That(receivedMessage, Is.Not.Null);

                _serviceBusConfiguration.Inbox.WorkQueue.ReleaseAsync(receivedMessage!.AcknowledgementToken).GetAwaiter().GetResult();
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

            var assertion = Guard.AgainstNull(GetAssertion(_assertionName));

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