using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Memory.Tests;

public class TransientStreamPipelineExceptionHandlingFixture : PipelineExceptionFixture
{
    [Test]
    public void Should_be_able_to_handle_exceptions_in_receive_stage_of_receive_pipeline()
    {
        TestExceptionHandling(TransientStreamConfiguration.GetServiceCollection(), "transient-stream://./{0}");
    }

    [Test]
    public async Task Should_be_able_to_handle_exceptions_in_receive_stage_of_receive_pipeline_async()
    {
        await TestExceptionHandlingAsync(TransientStreamConfiguration.GetServiceCollection(), "transient-stream://./{0}");
    }
}