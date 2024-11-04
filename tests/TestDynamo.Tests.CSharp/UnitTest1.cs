using Amazon.DynamoDBv2;
using Amazon.Runtime;
using TestDynamo.Client;
using TestDynamo.Model;
using Microsoft.Extensions.Logging;
using Microsoft.FSharp.Core;
using ILogger = Amazon.Runtime.Internal.Util.ILogger;

namespace TestDynamo.Tests.CSharp;

public class UnitTest1
{
    [Fact]
    public void Test1()
    {
        using var client = TestDynamoClient.Create();
        //client.Database.SubscribeToStream()
        // var tt = client.GetTable("").GetValues().Select(x => x[""]).Single();
        //tt.IsNull
        //tt.

    }

    public class Handler : IPipelineHandler
    {
        public void InvokeSync(IExecutionContext executionContext)
        {
            throw new NotImplementedException();
        }

        public Task<T> InvokeAsync<T>(IExecutionContext executionContext) where T : AmazonWebServiceResponse, new()
        {
            throw new NotImplementedException();
        }

        public ILogger Logger { get; set; }
        public IPipelineHandler InnerHandler { get; set; }
        public IPipelineHandler OuterHandler { get; set; }
    }

    public class X : AmazonDynamoDBClient
    {
        public X()
        {
            base.RuntimePipeline.AddHandler(new Handler());
        }
    }
}