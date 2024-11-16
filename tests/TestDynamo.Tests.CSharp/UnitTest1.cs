using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using TestDynamo.Api;
using TestDynamo.Client;
using TestDynamo.Model;
using AttributeValue = Amazon.DynamoDBv2.Model.AttributeValue;
using Database = TestDynamo.Api.Database;
using Microsoft.Extensions.Logging;
using TestDynamo.Serialization;

namespace TestDynamo.Tests.CSharp;

/// <summary>
/// An interceptor which implements the CreateBackup operation
/// </summary>
public class CreateBackupInterceptor : IRequestInterceptor
{
    public ValueTask<object> Intercept(Api.FSharp.Database database, object request, CancellationToken c)
    {
        // ignore other requests by returning default
        // if the interception is an async process, you can also return 
        // a ValueTask that resolves to null
        if (request is not CreateBackupRequest typedRequest)
            return default;

        // wrap the database in something that is more C# friendly
        var csDatabase = new Api.Database(database);
        
        // check whether this is a valid request or not
        var table = csDatabase.TryDescribeTable(typedRequest.TableName);
        if (table.IsNone)
            throw new AmazonDynamoDBException("Cannot find table");

        var response = new CreateBackupResponse
        {
            BackupDetails = new BackupDetails
            {
                BackupStatus = BackupStatus.AVAILABLE
            }
        };

        return new ValueTask<object>(response);
    }
}
public class UnitTest1
{
    [Fact]
    public void SomeCSharpSmokeTests()
    {
        using var client11 = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();
        using var database = new Api.Database(new DatabaseId("us-west-1"));

        database
            .TableBuilder("Beatles", ("FirstName", "S"))
            .WithGlobalSecondaryIndex("SecondNameIndex", ("SecondName", "S"), ("FirstName", "S"))
            .AddTable();
        
        database
            .ItemBuilder("Beatles")
            .Attribute("FirstName", "Ringo")
            .Attribute("SecondName", "Starr")
            .Attribute("Attr2", new byte[]{1})
            .Attribute("Attr3", true)
            .Attribute("Attr4", new []{ "v"})
            .Attribute("Attr5", new []{ 5m })
            .Attribute("Attr6", new []{ (byte)1 })
            .Attribute("Attr7", new MapBuilder().Attribute("Attr", "x"))
            .Attribute("Attr7", new ListBuilder().Append("x"))
            .AddItem();
        
        var x = database
            .GetTable("Beatles")
            .GetValues()
            .Single(v => v["FirstName"].S == "Ringo");
        
        Assert.Equal("Starr", x["SecondName"].S);
    }
}