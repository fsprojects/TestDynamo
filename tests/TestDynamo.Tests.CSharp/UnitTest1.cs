using Amazon.DynamoDBv2;
using TestDynamo.Api;
using TestDynamo.Model;

namespace TestDynamo.Tests.CSharp;

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
            .Attribute("Attr2", new byte[] { 1 })
            .Attribute("Attr3", true)
            .Attribute("Attr4", new[] { "v" })
            .Attribute("Attr5", new[] { 5m })
            .Attribute("Attr6", new[] { (byte)1 })
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