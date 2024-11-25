using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using TestDynamo.Api.FSharp;
using TestDynamo.Client;
using TestDynamo.Model;

namespace TestDynamo.Tests.CSharp;
/// <summary>
/// An interceptor which implements the CreateBackup operation
/// </summary>
public class CreateBackupInterceptor(Dictionary<string, DatabaseCloneData> backupStore) : IRequestInterceptor
{
    public async ValueTask<object?> InterceptRequest(Api.FSharp.Database database, object request, CancellationToken c)
    {
        if (request is CreateBackupRequest create)
            return CreateBackup(database, create.TableName);

        if (request is RestoreTableFromBackupRequest restore)
            return await RestoreBackup(database, restore.BackupArn);

        // return null to allow the client to process the request as normal
        return null;
    }

    private CreateBackupResponse CreateBackup(Api.FSharp.Database database, string tableName)
    {
        // wrap the database in something that is more C# friendly
        using var csDatabase = new Api.Database(database);

        // clone the required database and remove all other tables
        var cloneData = csDatabase.BuildCloneData();
        cloneData = new DatabaseCloneData(
            cloneData.data.ExtractTables(new [] { tableName }),
            cloneData.databaseId);

        // create a fake arn and store a cloned DB as a backup
        var arn = $"{database.Id.regionId}/{tableName}";
        backupStore.Add(arn, cloneData);

        return new CreateBackupResponse
        {
            BackupDetails = new BackupDetails
            {
                BackupArn = arn,
                BackupStatus = BackupStatus.AVAILABLE
            }
        };
    }

    private async ValueTask<RestoreTableFromBackupResponse> RestoreBackup(Api.FSharp.Database database, string arn)
    {
        var arnParts = arn.Split("/");
        if (arnParts.Length != 2)
            throw new AmazonDynamoDBException("Invalid backup arn");

        var tableName = arnParts[1];
        if (!backupStore.TryGetValue(arn, out var backup))
            throw new AmazonDynamoDBException("Invalid backup arn");

        // wrap the database in something that is more C# friendly
        using var csDatabase = new Api.Database(database);

        // delete any existing data to make room for restore data
        if (csDatabase.TryDescribeTable(tableName).IsSome)
            await csDatabase.DeleteTable(tableName);

        csDatabase.Import(backup.data);
        return new RestoreTableFromBackupResponse
        {
            TableDescription = new TableDescription
            {
                TableName = tableName
            }
        };
    }

    // no need to intercept responses
    public ValueTask<object?> InterceptResponse(Api.FSharp.Database database, object request, object response, CancellationToken c) => default;
}

/// <summary>
/// An interceptor which implements BillingMode functionality for CreateTableAsync
/// </summary>
public class BillingModeInterceptor : IRequestInterceptor
{
    public static readonly DateTime ConstantLastUpdateToPayPerRequestDateTime = DateTime.UtcNow.AddMinutes(-1);

    // request interception is not requred
    public ValueTask<object?> InterceptRequest(Api.FSharp.Database database, object request, CancellationToken c) => default;

    public ValueTask<object?> InterceptResponse(Api.FSharp.Database database, object request, object response, CancellationToken c)
    {
        if (request is not CreateTableRequest req || response is not CreateTableResponse resp)
            return default;

        // modify the output
        resp.TableDescription.BillingModeSummary = new BillingModeSummary
        {
            BillingMode = req.BillingMode ?? BillingMode.PAY_PER_REQUEST,
            LastUpdateToPayPerRequestDateTime = ConstantLastUpdateToPayPerRequestDateTime
        };

        // Return default so that the response will be passed on after modification
        // If a non null item is returned here, it will be passed on instead 
        return default;
    }
}

/// <summary>
/// Important. Tests an example from the docs
/// The example is referenced in features doc as a copy pastable extra feature
///
/// This class also tests Database.Import method, DatabaseClone.ExtractTables method and
/// IRequestInterceptor.InterceptResponse which are not tested elsewhere
/// </summary>
public class CreateBackupExample
{
    [Fact]
    public async Task BackupExample()
    {
        var backups = new Dictionary<string, DatabaseCloneData>();
        var interceptor = new CreateBackupInterceptor(backups);
        using var database = new Api.Database(new DatabaseId("us-west-1"));
        using var client = database.CreateClient<AmazonDynamoDBClient>(interceptor);

        database
            .TableBuilder("Beatles", ("FirstName", "S"))
            .WithGlobalSecondaryIndex("SecondNameIndex", ("SecondName", "S"), ("FirstName", "S"))
            .AddTable();

        database
            .ItemBuilder("Beatles")
            .Attribute("FirstName", "Ringo")
            .Attribute("SecondName", "Starr")
            .AddItem();

        var backupResponse = await client.CreateBackupAsync(new CreateBackupRequest
        {
            TableName = "Beatles"
        });

        database
            .ItemBuilder("Beatles")
            .Attribute("FirstName", "George")
            .Attribute("SecondName", "Harrison")
            .AddItem();

        Assert.Equal(2, database.DebugTables.Single().Values.Count());

        await client.RestoreTableFromBackupAsync(new RestoreTableFromBackupRequest
        {
            BackupArn = backupResponse.BackupDetails.BackupArn
        });

        Assert.Single(database.DebugTables.Single().Values);
    }

    [Fact]
    public async Task BillingModeExample()
    {
        var interceptor = new BillingModeInterceptor();
        using var database = new Api.Database(new DatabaseId("us-west-1"));
        using var client = database.CreateClient<AmazonDynamoDBClient>(interceptor);

        var response = await client.CreateTableAsync(new CreateTableRequest
        {
            TableName = "Beatles",
            KeySchema = new List<KeySchemaElement>
            {
                new KeySchemaElement("KN", KeyType.HASH)
            },
            AttributeDefinitions = new List<AttributeDefinition>
            {
                new AttributeDefinition("KN", ScalarAttributeType.B)
            }
        });

        Assert.Equal(
            BillingModeInterceptor.ConstantLastUpdateToPayPerRequestDateTime, 
            response.TableDescription.BillingModeSummary.LastUpdateToPayPerRequestDateTime);
    }
}