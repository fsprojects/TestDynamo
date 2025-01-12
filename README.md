# TestDynamo

An in-memory dynamodb client for automated testing

[![Code coverage](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fraw.githubusercontent.com%2FShaneGH%2FTestDynamo%2Frefs%2Fheads%2Fmain%2FautomatedBuildResults.json&query=%24.projects.TestDynamo.coverage&label=Code%20Coverage&color=green "Code coverage")](https://github.com/ShaneGH/TestDynamo/blob/main/tests/coverage.ps1)
[![Github](https://img.shields.io/badge/Github-TestDynamo-black "Github")](https://github.com/ShaneGH/TestDynamo)
[![NuGet](https://img.shields.io/badge/NuGet-TestDynamo-blue "NuGet")](https://www.nuget.org/packages/TestDynamo)
[![NuGet](https://img.shields.io/badge/NuGet-TestDynamo.Serialization-blue "NuGet")](https://www.nuget.org/packages/TestDynamo.Serialization)
[![NuGet](https://img.shields.io/badge/NuGet-TestDynamo.Lambda-blue "NuGet")](https://www.nuget.org/packages/TestDynamo.Lambda)

TestDynamo is a rewrite of dynamodb in dotnet designed for testing and debugging. 
It implements a partial feature set of `IAmazonDynamoDb` to manage schemas and read and write items.

 * [Core features](https://github.com/ShaneGH/TestDynamo/blob/main/Features.md)
    * Table management (Create/Update/Delete table)
    * Index management (Create/Update/Delete index)
    * Item operations (Put/Delete/Update etc)
    * Queries and Scans
    * Batching and Transactional writes
 * Document model and `DynamoDBContext`
 * Multi region setups
 * Global tables and replication
 * Streams and stream subscribers
 * Efficient cloning and deep copying of databases for isolation
 * Full database serialization and deserialization for data driven testing
 * Basic cloudformation template support for creating Tables and GlobalTables
 * Mocking utils

## Installation

 * Core functionality: `dotnet add package TestDynamo`
 * Add lambda support: `dotnet add package TestDynamo.Lambda`
 * Add serialization or cloud formation support: `dotnet add package TestDynamo.Serialization`

## The basics

```C#
using TestDynamo;

[Test]
public async Task GetPersonById_WithValidId_ReturnsPerson()
{
   // arrange
   using var client = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();

   // create a table and add some items
   await client.CreateTableAsync(...);
   await client.BatchWriteItemAsync(...);

   var testSubject = new MyBeatlesService(client);

   // act
   var beatle = testSubject.GetBeatle("Ringo");

   // assert
   Assert.Equal("Starr", beatle.SecondName);
}
```

## The details

TestDynamo has a suite of features and components to model a dynamodb environment and simplify the process of writing tests.

 * Support for [DynamoDb versions](https://www.nuget.org/packages/AWSSDK.DynamoDBv2#versions-body-tab) >= and 3.5.0
 * [`Api.Database`](#database) contains tables from a single region.
    * [F# Support](#f-database) out of the box
    * [Full expression engine](#using-expressions) so you can test your queries, scans, projections and conditions
    * [Schema and item change](#schema-and-item-change) tools make creating and populating test databases easier
    * [Database cloning](#database-cloning) allows you to make copies of entire AWS regions which can be safely used in other tests.
    * [Debug properties](#debug-properties) are optimized for reading in a debugger.
    * [Test query tools](#test-tools) to get data in and out of the database with as little code as possible
    * [Streaming and Subscriptions](#streaming-and-subscriptions) can model lambdas subscribed to dynamodb streams
 * [`DynamoDBContext`](#dynamodbcontext) works out of the box
 * [`Api.GlobalDatabase`](#global-database) models an AWS account spread over multiple regions. It contains `Api.Database`s.
    * [Global databases](#global-database-cloning) can be cloned too
 * [`TestDynamoClient`](#testdynamoclient) is the entry point for linking a database to a `AmazonDynamoDBClient`.
    * Check the [features](https://github.com/ShaneGH/TestDynamo/blob/main/Features.md) for a full list of endpoints, request args and responses that are supported.
 * [`DatabaseSerializer`](#database-serializers) is a json serializer/deserializer for entire databases and global databases.
 * [Cloud Formation Templates](#cloud-formation-templates) can be consumed to to initialize databases and global databases
 * [Locking and atomic transactions](#locking-and-atomic-transactions)
 * [Transact write ClientRequestToken](#transact-write-clientrequesttoken)
 * [Recorders](#recorders) can be used to record the activity on a client which can be asserted on later
 * [Interceptors](#interceptors) can be used to modify the functionality of the database, either to add more traditional mocking or to polyfill unsupported features
 * [Logging](#logging) can be configured at the database level or the `AmazonDynamoDBClient` level

### Using Expressions

All [dynamodb expression types](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html) are supported

### Database

The database is the core of TestDynamo and it models a single region and it's tables. The database 
is both fast and lightweight, built from simple data structures.

Databases can be injected into an `AmazonDynamoDBClient` to then be passed into a test

```C#
using TestDynamo;

using var db = new Api.Database(new DatabaseId("us-west-1"));
using var client = db.CreateClient<AmazonDynamoDBClient>();
```

### F# Database

TestDynamo is written in F# and has a lot of F# first constructs

```F#
open TestDynamo

use db = new Api.FSharp.Database({ regionId = "us-west-1" })
use client = ValueSome db |> TestDynamoClient.createClient ValueNone false ValueNone false
```

In general, functions and extension methods with in `camelCase` are targeted at F#, where as those is `PascalCase` are targeted at C#

### Schema and Item Change

Databases have some convenience methods to make adding tables and items easier

```C#
using TestDynamo;

using var database = new Api.Database(new DatabaseId("us-west-1"));

// add a table
database
   .TableBuilder("Beatles", ("FirstName", "S"))
   .WithGlobalSecondaryIndex("SecondNameIndex", ("SecondName", "S"), ("FirstName", "S"))
   .AddTable();

// add some data
database
   .ItemBuilder("Beatles")
   .Attribute("FirstName", "Ringo")
   .Attribute("SecondName", "Starr")
   .AddItem();
```

### Database Cloning

Databases are built for cloning. This allows you to create and populate a database once, and then clone it to be used 
in any test with full isolation. TestDynamo uses immutable data structures for most things, which means that nothing is 
actually copied during a clone so cloning has almost no overhead.

```C#
using TestDynamo;

private static Api.Database _sharedRootDatabase = BuildDatabase();

private static Api.Database BuildDatabase()
{
   var database = new Api.Database(new DatabaseId("us-west-1"));

   // add a table
   database
      .TableBuilder("Beatles", ("FirstName", "S"))
      .WithGlobalSecondaryIndex("SecondNameIndex", ("SecondName", "S"), ("FirstName", "S"))
      .AddTable();

   // add some data
   database
      .ItemBuilder("Beatles")
      .Attribute("FirstName", "Ringo")
      .Attribute("SecondName", "Starr")
      .AddItem();

   return database;
}

[Test]
public async Task TestSomething()
{
   // clone the database to get working copy
   // without altering the original
   using var database = _sharedRootDatabase.Clone();
   using var client = database.CreateClient<AmazonDynamoDBClient>();

   // act
   ...

   // assert
   ...
}
```

### Debug Properties

Use the handy debug properties on `Api.Database` and `Api.GlobalDatabase` in your debugger of choice

![https://raw.githubusercontent.com/ShaneGH/TestDynamo/refs/heads/main/Docs/DbDebugger.png](./Docs/DbDebugger.png "Debugger")

### Test Tools

Use query tools to find items in a table

```C#
using var database = GetMeADatabase();

var ringo = database
    .GetTable("Beatles")
    .GetValues()
    .Single(v => v["FirstName"].S == "Ringo");
```

### Streaming and Subscriptions

If streams are enabled on tables they can be used for global table 
replication and custom subscribers. TestDynamo differs from dynamodb as follows

 * There is no limit to the number of subscribers that you can have on a stream
 * Strem settings (e.g. `NEW_AND_OLD_IMAGES`) are configured per subscriber. If these values are set on a stream they will be ignored

To subscribe to changes with a lambda stream subscription syntax you can import the `TestDynamo.Lambda` package from nuget

```C#
using TestDynamo;
using TestDynamo.Lambda;
using Amazon.Lambda.DynamoDBEvents;

var subscription = database.AddSubscription<DynamoDBEvent>(
   "Beatles",
   (dynamoDbStreamsEvent, cancellationToken) =>
   {
      var added = dynamoDbStreamsEvent.Records.FirstOrDefault()?.Dynamodb.NewImage?["FirstName"]?.S;
      if (added != null)
         Console.WriteLine($"{added} has joined the Beatles");

      var removed = dynamoDbStreamsEvent.Records.FirstOrDefault()?.Dynamodb.OldImage?["FirstName"]?.S;
      if (removed != null)
         Console.WriteLine($"{removed} has left the Beatles");

      return default;
   });

// disposing will remove the subscription
subscription.Dispose();
```

Subscribe to raw changes

```C#
var subscription = database
   .SubscribeToStream("Beatles", (cdcPacket, cancellationToken) =>
   {
      var added = cdcPacket.data.packet.changeResult.OrderedChanges
         .Select(x => x.Put)
         .Where(x => x.IsSome)
         .Select(x => x.Value["FirstName"].S)
         .FirstOrDefault();

      if (added != null)
         Console.WriteLine($"{added} has joined the Beatles");

      var removed = cdcPacket.data.packet.changeResult.OrderedChanges
         .Select(x => x.Deleted)
         .Where(x => x.IsSome)
         .Select(x => x.Value["FirstName"].S)
         .FirstOrDefault();

      if (removed != null)
         Console.WriteLine($"{removed} has left the Beatles");

      return default;
   });

// disposing will remove the subscription
subscription.Dispose();
```

Subscriptions synchonicity and error handling can be customized with `SubscriberBehaviour` through the 
`SubscribeToStream` and `AddSubscription` methods. 

Subscriptions can be executed synchonously or asynchonously. For example, if a subscription 
is configured to execute synchronously, and a PUT request is executed, the `AmazonDynamoDBClient` will not return
a PUT response until the subscriber has completed it's work. If the subscription is asynchronous, then subscriber
execution is disconnected from the event trigger.

If a subscriber is synchronous, its errors can be propagated back to the trigger method, allowing for more direct
test results. Otherwise, errors are cached and can be retrieved in the form of Exceptions when an `AwaitAllSubscribers`
method is called

#### AwaitAllSubscribers

The `Api.Database`, `Api.GlobalDatabase` and `AmazonDynamoDBClient` have `AwaitAllSubscribers` methods to pause test execution
until all subscribers have executed. This method will throw any exceptions that were experienced within subscribers and were not
propagated synchronously. For `AmazonDynamoDBClient`, the `AwaitAllSubscribers` method is static and available on the `TestDynamoClient` class.

### DynamoDBContext

```C#
using TestDynamo;

using var client = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();

... // initialize the database schema

using var context = new DynamoDbContext(client)
await context.SaveAsync(new Beatle
{
   FirstName = "Ringo",
   SecondName = "Starr"
})
```

### Global Database

The global database models an AWS account with a collection of regions. Each region is an [`Api.Database`](#database). It is used to test global table functionality 

Creating global tables is a synchonous operation. The global table will 
be ready to use as soon as the `AmazonDynamoDBClient` client returns a response.

```C#
using TestDynamo;

using var globalDatabase = new GlobalDatabase();

// create a global table from ap-south-2
using var apSouth2Client = globalDatabase.CreateClient<AmazonDynamoDBClient>(new DatabaseId("ap-south-2"));
await apSouth2Client.CreateGlobalTableAsync(...);

// create a local table in cn-north-1
using var cnNorthClient = globalDatabase.CreateClient<AmazonDynamoDBClient>(new DatabaseId("cn-north-1"));
await cnNorthClient.CreateTableAsync(...);
```

### Global Database Cloning

```C#
using TestDynamo;

using var globalDatabase = new GlobalDatabase();
using var db2 = globalDatabase.Clone();
```

#### AwaitAllSubscribers

The `Api.GlobalDatabase` and `AmazonDynamoDBClient` have `AwaitAllSubscribers` methods to pause test execution
until all data has been replicated between databases. For `AmazonDynamoDBClient`, the `AwaitAllSubscribers` method is static and available on the `TestDynamoClient` class.

### TestDynamoClient

`TestDynamoClient` links an `AmazonDynamoDBClient` to an `Api.Database` or an `Api.GlobalDatabase`. It has several useful extension methods

#### Create methods

```C#
using TestDynamo;

// create a client with an empty database
using var client1 = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();

// create a client from an existing database
using var db1 = new Api.Database();
using var client21 = db1.CreateClient<AmazonDynamoDBClient>();

using var db2 = new Api.GlobalDatabase();
using var client22 = db2.CreateClient<AmazonDynamoDBClient>();

// attach a database to an existing client
using var db3 = new Api.Database();
using var client3 = new AmazonDynamoDBClient();
db3.Attach(client3);
```

#### Get methods

```C#
using TestDynamo;

using var client1 = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();

// get the underlying database from a client
var db1 = TestDynamoClient.GetDatabase(client1);

// get a debug table from a client
var beatles = TestDynamoClient.GetTable(client1, "Beatles");
```

Or in F#

```F#
open TestDynamo

use client1 = TestDynamoClient.createClient<AmazonDynamoDBClient> ValueNone false ValueNone false ValueNone

// get the underlying database from a client
let db1 = TestDynamoClient.getDatabase client1

// get a debug table from a client
let beatles = TestDynamoClient.getTable "Beatles" client1
```

#### Set methods

```C#
using TestDynamo;

using var client = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();

// set an artificial processing delay
TestDynamoClient.SetProcessingDelay(client, TimeSpan.FromSeconds(0.1));

// set paging settings for database
TestDynamoClient.SetScanLimits(client, ...);

// set the AWS account id for the client
TestDynamoClient.SetAwsAccountId(client, "12345678");
```

### Database Serializers

Database serializers are available from the `TestDynamo.Serialization` nuget package.

Database serializers can serialize or deserialize an entire database or global database to facilitate data driven testing.

```C#
using TestDynamo;
using TestDynamo.Serialization;

using var db1 = new Api.Database();
... populate database

DatabaseSerializer.Database.ToFile(db1, @"TestData.json");

using var db2 = DatabaseSerializer.Database.FromFile(@"TestData.json");

// there are also tools to serialize and deserialze global databases
var json = DatabaseSerializer.GlobalDatabase.ToString(globalDb);
```

Serialization is designed to share data between test runs, but ultimately, it scales with the number of items in the database. This means
that it may take more time than is ideal for executing fast unit tests. [Database cloning](#database-cloning) is a better solution for large databases which are shared between multiple tests, as it executes instantly for any sized database or global database

### Cloud Formation Templates

Static [Cloud Formation templates](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/AWS_DynamoDB.html) 
can be consumed to create databases and global databases. Dynamic templates with functions are not supported.

Import the `dotnet add package TestDynamo.Serialization` package to use cloudformation templates

```C#
using TestDynamo.Serialization;

var cfnFile1 = new CloudFormationFile(await File.ReadAllTextAsync("myTemplate1.json"), "eu-north-1");
var cfnFile2 = new CloudFormationFile(await File.ReadAllTextAsync("myTemplate2.json"), "us-west-2");
using var database = await CloudFormationParser.BuildDatabase(new[] { cfnFile1, cfnFile2 }, new CloudFormationSettings(true));
...
```

### Locking and Atomic transactions

Test dynamo is more consistant than DynamoDb. In general, all operations on a single database (region) are atomic. 
Within the `AmazonDynamoDBClient` client, BatchRead and BatchWrite operations are executed as several independant operations in order
to simulate non consistency.

The biggest differences you will see are

 * Reads are always atomic
 * Writes from tables to global secondary indexes are always atomic

### Transact write ClientRequestToken

[Client request tokens](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#API_TransactWriteItems_RequestSyntax) are used in transact write operations as an idempotency key. If 2 requests have the
same client request token, the second one will not be executed. By default AWS keeps client request tokens for 10 minutes. TestDynamo
keeps client request tokens for 10 seconds. This cache time can be updated in `Settings`.

### Recorders

Recorders can be added to record all inputs and outputs of a database

```C#
using TestDynamo;

using var client = TestDynamoClient.CreateClient<AmazonDynamoDBClient>(recordCalls: true);

// successful request to create a table
await client.CreateTableAsync(...);
try
{
   // failed request to put an item
   await client.PutItemAsync(...);
}
catch
{
   // do nothing
}


var recordings = TestDynamoClient
    .GetRecordings(client)
    .ToList();

Assert.True(recordings[0].request is CreateTableRequest);
Assert.True(recordings[0].IsSuccess);
Assert.True(recordings[0].SuccessResponse is CreateTableResponse);

Assert.True(recordings[1].request is PutItemRequest);
Assert.False(recordings[1].IsSuccess);
Assert.NotNull(recordings[1].Exception);
```

### Interceptors

Interceptors can be added to intercept and override certain database functionality.

For example, this sample implements create and restore backup functionality

#### Implement backups functionality

```C#
using TestDynamo;
using TestDynamo.Api.FSharp;
using TestDynamo.Client;
using TestDynamo.Model;

/// <summary>
/// An interceptor which implements the CreateBackup and RestoreTableFromBackup operations
/// </summary>
public class CreateBackupInterceptor(Dictionary<string, DatabaseCloneData> backupStore) : IRequestInterceptor
{
    public async ValueTask<object?> InterceptRequest(Api.FSharp.Database database, object request, CancellationToken c)
    {
        if (request is CreateBackupRequest create)
            return CreateBackup(database, create.TableName);

        if (request is RestoreTableFromBackupRequest restore)
            return await RestoreBackup(database, restore.BackupArn);

        // return null to allow the client to process other request types as normal
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
        // parse fake ARN created in the CreateBackup method
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

// create an in memory store for backups
var backups = new Dictionary<string, DatabaseCloneData>();

using var database = new Api.Database(new DatabaseId("us-west-1"));

// create an interceptor and use in a client
var interceptor = new CreateBackupInterceptor(backups);
using var client = database.CreateClient<AmazonDynamoDBClient>(interceptor);

// execute some requests which are not intercepted. These will not be intercepted
await client.PutItemAsync(...);
await client.PutItemAsync(...);

// create a backup. This will be intercepted
var backupResponse = await client.CreateBackupAsync(new CreateBackupRequest
{
   TableName = "Beatles"
});

// restore from backup. This will be intercepted
await client.RestoreTableFromBackupAsync(new RestoreTableFromBackupRequest
{
   BackupArn = backupResponse.BackupDetails.BackupArn
});
```

#### Implement BillingMode functionality

Here is another example of how to implement some out of scope [BillingMode](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BillingModeSummary.html) functionality with an interceptor

```C#
/// <summary>
/// An interceptor which implements BillingMode functionality for CreateTableAsync
/// </summary>
public class BillingModeInterceptor : IRequestInterceptor
{
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
            LastUpdateToPayPerRequestDateTime = DateTime.UtcNow
        };

        // Return default so that the response will be passed on after modification
        // If a non null item is returned here, it will be passed on instead 
        return default;
    }
}
```

### Logging

Logging is implemented by `Microsoft.Extensions.Logging.ILogger`. Databases can be created with loggers. Clients can also be created with loggers, which will override the datase logging