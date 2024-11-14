# TestDynamo

An in-memory dynamodb client for automated testing

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

## Installation

 * Core functionality: `dotnet add package TestDynamo`
 * Add lambda support: `dotnet add package TestDynamo.Lambda`
 * Add serialization: `dotnet add package TestDynamo.Serialization`

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

 * [`Api.Database`](#database) contains tables from a single region.
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
 * [Locking and atomic transactions](#locking-and-atomic-transactions)
 * [Transact write ClientRequestToken](#transact-write-clientrequesttoken)
 * [Interceptors](#interceptors) can be used to modify the functionality of the database, either to add more traditional mocking or to polyfill unsupported features 
 * [Logging](#logging) can be configured at the database level or the `AmazonDynamoDBClient` level

```C#
using TestDynamo;

using var client = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();
var myService = new MyService(client);

...
```

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

![Debugger](./Docs/DbDebugger.png "Debugger")

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

var subscription = database.AddSubscription(
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
propagated synchronously. For `AmazonDynamoDBClient`, the `AwaitAllSubscribers` method is an extension method.

### DynamoDBContext

```C#
using TestDynamo;

using var client = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();
var context = new DynamoDbContext(client)

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
until all data has been replicated between databases. For `AmazonDynamoDBClient`, the `AwaitAllSubscribers` method is an extension method.

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
client3.Attach(db3);
```

#### Get methods

```C#
using TestDynamo;

using var client1 = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();

// get the underlying database from a client
var db1 = client.GetDatabase();

// get a debug table from a client
var beatles = client.GetTable("Beatles");
```

#### Set methods

```C#
using TestDynamo;

using var client = TestDynamoClient.CreateClient<AmazonDynamoDBClient>();

// set an artificial processing delay
client.SetProcessingDelay(TimeSpan.FromSeconds(0.1));

// set paging settings for database
client.SetScanLimits(...);

// set the AWS account id for the client
client.SetAwsAccountId("12345678");
```

### Database Serializers

Database serializers are available from the `TestDynamo.Serialization` nuget package

Database serializers can serialize or deserialize an entire database or global database to facilitate data driven testing

```C#
using TestDynamo;
using TestDynamo.Serialization;

using var db1 = new Api.Database();
... populate database

DatabaseSerializer.Database.ToFile(db1, @"C:\Tests\TestData.json");

using var db2 = DatabaseSerializer.Database.FromFile(@"C:\Tests\TestData.json");
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

### Interceptors

Interceptors can be added to intercept and override certain database functionality.

```C#
using TestDynamo;
using TestDynamo.Client;

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

using var client = TestDynamoClient.CreateClient<AmazonDynamoDBClient>(new CreateBackupInterceptor());
var createBackupResponse = await client.CreateBackupAsync(...);
```

### Logging

Logging is implemented by `Microsoft.Extensions.Logging.ILogger`. Databases can be created with loggers. Clients can also be created with loggers, which will override the datase logging