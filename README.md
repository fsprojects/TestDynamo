# TestDynamo

An in-memory dynamodb client for automated testing

TestDynamo is a rewrite of dynamodb in dotnet designed for testing and debugging. 
It implements a partial feature set of `IAmazonDynamoDb` to manage schemas and read and write items.

 * [Core features](./Features.md)
    * Table management (Create/Update/Delete table)
    * Index management (Create/Update/Delete index)
    * Item operations (Put/Delete/Update etc)
    * Queries and Scans
    * Batching and Transactional writes
 * Multi region setups
 * Global tables and replication
 * Streams and stream subscribers
 * Efficient cloning and deep copying of databases for isolation
 * Full database serialization and deserialization for data driven testing

## Installation

TODO

## The basics

```C#
using TestDynamo;

[Test]
public async Task GetPersonById_WithValidId_ReturnsPerson()
{
   // arrange
   using var client = TestDynamoClient.Create();

   // create a table and add some items
   await client.CreateTableAsync(...);
   await client.BatchWriteItemAsync(...);

   var testSubject = new MyBeatlesService(client);

   // act
   var person = testSubject.GetBeatle("Ringo");

   // assert
   Assert.Equal("Starr", person.SecondName);
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
 * [`Api.GlobalDatabase`](#global-database) models an AWS account spread over multiple regions. It contains `Api.Database`s.
    * [Global databases](#global-database-cloning) can be cloned too
 * [`ITestDynamoClient`](#itestdynamoclient) is the entry point for dynamodb operations. It implements `IAmazonDynamoDb`.
    * Check the [features](./Features.md) for a full list of endpoints, request args and responses that are supported.
 * [`DatabaseSerializer`](#database-serializers) is a json serializer/deserializer for entire databases and global databases.

### ITestDynamoClient

`ITestDynamoClient` is a client which can be used by test classes

```C#
using TestDynamo;

using var client = TestDynamoClient.Create();
var myService = new MyService(client);

...
```

### Using Expressions

All [dynamodb expression types](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html) are suppoerted

### Database

The database is the core of TestDynamo and it models a single region and it's tables. The database 
is both fast and lightweight, built from simple data structures.

Databases can be injected into an `ITestDynamoClient` to then be passed into a test


```C#
using TestDynamo;

using var db = new Api.Database(new DatabaseId("us-west-1"));
using var client = TestDynamoClient.Create(db);
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
actually copied during a clone and cloning has almost no overhead.

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
   using var client = TestDynamo.Create(database);

   // act
   ...

   // assert
   ...
}
```

### Debug Properties

Use the handy debug properties in your debugger of choice

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

Subscribe to changes with a lambda stream subscription syntax

```C#
var subscription = database
   .SubscribeToLambdaStream("Beatles", "1234567", LambdaStreamSubscriber
      .Build((dynamoDbStreamsEvent, cancellationToken) =>
      {
         var added = dynamoDbStreamsEvent.Records.FirstOrDefault()?.Dynamodb.NewImage?["FirstName"]?.S;
         if (added != null)
            Console.WriteLine($"{added} has joined the Beatles");
         
         var removed = dynamoDbStreamsEvent.Records.FirstOrDefault()?.Dynamodb.OldImage?["FirstName"]?.S;
         if (removed != null)
            Console.WriteLine($"{removed} has left the Beatles");
         
         return default;
      }));

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

### Global Database

The global database models an aws account with a collection of regions. Each region is an [`Api.Database`](#database). It is used test global table functionality 

```C#
using TestDynamo;

using var globalDatabase = new GlobalDatabase();

// create a global table from ap-south-2
using var apSouth2Client = TestDynamoClient.Create(globalDatabase, new DatabaseId("ap-south-2"));
await apSouth2Client.CreateGlobalTableAsync(...);

// create a local table in cn-north-1
using var cnNorthClient = TestDynamoClient.Create(globalDatabase, new DatabaseId("cn-north-1"));
await cnNorthClient.CreateTableAsync(...);
```

### Global Database Cloning

```C#
using TestDynamo;

using var globalDatabase = new GlobalDatabase();
using var db2 = globalDatabase.Clone();
```

### Database Serializers

Database serializers can serialize or deserialize an entire database or global database to facilitate data driven testing

```C#
using TestDynamo;
using TestDynamo.Serialization;

using var db1 = new Api.Database();
... populate database

DatabaseSerializer.Database.ToFile(db1, @"C:\Tests\TestData.json");

using var db2 = DatabaseSerializer.Database.FromFile(@"C:\Tests\TestData.json");
```