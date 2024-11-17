namespace TestDynamo.Tests

open System
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Serialization.Data
open TestDynamo.Utils
open Microsoft.Extensions.Logging
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open TestDynamo.Api.FSharp
open Tests.Loggers
open Tests.Requests.Queries
open TestDynamo.Serialization

#nowarn "0025"

type SerializationTests(output: ITestOutputHelper) =

    let mapTask f x =
        task {
            let! x' = x
            return (f x')
        }

    let bindTask f x =
        task {
            let! x' = x
            return! (f x')
        }

    let random = randomBuilder output

    let clonedHost writer =
        task {
            // make sure that tables are populated before getting common host
            let! _ = sharedTestData ValueNone
            let hostData = commonHost.BuildCloneData()

            let globalData =
                  { databases = [{ hostData with databaseId = { regionId = "eu-west-1"}}]
                    replicationKeys = [] }

            return new GlobalDatabase(globalData, logger = writer)
        }

    static let replicate (client: AmazonDynamoDBClient) regionName tableName =
        let req = UpdateTableRequest()
        req.TableName <- tableName
        req.ReplicaUpdates <- System.Collections.Generic.List()
        req.ReplicaUpdates.Add(ReplicationGroupUpdate())
        req.ReplicaUpdates[0].Create <- CreateReplicationGroupMemberAction()
        req.ReplicaUpdates[0].Create.RegionName <- regionName
        req.ReplicaUpdates[0].Create.GlobalSecondaryIndexes <- MList<_>([
            let i = ReplicaGlobalSecondaryIndex()
            i.IndexName <- "TheIndex"
            i
        ])

        client.UpdateTableAsync(req) |> Io.ignoreTask

    let setUp2Regions logger doReplication =
        task {
            let! host = clonedHost logger
            let client1 = TestDynamoClientBuilder.Create(host, { regionId = "eu-west-1"})
            let client2 = TestDynamoClientBuilder.Create(host, { regionId = "eu-north-1"})
            let! tables = sharedTestData ValueNone
            let table = Tables.getByStreamsEnabled true tables

            do!
                if doReplication
                then replicate client1 "eu-north-1" table.name
                else Task.CompletedTask

            let disposer =
                { new IDisposable with
                     member _.Dispose() =
                         host.Dispose()
                         client1.Dispose()
                         client2.Dispose() }

            return struct (table, host, client1, client2, disposer)
        }

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Serialize database`` ``omit data`` =

        task {
            // arrange
            use logger = new TestLogger(output, LogLevel.Warning) 
            let! struct (_, cluster, _, _, _) = setUp2Regions logger true
            use cluster = new Api.GlobalDatabase(cluster)
            let db1 = cluster.GetDatabase({regionId = "eu-north-1" })

            // act
            let ser1 = DatabaseSerializer.Database.ToString(db1, schemaOnly = ``omit data``)
            use db2 = DatabaseSerializer.Database.FromString(ser1)
            let ser2 = DatabaseSerializer.Database.ToString(db2, schemaOnly = ``omit data``)
            
            // assert
            Assert.True(ser1.Length > if ``omit data`` then 200 else 10_000)
            Assert.Equal(ser1, ser2)

            let tableCount (db: Api.Database) = Seq.length db.DebugTables
            let indexCount (db: Api.Database) = db.DebugTables |> Seq.collect _.Indexes |> Seq.length
            let itemCount (db: Api.Database) = db.DebugTables |> Seq.collect _.Values |> Seq.length 

            Assert.Equal(tableCount db1, tableCount db2)
            Assert.Equal(indexCount db1, indexCount db2)
            Assert.Equal((if ``omit data`` then 0 else itemCount db1), itemCount db2)
            Assert.True(itemCount db1 > 20, itemCount db1 |> toString)
        }

    [<Theory>]
    [<InlineData(true)>]
    [<InlineData(false)>]
    let ``Serialize global database`` ``omit data`` =

        task {
            // arrange
            use logger = new TestLogger(output, LogLevel.Trace) 
            let! struct (_, db1, _, _, _) = setUp2Regions logger true

            // act
            let ser1 = DatabaseSerializer.FSharp.GlobalDatabase.ToString(db1, schemaOnly = ``omit data``)
            let db2 = DatabaseSerializer.FSharp.GlobalDatabase.FromString(ser1)
            let ser2 = DatabaseSerializer.FSharp.GlobalDatabase.ToString(db2)

            // assert
            if ``omit data``
            then Assert.True(ser1.Length > 100)
            else Assert.True(ser1.Length > 10_000)

            Assert.Equal(ser1, ser2)

            let databases (db: GlobalDatabase) = db.GetDatabases() |> MapUtils.toSeq |> Seq.map sndT
            let dbCount = databases >> Seq.length
            let tableCount = databases >> Seq.sumBy (_.DebugTables >> List.length)
            let indexCount = databases >> Seq.sumBy (_.DebugTables >> Seq.collect _.Indexes >> Seq.length)
            let indexes = databases >> Seq.collect (_.DebugTables >> Seq.collect (fun t -> t.Indexes |> Seq.map (_.Name >> tpl t.Name >> toString))) >> Str.join "; "
            let itemCount = databases >> Seq.sumBy (_.DebugTables >> Seq.collect _.Values >> Seq.length)

            // output.WriteLine ser1
            Assert.Equal(dbCount db1, dbCount db2)
            Assert.True(dbCount db1 > 1, dbCount db1 |> toString)
            Assert.Equal(tableCount db1, tableCount db2)
            Assert.True(indexCount db1 >= 6, indexes db1)
            Assert.Equal(indexCount db1, indexCount db2)

            Assert.True(itemCount db1 > 100, itemCount db1 |> toString)
            if ``omit data``
            then Assert.Equal(0, itemCount db2)
            else Assert.Equal(itemCount db1, itemCount db2)
        }

    [<Theory>]
    [<InlineData(5_000)>]
    // [<InlineData(1_000_000)>]  // This takes 10 minutes
    let ``Serialize a global database, massive data smoke test`` ``item count`` =

        task {
            // arrange
            use logger = new TestLogger(output, LogLevel.Error) 
            let! struct (table, db1, client1, _, _) = setUp2Regions logger true

            let start = DateTimeOffset.Now
            let requests =
                [0..``item count``]
                |> Seq.map (fun i ->
                    ItemBuilder.empty
                    |> ItemBuilder.withTableName table.name
                    |> ItemBuilder.withAttribute "TablePk" "S" (i.ToString())
                    |> ItemBuilder.withAttribute "TableSk" "N" (i.ToString())
                    |> ItemBuilder.asPutReq)
                |> Collection.window 100

            for batch in requests do
                do!
                    batch
                    |> Seq.map (client1.PutItemAsync >> Io.fromTask)
                    |> Io.traverse
                    |> Io.ignore

            do! db1.AwaitAllSubscribers ValueNone CancellationToken.None    
            output.WriteLine($"ADD {DateTimeOffset.Now - start}")
            let start = DateTimeOffset.Now

            // act
            let! ser1 = DatabaseSerializer.FSharp.GlobalDatabase.ToStreamAsync(db1)
            output.WriteLine($"SERIALIZE {DateTimeOffset.Now - start}, SERIALIZED LENGTH {ser1.Length}")
            let start = DateTimeOffset.Now

            let! db2 = DatabaseSerializer.FSharp.GlobalDatabase.FromStreamAsync(ser1)
            output.WriteLine($"DESERIALIZE {DateTimeOffset.Now - start}")

            // assert
            let databases (db: GlobalDatabase) = db.GetDatabases() |> MapUtils.toSeq |> Seq.map sndT
            let itemCount = databases >> Seq.sumBy (_.DebugTables >> Seq.collect _.Values >> Seq.length)

            let itemCount1 = itemCount db1
            Assert.True(itemCount1 > ``item count`` * 2, itemCount1 |> toString)
            Assert.Equal(itemCount1, itemCount db2)
        }

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``DeSerialize global database, preserves replication`` ``insert new replica at top`` =

        task {
            // arrange
            use logger = new TestLogger(output, LogLevel.Debug) 
            let! struct (table, preSerialization, _, toDb, _) = setUp2Regions logger true

            // add non replicated table to somewhere-else
            let somewhereElse = { regionId = "somewhere-else" }
            (new Api.GlobalDatabase(preSerialization))
                .GetDatabase(somewhereElse)
                .TableBuilder(table.name, struct ("TablePk", "S"), struct ("TableSk", "N"))
                .WithGlobalSecondaryIndex("TheIndex", struct ("IndexPk", "N"), struct ("IndexSk", "S"))
                .WithStreamsEnabled(true)
                .AddTable()

            let dbString = DatabaseSerializer.FSharp.GlobalDatabase.ToString(preSerialization)
            let dbJson = JsonObject.Parse(dbString)

            // add replicate to somewhere-else
            let newReplica =
                Version1.SerializableReplication(
                   TestDynamoClient.GetDatabase(toDb).Id,
                   somewhereElse,
                   table.name)
                |> RawSerializers.Strings.write false
                |> JsonObject.Parse

            let replicas = (dbJson["data"].AsObject()["replications"]).AsArray()
            // position affects how replicas are added. if new replica is inserted, it's processing needs to be delayed
            if ``insert new replica at top``
            then replicas.Insert(0, newReplica)
            else replicas.Add(newReplica)

            // output.WriteLine(dbJson.ToString())
            let db = DatabaseSerializer.FSharp.GlobalDatabase.FromString(dbJson.ToString())
            use client1 = TestDynamoClientBuilder.Create(db.GetDatabase (ValueSome logger) { regionId = "eu-north-1" }, logger)
            use client2 = TestDynamoClientBuilder.Create(db.GetDatabase (ValueSome logger) { regionId = "eu-west-1" }, logger)
            use client3 = TestDynamoClientBuilder.Create(db.GetDatabase (ValueSome logger) { regionId = "somewhere-else" }, logger)

            let put1 =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.withAttribute "TablePk" "S" "&&&"
                |> ItemBuilder.withAttribute "TableSk" "N" "777"

            let put2 = ItemBuilder.withAttribute "TableSk" "N" "888" put1

            // act
            let! _ = client1.PutItemAsync (ItemBuilder.asPutReq put1)
            let! _ = client2.PutItemAsync (ItemBuilder.asPutReq put2)
            do! db.AwaitAllSubscribers (ValueSome logger) CancellationToken.None

            // assert
            let req =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (AttributeValue.String "&&&")
                |> QueryBuilder.queryRequest

            let! items1 = client1.QueryAsync req
            let! items2 = client2.QueryAsync req
            let! items3 = client3.QueryAsync req

            Assert.Equal(2, items1.Count)
            Assert.Equal(2, items2.Count)
            Assert.Equal(2, items3.Count)
        }