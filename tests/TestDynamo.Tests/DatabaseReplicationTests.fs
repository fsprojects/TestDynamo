namespace TestDynamo.Tests

open System
open System.Linq
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open Microsoft.Extensions.Logging
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open TestDynamo.Api.FSharp
open Tests.Table
open Tests.Loggers
open Tests.Requests.Queries

#nowarn "0025"

type ChangeType =
    | CreateGlobal = 1
    | UpdateGlobal = 2
    | Update = 3

type DatabaseReplicationTests(output: ITestOutputHelper) =

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
                  { databases = [{ hostData with databaseId = { regionId = "eu-west-1" }}]
                    replicationKeys = [] }

            return new GlobalDatabase(globalData, logger = writer)
        }

    static let rec replicate' (output: ITestOutputHelper, random) (client: AmazonDynamoDBClient) regionName tableName changeType =

        output.WriteLine($"Replicating {tableName}: {(TestDynamoClient.GetDatabase(client)).Id.regionId} => {regionName}")

        match changeType with
        | ValueNone ->
            let vs =
                Enum.GetValues<ChangeType>()
                |> randomSort random
                |> Seq.head
                |> ValueSome

            output.WriteLine($"Randomly chosen ChangeType {vs}")
            replicate' (output, random) (client) regionName tableName vs

        | ValueSome ChangeType.Update ->
            let req = UpdateTableRequest()
            req.TableName <- tableName
            req.ReplicaUpdates <- System.Collections.Generic.List()
            req.ReplicaUpdates.Add(ReplicationGroupUpdate())
            req.ReplicaUpdates[0].Create <- CreateReplicationGroupMemberAction()
            req.ReplicaUpdates[0].Create.RegionName <- regionName

            output.WriteLine("Update")
            client.UpdateTableAsync(req) |> Io.ignoreTask
        | ValueSome ChangeType.CreateGlobal ->
            let req = CreateGlobalTableRequest()
            req.GlobalTableName <- tableName
            req.ReplicationGroup <- System.Collections.Generic.List()
            req.ReplicationGroup.Add(Replica())
            req.ReplicationGroup[0].RegionName <- regionName

            output.WriteLine("CreateGlobal")
            client.CreateGlobalTableAsync(req) |> Io.ignoreTask
        | ValueSome ChangeType.UpdateGlobal ->
            let req = UpdateGlobalTableRequest()
            req.GlobalTableName <- tableName
            req.ReplicaUpdates <- System.Collections.Generic.List()
            req.ReplicaUpdates.Add(ReplicaUpdate())
            req.ReplicaUpdates[0].Create <- CreateReplicaAction()
            req.ReplicaUpdates[0].Create.RegionName <- regionName

            output.WriteLine("UpdateGlobal")
            client.UpdateGlobalTableAsync(req) |> Io.ignoreTask
        | x -> invalidOp $"{x}"

    static let rec replicateWithLevel (output: ITestOutputHelper, random, logLevel) (host: GlobalDatabase, dbId) regionName tableName changeType changeFromDefaultBehaviour  =

        task {
            let dbId = { regionId = dbId }
            let logger = new TestLogger(output, logLevel)
            use client = TestDynamoClientBuilder.Create(host, dbId, logger)
            do! replicate'  (output, random) client regionName tableName changeType

            let key =
                { fromDb = dbId
                  toDb = { regionId = regionName }
                  tableName = tableName }

            output.WriteLine($"#### {changeFromDefaultBehaviour}")
            changeFromDefaultBehaviour
            ?|> flip (host.UpdateReplication (ValueSome logger) key) true
            ?|? ()
        } |> Io.ignoreTask

    static let rec replicate (output: ITestOutputHelper, random) (host: GlobalDatabase, dbId) regionName tableName changeType changeFromDefaultBehaviour  =
        replicateWithLevel (output, random, LogLevel.Debug) (host, dbId) regionName tableName changeType changeFromDefaultBehaviour

    let deReplicate (client: AmazonDynamoDBClient) regionName tableName = function
        | ChangeType.Update ->
            let req = UpdateTableRequest()
            req.TableName <- tableName
            req.ReplicaUpdates <- System.Collections.Generic.List()
            req.ReplicaUpdates.Add(ReplicationGroupUpdate())
            req.ReplicaUpdates[0].Delete <- DeleteReplicationGroupMemberAction()
            req.ReplicaUpdates[0].Delete.RegionName <- regionName

            client.UpdateTableAsync(req) |> Io.ignoreTask
        | ChangeType.UpdateGlobal ->
            let req = UpdateGlobalTableRequest()
            req.GlobalTableName <- tableName
            req.ReplicaUpdates <- System.Collections.Generic.List()
            req.ReplicaUpdates.Add(ReplicaUpdate())
            req.ReplicaUpdates[0].Delete <- DeleteReplicaAction()
            req.ReplicaUpdates[0].Delete.RegionName <- regionName

            client.UpdateGlobalTableAsync(req) |> Io.ignoreTask
        | x -> invalidOp $"{x}"

    let setUp2RegionsWithCustomSettings settings logger doReplication =
        task {
            let! host = clonedHost logger
            let client1 = TestDynamoClientBuilder.Create(host, { regionId = "eu-west-1"})
            let client2 = TestDynamoClientBuilder.Create(host, { regionId = "eu-north-1"})
            let! tables = sharedTestData ValueNone
            let table = Tables.getByStreamsEnabled true tables

            do!
                if doReplication
                then replicate (output, random) (host, "eu-west-1") "eu-north-1" table.name ValueNone settings
                else Task.CompletedTask

            let disposer =
                { new IDisposable with
                     member _.Dispose() =
                         host.Dispose()
                         client1.Dispose()
                         client2.Dispose() }

            return struct (table, host, client1, client2, disposer)
        }

    let setUp2RegionsWithCustomDelay delay =
        {subscriberTimeout = TimeSpan.FromSeconds(float 1)
         delay = delay |> RunAsynchronously } |> ValueSome |> setUp2RegionsWithCustomSettings

    let setUp2Regions = setUp2RegionsWithCustomDelay (TimeSpan.FromSeconds(0.01))

    let put tableName item (client: AmazonDynamoDBClient) =
        client.PutItemAsync(tableName, itemToDynamoDb item)
        |> Io.ignoreTask

    let assertItem tableName keys expected (client: AmazonDynamoDBClient) =
        task {
            let! actual = client.GetItemAsync(tableName, itemToDynamoDb keys)
            Assert.NotEmpty(actual.Item)

            assertModelItems ([expected], [itemFromDynamodb actual.Item], true)
        }

    let assertNoItem tableName keys (client: AmazonDynamoDBClient) =
        task {
            let! actual = client.GetItemAsync(tableName, itemToDynamoDb keys)
            if actual.Item = null then ()
            else assertNullOrEmpty actual.Item
        }

    let assertExists tableName keys = function
        | ValueSome x -> assertItem tableName keys x
        | ValueNone -> assertNoItem tableName keys

    let executePutSmokeTest direction item itemKeys struct (table: TestDynamo.Tests.TableDescription, host: GlobalDatabase, client1: AmazonDynamoDBClient, client2: AmazonDynamoDBClient, _) =

        task {
            do!
                match direction with
                | "parent => child" -> client1
                | "child => parent" -> client2
                | x -> invalidOp x
                |> _.PutItemAsync(table.name, itemToDynamoDb item)
                |> Io.ignoreTask
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            // assert
            let! item1 = client1.GetItemAsync(table.name, itemToDynamoDb itemKeys)
            let! item2 = client2.GetItemAsync(table.name, itemToDynamoDb itemKeys)

            assertDynamoDbItems ([item1.Item], [item2.Item], true)
        }

    [<Theory>]
    [<InlineData("parent => child")>]
    [<InlineData("child => parent")>]
    let ``Replication smoke tests, PUT, new`` direction =

        task {
            // arrange
            use logger = new TestLogger(output) 
            let! (struct (_, _, _, _, disposer) & args) = setUp2Regions logger true
            use _ = disposer

            let pk = AttributeValue.String $"{IncrementingId.next()}"
            let sk = IncrementingId.next() |> IncrementingId.value |> decimal |> AttributeValue.Number
            let prop = AttributeValue.String $"{IncrementingId.next()}"
            let newItemKeys =
                Map.add "TablePk" pk Map.empty
                |> Map.add "TableSk" sk
            let newItem =
                newItemKeys
                |> Map.add "Something elsse" prop

            // act
            // assert
            do! executePutSmokeTest direction newItem newItemKeys args
        }

    [<Fact>]
    let ``Clear table smoke tests. Kinda about replication, kinda not`` () =

        task {
            // arrange
            use logger = new TestLogger(output) 
            let! struct (table, host, _, _, disposer) = setUp2Regions logger true
            let db1 = host.GetDatabase (ValueSome logger) { regionId = "eu-west-1" }
            let db2 = host.GetDatabase (ValueSome logger) { regionId = "eu-north-1" }
            use _ = disposer

            let tableEmpty (db: Database) = db.GetTable ValueNone table.name |> _.GetValues() |> Seq.isEmpty
            Assert.False(tableEmpty db1)
            Assert.False(tableEmpty db2)

            // act
            db1.ClearTable ValueNone table.name
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            // assert
            Assert.True(tableEmpty db1)
            Assert.True(tableEmpty db2)
        }

    [<Theory>]
    [<InlineData("parent => child")>]
    [<InlineData("child => parent")>]
    let ``Replication smoke tests, PUT existing`` direction =

        task {
            // arrange
            use logger = new TestLogger(output) 
            let! (struct (table, _, _, _, disposer) & args) = setUp2Regions logger true
            use _ = disposer

            let item =
                groupedItems table.hasSk
                |> fstT
                |> Seq.collect sndT
                |> Seq.map sndT
                |> Seq.head

            let pk = item.tablePk |> AttributeValue.String
            let sk = if table.hasSk then item.tableSk |> AttributeValue.Number |> ValueSome else ValueNone
            let prop = AttributeValue.String $"{IncrementingId.next()}"
            let newItemKeys =
                Map.add "TablePk" pk Map.empty
                |> (ValueOption.map (Map.add "TableSk") sk |> ValueOption.defaultValue id)

            let newItem =
                newItemKeys
                |> Map.add "Something elsse" prop

            // act
            // assert
            do! executePutSmokeTest direction newItem newItemKeys args
        }

    [<Theory>]
    [<InlineData("parent => child")>]
    [<InlineData("child => parent")>]
    let ``Replication smoke tests, DELETE`` direction =

        task {
            // arrange
            use logger = new TestLogger(output) 
            let! struct (table, host, client1, client2, disposer) = setUp2Regions logger true
            use _ = disposer

            let item =
                groupedItems table.hasSk
                |> fstT
                |> Seq.collect sndT
                |> Seq.map sndT
                |> Seq.head

            let pk = item.tablePk |> AttributeValue.String
            let sk = if table.hasSk then item.tableSk |> AttributeValue.Number |> ValueSome else ValueNone
            let keys =
                Map.add "TablePk" pk Map.empty
                |> (ValueOption.map (Map.add "TableSk") sk |> ValueOption.defaultValue id)

            let assertExists (client: AmazonDynamoDBClient) exists =
                task {
                    let! response = client.GetItemAsync(table.name, itemToDynamoDb keys)
                    if exists then Assert.NotEmpty(response.Item)
                    elif response.Item = null then ()
                    else Assert.Empty(response.Item)

                    return ()
                }

            do! assertExists client1 true 
            do! assertExists client2 true

            // act
            do!
                match direction with
                | "parent => child" -> client1
                | "child => parent" -> client2
                | x -> invalidOp x
                |> _.DeleteItemAsync(table.name, itemToDynamoDb keys)
                |> Io.ignoreTask
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            // assert
            do! assertExists client1 false 
            do! assertExists client2 false
        }

    [<Theory>]
    [<InlineData("parent")>]
    [<InlineData("child")>]
    let ``Replication smoke tests, Competing PUTS`` dominantDirection =

        task {
            // arrange
            use logger = new TestLogger(output) 
            let! struct (table, host, client1, client2, disposer) = setUp2RegionsWithCustomDelay (TimeSpan.FromSeconds(0.1)) logger true
            use _ = disposer

            TestDynamoClient.SetProcessingDelay(client1, TimeSpan.Zero)
            TestDynamoClient.SetProcessingDelay(client2, TimeSpan.Zero)

            let item =
                groupedItems table.hasSk
                |> fstT
                |> Seq.collect sndT
                |> Seq.map sndT
                |> Seq.head

            let pk = item.tablePk |> AttributeValue.String
            let sk = if table.hasSk then item.tableSk |> AttributeValue.Number |> ValueSome else ValueNone
            let keys =
                Map.add "TablePk" pk Map.empty
                |> (ValueOption.map (Map.add "TableSk") sk |> ValueOption.defaultValue id)

            let dominant = Map.add "Type" (AttributeValue.String "Dominant") keys
            let notDominant = Map.add "Type" (AttributeValue.String "Not dominant") keys

            let struct (dominantClient, notDominantClient) =
                match dominantDirection with
                | "parent" -> struct (client1, client2)
                | "child" -> struct (client2, client1)

            // act
            // dominant is defined as the second put request
            do! put table.name notDominant notDominantClient
            do! put table.name dominant dominantClient
            output.WriteLine("\n\n############## START TEST")
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            // assert
            do! assertItem table.name keys dominant client1
            do! assertItem table.name keys dominant client2
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Replication smoke tests, Competing PUT and DELETE`` parentDominant putDominant itemExists =

        task {
            // arrange
            use logger = new TestLogger(output) 
            let! struct (table, host, client1, client2, disposer) = setUp2RegionsWithCustomDelay (TimeSpan.FromSeconds(0.1)) logger true
            use _ = disposer

            TestDynamoClient.SetProcessingDelay(client1, TimeSpan.Zero)
            TestDynamoClient.SetProcessingDelay(client2, TimeSpan.Zero)

            let struct (pk, sk) =
                if itemExists then
                    groupedItems table.hasSk
                    |> fstT
                    |> Seq.collect sndT
                    |> Seq.map sndT
                    |> Seq.head
                    |> tplDouble
                    |> mapFst (_.tablePk >> AttributeValue.String)
                    |> mapSnd (if table.hasSk then _.tableSk >> AttributeValue.Number >> ValueSome else asLazy ValueNone)
                else
                    AttributeValue.String $"Id-{IncrementingId.next()}"
                    |> flip tpl (
                        if table.hasSk
                            then IncrementingId.next() |> IncrementingId.value |> decimal |> AttributeValue.Number |> ValueSome
                            else ValueNone)

            let keys =
                Map.add "TablePk" pk Map.empty
                |> (ValueOption.map (Map.add "TableSk") sk |> ValueOption.defaultValue id)

            let value = Map.add "Val" (AttributeValue.String "U") keys

            let doPut (client: AmazonDynamoDBClient) = client.PutItemAsync(table.name, itemToDynamoDb value) |> Io.ignoreTask
            let doDelete (client: AmazonDynamoDBClient) = client.DeleteItemAsync(table.name, itemToDynamoDb keys) |> Io.ignoreTask

            let struct (dominantClient, notDominantClient) =
                match parentDominant with
                | true -> struct (client1, client2)
                | false -> struct (client2, client1)

            let struct (firstAction, secondAction, expectedResult) =
                match putDominant with
                | true -> struct (doDelete, doPut, ValueSome value)
                | false -> struct (doPut, doDelete, ValueNone)

            // act
            // dominant is defined as the second put request
            do! firstAction notDominantClient
            do! secondAction dominantClient
            output.WriteLine("\n\n############## START TEST")
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            // assert
            do! assertExists table.name keys expectedResult client1
            do! assertExists table.name keys expectedResult client2
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Replicate DB, 2 regions, CRUD, behaves correctly`` ``wait for each operation`` synchronous =
        task {
            // arrange
            let subscriberSettings =
                if synchronous then {subscriberTimeout = TimeSpan.FromSeconds(float 1); delay = RunSynchronously }
                else {subscriberTimeout = TimeSpan.FromSeconds(float 1); delay = TimeSpan.FromSeconds(0.01) |> RunAsynchronously }

            use logger = new TestLogger(output) 
            let! struct (table, host, client1, client2, disposer) = setUp2RegionsWithCustomSettings (ValueSome subscriberSettings) logger false
            use _ = disposer

            let items =
                groupedItems table.hasSk
                |> fstT
                |> Seq.collect sndT
                |> Seq.map sndT
                |> List.ofSeq

            let create1::create2::create3::update1::update2::update3::delete1::delete2::delete3::_ =
                randomSeqItems 9 (fun _ -> true) random items |> List.ofSeq

            let keyCols = if table.hasSk then ["TablePk"; "TableSk"] else ["TablePk"]
            let delete1::delete2::delete3::_ =
                [delete1; delete2; delete3]
                |> Seq.map (
                    TableItem.asAttributes
                    >> Seq.filter (fun x -> List.contains x.Key keyCols)
                    >> Enumerable.ToDictionary)
                |> List.ofSeq

            let create1::create2::create3::_ =
                [create1; create2; create3]
                |> Seq.map (
                    TableItem.asItem
                    >> Map.add "TablePk" (String $"new-item-${IncrementingId.next()}")
                    >> itemToDynamoDb)
                |> List.ofSeq

            let update1::update2::update3::_ =
                [update1; update2; update3]
                |> Seq.mapi (fun i ->
                    TableItem.asItem
                    >> Map.add "SomeData" (String $"new-item-${i}")
                    >> itemToDynamoDb)
                |> List.ofSeq

            let! _ = client1.DeleteItemAsync(table.name, delete1)
            let! _ = client1.PutItemAsync(table.name, update1)
            let! _ = client1.PutItemAsync(table.name, create1)

            // act
            do! replicate (output, random) (host, "eu-west-1") "eu-north-1" table.name ValueNone (ValueSome subscriberSettings)
            if ``wait for each operation`` then do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            let! _ = client1.DeleteItemAsync(table.name, delete2)
            if ``wait for each operation`` then do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            let! _ = client2.DeleteItemAsync(table.name, delete3)
            if ``wait for each operation`` then do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            let! _ = client1.PutItemAsync(table.name, update2)
            if ``wait for each operation`` then do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            let! _ = client2.PutItemAsync(table.name, update3)
            if ``wait for each operation`` then do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            let! _ = client1.PutItemAsync(table.name, create2)
            if ``wait for each operation`` then do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            let! _ = client2.PutItemAsync(table.name, create3)
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            // assert
            let! items1 = client1.ScanAsync(
                ScanBuilder.empty
                |> ScanBuilder.withTableName table.name
                |> ScanBuilder.req)
            let! items2 = client2.ScanAsync(
                ScanBuilder.empty
                |> ScanBuilder.withTableName table.name
                |> ScanBuilder.req)

            Assert.Equal(items1.Items.Count, items2.Items.Count)

            let is2 =
                Seq.map itemFromDynamodb items2.Items |> Seq.sort
                |> Seq.zip (Seq.map itemFromDynamodb items1.Items |> Seq.sort)

            for (expected, actual) in is2 do
                Assert.True((expected = actual), attrsToString [expected; actual] |> sprintf "EXPECTED; ACTUAL\n%s")

            assertDynamoDbItems (items1.Items, items2.Items, false)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Replicate DB, 4 regions, Operations propagate correctly`` ``wait for each operation`` synchronous =

        task {

            // arrange
            use writer = new TestLogger(output)
            let subscriberSettings =
                if synchronous then {subscriberTimeout = TimeSpan.FromSeconds(float 1); delay = RunSynchronously }
                else {subscriberTimeout = TimeSpan.FromSeconds(float 1); delay = TimeSpan.FromSeconds(0.01) |> RunAsynchronously }

            use! host = clonedHost writer
            use client1 = TestDynamoClientBuilder.Create(host, { regionId = "eu-west-1"})
            use client2 = TestDynamoClientBuilder.Create(host, { regionId = "eu-north-1"})
            use client3 = TestDynamoClientBuilder.Create(host, { regionId = "ap-south-2"})
            use client4 = TestDynamoClientBuilder.Create(host, { regionId = "us-east-2"})
            let! tables = sharedTestData ValueNone
            let table = Tables.getByStreamsEnabled true tables

            let items =
                groupedItems table.hasSk
                |> fstT
                |> Seq.collect sndT
                |> Seq.map sndT
                |> List.ofSeq

            let update1::update2::_ =
                randomSeqItems 2 (asLazy true) random items |> List.ofSeq

            let someData = "SomeData"
            let update1::update4::_ =
                [update1; update2]
                |> Seq.mapi (fun i ->
                    TableItem.asItem
                    >> Map.add someData (String $"new-item-${i}")
                    >> itemToDynamoDb)
                |> List.ofSeq

            let updateData1 = update1[someData].S
            let updateData4 = update4[someData].S

            // act
            let! _ = replicate (output, random) (host, "eu-west-1") "eu-north-1" table.name (ValueSome ChangeType.CreateGlobal) (ValueSome subscriberSettings)
            let! _ = replicate (output, random) (host, "eu-north-1") "ap-south-2" table.name (ValueSome ChangeType.UpdateGlobal) (ValueSome subscriberSettings)
            let! _ = replicate (output, random) (host, "ap-south-2") "us-east-2" table.name (ValueSome ChangeType.Update) (ValueSome subscriberSettings)
            if ``wait for each operation`` then do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            let! _ = client1.PutItemAsync(table.name, update1)
            if ``wait for each operation`` then do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            let! _ = client4.PutItemAsync(table.name, update4)
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            // assert
            let scan (client: AmazonDynamoDBClient) =
                client.ScanAsync(
                    ScanBuilder.empty
                    |> ScanBuilder.withTableName table.name
                    |> ScanBuilder.req)

            let! items1 = scan client1
            let! items2 = scan client2
            let! items3 = scan client3
            let! items4 = scan client4

            let itemCount1 =
                items1.Items
                |> Seq.filter (fun x -> x.ContainsKey(someData) && x[someData].S = updateData1)
                |> Seq.length
            let itemCount2 =
                items1.Items
                |> Seq.filter (fun x -> x.ContainsKey(someData) && x[someData].S = updateData4)
                |> Seq.length

            Assert.Equal(1, itemCount1)
            Assert.Equal(1, itemCount2)

            Assert.Equal(items1.Items.Count, items2.Items.Count)
            assertDynamoDbItems (items1.Items, items2.Items, false)

            Assert.Equal(items3.Items.Count, items4.Items.Count)
            assertDynamoDbItems (items3.Items, items4.Items, false)

            assertDynamoDbItems (items1.Items, items3.Items, false)

            // random double dispose of client
            client2.Dispose()
        }

    [<Fact>]
    let ``Global DB, some random disposal tests`` () =

        task {

            // arrange
            let subscriberSettings =
                { subscriberTimeout = TimeSpan.FromSeconds(float 1)
                  delay = TimeSpan.FromSeconds(0.1) |> RunAsynchronously }

            use writer = new TestLogger(output)
            use! host = clonedHost writer
            let r1 = { regionId = "eu-west-1"}
            let r2 = { regionId = "eu-north-1"}
            use client1 = TestDynamoClientBuilder.Create(host, r1)
            use client2 = TestDynamoClientBuilder.Create(host, r2)
            TestDynamoClient.SetProcessingDelay(client1, TimeSpan.Zero)
            TestDynamoClient.SetProcessingDelay(client2, TimeSpan.Zero)

            do!
                TableBuilder.empty
                |> TableBuilder.withTableName "Tabbb1"
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withKeySchema "TablePk" ValueNone
                |> TableBuilder.withStreamAction (CreateOrDelete.Create())
                |> TableBuilder.req
                |> client1.CreateTableAsync
                |> Io.ignoreTask

            do! replicate (output, random) (host, r1.regionId) r2.regionId "Tabbb1" ValueNone (ValueSome subscriberSettings)
            let struct (_, struct (_, data)) = randomItem false random

            // act
            let! _ = client1.PutItemAsync("Tabbb1", data |> itemToDynamoDb)
            (host.GetDatabase ValueNone { regionId = r2.regionId}).Dispose()

            // assert
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None
            let! _ = client1.PutItemAsync("Tabbb1", data |> itemToDynamoDb)
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None
        }

    [<Theory>]
    [<InlineData(true, false, false, false)>]
    [<InlineData(false, true, false, false)>]
    [<InlineData(false, false, true, false)>]
    [<InlineData(false, false, false, true)>]
    let ``Replicate DB, failure scenarios, fails and does not replicate`` ``table exists already`` ``no streams enabled`` ``has invalid table name`` ``invalid region`` =

        task {

            // arrange
            use writer = new TestLogger(output)
            use host = new GlobalDatabase(logger = writer)
            let region1 = "eu-west-1"
            let region2 = "eu-north-1"
            use client1 = TestDynamoClientBuilder.Create(host, { regionId = region1})
            use client2 = TestDynamoClientBuilder.Create(host, { regionId = region2})
            let tableName = "Tab1"            
            let createTable (client: AmazonDynamoDBClient) =
                let req =
                    TableBuilder.empty
                    |> TableBuilder.withTableName tableName
                    |> TableBuilder.withAttribute "TablePk" "S"
                    |> TableBuilder.withKeySchema "TablePk" ValueNone
                    |> if ``no streams enabled``
                       then id
                       else TableBuilder.withStreamAction (CreateOrDelete.Create ())

                TableBuilder.req req |> client.CreateTableAsync

            let! _ = createTable client1
            let! _ =
                if ``table exists already`` then
                    task {
                        let! _ = createTable client2
                        return ()
                    }
                else System.Threading.Tasks.Task.FromResult(())

            // act
            let! _ = Assert.ThrowsAnyAsync(fun () ->
                let r = if ``invalid region`` then region1 else region2
                let t = if ``has invalid table name`` then "invalid" else tableName
                replicate (output, random) (host, region1) r t ValueNone ValueNone)

            writer.Record true

            // assert
            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" "i1"

            // act
            let! _ = client1.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)
            do! (host.GetDatabase ValueNone {regionId = region1}).AwaitAllSubscribers ValueNone CancellationToken.None

            let allData =
                host.DebugTables
                |> Map.filter (fun _ v -> List.isEmpty v |> not)

            let w =  (ValueSome (writer  :> Microsoft.Extensions.Logging.ILogger))
            Assert.Equal(ValueNone, (host.GetDatabase w { regionId = region2 }).TryDescribeTable w tableName)

            Assert.Equal(1, MapUtils.toSeq allData |> Seq.map sndT |> Seq.collect id |> Seq.map _.Values |> Seq.collect id |> Seq.length)

            Assert.Equal(
                1,
                allData
                |> Map.find { regionId = region1 }
                |> List.filter (_.Name >> ((=) tableName))
                |> Seq.collect _.Values
                |> Seq.length)

            let recorded = writer.RecordedStrings()
            Assert.NotEmpty(recorded)
            Assert.Empty(Seq.filter (fun (x: string) -> x.Contains("error", StringComparison.OrdinalIgnoreCase)) recorded)
        }

    /// <summary>
    /// Test for a thread deadlock bug.
    /// If
    ///     1. There are 4 databases connected with replication like so A => B => C => D
    ///     2. Databases replicate synchronously
    ///     3. Db A and D are updated on different threads
    /// Update a propagates to B on thread 2
    /// Update d propagates to C on thread 1
    /// Update a and d on databases B and C are unable to acquire a lock as they each try to propagate to eachother 
    /// </summary>
    [<Fact>]
    let ``Unaligned concurrency deadlock test`` () =

        task {

            // arrange
            let level = LogLevel.Trace
            use writer = new TestLogger(output, level)
            let subscriberSettings =
                {subscriberTimeout = TimeSpan.FromSeconds(float 1); delay = RunSynchronously }

            use! host = clonedHost writer
            use client1 = TestDynamoClientBuilder.Create(host, { regionId = "eu-west-1"})
            TestDynamoClient.setProcessingDelay (TimeSpan.FromMilliseconds(10)) client1

            use client2 = TestDynamoClientBuilder.Create(host, { regionId = "eu-north-1"})
            TestDynamoClient.setProcessingDelay (TimeSpan.FromMilliseconds(10)) client2

            use client3 = TestDynamoClientBuilder.Create(host, { regionId = "ap-south-2"})
            TestDynamoClient.setProcessingDelay (TimeSpan.FromMilliseconds(10)) client3

            use client4 = TestDynamoClientBuilder.Create(host, { regionId = "us-east-2"})
            TestDynamoClient.setProcessingDelay (TimeSpan.FromMilliseconds(10)) client4

            let! tables = sharedTestData ValueNone
            let table = Tables.getByStreamsEnabled true tables

            let items =
                groupedItems table.hasSk
                |> fstT
                |> Seq.collect sndT
                |> Seq.map sndT
                |> List.ofSeq

            let update1::update2::_ =
                randomSeqItems 2 (asLazy true) random items |> List.ofSeq

            let someData = "SomeData"
            let update1::update4::_ =
                [update1; update2]
                |> Seq.mapi (fun i ->
                    TableItem.asItem
                    >> Map.add someData (String $"new-item-${i}")
                    >> itemToDynamoDb)
                |> List.ofSeq

            let updateData1 = update1[someData].S
            let updateData4 = update4[someData].S

            let countLogs (msg: string) =
                writer.Recorded()
                |> Seq.map sndT
                |> Seq.filter (fun x -> x <> null && x.Contains msg)
                |> Seq.length

            // act
            let! _ = replicateWithLevel (output, random, level) (host, "eu-west-1") "eu-north-1" table.name (ValueSome ChangeType.UpdateGlobal) (ValueSome subscriberSettings)
            let! _ = replicateWithLevel (output, random, level) (host, "eu-north-1") "ap-south-2" table.name (ValueSome ChangeType.Update) (ValueSome subscriberSettings)
            let! _ = replicateWithLevel (output, random, level) (host, "ap-south-2") "us-east-2" table.name (ValueSome ChangeType.CreateGlobal) (ValueSome subscriberSettings)

            // wait for the system to be at rest and begin monitoring logs
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            output.WriteLine("### Recording started")
            writer.Record true

            // don't wait for p1 to complete before doing p2
            let p1 = client1.PutItemAsync(table.name, update1)
            let! _ = client4.PutItemAsync(table.name, update4)
            let! _ = p1
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            output.WriteLine("### Stopping record")
            writer.Record false
            output.WriteLine("### Recording stopped")

            // assert

            // assert that the bug is actually being tested.
            let expectedPropagations =  // 12
                let midNodes = 2
                let edgeNodes = 2
                let totalMessages = 2
                totalMessages * ((midNodes * 2) + edgeNodes)

            Assert.Equal(0, countLogs "Propagating asynchronously")
            Assert.Equal(expectedPropagations, countLogs "Propagating synchronously")
            Assert.True(countLogs "Acquire lock failed" > 0)

            let scan (client: AmazonDynamoDBClient) =
                client.ScanAsync(
                    ScanBuilder.empty
                    |> ScanBuilder.withTableName table.name
                    |> ScanBuilder.req)

            let! items1 = scan client1
            let! items2 = scan client2
            let! items3 = scan client3
            let! items4 = scan client4

            let itemCount1 =
                items1.Items
                |> Seq.filter (fun x -> x.ContainsKey(someData) && x[someData].S = updateData1)
                |> Seq.length
            let itemCount2 =
                items1.Items
                |> Seq.filter (fun x -> x.ContainsKey(someData) && x[someData].S = updateData4)
                |> Seq.length

            Assert.Equal(1, itemCount1)
            Assert.Equal(1, itemCount2)

            Assert.Equal(items1.Items.Count, items2.Items.Count)
            assertDynamoDbItems (items1.Items, items2.Items, false)

            Assert.Equal(items3.Items.Count, items4.Items.Count)
            assertDynamoDbItems (items3.Items, items4.Items, false)

            assertDynamoDbItems (items1.Items, items3.Items, false)
        }

    let put (client: AmazonDynamoDBClient) tableName pk =
        let item =
            ItemBuilder.empty
            |> ItemBuilder.withTableName tableName
            |> ItemBuilder.withAttribute "TablePk" "S" pk

        client.PutItemAsync(
            ItemBuilder.tableName item,
            ItemBuilder.dynamoDbAttributes item)

    let ``Create host with replications`` region1 region2 tableName writer streamSubscriberOptions =

        task {

            // arrange
            let host = new GlobalDatabase(logger = writer)
            use client1 = TestDynamoClientBuilder.Create(host, { regionId = region1})
            use client2 = TestDynamoClientBuilder.Create(host, { regionId = region2})
            let createTable (client: AmazonDynamoDBClient) =
                let req =
                    TableBuilder.empty
                    |> TableBuilder.withTableName tableName
                    |> TableBuilder.withAttribute "TablePk" "S"
                    |> TableBuilder.withKeySchema "TablePk" ValueNone
                    |> TableBuilder.withStreamAction (CreateOrDelete.Create ())

                TableBuilder.req req |> client.CreateTableAsync

            let! _ = createTable client1
            let! _ = replicate (output, random) (host, region1) region2 tableName ValueNone (streamSubscriberOptions)
            let! _ = put client1 tableName "v1"
            let! _ = put client2 tableName "v2"
            do! (host.GetDatabase ValueNone {regionId = region1}).AwaitAllSubscribers ValueNone CancellationToken.None
            do! (host.GetDatabase ValueNone {regionId = region2}).AwaitAllSubscribers ValueNone CancellationToken.None

            return host
        }

    [<Fact>]
    let ``Delete replication, behaves correctly`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            writer.Record true
            let region1 = "eu-west-1"
            let region2 = "eu-north-1"
            let tableName = "Tab1"
            use! host = ``Create host with replications`` region1 region2 tableName writer ValueNone
            use client1 = TestDynamoClientBuilder.Create(host, { regionId = region1})
            use client2 = TestDynamoClientBuilder.Create(host, { regionId = region2})
            // act
            let! _ = deReplicate client1 region2 tableName ChangeType.Update
            let! _ = put client1 tableName "v3"
            let! _ = put client2 tableName "v4"
            do! (host.GetDatabase ValueNone {regionId = region1}).AwaitAllSubscribers ValueNone CancellationToken.None
            do! (host.GetDatabase ValueNone {regionId = region2}).AwaitAllSubscribers ValueNone CancellationToken.None

            // assert
            let single fn xs =
                match Seq.filter fn xs |> List.ofSeq with
                | [x] -> x
                | []
                | _::_ -> invalidOp ""

            let assertItem (client: AmazonDynamoDBClient) pk =
                TestDynamoClient.GetTable(client, tableName)
                |> fun x -> x.GetValues [struct ("TablePk", String pk)]
                |> single (asLazy true)
                |> ignore

            let w = ValueSome (writer  :> Microsoft.Extensions.Logging.ILogger)
            let allData =
                host.DebugTables
                |> Map.filter (fun _ v -> List.isEmpty v |> not)

            Assert.Equal(6, MapUtils.toSeq allData |> Seq.map sndT |> Seq.collect id |> Seq.map _.Values |> Seq.collect id |> Seq.length)
            assertItem client1 "v1"
            assertItem client1 "v2"
            assertItem client1 "v3"
            assertItem client2 "v1"
            assertItem client2 "v2"
            assertItem client2 "v4"

            let recorded = writer.RecordedStrings()
            Assert.NotEmpty(recorded)
            Assert.Empty(Seq.filter (fun (x: string) ->
                x.Contains("error", StringComparison.OrdinalIgnoreCase) ||
                x.Contains("warning", StringComparison.OrdinalIgnoreCase)) recorded)
        }

    [<Fact>]
    let ``Clone host with replicas, behaves correctly`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            let wr = (ValueSome (writer  :> Microsoft.Extensions.Logging.ILogger))
            writer.Record true
            let region1 = "eu-west-1"
            let region2 = "eu-north-1"
            let tableName = "Tab1"
            use! originalHost = ``Create host with replications`` region1 region2 tableName writer ValueNone

            // act
            use host2 = originalHost.Clone wr
            use client1 = TestDynamoClientBuilder.Create(host2, { regionId = region1})
            use client2 = TestDynamoClientBuilder.Create(host2, { regionId = region2})
            let! _ = put client1 tableName "v3"
            let! _ = put client2 tableName "v4"
            do! host2.AwaitAllSubscribers wr CancellationToken.None

            // assert
            let single fn xs =
                match Seq.filter fn xs |> List.ofSeq with
                | [x] -> x
                | []
                | _::_ -> invalidOp "not found"

            let assertItem (client: AmazonDynamoDBClient) pk =
                TestDynamoClient.GetTable(client, tableName)
                |> fun x -> x.GetValues [struct ("TablePk", String pk)]
                |> single (asLazy true)
                |> ignore

            let allData =
                host2.DebugTables
                |> Map.filter (fun _ v -> List.isEmpty v |> not)

            Assert.Equal(8, MapUtils.toSeq allData |> Seq.map sndT |> Seq.collect id |> Seq.map _.Values |> Seq.collect id |> Seq.length)
            assertItem client1 "v1"
            assertItem client1 "v2"
            assertItem client1 "v3"
            assertItem client1 "v4"
            assertItem client2 "v1"
            assertItem client2 "v2"
            assertItem client2 "v3"
            assertItem client2 "v4"

            let recorded = writer.RecordedStrings()
            Assert.NotEmpty(recorded)
            Assert.Empty(Seq.filter (fun (x: string) ->
                x.Contains("error", StringComparison.OrdinalIgnoreCase) ||
                x.Contains("warning", StringComparison.OrdinalIgnoreCase)) recorded)

            // random double dispose of client
            client2.Dispose()
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Host with replicas, delete a table, still works`` ``delete primary`` =

        task {
            // arrange
            use writer = new TestLogger(output)
            writer.Record true
            let region1 = "eu-west-1"
            let region2 = "eu-north-1"
            let tableName = "Tab1"
            use! host = ``Create host with replications`` region1 region2 tableName writer ValueNone

            // act
            let struct (keep, delete) =
                if ``delete primary`` then struct (region2, region1) else struct (region1, region2)

            use deleteClient = TestDynamoClientBuilder.Create(host, { regionId = delete})
            use keepClient = TestDynamoClientBuilder.Create(host, { regionId = keep})
            let! _ = deleteClient.DeleteTableAsync(tableName)
            do! TestDynamoClient.AwaitAllSubscribers(deleteClient, null, CancellationToken.None)

            let! _ = put keepClient tableName "v3"
            do! TestDynamoClient.AwaitAllSubscribers(deleteClient, null, CancellationToken.None)

            // assert
            let single fn xs =
                match Seq.filter fn xs |> List.ofSeq with
                | [x] -> x
                | []
                | _::_ -> invalidOp "not found"

            let assertItem (client: AmazonDynamoDBClient) pk =
                TestDynamoClient.GetTable(client, tableName)
                |> fun x -> x.GetValues [struct ("TablePk", String pk)]
                |> single (asLazy true)
                |> ignore

            let allData =
                host.DebugTables
                |> Map.filter (fun _ v -> List.isEmpty v |> not)

            Assert.Equal(3, MapUtils.toSeq allData |> Seq.map sndT |> Seq.collect id |> Seq.map _.Values |> Seq.collect id |> Seq.length)
            assertItem keepClient "v1"
            assertItem keepClient "v2"
            assertItem keepClient "v3"

            let recorded = writer.RecordedStrings()
            Assert.NotEmpty(recorded)
            Assert.Empty(Seq.filter (fun (x: string) ->
                x.Contains("error", StringComparison.OrdinalIgnoreCase) ||
                x.Contains("warning", StringComparison.OrdinalIgnoreCase)) recorded)
        }

    [<Theory>]
    [<InlineData("CHILD", "NONE")>]
    [<InlineData("CHILD", "LOCAL")>]
    [<InlineData("CHILD", "GLOBAL")>]
    [<InlineData("PARENT", "NONE")>]
    [<InlineData("PARENT", "LOCAL")>]
    [<InlineData("PARENT", "GLOBAL")>]
    let ``Update global table, create index, new index conflicts with existing data, fails`` ``index creator`` ``fix type`` =

        task {
            // arrange
            // set up clients
            let parentDbId = { regionId = "parent-db"}
            let childDbId = { regionId = "child-db"}
            let tableName = "Tab1"

            let settings = 
                { subscriberTimeout = TimeSpan.FromSeconds(float 1)
                  delay = TimeSpan.FromSeconds(0.1) |> RunAsynchronously } |> ValueSome

            // use writer = new TestLogger(output, TimeSpan.FromSeconds(10)) removed log grouping
            use writer = new TestLogger(output)
            use! host = ``Create host with replications`` parentDbId.regionId childDbId.regionId tableName writer settings
            use parentClient = TestDynamoClientBuilder.Create(host, parentDbId, (writer :> Microsoft.Extensions.Logging.ILogger))
            use childClient = TestDynamoClientBuilder.Create(host, childDbId, (writer :> Microsoft.Extensions.Logging.ILogger))
            TestDynamoClient.SetProcessingDelay(parentClient, TimeSpan.Zero)
            TestDynamoClient.SetProcessingDelay(childClient, TimeSpan.Zero)

            output.WriteLine "\n####### BEGIN"
            let putReq =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" "123456"
                |> ItemBuilder.withAttribute "Attr1" "S" "99"
                |> ItemBuilder.asPutReq

            let createIndexReq =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withSimpleGsi "TheIndex" "Attr1" ValueNone false
                |> TableBuilder.withAttribute "Attr1" "N"
                |> TableBuilder.updateReq

            let struct (struct (indexCreator, indexCreatorId), struct (corrupted, corruptedId)) =
                match ``index creator`` with
                | "PARENT" -> struct (struct (parentClient, parentDbId), struct (childClient, childDbId))
                | "CHILD" -> struct (struct (childClient, childDbId), struct (parentClient, parentDbId))
                | x -> invalidOp x

            // act
            let resp1 = corrupted.PutItemAsync putReq |> Io.ignoreTask
            do! indexCreator.UpdateTableAsync createIndexReq |> Io.ignoreTask
            do! resp1
            let! replicationError1 = Assert.ThrowsAsync<AggregateException>(fun _ ->
                (host.AwaitAllSubscribers ValueNone CancellationToken.None).AsTask())

            // assert
            Assert.Equal(2, Seq.length replicationError1.InnerExceptions)
            let errReplicatingItem = """Expected attribute "Attr1" from item Tab1/PRIMARY/S:123456"""
            let errReplicatingIndex = $"""The database {corruptedId.regionId} has encountered an error when synchronizing from {indexCreatorId.regionId}"""
            assertErrors output [errReplicatingItem; errReplicatingIndex] replicationError1

            // assert DB1 is broken
            let! replicationError2 = Assert.ThrowsAnyAsync(fun _ ->
                corrupted.ScanAsync(tableName, MList<_>()))
            assertErrors output [errReplicatingIndex] replicationError2

            // assert error does not keep throwing
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            // assert DB1 again. Specific bug where AwaitAllSubscribers was fixing sync error
            let! replicationError2 = Assert.ThrowsAnyAsync(fun _ ->
                corrupted.ScanAsync(tableName, MList<_>()))
            assertErrors output [errReplicatingIndex] replicationError2

            // assert DB2 is not broken
            do! indexCreator.ScanAsync(tableName, MList<_>()) |> Io.ignoreTask

            // act again - test recovery
            let db1 = host.GetDatabase (ValueSome writer) indexCreatorId
            let db2 = host.GetDatabase (ValueSome writer) corruptedId
            output.WriteLine(db1.Print())
            output.WriteLine(db2.Print())

            let! isFixed =
                match ``fix type`` with
                | "GLOBAL" ->
                    let req = UpdateGlobalTableRequest()
                    req.GlobalTableName <- tableName
                    req.ReplicaUpdates <- System.Collections.Generic.List()
                    req.ReplicaUpdates.Add(ReplicaUpdate())
                    req.ReplicaUpdates[0].Delete <- DeleteReplicaAction()
                    req.ReplicaUpdates[0].Delete.RegionName <- childDbId.regionId

                    parentClient.UpdateGlobalTableAsync(req)
                    |> Io.fromTask
                    |%|> (fun _ -> true)
                | "LOCAL" ->
                    let req = UpdateTableRequest()
                    req.TableName <- tableName
                    req.ReplicaUpdates <- System.Collections.Generic.List()
                    req.ReplicaUpdates.Add(ReplicationGroupUpdate())
                    req.ReplicaUpdates[0].Delete <- DeleteReplicationGroupMemberAction()
                    req.ReplicaUpdates[0].Delete.RegionName <- childDbId.regionId

                    parentClient.UpdateTableAsync(req)
                    |> Io.fromTask
                    |%|> (fun _ -> true)
                | "NONE" -> ValueTask<_>(false)
                | x -> invalidOp x

            if isFixed then
                output.WriteLine $"\n####### RECOVER STARTED {DateTime.UtcNow:O}"
                do! host.AwaitAllSubscribers ValueNone CancellationToken.None
                output.WriteLine $"\n####### RECOVERED {DateTime.UtcNow:O}"

                // assert
                do! corrupted.ScanAsync(tableName, MList<_>()) |> Io.ignoreTask

            // if not fixed case verifies that Dispose does not throw
        }

    [<Fact>]
    let ``Delete replication twice, throws`` () =

        task {

            // arrange
            use writer = new TestLogger(output)
            writer.Record true
            use host = new GlobalDatabase(logger = writer)
            let region1 = "eu-west-1"
            let region2 = "eu-north-1"
            use client1 = TestDynamoClientBuilder.Create(host, { regionId = region1})
            use client2 = TestDynamoClientBuilder.Create(host, { regionId = region2})
            let tableName = "Tab1"            
            let createTable (client: AmazonDynamoDBClient) =
                let req =
                    TableBuilder.empty
                    |> TableBuilder.withTableName tableName
                    |> TableBuilder.withAttribute "TablePk" "S"
                    |> TableBuilder.withKeySchema "TablePk" ValueNone
                    |> TableBuilder.withStreamAction (CreateOrDelete.Create ())

                TableBuilder.req req |> client.CreateTableAsync

            let! _ = createTable client1
            let! _ = replicate (output, random) (host, region1) region2 tableName ValueNone ValueNone
            do! TestDynamoClient.AwaitAllSubscribers (client2, null, CancellationToken.None)
            let! _ = deReplicate client1 region2 tableName ChangeType.UpdateGlobal

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> deReplicate client1 region2 tableName ChangeType.Update)
            assertError output "not found" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Create replication, then delete a table, replication is also deleted`` ``delete source`` =

        let tableName = "Tab1"
        let dataCount (host: GlobalDatabase) =
            host.DebugTables
            |> MapUtils.toSeq
            |> Seq.sumBy (sndT >> List.sumBy (_.Values >> Seq.length))

        let put (client: AmazonDynamoDBClient) =
            task {
                let!_=client.PutItemAsync(
                    ItemBuilder.empty
                    |> ItemBuilder.withTableName tableName
                    |> ItemBuilder.withAttribute "TablePk" "S" $"{System.Guid.NewGuid()}"
                    |> ItemBuilder.asPutReq)

                return ()
            }

        task {

            // arrange
            use writer = new TestLogger(output)
            writer.Record true
            use host = new GlobalDatabase(logger = writer)
            let region1 = "eu-west-1"
            let region2 = "eu-north-1"
            use client1 = TestDynamoClientBuilder.Create(host, { regionId = region1})
            use client2 = TestDynamoClientBuilder.Create(host, { regionId = region2})
            let createTable (client: AmazonDynamoDBClient) =
                let req =
                    TableBuilder.empty
                    |> TableBuilder.withTableName tableName
                    |> TableBuilder.withAttribute "TablePk" "S"
                    |> TableBuilder.withKeySchema "TablePk" ValueNone
                    |> TableBuilder.withStreamAction (CreateOrDelete.Create ())

                TableBuilder.req req |> client.CreateTableAsync

            let! _ = createTable client1
            let! _ = replicate (output, random) (host, region1) region2 tableName ValueNone ValueNone
            do! TestDynamoClient.AwaitAllSubscribers(client2, null, CancellationToken.None)

            do! put client1
            do! put client2
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // 2 objects in each table
            Assert.Equal(4, dataCount host)

            let struct (deleted, remaining) =
                if  ``delete source`` then id
                else flipTpl
                <| struct (client1, client2)

            // act
            let! _ = deleted.DeleteTableAsync(tableName)
            do! put remaining
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // 2 original objects from non deleted table + 1 new
            Assert.Equal(3, dataCount host)
        }

    [<Theory>]
    [<InlineData("PUT")>]
    [<InlineData("DELETE")>]
    [<InlineData("UPDATE")>]
    // NOTE: there are similar tests for batch write in batchWriteItemTests
    let ``Write action with failed condition, does not replicate`` action =

        let put pk sk table (client: AmazonDynamoDBClient) =
            ItemBuilder.empty
            |> ItemBuilder.withTableName table
            |> ItemBuilder.withAttribute "TablePk" "S" pk
            |> (sk ?|> (ItemBuilder.withAttribute "TableSk" "N") ?|? id)
            |> ItemBuilder.withAttribute "TablePk_Copy" "S" "MODIFIED"
            |> ItemBuilder.withCondition "attribute_exists(akljsgdaksjlgdkaslgd)"
            |> ItemBuilder.asPutReq
            |> client.PutItemAsync
            |> Io.ignoreTask

        let delete pk sk table (client: AmazonDynamoDBClient) =
            ItemBuilder.empty
            |> ItemBuilder.withTableName table
            |> ItemBuilder.withAttribute "TablePk" "S" pk
            |> (sk ?|> (ItemBuilder.withAttribute "TableSk" "N") ?|? id)
            |> ItemBuilder.withCondition "attribute_exists(akljsgdaksjlgdkaslgd)"
            |> ItemBuilder.asDeleteReq
            |> client.DeleteItemAsync
            |> Io.ignoreTask

        let update pk sk table (client: AmazonDynamoDBClient) =
            QueryBuilder.empty ValueNone
            |> QueryBuilder.setTableName table
            |> QueryBuilder.setUpdateExpression "REMOVE TablePk_Copy"
            |> QueryBuilder.setUpdateKey (
                Map.add "TablePk" (AttributeValue.String pk) Map.empty
                |> (sk ?|> (decimal >> AttributeValue.Number >> Map.add "TableSk") ?|? id))
            |> QueryBuilder.setConditionExpression "attribute_exists(akljsgdaksjlgdkaslgd)"
            |> QueryBuilder.updateRequest
            |> client.UpdateItemAsync
            |> Io.ignoreTask

        let execute pk sk table client =
            task {
                let! e = 
                    Assert.ThrowsAnyAsync(fun _ ->
                        match action with
                        | "PUT" -> put pk sk table client
                        | "DELETE" -> delete pk sk table client
                        | "UPDATE" -> update pk sk table client)

                assertError output "ConditionalCheckFailedExceptio" e
            }

        task {
            // arrange
            use logger = new TestLogger(output) 
            let! struct (table, host, client1, client2, disposer) = setUp2Regions logger true
            use _ = disposer

            let item =
                groupedItems table.hasSk
                |> fstT
                |> Seq.collect sndT
                |> Seq.map sndT
                |> Seq.head

            let sk = if table.hasSk then item.tableSk |> toString |> ValueSome else ValueNone
            do! execute item.tablePk sk table.name client1
            do! host.AwaitAllSubscribers ValueNone CancellationToken.None

            let! replicated =
                client2.GetItemAsync(
                    table.name,
                    ItemBuilder.empty
                    |> ItemBuilder.withAttribute "TablePk" "S" item.tablePk
                    |> (sk ?|> (ItemBuilder.withAttribute "TableSk" "N") ?|? id)
                    |> ItemBuilder.attributes
                    |> itemToDynamoDb)

            Assert.Equal(replicated.Item["TablePk_Copy"].S, replicated.Item["TablePk"].S)
        }

    static member replicateDb = replicate

type DatabaseReplicationTests2(output: ITestOutputHelper) =

    let random = randomBuilder output

    // moved into own class to isolate 3s test
    [<Fact>]
    let ``Replication, load testing`` () =
        let rec ids depth parent: string list seq =
            let items = [1..3] |> Seq.map (fun x -> x.ToString()::parent)
            if depth <= 0 then items
            else Seq.concat [items; Seq.collect (ids (depth - 1)) items ]

        task {

            // arrange
            use writer = new TestLogger(output)
            writer.Record true
            use host = new GlobalDatabase()
          //  use host = new GlobalDatabase(writer) // There are a lot of logs

            let tableName = "Tab1"
            let createTable (client: AmazonDynamoDBClient) =
                let req =
                    TableBuilder.empty
                    |> TableBuilder.withTableName tableName
                    |> TableBuilder.withAttribute "TablePk" "S"
                    |> TableBuilder.withKeySchema "TablePk" ValueNone
                    |> TableBuilder.withStreamAction (CreateOrDelete.Create ())

                TableBuilder.req req |> client.CreateTableAsync

            let put (client: AmazonDynamoDBClient) pk =
                let item =
                    ItemBuilder.empty
                    |> ItemBuilder.withTableName tableName
                    |> ItemBuilder.withAttribute "TablePk" "S" pk

                let r = random.Next(10)
                task {
                    if r > 0 then do! Task.Delay(r)

                    return! client.PutItemAsync(
                        ItemBuilder.tableName item,
                        ItemBuilder.dynamoDbAttributes item) }

            let buildReplicatedClient path =
                match path with
                | [] -> invalidOp ""
                | [x] ->
                    struct (x, TestDynamoClientBuilder.Create(host, { regionId = x}))
                    |> Task.FromResult
                | head::tail ->
                    let parentName = List.rev tail |> Str.join "-"
                    let name = $"{parentName}-{head}"
                    let client = TestDynamoClientBuilder.Create(host, { regionId = name})
                    task {
                        use parent = TestDynamoClientBuilder.Create(host, { regionId = parentName})
                        TestDynamoClient.SetProcessingDelay(parent, TimeSpan.Zero)

                        let msg = $"{tableName} {parentName} => {name}"
                        try
                            output.WriteLine($"### Replicating table {msg}")
                            let! _ = DatabaseReplicationTests.replicateDb (output, random) (host, parentName) name tableName ValueNone ValueNone
                            output.WriteLine($"### Complete {msg}")
                            return struct (name, client)
                        with
                        | e ->
                            output.WriteLine($"### Failed {msg}")
                            Exception("", e) |> raise
                            return Unchecked.defaultof<_>
                    }

            let! root = buildReplicatedClient ["root"]
            let! _ = createTable (sndT root)

            let! clients =
                ids 2 ["root"]
                |> Seq.sortBy List.length
                |> Seq.fold (fun s x ->
                    task {
                        let! acc = s
                        let! result = buildReplicatedClient x
                        return result::acc
                    }) (Task.FromResult([]))
                |> Io.fromTask
                |> Io.map List.rev
                |%|> (Collection.prepend root >> randomSort random >> List.ofSeq)

            let start = DateTimeOffset.UtcNow
            let! putRequests =
                clients
                |> Seq.collect (fun struct (name, client) -> [1..5] |> Seq.map (fun i -> put client $"{name}-{i}"))
                |> Task.WhenAll

            do! host.AwaitAllSubscribers ValueNone CancellationToken.None
            let ed = DateTimeOffset.UtcNow - start

            // assert
            let expectedSum =
                clients
                |> Seq.map (sndT >> fun x -> TestDynamoClient.GetTable(x, tableName).GetValues().Count())
                |> Seq.sum

            let expectedCount = Array.length putRequests * List.length clients
            output.WriteLine($"{expectedCount} items in {ed} for {List.length clients} clients, {Array.length putRequests / List.length clients} requests per client and {Array.length putRequests} items per db")
            output.WriteLine($"{Math.Round(ed.TotalSeconds / float expectedCount, 5)}s per item")

            let total =
                host.DebugTables
                |> MapUtils.toSeq
                |> Seq.map (
                    sndT
                    >> Seq.map (_.Values >> Seq.length)
                    >> Seq.sum)
                |> Seq.sum

            Assert.Equal(expectedSum, expectedCount)
            Assert.Equal(expectedCount, total)
        }
