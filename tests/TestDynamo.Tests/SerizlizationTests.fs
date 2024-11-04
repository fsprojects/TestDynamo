namespace TestDynamo.Tests

open System
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Client
open TestDynamo.Utils
open Microsoft.Extensions.Logging
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
                { data =
                      { databases = [{ hostData with databaseId = { regionId = "eu-west-1"}}]
                        replicationKeys = [] } }: GlobalDatabaseCloneData

            return new GlobalDatabase(globalData, logger = writer)
        }

    static let replicate (client: ITestDynamoClient) regionName tableName = function
        | ChangeType.Update ->
            let req = UpdateTableRequest()
            req.TableName <- tableName
            req.ReplicaUpdates <- System.Collections.Generic.List()
            req.ReplicaUpdates.Add(ReplicationGroupUpdate())
            req.ReplicaUpdates[0].Create <- CreateReplicationGroupMemberAction()
            req.ReplicaUpdates[0].Create.RegionName <- regionName

            client.UpdateTableAsync(req) |> Io.ignoreTask
        | ChangeType.CreateGlobal ->
            let req = CreateGlobalTableRequest()
            req.GlobalTableName <- tableName
            req.ReplicationGroup <- System.Collections.Generic.List()
            req.ReplicationGroup.Add(Replica())
            req.ReplicationGroup[0].RegionName <- regionName

            client.CreateGlobalTableAsync(req) |> Io.ignoreTask
        | ChangeType.UpdateGlobal ->
            let req = UpdateGlobalTableRequest()
            req.GlobalTableName <- tableName
            req.ReplicaUpdates <- System.Collections.Generic.List()
            req.ReplicaUpdates.Add(ReplicaUpdate())
            req.ReplicaUpdates[0].Create <- CreateReplicaAction()
            req.ReplicaUpdates[0].Create.RegionName <- regionName

            client.UpdateGlobalTableAsync(req) |> Io.ignoreTask
        | x -> invalidOp $"{x}"

    let setUp2Regions logger doReplication =
        task {
            let! host = clonedHost logger
            let client1 = TestDynamoClient.Create(host, { regionId = "eu-west-1"})
            let client2 = TestDynamoClient.Create(host, { regionId = "eu-north-1"})
            let! tables = sharedTestData ValueNone
            let table = Tables.getByStreamsEnabled true tables

            do!
                if doReplication
                then replicate client1 "eu-north-1" table.name ChangeType.Update
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
            let db1 = cluster.GetDatabase ValueNone {regionId = "eu-north-1" }

            // act
            let ser1 = DatabaseSerializer.Database.ToString(db1, schemaOnly = ``omit data``)
            let db2 = DatabaseSerializer.Database.FromString(ser1)
            let ser2 = DatabaseSerializer.Database.ToString(db2, schemaOnly = ``omit data``)
            
            // assert
            Assert.True(ser1.Length > if ``omit data`` then 200 else 10_000)
            Assert.Equal(ser1, ser2)
                        
            let tableCount (db: Api.FSharp.Database) = List.length db.DebugTables
            let indexCount (db: Api.FSharp.Database) = db.DebugTables |> Seq.collect _.Indexes |> Seq.length
            let itemCount (db: Api.FSharp.Database) = db.DebugTables |> Seq.collect _.Values |> Seq.length 
            
            Assert.Equal(tableCount db1, tableCount db2)
            Assert.Equal(indexCount db1, indexCount db2)
            Assert.Equal((if ``omit data`` then 0 else itemCount db1), itemCount db2)
            Assert.True(itemCount db1 > 20, itemCount db1 |> toString)
        }

    [<Fact>]
    let ``Serialize global database`` () =

        task {
            // arrange
            use logger = new TestLogger(output, LogLevel.Warning) 
            let! struct (_, db1, _, _, _) = setUp2Regions logger true

            // act
            let ser1 = DatabaseSerializer.GlobalDatabase.ToString(db1)
            let db2 = DatabaseSerializer.GlobalDatabase.FromString(ser1)
            let ser2 = DatabaseSerializer.GlobalDatabase.ToString(db2)
            
            // assert
            Assert.True(ser1.Length > 10_000)
            Assert.Equal(ser1, ser2)
                        
            let databases (db: GlobalDatabase) = db.GetDatabases() |> MapUtils.toSeq |> Seq.map sndT
            let dbCount = databases >> Seq.length
            let tableCount = databases >> Seq.sumBy (_.DebugTables >> List.length)
            let indexCount =  databases >> Seq.sumBy (_.DebugTables >> Seq.collect _.Indexes >> Seq.length)
            let itemCount =  databases >> Seq.sumBy (_.DebugTables >> Seq.collect _.Values >> Seq.length)
            
            Assert.Equal(dbCount db1, dbCount db2)
            Assert.True(dbCount db1 > 1, dbCount db1 |> toString)
            Assert.Equal(tableCount db1, tableCount db2)
            Assert.Equal(indexCount db1, indexCount db2)
            Assert.Equal(itemCount db1, itemCount db2)
            Assert.True(itemCount db1 > 100, itemCount db1 |> toString)
        }

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``DeSerialize global database, preserves replication`` ``insert new replicat at top`` =

        task {
            // arrange
            use logger = new TestLogger(output, LogLevel.Debug) 
            let! struct (table, preSerialization, _, toDb, _) = setUp2Regions logger true

            let dbString = DatabaseSerializer.GlobalDatabase.ToString(preSerialization)
            let dbJson = JsonObject.Parse(dbString)
            
            // add a new db via json
            let newReplica = JsonObject.Parse(System.Text.Json.JsonSerializer.Serialize(
                    {| from = toDb.FsDatabase.Id.regionId
                       ``to`` = "somewhere-else"
                       tableName = table.name |}))
            
            let replicas = (dbJson["data"].AsObject()["replications"]).AsArray()
            // position affects how replicas are added. if new replica is inserted, it's processing needs to be delayed
            if ``insert new replicat at top``
            then replicas.Insert(0, newReplica)
            else replicas.Add(newReplica)
            
            let db = DatabaseSerializer.GlobalDatabase.FromString(dbJson.ToString())
            use client1 = TestDynamoClient.Create(db.GetDatabase (ValueSome logger) { regionId = "eu-north-1" }, logger)
            use client2 = TestDynamoClient.Create(db.GetDatabase (ValueSome logger) { regionId = "eu-west-1" }, logger)
            use client3 = TestDynamoClient.Create(db.GetDatabase (ValueSome logger) { regionId = "somewhere-else" }, logger)
            
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