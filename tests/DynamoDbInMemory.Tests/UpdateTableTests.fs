
namespace DynamoDbInMemory.Tests

open System.Threading
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory
open DynamoDbInMemory.Api
open DynamoDbInMemory.Client
open DynamoDbInMemory.Data.BasicStructures
open Microsoft.Extensions.Logging
open Tests.Items
open Tests.Requests.Queries
open Tests.Table
open Tests.Utils
open Xunit
open Amazon.DynamoDBv2
open Xunit.Abstractions
open DynamoDbInMemory.Utils
open DynamoDbInMemory.Model
open Tests.Loggers

type UpdateTableTests(output: ITestOutputHelper) =

    let random = randomBuilder output

    let (%>>=) x f =
        task {
            let! x' = x
            return! f x'
        }

    let (|%|>) x f =
        task {
            let! x' = x
            return f x'
        }

    let createTable (client: IAmazonDynamoDB) request = function
        | true ->
            let r = TableBuilder.req request
            client.CreateTableAsync(
                r.TableName,
                r.KeySchema,
                r.AttributeDefinitions,
                r.ProvisionedThroughput)
        | false -> client.CreateTableAsync(TableBuilder.req request)

    let basicAdd tableName (client: IAmazonDynamoDB) =
        task {
            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" "x"
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" "55"
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"
                |> ItemBuilder.withAttribute "AnotherThing" "S" "Else2"

            // act
            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)

            return ()
        }

    let basicQuery tableName hasSortKey (client: IAmazonDynamoDB) =
        task {
            let q1 =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tableName
                |>  match hasSortKey with
                    | true -> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                    | false -> QueryBuilder.setKeyConditionExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute "S" "x")
                |> if hasSortKey
                    then QueryBuilder.setExpressionAttrValues ":s" (ItemBuilder.buildItemAttribute "N" "8")
                    else id
                |> QueryBuilder.queryRequest

            // act
            return! client.QueryAsync q1
        }

    let indexQuery tableName indexName hasSortKey (client: IAmazonDynamoDB) =
        task {
            let q1 =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tableName
                |> QueryBuilder.setIndexName indexName
                |> match hasSortKey with
                    | true ->
                        QueryBuilder.setKeyConditionExpression "IndexPk = :p AND IndexSk = :s"
                        >> QueryBuilder.setExpressionAttrValues ":s" (ItemBuilder.buildItemAttribute "S" "UU")
                    | false -> QueryBuilder.setKeyConditionExpression "IndexPk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute "N" "55")
                |> QueryBuilder.queryRequest

            // act
            return! client.QueryAsync q1
        }

    let indexAssert projectAttributes hasSortKey (result: QueryResponse) =
        // assert
        let item = Assert.Single result.Items
        Assert.Equal("x", item["TablePk"].S)
        Assert.Equal("55", item["IndexPk"].N)

        if projectAttributes then
            Assert.Equal("Else", item["Something"].S)
            Assert.Equal("Else2", item["AnotherThing"].S)
            Assert.Equal("8", item["TableSk"].N)
        if projectAttributes || hasSortKey then
            Assert.Equal("UU", item["IndexSk"].S)

        let k = if projectAttributes then 6 elif hasSortKey then 3 else 2
        Assert.Equal(k, item.Count)

    let basicAssert (result: QueryResponse) =
        // assert
        let item = Assert.Single result.Items
        Assert.Equal("x", item["TablePk"].S)
        Assert.Equal("8", item["TableSk"].N)
        Assert.Equal("55", item["IndexPk"].N)
        Assert.Equal("UU", item["IndexSk"].S)
        Assert.Equal("Else", item["Something"].S)
        Assert.Equal("Else2", item["AnotherThing"].S)

        Assert.Equal(6, item.Count)

    let assertNoTable tableName =
        let table = commonHost.TryDescribeTable ValueNone tableName
        Assert.Equal(ValueNone, table)

    let basicAddAndAssert tableName hasSortKey client =
        basicAdd tableName client
        %>>= fun _ -> basicQuery tableName hasSortKey client
        |%|>basicAssert

    let indexAddAndAssert tableName indexName hasSortKey projectAttributes client =
        basicAdd tableName client
        %>>= fun _ -> indexQuery tableName indexName hasSortKey client
        |%|>indexAssert projectAttributes hasSortKey

    static let defaultDbId = { regionId = "local-machine-1"}
    static let defaultTable = "Tab1"
    static let defaultIndex1 = "Idx1"
    static let defaultIndex2 = "Idx2"
    static let randomData = "Random data"

    static let struct (baseTableWithStream, baseTableWithoutStream) =
        let withStreamCollector = OutputCollector()
        let withoutStreamCollector = OutputCollector()

        let create collector streamType =
            task {
                use writer = new TestLogger(collector)

                use host = new DistributedDatabase(logger = writer)
                use client = InMemoryDynamoDbClient.Create(host, defaultDbId, writer)
                let! _ =                    
                    TableBuilder.empty
                    |> TableBuilder.withTableName defaultTable
                    |> TableBuilder.withAttribute "TablePk" "S"
                    |> TableBuilder.withAttribute "IndexPk" "S"
                    |> TableBuilder.withKeySchema "TablePk" ValueNone
                    |> TableBuilder.withSimpleGsi defaultIndex1 "IndexPk" ValueNone true
                    |> TableBuilder.withSimpleGsi defaultIndex2 "IndexPk" ValueNone true
                    |> (ValueOption.map TableBuilder.withStreamAction streamType |> ValueOption.defaultValue id)
                    |> TableBuilder.req
                    |> client.CreateTableAsync

                let item =
                    ItemBuilder.empty
                    |> ItemBuilder.withTableName defaultTable
                    |> ItemBuilder.withAttribute "TablePk" "S" "tabPk"
                    |> ItemBuilder.withAttribute "IndexPk" "S" "idxPk"
                    |> ItemBuilder.withAttribute randomData "S" "Random data"

                let! _ = client.PutItemAsync(
                    ItemBuilder.tableName item,
                    ItemBuilder.dynamoDbAttributes item)

                do! client.AwaitAllSubscribers CancellationToken.None
                return host.BuildCloneData(ValueSome writer)
            }

        let wStreams = create withStreamCollector (ValueSome (CreateOrDelete.Create ()))
        let woStreams = create withoutStreamCollector ValueNone

        let builder (collector: OutputCollector) init output =
            task {                
                let create =
                    match output with
                    | ValueSome x ->
                        collector.Emit x
                        let w = new TestLogger(x)
                        fun (x: DistributedDatabaseCloneData) -> new DistributedDatabase(x, w)
                    | ValueNone ->
                        fun x -> new DistributedDatabase(x)

                let! x = init
                return create x
            }

        struct (builder withStreamCollector wStreams, builder withoutStreamCollector woStreams)

    let ``Create host with replication`` ``cloned host`` writer =

        task {
            // arrange
            let! host = baseTableWithStream (ValueSome output)
            use originalClient = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))

            do!
                ItemBuilder.empty
                |> ItemBuilder.withTableName defaultTable
                |> ItemBuilder.withAttribute "TablePk" "S" "123456XXXX"
                |> ItemBuilder.withAttribute "Pk1" "S" "-99"
                |> ItemBuilder.withAttribute "Sk1" "S" "99"
                |> ItemBuilder.asPutReq
                |> originalClient.PutItemAsync
                |> Io.ignoreTask

            let! _ = originalClient.UpdateTableAsync(
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withReplication "another region"
                |> TableBuilder.updateReq)

            if ``cloned host``
            then
                let cloned =  host.Clone(ValueSome writer)
                host.Dispose()
                return cloned
            else return host
        }

    [<Fact>]
    let ``Update table, all nulls, smoke test`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            // act
            // assert
            let! response = client.UpdateTableAsync (TableBuilder.empty |> TableBuilder.withTableName defaultTable |> TableBuilder.updateReq)
            Assert.Equal($"arn:aws:dynamodb:{defaultDbId}:123456789012:table/{defaultTable}", response.TableDescription.TableArn)
        }

    [<Theory>]
    [<InlineData(1, 1)>]
    [<InlineData(2, 0)>]
    [<InlineData(0, 2)>]
    let ``Update table, multiple index updates, throws`` ``index creates`` ``index deletes`` =

        let rec createIndexes i builder =
            if i <= 0 then builder
            else
                TableBuilder.withSimpleGsi $"Gsi{i}" "IndexPk" ValueNone true builder
                |> createIndexes (i - 1)

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |>
                    match ``index deletes`` with
                    | 0 -> id
                    | 1 -> TableBuilder.withDeleteGsi defaultIndex1
                    | 2 -> TableBuilder.withDeleteGsi defaultIndex1 >> TableBuilder.withDeleteGsi defaultIndex2
                    | _ -> invalidOp "not supported"
                |> createIndexes ``index creates``
                |> TableBuilder.updateReq

            // act
            let! x = Assert.ThrowsAnyAsync(fun () -> client.UpdateTableAsync req)

            // assert
            let indexes =
                Map.find defaultDbId host.DebugTables
                |> Collection.singleOrDefault
                |> Maybe.expectSome
                |> _.Indexes
                |> Seq.length

            assertError output "You can only create or delete one global secondary index per UpdateTable operation." x
            Assert.Equal(2, indexes)
        }

    [<Fact>]
    let ``Update table, delete index, works correctly`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withDeleteGsi defaultIndex1
                |> TableBuilder.updateReq

            // act
            let! _ = client.UpdateTableAsync req

            // assert
            let indexes =
                Map.find defaultDbId host.DebugTables
                |> Collection.singleOrDefault
                |> Maybe.expectSome
                |> _.Indexes
                |> Seq.length

            Assert.Equal(1, indexes)
        }

    [<Fact>]
    let ``Update table, with missing attributes, throws`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withSimpleGsi $"NewGsi" "another pk" ValueNone false
                |> TableBuilder.updateReq

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> client.UpdateTableAsync req)
            assertError output "Cannot find key attribute \"another pk\"" e
        }

    [<Fact>]
    let ``Update table, with superfluous attributes, throws`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withSimpleGsi $"NewGsi" "IndexPk" ValueNone false
                |> TableBuilder.withAttribute "IndexPk" "S"
                |> TableBuilder.withAttribute "super" "S"
                |> TableBuilder.updateReq

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> client.UpdateTableAsync req)
            assertError output "Found unused definitions for cols" e
        }

    [<Fact>]
    let ``Update table, change attribute type, fails`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withSimpleGsi $"NewGsi" "IndexPk" ValueNone false
                |> TableBuilder.withAttribute "IndexPk" "B"
                |> TableBuilder.updateReq

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> client.UpdateTableAsync req)
            assertError output """struct ("IndexPk", Binary)""" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update table, create index, works correctly`` ``sparse transfer`` =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            let newPk = if ``sparse transfer`` then "IndexPk" else "another pk"
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withSimpleGsi $"NewGsi" newPk ValueNone false
                |> TableBuilder.withAttribute newPk "S"
                |> TableBuilder.updateReq

            // act
            let! response = client.UpdateTableAsync req

            // assert
            Assert.Equal(
                ProjectionType.KEYS_ONLY.Value,
                response.TableDescription.GlobalSecondaryIndexes
                |> Seq.filter (_.IndexName >> ((=)"NewGsi"))
                |> Seq.head
                |> _.Projection.ProjectionType.Value)

            let table =
                Map.find defaultDbId host.DebugTables
                |> Collection.singleOrDefault
                |> Maybe.expectSome

            let indexes = table |> _.Indexes
            let data = table |> _.Values

            Assert.Equal(3, indexes |> Seq.length)
            Assert.Equal(1, data |> Seq.length)

            let newIndex =
                Seq.filter (fun (x: DebugIndex) -> x.Name = "NewGsi") indexes
                |> Collection.singleOrDefault
                |> Maybe.expectSome

            if not ``sparse transfer`` then Assert.Empty(newIndex.Values)
            else
                let singleValue (item: DebugItem seq) =
                    item
                    |> Seq.map (_.InternalItem >> Item.attributes)
                    |> Collection.singleOrDefault
                    |> Maybe.expectSome

                let tableValue = singleValue data
                let indexValue = singleValue newIndex.Values
                Assert.Equal(3, tableValue |> Map.count)
                Assert.Equal(2, indexValue |> Map.count)

                Assert.Equal<Map<string,AttributeValue>>(Map.add randomData (Map.find randomData tableValue) indexValue, tableValue)
        }

    [<Fact>]
    let ``Update table, create index with same cols as existing index, works correctly`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host1 = baseTableWithStream (ValueSome output)
            let host = host1.Clone (ValueSome writer)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            do!
                ItemBuilder.empty
                |> ItemBuilder.withTableName defaultTable
                |> ItemBuilder.withAttribute "TablePk" "S" "123456"
                |> ItemBuilder.withAttribute "Pk1" "S" "-99"
                |> ItemBuilder.withAttribute "Sk1" "S" "99"
                |> ItemBuilder.asPutReq
                |> client.PutItemAsync
                |> Io.ignoreTask

            let execute indexName =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withSimpleGsi indexName "Pk1" (ValueSome "Sk1") false
                |> TableBuilder.withAttribute "Pk1" "S"
                |> TableBuilder.withAttribute "Sk1" "S"
                |> TableBuilder.updateReq
                |> client.UpdateTableAsync
                |> Io.ignoreTask

            let get indexName =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName defaultTable
                |> QueryBuilder.setIndexName indexName
                |> QueryBuilder.setKeyConditionExpression "Pk1 = :x AND Sk1 = :y"
                |> QueryBuilder.setExpressionAttrValues ":x" (String "-99")
                |> QueryBuilder.setExpressionAttrValues ":y" (String "99")
                |> QueryBuilder.setIndexName indexName
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // act
            do! execute "ix1"
            do! execute "ix2"

            // assert
            let! v1 = get "ix1"
            let! v2 = get "ix2"

            Assert.Equal(1, v1.Items.Count)
            Assert.Equal(1, v2.Items.Count)
        }

    [<Fact>]
    let ``Update table, create index, new index conflicts with existing data, fails`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host1 = baseTableWithStream (ValueSome output)
            let host = host1.Clone (ValueSome writer)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            do!
                ItemBuilder.empty
                |> ItemBuilder.withTableName defaultTable
                |> ItemBuilder.withAttribute "TablePk" "S" "123456"
                |> ItemBuilder.withAttribute "Attr1" "S" "99"
                |> ItemBuilder.asPutReq
                |> client.PutItemAsync
                |> Io.ignoreTask

            // act
            let! e = Assert.ThrowsAnyAsync(fun _ ->
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withSimpleGsi "TheIndex" "Attr1" ValueNone false
                |> TableBuilder.withAttribute "Attr1" "N"
                |> TableBuilder.updateReq
                |> client.UpdateTableAsync
                |> Io.ignoreTask)

            // assert
            assertErrors output ["""Expected attribute "Attr1" from item Tab1/PRIMARY/S:123456"""; """to have type: Number, got type String"""] e
        }

    [<Fact>]
    let ``Update table, delete index and do replication, works`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            use client = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withDeleteGsi defaultIndex1
                |> TableBuilder.withReplication "newRegion"
                |> TableBuilder.updateReq

            // act
            let! response = client.UpdateTableAsync req
            do! client.AwaitAllSubscribers CancellationToken.None

            // assert
            Assert.Equal(1, response.TableDescription.Replicas.Count)

            let table =
                Map.find { regionId = "newRegion" } host.DebugTables
                |> Collection.singleOrDefault
                |> Maybe.expectSome

            Assert.Equal(defaultTable, table.Name)
            Assert.Equal(1, Seq.length table.Indexes)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Update table, delete stream with replication, fails`` ``cloned host`` ``delete on source table`` =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = ``Create host with replication`` ``cloned host`` writer

            let struct (deleteClient, otherClient) =
                let c1 = InMemoryDynamoDbClient.Create(host, defaultDbId, (writer :> ILogger))
                let c2 = InMemoryDynamoDbClient.Create(host, { regionId = "another region"}, (writer :> ILogger))

                if ``delete on source table``
                then struct (c1, c2)
                else struct (c2, c1)

            // act
            // assert - 1
            let! e = Assert.ThrowsAnyAsync(fun () -> deleteClient.UpdateTableAsync(
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.deleteStream
                |> TableBuilder.updateReq))

            Assert.Contains("database replication", e.ToString())

            // assert - 2 
            let dataCount () =
                host.DebugTables
                |> MapUtils.toSeq
                |> Seq.sumBy (sndT >> List.sumBy (_.Values >> Seq.length))

            let dataB4 = dataCount()

            let put (client: IInMemoryDynamoDbClient) =
                client.PutItemAsync(
                    ItemBuilder.empty
                    |> ItemBuilder.withTableName defaultTable
                    |> ItemBuilder.withAttribute "TablePk" "S" $"{System.Guid.NewGuid()}"
                    |> ItemBuilder.asPutReq)

            let! _ = put deleteClient
            let! _ = put otherClient

            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None
            Assert.Equal(dataB4 + 4, dataCount())
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Update table, delete stream with replication after delete one half of replication, succeeds`` ``cloned host`` ``delete source table`` =
        
        task {
            // arrange
            use writer = new TestLogger(output)
            let w = (ValueSome (writer :> ILogger))
            let w' = (ValueSome (writer :> ILogger |> Either1))
            use! host = ``Create host with replication`` ``cloned host`` writer

            let struct (deleteClient, otherClient, otherDb) =
                let c1 = InMemoryDynamoDbClient.Create(host, defaultDbId, writer)
                let c2 = InMemoryDynamoDbClient.Create(host, { regionId = "another region"}, writer)

                if ``delete source table``
                then struct (c1, c2, host.GetDatabase w { regionId = "another region" })
                else struct (c2, c1, host.GetDatabase w defaultDbId)

            let! _ = deleteClient.DeleteTableAsync(defaultTable)

            let struct (sub, records) = recordSubscription w' defaultTable otherDb ValueNone ValueNone
            use _ = sub

            // act
            // assert - 1
            let! _ = otherClient.UpdateTableAsync(
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.deleteStream
                |> TableBuilder.updateReq)

            let! _ = otherClient.PutItemAsync(
                ItemBuilder.empty
                |> ItemBuilder.withTableName defaultTable
                |> ItemBuilder.withAttribute "TablePk" "S" $"{System.Guid.NewGuid()}"
                |> ItemBuilder.asPutReq)

            do! host.AwaitAllSubscribers w CancellationToken.None
            Assert.Empty(records)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Test deletion protection`` ``deletion protection added at create time`` =

        let tableExists (client: IInMemoryDynamoDbClient) =
            task {
                try 
                    let! _ = client.ScanAsync(
                        ScanBuilder.empty
                        |> ScanBuilder.withTableName defaultTable
                        |> ScanBuilder.req)

                    return true
                with
                | _ -> return false
            }

        task {
            use writer = new TestLogger(output)
            use host = new ApiDb(writer)
            use client = InMemoryDynamoDbClient.Create(host, writer)
            let! _ =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withDeletionProtection ``deletion protection added at create time``
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withKeySchema "TablePk" ValueNone
                |> TableBuilder.req
                |> client.CreateTableAsync

            if not ``deletion protection added at create time`` then
                let! _ =
                    TableBuilder.empty
                    |> TableBuilder.withTableName defaultTable
                    |> TableBuilder.withDeletionProtection true
                    |> TableBuilder.updateReq
                    |> client.UpdateTableAsync
                ()

            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> client.DeleteTableAsync defaultTable)
            Assert.Contains("with deletion protection enabled", e.ToString())

            // verify table still exists
            let! exists = tableExists client
            Assert.True(exists)

            // act again
            let! _ =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withDeletionProtection false
                |> TableBuilder.updateReq
                |> client.UpdateTableAsync

            let! _ = client.DeleteTableAsync defaultTable
            let! exists = tableExists client
            Assert.False(exists)
        }

    [<Fact>]
    let ``Update table, delete stream, works correctly`` () =

        let put (client: IInMemoryDynamoDbClient) =
            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName defaultTable
                |> ItemBuilder.withAttribute "TablePk" "S" "X"

            task {
                let! _ = client.PutItemAsync(
                    ItemBuilder.tableName item,
                    ItemBuilder.dynamoDbAttributes item)

                do! client.AwaitAllSubscribers CancellationToken.None
            }

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            let w = (ValueSome (writer :> ILogger))
            let w' = (ValueSome (writer :> ILogger |> Either1))
            let database = host.GetDatabase w defaultDbId
            use client = InMemoryDynamoDbClient.Create(database, writer)
            let struct (x, recorded) = recordSubscription w' defaultTable database ValueNone ValueNone
            use _ = x

            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.deleteStream
                |> TableBuilder.updateReq

            do! put client
            do! client.AwaitAllSubscribers CancellationToken.None
            Assert.Equal(1, recorded.Count)

            // act
            let! response = client.UpdateTableAsync req
            do! put client

            // assert
            do! client.AwaitAllSubscribers CancellationToken.None
            Assert.Equal(1, recorded.Count)
            Assert.Null(response.TableDescription.LatestStreamArn)
        }

    [<Fact>]
    let ``Update table, delete nonexistent stream, throws`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithoutStream (ValueSome output)
            let w = (ValueSome (writer :> ILogger))
            let database = host.GetDatabase w defaultDbId
            use client = InMemoryDynamoDbClient.Create(database, writer)
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () ->
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.deleteStream
                |> TableBuilder.updateReq
                |> client.UpdateTableAsync
                |> Io.ignoreTask)

            Assert.Contains("does not have a stream to disable", e.ToString())
        }

    [<Fact>]
    let ``Update table, re-create stream, throws`` () =

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithStream (ValueSome output)
            let w = (ValueSome (writer :> ILogger))
            let database = host.GetDatabase w defaultDbId
            use client = InMemoryDynamoDbClient.Create(database, writer)
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () ->
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withStreamAction (CreateOrDelete.Create ())
                |> TableBuilder.updateReq
                |> client.UpdateTableAsync
                |> Io.ignoreTask)

            Assert.Contains("already has streams enabled", e.ToString())
        }

    [<Fact>]
    let ``Update table, add stream, works correctly`` () =

        let put (client: IInMemoryDynamoDbClient) =
            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName defaultTable
                |> ItemBuilder.withAttribute "TablePk" "S" "X"

            task {
                let! _ = client.PutItemAsync(
                    ItemBuilder.tableName item,
                    ItemBuilder.dynamoDbAttributes item)

                do! client.AwaitAllSubscribers CancellationToken.None
            }

        task {
            // arrange
            use writer = new TestLogger(output)
            use! host = baseTableWithoutStream (ValueSome output)
            let w = (ValueSome (writer:> ILogger))
            let w' = (ValueSome (writer :> ILogger |> Either1))
            let database = host.GetDatabase w defaultDbId
            use client = InMemoryDynamoDbClient.Create(database, writer)
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName defaultTable
                |> TableBuilder.withStreamAction (CreateOrDelete.Create ())
                |> TableBuilder.updateReq

            // act
            let! response = client.UpdateTableAsync req
            do! put client

            // assert
            let struct (x, recorded) = recordSubscription w' defaultTable database ValueNone ValueNone
            use _ = x
            do! put client
            do! client.AwaitAllSubscribers CancellationToken.None
            Assert.Equal(1, recorded.Count)
            Assert.StartsWith("arn:aws:dynamodb:local-machine-1:123456789012:table/Tab1/stream/202", response.TableDescription.LatestStreamArn)
        }

    // not a valid use case
    // [<Fact>]
    // let ``Update table, update stream spec, works`` () =
    //
    //     task {
    //         // arrange
    //         let writer = Writer(output) :> Microsoft.Extensions.Logging.ILogger
    //         use! host = baseTable (ValueSome output) SubscriberOptions.defaultOptions
    //         use client = InMemoryDynamoDbClient.Create(host, defaultRegion, writer)
    //         
    //         let put () =
    //             let item =
    //                 ItemBuilder.empty
    //                 |> ItemBuilder.withTableName defaultTable
    //                 |> ItemBuilder.withAttribute "TablePk" "S" "X"
    //
    //             task {
    //                 let! _ = client.PutItemAsync(
    //                     ItemBuilder.tableName item,
    //                     ItemBuilder.dynamoDbAttributes item)
    //                 
    //                 do! client.AwaitAllSubscribers CancellationToken.None
    //             }
    //             
    //         do! put()
    //         let req =
    //             TableBuilder.empty
    //             |> TableBuilder.withTableName defaultTable
    //             |> TableBuilder.withStreamType (ValueSome OldImage)
    //             |> TableBuilder.updateReq
    //             
    //         let data = System.Collections.Generic.List<ChangeDataCapturePacket>()
    //         use _ = client.SubscribeToStream defaultTable (fun x _ ->
    //             data.Add(x)
    //             ValueTask.CompletedTask)
    //         
    //         // act
    //         do! put()
    //         let! _ = client.UpdateTableAsync req
    //         do! put()
    //         
    //         // assert
    //         Assert.Equal(2, data.Count)
    //         
    //         let c1 = data[0].data.changeResult.orderedChanges// |> Collection.singleOrDefault |> Maybe.expectSome
    //         let c2 = data[1].data.changeResult.orderedChanges// |> Collection.singleOrDefault |> Maybe.expectSome
    //         
    //         c1 |> function
    //             | [Delete _; Put _] -> ()
    //             | [Put _; Delete _] -> ()
    //             | xs -> invalidOp $"Unexpected {xs}"
    //             
    //         c2 |> function
    //             | [Delete _] -> ()
    //             | xs -> invalidOp $"Unexpected {xs}"
    //     }
