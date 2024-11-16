
namespace TestDynamo.Tests

open System.Linq
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Api.FSharp
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Client.ItemMapper
open RequestItemTestUtils
open TestDynamo.Model
open Tests.Loggers

type GetItemTests(output: ITestOutputHelper) =

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

    let queryOrScan doQuery req =
        let client = buildClient output
        let client = client.Client

        if doQuery
        then QueryBuilder.queryRequest req |> client.QueryAsync |> mapTask _.Items
        else QueryBuilder.scanRequest req |> client.ScanAsync |> mapTask _.Items

    let executeAsBatchGet (client: AmazonDynamoDBClient) (req: GetItemRequest) =
        let kAndAttr =
            let op = KeysAndAttributes()
            op.Keys.Add(req.Key)
            op.ProjectionExpression <- req.ProjectionExpression
            op.AttributesToGet <- req.AttributesToGet
            op.ExpressionAttributeNames <- req.ExpressionAttributeNames
            op

        let req' =
            let r = BatchGetItemRequest()
            r.RequestItems.Add(req.TableName, kAndAttr)
            r

        client.BatchGetItemAsync(req')
        |> Io.fromTask
        |%|> (_.Responses >> Seq.collect _.Value >> List.ofSeq >> function | [] -> Dictionary<_,_>() | [x] -> x | xs -> invalidOp "Expected 1")

    let executeAsTransactGet (client: AmazonDynamoDBClient) (req: GetItemRequest) =
        let kAndAttr =
            let op = TransactGetItem()
            op.Get <- Get()
            op.Get.TableName <- req.TableName
            op.Get.Key <- req.Key
            op.Get.ProjectionExpression <- req.ProjectionExpression
            op.Get.ExpressionAttributeNames <- req.ExpressionAttributeNames
            op

        let req' =
            let r = TransactGetItemsRequest()
            r.TransactItems.Add(kAndAttr)
            r

        client.TransactGetItemsAsync(req')
        |> Io.fromTask
        |%|> (_.Responses >> Seq.map _.Item >> List.ofSeq >> function | [] -> Dictionary<_,_>() | [x] -> x | xs -> invalidOp "Expected 1")

    let maybeExecuteAsBatchOrTransactGet ``batch get`` ``transact get`` =
        if ``batch get`` && ``transact get`` then invalidOp ""
        elif ``batch get`` then executeAsBatchGet
        elif ``transact get`` then executeAsTransactGet
        else fun client req -> client.GetItemAsync req |> Io.fromTask |%|> _.Item

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Get item, with projections, works correctly`` ``has projections`` ``batch get`` ``transact get`` =

        if ``transact get`` && ``batch get``
        then Task.CompletedTask // not a valid test case
        else
        task {
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.get true true tables
            let struct (pk, struct (sk, item)) = randomItem table.hasSk random

            let keys1 =
                let keyCols = if table.hasSk then ["TablePk"; "TableSk"] else ["TablePk"]
                item
                |> Map.filter (fun k _ -> List.contains k keyCols)
                |> itemToDynamoDb

            let req = GetItemRequest()
            req.TableName <- table.name
            req.Key <- keys1
            if ``has projections`` then
                req.ProjectionExpression <- "#attr, IndexSk_Copy, blablabla"
                req.ExpressionAttributeNames <-
                    Map.add "#attr" "TablePk_Copy" Map.empty
                    |> CSharp.toDictionary id id

            // act
            let! response = maybeExecuteAsBatchOrTransactGet ``batch get`` ``transact get`` client req

            // assert
            let actual = response |> itemFromDynamodb "$"
            let expected =
                if ``has projections``
                then 
                    let keyCols = if table.hasSk then ["TablePk_Copy"; "IndexSk_Copy"] else ["TablePk"]
                    Map.filter (fun k _ -> List.contains k keyCols)
                else id
                |> apply item

            Assert.Equal<Map<string, AttributeValue>>(expected, actual)    
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Get item, with attributes to get, works correctly`` ``batch get`` =

        task {
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.get true true tables
            let struct (pk, struct (sk, item)) = randomItem table.hasSk random

            let keys1 =
                let keyCols = if table.hasSk then ["TablePk"; "TableSk"] else ["TablePk"]
                item
                |> Map.filter (fun k _ -> List.contains k keyCols)
                |> itemToDynamoDb

            let req = GetItemRequest()
            req.TableName <- table.name
            req.Key <- keys1
            req.AttributesToGet <- MList<_>([
                "TablePk_Copy"
                "IndexSk_Copy"
                "blablabla"
            ])

            // act
            let! response = maybeExecuteAsBatchOrTransactGet ``batch get`` false client req

            // assert
            let actual = response |> itemFromDynamodb "$"
            let expected =
                let keyCols = if table.hasSk then ["TablePk_Copy"; "IndexSk_Copy"] else ["TablePk"]
                Map.filter (fun k _ -> List.contains k keyCols)
                |> apply item

            Assert.Equal<Map<string, AttributeValue>>(expected, actual)    
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Get item, item missing, returns null or empty`` ``batch get`` ``transact get`` =

        if ``transact get`` && ``batch get``
        then Task.CompletedTask // not a valid test case
        else
        task {
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.get true true tables
            let struct (pk, struct (sk, item)) = randomItem table.hasSk random

            let keys1 =
                let keyCols = if table.hasSk then ["TablePk"; "TableSk"] else ["TablePk"]
                item
                |> Map.filter (fun k _ -> List.contains k keyCols)
                |> Map.add "TablePk" (String "invalid")
                |> itemToDynamoDb

            let req = GetItemRequest()
            req.TableName <- table.name
            req.Key <- keys1

            // act
            let! response = maybeExecuteAsBatchOrTransactGet ``batch get`` ``transact get`` client req
            Assert.True(response = null || response.Count = 0) 
        }

    [<Theory>]
    [<InlineData(true, false, true, false, false)>]
    [<InlineData(true, false, true, true, false)>]
    [<InlineData(true, false, true, false, true)>]
    [<InlineData(true, false, false, false, false)>]
    [<InlineData(true, false, false, true, true)>]
    [<InlineData(true, false, false, false, true)>]

    [<InlineData(false, true, true, false, false)>]
    [<InlineData(false, true, true, true, false)>]
    [<InlineData(false, true, true, false, true)>]
    [<InlineData(false, true, false, false, false)>]
    [<InlineData(false, true, false, true, true)>]
    [<InlineData(false, true, false, false, true)>]

    [<InlineData(false, false, true, false, false)>]
    [<InlineData(false, false, true, true, false)>]
    [<InlineData(false, false, true, false, true)>]
    [<InlineData(false, false, false, false, false)>]
    [<InlineData(false, false, false, true, true)>]
    [<InlineData(false, false, false, false, true)>]
    let ``Get item, with invalid keys, throws error`` ``batch get`` ``transact get`` ``table has sk`` ``include pk`` ``include sk`` =

        task {
            // arrange
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.get ``table has sk`` true tables

            let tablePk = $"{IncrementingId.next()}"
            let tableSk = (IncrementingId.next()).Value

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.withAttribute "TablePk" "S" tablePk
                |> ItemBuilder.withAttribute "TableSk" "N" $"{tableSk}"

            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)

            let keys =
                let keyCols =
                    match struct (``include pk``, ``include sk``) with
                    | true, true -> ["TablePk"; "TableSk"]
                    | true, false -> ["TablePk"]
                    | false, true -> ["TableSk"]
                    | false, false -> []

                ItemBuilder.dynamoDbAttributes item
                |> Seq.filter (fun x -> List.contains x.Key keyCols)
                |> Enumerable.ToDictionary

            let req =
                let op = GetItemRequest()
                op.TableName <- ItemBuilder.tableName item
                op.Key <- keys
                op

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> (maybeExecuteAsBatchOrTransactGet ``batch get`` ``transact get`` client req).AsTask())

            match ``table has sk``, ``include pk``, ``include sk`` with
            | false, true, true -> assertError output "Found non key attributes" e
            |_ -> assertError output "Could not find" e
        }

    [<Theory>]
    [<InlineData(100, true)>]
    [<InlineData(101, false)>]
    let ``Batch get item, variable request count, maybe throws`` ``req count`` ``success`` =

        task {
            use writer = new TestLogger(output)

            // arrange
            let keys hasSk (table: TestDynamo.Tests.TableDescription) =
                Map.add "TablePk" (AttributeValue.String "Str") Map.empty
                |> if hasSk
                    then Map.add "TableSk" (AttributeValue.Number 888888M)
                    else id
                |> itemToDynamoDb
                |> tpl table.name

            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let last = Tables.get false false tables |> keys false
            let tableVals =
                [
                    Tables.get true true tables |> keys true
                    Tables.get true false tables |> keys true
                    Tables.get false true tables |> keys false
                    last
                ]
                // make an infinite seq where the last item repeats
                |> flip Collection.concat2 (Seq.initInfinite (last |> asLazy))

            let req =
                let r = BatchGetItemRequest()
                [1..``req count``]
                |> Collection.window (``req count`` / 4)
                |> Collection.zip tableVals
                |> Seq.fold (fun s struct (struct (name, key), xs) ->
                    let kAndAttr =
                        match r.RequestItems.TryGetValue(name) with
                        | true, v -> v
                        | false, _ ->
                            let v = KeysAndAttributes()
                            r.RequestItems.Add(name, v)
                            v

                    for _ in xs do
                        kAndAttr.Keys.Add(key)) ()

                r

            if success then do! client.BatchGetItemAsync(req) |> Io.ignoreTask
            else
                let! e = Assert.ThrowsAnyAsync(fun _ -> client.BatchGetItemAsync(req) |> Io.ignoreTask)
                assertError output "Max allowed items is 100 for the BatchGetItem call" e
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Batch get item, invalid region, throws`` ``global`` ``invalid region``: Task<unit> =

        task {
            // arrange
            use writer = new TestLogger(output)
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            use dHost = new GlobalDatabase(host.BuildCloneData())
            let tableName = (Tables.get true true tables).name
            use client =
                if ``global`` then TestDynamoClientBuilder.Create(dHost, host.Id)
                else
                    TestDynamoClientBuilder.Create(cloneHost writer)

            TestDynamoClient.SetAwsAccountId(client, "12233445")

            let req =
                let r = BatchGetItemRequest()
                let k = KeysAndAttributes()
                k.Keys.Add(Dictionary<_, _>())
                r.RequestItems.Add(
                    if ``invalid region``
                        then $"arn:aws:dynamodb:invalid-region:{TestDynamoClient.GetAwsAccountId(client)}:table/{tableName}"
                        else $"arn:aws:dynamodb:{host.Id}:999999:table/{tableName}"
                    ,
                    k)
                r

            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> client.BatchGetItemAsync(req) |> Io.ignoreTask)
            match struct (``global``, ``invalid region``) with
            | struct (true, false)
            | struct (false, false) -> "Invalid aws account id 999999 in ARN"
            | struct (true, true) -> "No resources have been created in DB region invalid-region"
            | struct (false, true) -> "Some update table requests only work on GlobalDatabases"
            |> flip (assertError output) e
        }

    [<Fact>]
    let ``Batch get item, two tables in different regions, returns item`` (): Task<unit> =

        task {
            // arrange
            use writer' = new TestLogger(output)
            let writer = ValueSome (writer' :> Microsoft.Extensions.Logging.ILogger)
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer'
            use dHost = new GlobalDatabase(host.BuildCloneData())
            use client = TestDynamoClientBuilder.Create(dHost, host.Id)
            let table = Tables.get true true tables
            dHost.UpdateTable host.Id writer table.name { createStreamsForReplication = true; replicaInstructions = [Either1 { copyIndexes = ReplicateAll; databaseId = {regionId = "new-region" }}] } |> ignoreTyped<TableDetails>
            do! dHost.AwaitAllSubscribers writer CancellationToken.None

            let struct (pk, struct (sk, item)) = randomItem table.hasSk random
            let key =
                Map.add "TablePk" (AttributeValue.String pk) Map.empty
                |> Map.add "TableSk" (AttributeValue.Number sk)
                |> itemToDynamoDb

            let req =
                let r = BatchGetItemRequest()
                let k = KeysAndAttributes()
                k.Keys.Add(key)
                r.RequestItems.Add(
                    $"arn:aws:dynamodb:{host.Id.regionId}:{Settings.DefaultAwsAccountId}:table/{table.name}",
                    k)

                let k = KeysAndAttributes()
                k.Keys.Add(key)
                r.RequestItems.Add(
                    $"arn:aws:dynamodb:new-region:{Settings.DefaultAwsAccountId}:table/{table.name}",
                    k)
                r

            // act
            let! results = client.BatchGetItemAsync(req)

            // assert
            let resp = results.Responses |> Array.ofSeq
            Assert.Equal(2, resp.Length)
            Assert.Equal(1, Array.get resp 0 |> _.Value |> Seq.length)
            Assert.Equal(1, Array.get resp 0 |> _.Value |> Seq.length)

            // random double dispose of host
            dHost.Dispose()
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Batch get item, multiple pages, returns correct items`` ``consistant read``: Task<unit> =

        let rec executeUntilDone (client: AmazonDynamoDBClient) acc (req: Dictionary<string,KeysAndAttributes>) =
            task {
                let! x = client.BatchGetItemAsync(req)
                if x.UnprocessedKeys.Count = 0
                then return x::acc
                else return! executeUntilDone client (x::acc) x.UnprocessedKeys  
            }

        task {
            // arrange
            use writer' = new TestLogger(output)
            let writer = ValueSome (writer' :> Microsoft.Extensions.Logging.ILogger)
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let table = Tables.get false false tables

            use host = cloneHost writer'

            let bigBinary = Array.create 399_000 1uy |> AttributeValue.Binary
            let pks = [1..50] |> List.map (_.ToString() >> AttributeValue.String >> flip (Map.add "TablePk")  Map.empty) 
            let conditionExpression =
              { tableName = table.name
                conditionExpression = ValueNone
                expressionAttrNames = Map.empty
                expressionAttrValues = Map.empty
                returnValues = None }

            pks
            |> Seq.map (Map.add "BData" bigBinary)
            |> Seq.fold (fun _ item ->
                { item = item
                  conditionExpression = conditionExpression }
                |> host.Put writer
                |> ignoreTyped<Map<string,AttributeValue> voption>) ()

            use dHost = new GlobalDatabase(host.BuildCloneData())

            use client = TestDynamoClientBuilder.Create(dHost, host.Id)
            dHost.UpdateTable host.Id writer table.name { createStreamsForReplication = true; replicaInstructions = [Either1 { copyIndexes = ReplicateAll; databaseId = {regionId = "new-region" }}] } |> ignoreTyped<TableDetails>
            do! dHost.AwaitAllSubscribers writer CancellationToken.None

            let req =
                let r = BatchGetItemRequest()
                let add arn =
                    let k = KeysAndAttributes()
                    k.ConsistentRead <- ``consistant read``
                    k.Keys.AddRange(pks |> Seq.map itemToDynamoDb)
                    r.RequestItems.Add(
                        arn,
                        k)

                add $"arn:aws:dynamodb:{host.Id.regionId}:{Settings.DefaultAwsAccountId}:table/{table.name}"
                add $"arn:aws:dynamodb:new-region:{Settings.DefaultAwsAccountId}:table/{table.name}"
                r

            // act
            let! results = executeUntilDone client [] req.RequestItems
            Assert.True(List.length results > 1)

            let all =
                results
                |> Seq.collect (_.Responses)
                |> Seq.collect (_.Value)
                |> Collection.groupBy (fun x -> x["TablePk"].S)
                |> Collection.mapSnd (List.ofSeq)
                |> List.ofSeq

            // assert
            Assert.Equal(50, List.length all)   // 50 keys generated earlier
            for struct (_, x) in all do
                Assert.Equal(2, List.length x)  // 1 for each region
        }
