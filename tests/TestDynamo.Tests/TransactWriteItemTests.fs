
namespace TestDynamo.Tests

open System.IO
open System.Linq
open System.Threading
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Client
open Microsoft.Extensions.Logging
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open TestDynamo.Data.Monads.Operators
open Utils
open TestDynamo.Client.ItemMapper
open RequestItemTestUtils
open TestDynamo.Model
open TestDynamo.Api.FSharp
open Tests.Loggers

type ReqWrapper =
    { update: Update
      put: Put
      delete: Delete
      condition: ConditionCheck }

type TransactWriteItemTests(output: ITestOutputHelper) =

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

    let asBatchReq (req: DeleteItemRequest) =
        let del = WriteRequest()
        del.DeleteRequest <- DeleteRequest()
        del.DeleteRequest.Key <- req.Key

        let req2 = BatchWriteItemRequest()
        req2.RequestItems <- System.Collections.Generic.Dictionary()
        req2.RequestItems.Add(
            req.TableName,
            [del] |> Enumerable.ToList)

        req2

    let maybeBatch batch (client: AmazonDynamoDBClient) (req: DeleteItemRequest) =
        if batch
        then asBatchReq req |> client.BatchWriteItemAsync |> Io.fromTask |%|> (asLazy ValueNone)
        else client.DeleteItemAsync req |> Io.fromTask |%|> ValueSome
        
    let getTable () =
        task {
            let! tables = sharedTestData ValueNone // (ValueSome output)                
            return Tables.get false false tables
        }
        
    let getTableAndClient logLevel client =
        task {
            let! table = getTable ()
            let client =
                client
                |> ValueOption.defaultWith (fun _ ->
                    let writer = new TestLogger(output, logLevel)

                    output.WriteLine("Cloning host")
                    let host = cloneHost writer
                    TestDynamoClientBuilder.Create(host, writer))
            
            return struct (client, table.name)
        }
        
    let setUpTable logLevel client =
        task {
            let! table = getTable ()
            let client =
                client
                |> ValueOption.defaultWith (fun _ ->
                    let writer = new TestLogger(output, logLevel)

                    output.WriteLine("Cloning host")
                    let host = cloneHost writer
                    TestDynamoClientBuilder.Create(host, writer))
                
            output.WriteLine("Getting table")
            
            do!
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "S" "UpdateItem"
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.asPutReq
                |> client.PutItemAsync
                |> Io.ignoreTask
                
            do!
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "S" "DeleteItem"
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.asPutReq
                |> client.PutItemAsync
                |> Io.ignoreTask
                
            do!
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "S" "ConditionItem"
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.asPutReq
                |> client.PutItemAsync
                |> Io.ignoreTask
            
            return struct (client, table.name)
        }
        
    let getFromWriteReq (client: AmazonDynamoDBClient) (req: TransactWriteItemsRequest) =
        task {
            let pkOnly (item: Dictionary<string, DynamoAttributeValue>) =
                let k = Dictionary<_, _>()
                k.Add("TablePk", item["TablePk"])
                k
            
            let items =
                req.TransactItems
                |> Seq.collect (fun x ->
                    [
                        x.Update |> CSharp.toOption |> ValueOption.map (fun x -> struct (x.TableName, x.Key))
                        x.Put |> CSharp.toOption |> ValueOption.map (fun x -> struct (x.TableName, x.Item |> pkOnly))
                        x.Delete |> CSharp.toOption |> ValueOption.map (fun x -> struct (x.TableName, x.Key))
                        x.ConditionCheck |> CSharp.toOption |> ValueOption.map (fun x -> struct (x.TableName, x.Key))
                    ])
                |> Maybe.traverse
                |> Seq.map (fun struct (table, key) ->
                    client.GetItemAsync(table, key)
                    |> Io.fromTask
                    |> Io.map (tpl struct (table, attributeFromDynamodb "$" key["TablePk"])))
                |> Io.traverse
                |> Io.map (Seq.map (sndT >> fun x -> x.Item |> itemFromDynamodb "$") >> List.ofSeq)
                
            return! items.AsTask()
        }
        
    let attrStr x =
        let attr = DynamoAttributeValue()
        attr.S <- x
        attr

    let buildReq (client: AmazonDynamoDBClient) tableName f =

        task {
            let put = Put()
            put.TableName <- tableName
            put.Item <- Dictionary()
            put.Item.Add("TablePk", "PutItem" |> attrStr)
        
            let delete = Delete()
            delete.TableName <- tableName
            delete.Key <- Dictionary()
            delete.Key.Add("TablePk", "DeleteItem" |> attrStr)
        
            let update = Update()
            update.TableName <- tableName
            update.Key <- Dictionary()
            update.Key.Add("TablePk", "UpdateItem" |> attrStr)
            update.UpdateExpression <- "SET AValue = :v"
            update.ExpressionAttributeValues <- Dictionary()
            update.ExpressionAttributeValues.Add(":v", attrStr "xxx")
        
            let condition = ConditionCheck()
            condition.TableName <- tableName
            condition.Key <- Dictionary()
            condition.Key.Add("TablePk", "ConditionItem" |> attrStr)
            condition.ConditionExpression <- "TablePk = :pk"
            condition.ExpressionAttributeValues <- Dictionary()
            condition.ExpressionAttributeValues.Add(":pk", "ConditionItem" |> attrStr)
            
            let reqItems: ReqWrapper =
                { put = put
                  delete = delete
                  update = update
                  condition = condition } |> f
            
            let req = TransactWriteItemsRequest()
            req.TransactItems <- MList<_>()
            if reqItems.put <> null
            then req.TransactItems.Add(
                let t = TransactWriteItem()
                t.Put <- reqItems.put
                t)
            
            if reqItems.delete <> null
            then req.TransactItems.Add(
                let t = TransactWriteItem()
                t.Delete <- reqItems.delete
                t)
            
            if reqItems.update <> null
            then req.TransactItems.Add(
                let t = TransactWriteItem()
                t.Update <- reqItems.update
                t)
            
            if reqItems.condition <> null
            then req.TransactItems.Add(
                let t = TransactWriteItem()
                t.ConditionCheck <- reqItems.condition
                t)
            
            return req
        }
        
    let writeAndGet req (client: AmazonDynamoDBClient) tableName =
        task {
            do! client.TransactWriteItemsAsync req |> Io.ignoreTask
            let! defaultReq = buildReq client tableName id
            return! getFromWriteReq client defaultReq
        }

    let executeWrite clientContainer idemotencyKey writeInitialData f =

        task {
            
            let! struct (client, tableName) =
                if writeInitialData then setUpTable LogLevel.Debug clientContainer
                else getTableAndClient LogLevel.Debug clientContainer
            let! req = buildReq client tableName f
            req.ClientRequestToken <- idemotencyKey ?|>? fun _ -> System.Guid.NewGuid().ToString()
            return! writeAndGet req client tableName
        }

    let verifyWriteFailed client =

        task {
            
            let! struct (client, tableName) = setUpTable LogLevel.Debug client
            let! defaultReq = buildReq client tableName id
            
            let! result = getFromWriteReq client defaultReq
            let find pk = List.filter (MapUtils.tryFind "TablePk" >> ((=) (pk |> AttributeValue.String |> ValueSome))) result |> Collection.tryHead
            let deleted = find "DeleteItem"
            let updated = find "UpdateItem"
            let created = find "PutItem"
            
            sprintf "%A" result |> output.WriteLine
            Assert.NotEqual(ValueNone, deleted)
            Assert.Equal(ValueNone, created)
            Assert.False(updated.Value.ContainsKey("AValue"))
        }

    let executeFailedWrite clientContainer f =

        task {
            
            let! struct (client, tableName) = setUpTable LogLevel.Debug clientContainer
            let! req = buildReq client tableName f
            
            output.WriteLine("########### Act")
            let! e = Assert.ThrowsAnyAsync(fun _ ->
                client.TransactWriteItemsAsync req |> Io.ignoreTask)
                
            do! verifyWriteFailed (ValueSome client)
            return e
        }

    [<Fact>]
    let ``Transact write, smoke test`` () =

        task {
            // arrange
            // act
            let! result = executeWrite ValueNone ValueNone true id
            
            // assert
            let find pk = List.filter (MapUtils.tryFind "TablePk" >> ((=) (pk |> AttributeValue.String |> ValueSome))) result |> Collection.tryHead
            let deleted = find "DeleteItem"
            let updated = find "UpdateItem"
            let created = find "PutItem"
            
            sprintf "%A" result |> output.WriteLine
            Assert.Equal(ValueNone, deleted)
            Assert.NotEqual(ValueNone, created)
            Assert.Equal(AttributeValue.String "xxx", updated.Value["AValue"])
        }

    [<Fact>]
    let ``Transact write, delete nonexistent, still works`` () =

        task {
            // arrange
            // act
            let! result = executeWrite ValueNone ValueNone true (fun x ->
                x.delete.Key["TablePk"] <- attrStr "NonExistent"
                x)
            
            // assert
            let find pk = List.filter (MapUtils.tryFind "TablePk" >> ((=) (pk |> AttributeValue.String |> ValueSome))) result |> Collection.tryHead
            let deleted = find "DeleteItem"
            let updated = find "UpdateItem"
            let created = find "PutItem"
            
            sprintf "%A" result |> output.WriteLine
            Assert.NotEqual(ValueNone, deleted)
            Assert.NotEqual(ValueNone, created)
            Assert.Equal(AttributeValue.String "xxx", updated.Value["AValue"])
        }

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Transact write, condition on item not exists, still works`` ``condition on key`` =

        task {
            // arrange
            // act
            let! result = executeWrite ValueNone ValueNone true (fun x ->
                x.condition.Key <- Dictionary()
                x.condition.Key.Add("TablePk", uniqueId() |> toString |> attrStr)
                x.condition.ConditionExpression <- if ``condition on key`` then "attribute_not_exists(TablePk)" else "attribute_not_exists(MissingAttr)"
                x.condition.ExpressionAttributeValues.Clear()   
                x)
            
            // assert
            let find pk = List.filter (MapUtils.tryFind "TablePk" >> ((=) (pk |> AttributeValue.String |> ValueSome))) result |> Collection.tryHead
            let deleted = find "DeleteItem"
            let updated = find "UpdateItem"
            let created = find "PutItem"
            
            Assert.Equal(ValueNone, deleted)
            Assert.NotEqual(ValueNone, created)
            Assert.Equal(AttributeValue.String "xxx", updated.Value["AValue"])
        }

    [<Theory>]
    [<InlineData("put")>]
    [<InlineData("delete")>]
    [<InlineData("update")>]
    [<InlineData("condition")>]
    let ``Transact write, condition fails, throws and does not update`` ``condition failed on`` =

        task {
            // arrange  
            // act
            let! e = executeFailedWrite ValueNone (fun req ->
                match ``condition failed on`` with
                | "put" -> req.put.ConditionExpression <- "attribute_exists(NotExists)" 
                | "delete" -> req.delete.ConditionExpression <- "attribute_exists(NotExists)"
                | "update" -> req.update.ConditionExpression <- "attribute_exists(NotExists)"
                | "condition" -> req.condition.ConditionExpression <- req.condition.ConditionExpression + " AND attribute_exists(NotExists)"
                | x -> invalidOp x
                
                req)
            
            // assert
            assertError output "The conditional request failed" e
        }

    [<Theory>]
    [<InlineData("update", "delete")>]
    [<InlineData("update", "put")>]
    [<InlineData("update", "condition")>]
    [<InlineData("delete", "put")>]
    [<InlineData("delete", "condition")>]
    [<InlineData("put", "condition")>]
    let ``Transact write, item referenced twice, fails`` from ``to`` =

        task {
            let get (req: ReqWrapper) = function
                | "update" -> req.update.Key
                | "condition" -> req.condition.Key
                | "delete" -> req.delete.Key
                | "put" -> req.put.Item
                | x -> invalidOp x
            
            // arrange
            // act
            let! e = executeFailedWrite ValueNone (fun req ->
                let from = get req from
                let ``to`` = get req ``to``
                
                ``to``["TablePk"] <- from["TablePk"]
                req)
            
            // assert
            assertError output "Duplicate request on item" e
        }

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Transact write, over 100 items, fails`` ``is control`` =

        // duplicating put, delete, update
        // not duplicating condition (requires more complex data)
        let count = if ``is control`` then 33 else 34
        
        task {
            let! struct (client, tableName) = setUpTable LogLevel.Warning ValueNone
            let req = TransactWriteItemsRequest()
            req.TransactItems <- MList<_>()
            
            let put id =
                req.TransactItems.Add(
                    let t = TransactWriteItem()
                    let put = Put()
                    put.TableName <- tableName
                    put.Item <- Dictionary()
                    put.Item.Add("TablePk", $"PutItem{id}" |> attrStr)
                    t.Put <- put
                    t)
                
            let delete id =
                req.TransactItems.Add(
                    let t = TransactWriteItem()
                    let delete = Delete()
                    delete.TableName <- tableName
                    delete.Key <- Dictionary()
                    delete.Key.Add("TablePk", $"DeleteItem{id}" |> attrStr)
                    t.Delete <- delete
                    t)
                
            let update id =
                req.TransactItems.Add(
                    let t = TransactWriteItem()
                    let update = Update()
                    update.TableName <- tableName
                    update.Key <- Dictionary()
                    update.Key.Add("TablePk", $"UpdateItem{id}" |> attrStr)
                    update.UpdateExpression <- "SET AValue = :v"
                    update.ExpressionAttributeValues <- Dictionary()
                    update.ExpressionAttributeValues.Add(":v", attrStr "xxx")
                    t.Update <- update
                    t)
                
            let condition id =
                req.TransactItems.Add(
                    let t = TransactWriteItem()
                    let condition = ConditionCheck()
                    condition.TableName <- tableName
                    condition.Key <- Dictionary()
                    condition.Key.Add("TablePk", $"ConditionItem{id}" |> attrStr)
                    condition.ConditionExpression <- "TablePk = :pk"
                    condition.ExpressionAttributeValues <- Dictionary()
                    condition.ExpressionAttributeValues.Add(":pk", "ConditionItem" |> attrStr)
                    t.ConditionCheck <- condition
                    t)
                
            [
                put
                delete
                update
            ]
            |> Seq.collect (fun f -> [1..count] |> Seq.map f)
            |> Collection.prepend (condition())
            |> List.ofSeq
            |> ignoreTyped<unit list>
            
            // arrange
            // act
            if ``is control``
            then do! client.TransactWriteItemsAsync req |> Io.ignoreTask
            else 
                let! e = Assert.ThrowsAnyAsync(fun _ ->
                    client.TransactWriteItemsAsync req |> Io.ignoreTask)
                
                // assert
                assertError output "Maximum items in a transact write is" e
        }

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Transact write, items over 4MB, fails`` ``is control`` =
        
        let bigDataSize = 300_000
        let recordsNeededToOverflow = 4_000_000.0 / float bigDataSize
        let bigData = Array.create bigDataSize 0uy
        let bigDataAttribute () =
            let attr = DynamoAttributeValue()
            attr.B <- new MemoryStream(buffer = bigData)
            attr
        
        task {
            let! struct (client, tableName) = setUpTable LogLevel.Warning ValueNone
            let req = TransactWriteItemsRequest()
            req.TransactItems <- MList<_>()
            
            let put id =
                req.TransactItems.Add(
                    let t = TransactWriteItem()
                    let put = Put()
                    put.TableName <- tableName
                    put.Item <- Dictionary()
                    put.Item.Add("TablePk", $"PutItem{id}" |> attrStr)
                    put.Item.Add("BigData", bigDataAttribute ())
                    t.Put <- put
                    t)
                
            let update id =
                req.TransactItems.Add(
                    let t = TransactWriteItem()
                    let update = Update()
                    update.TableName <- tableName
                    update.Key <- Dictionary()
                    update.Key.Add("TablePk", $"UpdateItem{id}" |> attrStr)
                    update.UpdateExpression <- "SET BigData = :v"
                    update.ExpressionAttributeValues <- Dictionary()
                    update.ExpressionAttributeValues.Add(
                        ":v",
                        bigDataAttribute())
                    t.Update <- update
                    t)

            let triggeringRequests = (recordsNeededToOverflow / 2.0) |> System.Math.Ceiling |> int
            let triggeringRequests = if ``is control`` then triggeringRequests - 1 else triggeringRequests 
            [
                [1..triggeringRequests] |> Seq.map (fun _ -> put)
                [1..triggeringRequests] |> Seq.map (fun _ -> update)
            ]
            |> Seq.collect id
            |> Seq.fold (fun s f ->
                f s
                s + 1) 0
            |> ignoreTyped<int>

            // arrange
            // act
            if ``is control``
            then do! client.TransactWriteItemsAsync req |> Io.ignoreTask
            else 
                let! e = Assert.ThrowsAnyAsync(fun _ ->
                    client.TransactWriteItemsAsync req |> Io.ignoreTask)
                
                // assert
                assertError output "The maximum size of a transact write is" e
        }
        
    let setUpGlobalTable() =
        task {
            // arrange
            let! table = getTable() // pre populate table
            let writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            let struct (host, fromRegion) = cloneGlobalHost()
            let toRegion = {regionId = "to-region" }
            
            let _ = host
            let client1 = TestDynamoClientBuilder.Create(host, fromRegion, writer)
            let client2 = TestDynamoClientBuilder.Create(host, toRegion, writer)
            let clientContainer = new ClientContainer(host.GetDatabase writer' fromRegion, writer, false)
            
            let addStreamsReq = UpdateTableRequest()
            addStreamsReq.TableName <- table.name
            addStreamsReq.StreamSpecification <- StreamSpecification()
            addStreamsReq.StreamSpecification.StreamEnabled <- true
            addStreamsReq.StreamSpecification.StreamViewType <- StreamViewType.NEW_AND_OLD_IMAGES
            do! client1.UpdateTableAsync(addStreamsReq) |> Io.ignoreTask
            
            let createTableReq = CreateGlobalTableRequest()
            createTableReq.GlobalTableName <- table.name
            createTableReq.ReplicationGroup <- System.Collections.Generic.List()
            createTableReq.ReplicationGroup.Add(Replica())
            createTableReq.ReplicationGroup[0].RegionName <- toRegion.regionId
            do! client1.CreateGlobalTableAsync(createTableReq) |> Io.ignoreTask
            do! host.AwaitAllSubscribers writer' CancellationToken.None
            
            return struct (host, client1, client2, clientContainer)
        }

    [<Theory>]
    [<InlineData("KEY_ERR")>]
    [<InlineData("CONDITION")>]
    let ``Transact write, condition fails, does not replicate`` ``error type`` =

        task {
            let writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)
            let! struct (host, client1, client2, clientContainer) = setUpGlobalTable()
            use _ = host
            use _ = client1
            use _ = client2
            use _ = clientContainer
            
            // act
            let! e = executeFailedWrite (ValueSome clientContainer.Client) (fun req ->
                match ``error type`` with
                | "KEY_ERR" ->
                    req.delete.Key.Clear()
                | "CONDITION" ->
                    req.condition.ConditionExpression <- "attribute_exists(XXYYZZ)"
                    req.condition.ExpressionAttributeValues.Clear()
                | x -> invalidOp x
                
                req)
            
            do! host.AwaitAllSubscribers writer' CancellationToken.None
            
            // assert
            let msg = 
                match ``error type`` with
                | "KEY_ERR" -> "Invalid key specification for write request"
                | "CONDITION" -> "ConditionalCheckFailedException"
                | x -> invalidOp x
                
            assertError output msg e
            do! verifyWriteFailed (ValueSome client2)
        }

    [<Fact>]
    let ``Transact write, succeeds and replicates`` () =

        task {
            let! table = getTable()
            let writer = new TestLogger(output)
            let writer' = ValueSome (writer :> ILogger)
            let! struct (host, client1, client2, clientContainer) = setUpGlobalTable()
            use _ = host
            use _ = client1
            use _ = client2
            use _ = clientContainer
            
            // act
            do! executeWrite (ValueSome clientContainer.Client) ValueNone true id |> Io.ignoreTask
            do! host.AwaitAllSubscribers writer' CancellationToken.None
            
            // assert
            let! req = buildReq client1 table.name id
            let! written = getFromWriteReq client2 req
            let find pk = List.filter (MapUtils.tryFind "TablePk" >> ((=) (pk |> AttributeValue.String |> ValueSome))) written |> Collection.tryHead
            let deleted = find "DeleteItem"
            let updated = find "UpdateItem"
            let created = find "PutItem"
            
            Assert.Equal(ValueNone, deleted)
            Assert.NotEqual(ValueNone, created)
            Assert.Equal(AttributeValue.String "xxx", updated.Value["AValue"])
        }

    [<Fact>]
    let ``Transact write, with idempotency cache hit, does not write second time`` () =

        task {
            let! _ = getTable()
            let! struct (_host, client, _client2, clientContainer) = setUpGlobalTable()
            use _ = _host
            use _ = client
            use _ = _client2
            use _ = clientContainer
            
            let key = System.Guid.NewGuid().ToString() |> ValueSome
            
            // act
            do! executeWrite (ValueSome clientContainer.Client) key false id |> Io.ignoreTask
            let! written = executeWrite (ValueSome clientContainer.Client) key false (fun x ->
                // validate that this is never run
                x.condition.ConditionExpression <- "an invalid expression that would fail" 
                x)
            
            // assert
            let find pk = List.filter (MapUtils.tryFind "TablePk" >> ((=) (pk |> AttributeValue.String |> ValueSome))) written |> Collection.tryHead
            let deleted = find "DeleteItem"
            let updated = find "UpdateItem"
            let created = find "PutItem"
            
            Assert.Equal(ValueNone, deleted)
            Assert.NotEqual(ValueNone, created)
            Assert.Equal(AttributeValue.String "xxx", updated.Value["AValue"])
        }