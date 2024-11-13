namespace TestDynamo.Tests

open System
open System.Linq
open System.Text
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Lambda
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open Tests.Loggers

[<Struct>]
type Change =
    private
    | Cd of ChangeResult

    with
    static member create = Cd

    member this.Created =
        match this with
        | Cd cr -> ChangeResult.put cr ?|> Item.attributes
    member this.Deleted =
        match this with
        | Cd cr -> ChangeResult.deleted cr ?|> Item.attributes

[<Struct>]
type ChangeData =
    { changes: Change list }

type TableSubscriberTests(output: ITestOutputHelper) =

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

    let queryOrScan doQuery req =
        let client = buildClient output
        let client = client.Client

        if doQuery
        then QueryBuilder.queryRequest req |> client.QueryAsync |> mapTask _.Items
        else QueryBuilder.scanRequest req |> client.ScanAsync |> mapTask _.Items

    let asPutsAndDeletes (x: Change) =
        [
            x.Deleted ?|> Either2
            x.Created ?|> Either1
        ] |> Maybe.traverse

    let segregate (record: struct (_ * DatabaseSynchronizationPacket<TableCdcPacket>) seq) pkAttr =
        let hasPk = Map.find "TablePk" >> ((=) pkAttr)
        let putDelTpl (x: Change) = struct (x.Created, x.Deleted)

        record
        |> Seq.map sndT
        |> Seq.collect (fun x -> x.data.packet.changeResult.OrderedChanges |> List.map Change.create)
        |> Seq.filter (
            putDelTpl
            >> function
                | struct (ValueSome x, ValueSome y) -> hasPk x || hasPk y
                | ValueSome x, ValueNone -> hasPk x
                | ValueNone, ValueSome y -> hasPk y
                | ValueNone, ValueNone -> false)
        |> Seq.collect asPutsAndDeletes
        |> Either.partition

    let random = randomBuilder output

    [<Fact>]
    let ``Subscription, subscriptions not enabled, throws`` () =

        task {
            use writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let! tables = sharedTestData  (ValueSome output)
            use host = cloneHost writer
            let table = Tables.getByStreamsEnabled false tables
            let record = System.Collections.Generic.List<_>()

            // act
            // assert
            let _ = Assert.ThrowsAny(fun () ->
                host.SubscribeToStream writer' table.name struct (SubscriberBehaviour.defaultOptions, NewAndOldImages)
                <| fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask<_>(())
                |> ignore)

            ()
        }

    [<Theory>]
    [<InlineData("k")>]
    [<InlineData("n")>]
    [<InlineData("o")>]
    [<InlineData("no")>]
    let ``Subscription add tests`` ``stream type`` =

        let streamType =
            match ``stream type`` with
            | "k" -> KeysOnly
            | "n" -> NewImage
            | "o" -> OldImage
            | "no" -> NewAndOldImages
            | _ -> invalidOp ""

        task {
            use writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record = System.Collections.Generic.List<_>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value |> decimal

            let pkAttr =
                if table.isBinaryTable
                then Binary (Encoding.UTF8.GetBytes pk)
                else String pk
            let skAttr =
                if table.isBinaryTable
                then Binary (Encoding.UTF8.GetBytes $"{sk}")
                else Number sk

            use subscription =
                host.SubscribeToStream writer' table.name struct (SubscriberBehaviour.defaultOptions, streamType)
                <| fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask<_>(())

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> if table.isBinaryTable
                    then ItemBuilder.withAttribute "TablePk" "UTF8" pk
                    else ItemBuilder.withAttribute "TablePk" "S" pk
                |> if table.isBinaryTable
                    then ItemBuilder.withAttribute "TableSk" "UTF8" $"{sk}"
                    else ItemBuilder.withAttribute "TableSk" "N" $"{sk}"
                |> ItemBuilder.withAttribute "RandomData" "S" "random data"

            // act
            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)

            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // assert
            let struct (added, removed) = segregate record pkAttr

            let struct (expectSk, expectData) =
                match streamType with
                | KeysOnly -> struct (table.hasSk, false)
                | OldImage
                | NewImage
                | NewAndOldImages -> struct (true, true)

            match streamType with
            | OldImage ->
                let l = [added; removed] |> List.concat |> List.length
                Assert.True((l = 0), l.ToString())
            | _ ->
                Assert.True(List.length added = 1)
                Assert.True(List.length removed = 0)

                let added = List.head added
                let colCount = [true; expectSk; expectData] |> Seq.filter id |> Seq.length
                Assert.Equal(colCount, Map.count added)

                Assert.Equal(pkAttr, Map.find "TablePk" added)
                if expectSk then Assert.Equal(skAttr, Map.find "TableSk" added)
                if expectData then Assert.Equal(String "random data", Map.find "RandomData" added)

            // random double dispose of subscription
            subscription.Dispose() 
        }

    [<Fact>]
    let ``Streams event subscription`` () =

        task {
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record = System.Collections.Generic.List<struct (DateTimeOffset * Amazon.Lambda.DynamoDBEvents.DynamoDBEvent)>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value |> decimal

            let data1 =
                { tablePk = pk
                  tableSk = sk
                  indexPk = sk
                  indexSk = pk
                  binaryData = "hello"
                  boolData = true }

            let data2 = { data1 with indexPk = 777M }

            do! client.PutItemAsync(table.name, TableItem.asAttributes data1) |> Io.ignoreTask

            // act
            let func = asFunc2 (fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask.CompletedTask)
            use _ = Subscriptions.AddSubscription(host, table.name, func)
            do! client.PutItemAsync(table.name, TableItem.asAttributes data2) |> Io.ignoreTask
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // assert
            let r = Assert.Single(record) |> sndT
            let t = Assert.Single(r.Records)

            Assert.Equal<Map<string, AttributeValue>>(TableItem.asItem data1, LambdaSubscriberUtils.itemFromDynamoDb t.Dynamodb.OldImage)
            Assert.Equal<Map<string, AttributeValue>>(TableItem.asItem data2, LambdaSubscriberUtils.itemFromDynamoDb t.Dynamodb.NewImage)
        }

    [<Theory>]
    [<InlineData("k")>]
    [<InlineData("n")>]
    [<InlineData("o")>]
    [<InlineData("no")>]
    let ``Subscription update tests`` ``stream type`` =

        let streamType =
            match ``stream type`` with
            | "k" -> KeysOnly
            | "n" -> NewImage
            | "o" -> OldImage
            | "no" -> NewAndOldImages
            | _ -> invalidOp ""

        task {
            use writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record = System.Collections.Generic.List<_>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value |> decimal

            let pkAttr =
                if table.isBinaryTable
                then Binary (Encoding.UTF8.GetBytes pk)
                else String pk
            let skAttr =
                if table.isBinaryTable
                then Binary (Encoding.UTF8.GetBytes $"{sk}")
                else Number sk

            use subscription =
                host.SubscribeToStream writer' table.name struct (SubscriberBehaviour.defaultOptions, streamType)
                <| fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask<_>(())

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> if table.isBinaryTable
                    then ItemBuilder.withAttribute "TablePk" "UTF8" pk
                    else ItemBuilder.withAttribute "TablePk" "S" pk
                |> if table.isBinaryTable
                    then ItemBuilder.withAttribute "TableSk" "UTF8" $"{sk}"
                    else ItemBuilder.withAttribute "TableSk" "N" $"{sk}"
                |> ItemBuilder.withAttribute "RandomData" "S" "random data"

            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)

            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None
            record.Clear()

            let item = item |> ItemBuilder.withAttribute "RandomData" "S" "more random data"

            // act
            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)

            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // assert
            let struct (added, removed) = segregate record pkAttr

            let struct (expectSk, expectData, hasAdded, hasRemoved) =
                match streamType with
                | KeysOnly -> struct (table.hasSk, false, true, true)
                | OldImage -> struct (true, true, false, true)
                | NewImage -> struct (true, true, true, false)
                | NewAndOldImages -> struct (true, true, true, true)

            Assert.True(List.length added = if hasAdded then 1 else 0)
            Assert.True(List.length removed = if hasRemoved then 1 else 0)

            if hasRemoved then
                let removed = List.head removed
                let colCount = [true; expectSk; expectData] |> Seq.filter id |> Seq.length
                Assert.Equal(colCount, Map.count removed)

                Assert.Equal(pkAttr, Map.find "TablePk" removed)
                if expectSk then Assert.Equal(skAttr, Map.find "TableSk" removed)
                if expectData then Assert.Equal(String "random data", Map.find "RandomData" removed)

            if hasAdded then
                let added = List.head added
                let colCount = [true; expectSk; expectData] |> Seq.filter id |> Seq.length
                Assert.Equal(colCount, Map.count added)

                Assert.Equal(pkAttr, Map.find "TablePk" added)
                if expectSk then Assert.Equal(skAttr, Map.find "TableSk" added)
                if expectData then Assert.Equal(String "more random data", Map.find "RandomData" added)

            // random double dispose of subscription
            subscription.Dispose()
        }

    [<Theory>]
    [<InlineData("k")>]
    [<InlineData("n")>]
    [<InlineData("o")>]
    [<InlineData("no")>]
    let ``Subscription delete tests`` ``stream type`` =

        let streamType =
            match ``stream type`` with
            | "k" -> KeysOnly
            | "n" -> NewImage
            | "o" -> OldImage
            | "no" -> NewAndOldImages
            | _ -> invalidOp ""

        task {
            use writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record = System.Collections.Generic.List<_>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value |> decimal

            let pkAttr =
                if table.isBinaryTable
                then Binary (Encoding.UTF8.GetBytes pk)
                else String pk
            let skAttr =
                if table.isBinaryTable
                then Binary (Encoding.UTF8.GetBytes $"{sk}")
                else Number sk

            use _ =
                host.SubscribeToStream writer' table.name struct (SubscriberBehaviour.defaultOptions, streamType)
                <| fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask<_>(())

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> if table.isBinaryTable
                    then ItemBuilder.withAttribute "TablePk" "UTF8" pk
                    else ItemBuilder.withAttribute "TablePk" "S" pk
                |> if table.isBinaryTable
                    then ItemBuilder.withAttribute "TableSk" "UTF8" $"{sk}"
                    else ItemBuilder.withAttribute "TableSk" "N" $"{sk}"
                |> ItemBuilder.withAttribute "RandomData" "S" "random data"

            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)

            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None
            record.Clear()

            let keys =
                let keyCols = if table.hasSk then ["TablePk"; "TableSk"] else ["TablePk"]
                ItemBuilder.dynamoDbAttributes item
                |> Seq.filter (fun x -> List.contains x.Key keyCols)
                |> Enumerable.ToDictionary

            // act
            let! _ = client.DeleteItemAsync(
                ItemBuilder.tableName item,
                keys)

            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // assert
            let struct (added, removed) = segregate record pkAttr

            let struct (expectSk, expectData) =
                match streamType with
                | KeysOnly -> struct (table.hasSk, false)
                | OldImage
                | NewImage
                | NewAndOldImages -> struct (true, true)

            match streamType with
            | NewImage ->
                let l = [added; removed] |> List.concat |> List.length
                Assert.True((l = 0), l.ToString())
            | _ ->
                Assert.True(List.length added = 0)
                Assert.True(List.length removed = 1)

                let removed = List.head removed
                let colCount = [true; expectSk; expectData] |> Seq.filter id |> Seq.length
                Assert.Equal(colCount, Map.count removed)

                Assert.Equal(pkAttr, Map.find "TablePk" removed)
                if expectSk then Assert.Equal(skAttr, Map.find "TableSk" removed)
                if expectData then Assert.Equal(String "random data", Map.find "RandomData" removed)
        }

    [<Fact>]
    let ``Remove Subscription tests`` () =

        task {
            use writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record = System.Collections.Generic.List<_>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value
            let dataAttr = $"Attr{IncrementingId.next()}"

            use removeSubscription =
                host.SubscribeToStream writer' table.name struct (SubscriberBehaviour.defaultOptions, NewAndOldImages)
                <| fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask<_>(())

            let itemBase =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.withAttribute "TablePk" "S" pk
                |> ItemBuilder.withAttribute "TableSk" "N" $"{sk}"

            let struct (before, after) =
                [1..10]
                |> Seq.map _.ToString()
                |> Seq.mapi (fun i -> flip (ItemBuilder.withAttribute dataAttr "N") itemBase >> tpl i)
                |> Collection.partition (fun x -> fstT x % 2 = 0)
                |> mapFst (Seq.map sndT)
                |> mapSnd (Seq.map sndT)

            let! _ =
                before
                |> Seq.map (fun s -> client.PutItemAsync(
                    ItemBuilder.tableName s,
                    ItemBuilder.dynamoDbAttributes s))
                |> Task.WhenAll

            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None
            removeSubscription.Dispose()

            // act
            let! _ =
                after
                |> Seq.map (fun s -> client.PutItemAsync(
                    ItemBuilder.tableName s,
                    ItemBuilder.dynamoDbAttributes s))
                |> Task.WhenAll

            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // assert
            let testItems =
                record
                |> Seq.map sndT
                |> Seq.collect (_.data.packet.changeResult.OrderedChanges >> List.map Change.create)
                |> Seq.collect asPutsAndDeletes
                |> Seq.map Either.reduce
                |> Seq.filter (Map.containsKey dataAttr)
                |> Array.ofSeq

            Assert.True(Array.length testItems > 1)

            let good =
                before
                |> Seq.map (ItemBuilder.attributes >> Map.find  dataAttr)
                |> Array.ofSeq
            let bad =
                after
                |> Seq.map (ItemBuilder.attributes >> Map.find  dataAttr)
                |> Array.ofSeq

            Array.ForEach(testItems, fun item ->
                let dataAttr = item |> Map.find dataAttr
                Assert.True(Array.contains dataAttr good)
                Assert.False(Array.contains dataAttr bad))
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Subscription error handling tests`` ``fail async`` ``add delay`` =

        task {
            use writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let streamSettings =
                if ``add delay`` then { delay = TimeSpan.FromMilliseconds(10) |> RunAsynchronously; subscriberTimeout = TimeSpan.Zero }
                else { delay = RunSynchronouslyAndPropagateToEventTrigger; subscriberTimeout = TimeSpan.Zero }

            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record = System.Collections.Generic.List<_>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value

            use _ =
                host.SubscribeToStream writer' table.name struct (streamSettings, NewAndOldImages)
                <| fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    if ``fail async``
                    then
                        ValueTask<unit>(task {
                            do! Task.Delay(50)
                            invalidOp "####"
                            return ()
                        })
                    else invalidOp "####"

            let itemBase =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.withAttribute "TablePk" "S" pk
                |> ItemBuilder.withAttribute "TableSk" "N" $"{sk}"
                |> ItemBuilder.withAttribute "IndexPk" "N" "1"
                |> ItemBuilder.withAttribute "IndexSk" "S" "789"

            let! _ =
                [1..3]
                |> Seq.map (fun i -> ItemBuilder.withAttribute "x" "N" (i.ToString()) itemBase)
                |> Seq.map (fun s -> client.PutItemAsync(
                    ItemBuilder.tableName s,
                    ItemBuilder.dynamoDbAttributes s))
                |> Task.WhenAll

            let! exn = Assert.ThrowsAnyAsync<AggregateException>(fun () -> (host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None).AsTask())
            let myExceptions = exn.InnerExceptions |> Seq.filter _.ToString().Contains("####")

            Assert.Equal(3, Seq.length myExceptions)
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None // assert all errors are consumed
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Subscription error isolation tests`` ``fail async`` ``add delay`` =

        task {
            use writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let streamSettings =
                if ``add delay`` then { delay = TimeSpan.FromMilliseconds(10) |> RunAsynchronously; subscriberTimeout = TimeSpan.Zero }
                else { delay = RunSynchronouslyAndPropagateToEventTrigger; subscriberTimeout = TimeSpan.Zero }

            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record1 = System.Collections.Generic.List<_>()
            let record2 = System.Collections.Generic.List<_>()
            let record3 = System.Collections.Generic.List<_>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value

            use subscription =
                host.SubscribeToStream writer' table.name struct (streamSettings, NewAndOldImages)
                <| fun x _ ->
                    record1.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask<_>(())

            use _ =
                host.SubscribeToStream writer' table.name struct (streamSettings, NewAndOldImages)
                <| fun x _ ->
                    record2.Add(struct (DateTimeOffset.UtcNow, x))
                    if ``fail async``
                    then
                        ValueTask<unit>(task {
                            do! Task.Delay(50)
                            invalidOp "####"
                            return ()
                        })
                    else invalidOp "####"

            use _ =
                host.SubscribeToStream writer' table.name struct (streamSettings, NewAndOldImages)
                <| fun x _ ->
                    record3.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask<_>(())

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.withAttribute "TablePk" "S" pk
                |> ItemBuilder.withAttribute "TableSk" "N" $"{sk}"
                |> ItemBuilder.withAttribute "IndexPk" "N" "1"
                |> ItemBuilder.withAttribute "IndexSk" "S" "789"

            // act
            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)
            let! e = Assert.ThrowsAnyAsync(fun () -> (host.AwaitAllSubscribers ValueNone CancellationToken.None).AsTask())
            assertError output "####" e

            // assert
            Assert.Equal(1, record1.Count)
            Assert.Equal(1, record2.Count)
            Assert.Equal(1, record3.Count)

            // random double dispose of subscription
            subscription.Dispose()
        }

    [<Fact>]
    let ``Subscription synchronous error handling tests`` () =

        task {
            use writer = new TestLogger(output)
            let writer' = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let streamSettings =
                { delay = RunSynchronously; subscriberTimeout = TimeSpan.Zero }

            let! tables = sharedTestData ValueNone // (ValueSome output)
            output.WriteLine "CLONING"
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record = System.Collections.Generic.List<_>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value

            use _ =
                host.SubscribeToStream writer' table.name struct (streamSettings, NewAndOldImages)
                <| fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    InvalidCastException("####") |> raise
                    ValueTask<_>(())

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.withAttribute "TablePk" "S" pk
                |> ItemBuilder.withAttribute "TableSk" "N" $"{sk}"
                |> ItemBuilder.withAttribute "IndexPk" "N" "1"
                |> ItemBuilder.withAttribute "IndexSk" "S" "789"

            // assert
            let execute () = client.PutItemAsync(ItemBuilder.tableName item, ItemBuilder.dynamoDbAttributes item)
            let! xx = Assert.ThrowsAsync<InvalidCastException>(fun () -> execute())
            Assert.Contains("####", xx.Message)

            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert item not added
            Assert.Equal(0, result.Items.Count)
        }

    [<Fact>]
    let ``Streams event subscription, with failed puts, updates and deletes, nothing sent`` () =

        task {
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let record = System.Collections.Generic.List<struct (DateTimeOffset * Amazon.Lambda.DynamoDBEvents.DynamoDBEvent)>()
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value |> decimal

            let data =
                { tablePk = pk
                  tableSk = sk
                  indexPk = sk
                  indexSk = pk
                  binaryData = "hello"
                  boolData = true }

            let invalidPut1 () =
                let data = TableItem.asAttributes data
                data["TablePk"].S <- null
                data["TablePk"].N <- "55"

                Assert.ThrowsAnyAsync(fun _ -> client.PutItemAsync(table.name, data) |> Io.ignoreTask)
                |> ValueTask<Exception>
                |%|> assertError output "Item not valid for table"

            let invalidPut2 () =
                let req = PutItemRequest()
                req.Item <- TableItem.asAttributes data
                req.TableName <- table.name
                req.ConditionExpression <- "attribute_exists(XXXXXXX)"

                Assert.ThrowsAnyAsync(fun _ -> client.PutItemAsync(req) |> Io.ignoreTask)
                |> ValueTask<Exception>
                |%|> assertError output "ConditionalCheckFailedException"

            let invalidDelete1 () =
                let data = TableItem.asAttributes data

                Assert.ThrowsAnyAsync(fun _ -> client.DeleteItemAsync(table.name, data) |> Io.ignoreTask)
                |> ValueTask<Exception>
                |%|> assertError output "Found non key attributes"

            let invalidDelete2 () =
                let data = TableItem.asAttributes data
                let req = DeleteItemRequest()
                req.Key <- Dictionary()
                req.Key["TablePk"] <- data["TablePk"]
                if table.hasSk then req.Key["TableSk"] <- data["TableSk"]
                req.TableName <- table.name
                req.ConditionExpression <- "attribute_exists(XXXXXXX)"

                Assert.ThrowsAnyAsync(fun _ -> client.DeleteItemAsync(req) |> Io.ignoreTask)
                |> ValueTask<Exception>
                |%|> assertError output "ConditionalCheckFailedException"

            let invalidDelete3 () =
                let data = TableItem.asAttributes data
                let req = DeleteItemRequest()
                req.Key <- Dictionary()
                req.Key["TablePk"] <- DynamoAttributeValue()
                req.Key["TablePk"].S <- "invalid"
                if table.hasSk then req.Key["TableSk"] <- data["TableSk"]
                req.TableName <- table.name

                client.DeleteItemAsync(req) |> Io.ignoreTask

            let invalidUpdate1 () =
                let data = TableItem.asAttributes data
                let req = UpdateItemRequest()
                req.Key <- Dictionary()
                req.Key["TablePk"] <- data["TablePk"]
                if table.hasSk then req.Key["TableSk"] <- data["TableSk"]
                req.TableName <- table.name
                req.ConditionExpression <- "attribute_exists(XXXXXXX)"
                req.UpdateExpression <- "REMOVE uuuuu"

                Assert.ThrowsAnyAsync(fun _ -> client.UpdateItemAsync(req) |> Io.ignoreTask)
                |> ValueTask<Exception>
                |%|> assertError output "ConditionalCheckFailedException"

            let invalidUpdate2 () =
                let data = TableItem.asAttributes data
                let req = DeleteItemRequest()
                req.Key <- Dictionary()
                if table.hasSk then req.Key["TableSk"] <- data["TableSk"]
                req.TableName <- table.name

                Assert.ThrowsAnyAsync(fun _ -> client.DeleteItemAsync(req) |> Io.ignoreTask)
                |> ValueTask<Exception>
                |%|> assertError output "Could not find partition key attribute"

            // act
            use _ =
                let sub = asFunc2 (fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask.CompletedTask)

                Subscriptions.AddSubscription(host, table.name, sub)

            do! invalidPut1()
            do! invalidPut2()
            do! invalidDelete1()
            do! invalidDelete2()
            do! invalidDelete3()
            do! invalidUpdate1()
            do! invalidUpdate2()
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // assert
            Assert.Empty(record)
        }

    [<Fact>]
    let ``Subscribe to deletes, item put, no update + opposite`` () =

        task {
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            let table = Tables.getByStreamsEnabled true tables
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value |> decimal

            let data1 =
                { tablePk = pk
                  tableSk = sk
                  indexPk = sk
                  indexSk = pk
                  binaryData = "hello"
                  boolData = true }

            let data2 = { data1 with tablePk = "something new" }
            let data1Keys =
                let attrs = TableItem.asAttributes data1
                let keys = Dictionary<_, _>()
                keys["TablePk"] <- attrs["TablePk"]
                if table.hasSk then keys["TableSk"] <- attrs["TableSk"]
                keys

            do! client.PutItemAsync(table.name, TableItem.asAttributes data1) |> Io.ignoreTask

            // act
            let recordNew = System.Collections.Generic.List<_>()
            let recordOld = System.Collections.Generic.List<_>()
            use _ =
                let sub = asFunc2 (fun x _ ->
                    recordNew.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask.CompletedTask)

                Subscriptions.AddSubscription(host, table.name, sub, streamViewType = Amazon.DynamoDBv2.StreamViewType.NEW_IMAGE)

            use _ =
                let sub = asFunc2 (fun x _ ->
                    recordOld.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask.CompletedTask)

                Subscriptions.AddSubscription(host, table.name, sub, streamViewType = Amazon.DynamoDBv2.StreamViewType.OLD_IMAGE)

            do! client.PutItemAsync(table.name, TableItem.asAttributes data2) |> Io.ignoreTask
            do! client.DeleteItemAsync(table.name, data1Keys) |> Io.ignoreTask
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // assert
            Assert.Single(recordOld) |> ignore
            Assert.Single(recordNew) |> ignore
        }

    [<Fact>]
    let ``Subscriber packet ordering`` () =

        task {
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClientBuilder.Create(host)
            client.SetProcessingDelay TimeSpan.Zero
            let table = Tables.getByStreamsEnabled true tables
            let pk = $"Sub-{IncrementingId.next()}"
            let sk = IncrementingId.next().Value |> decimal

            let data1 =
                { tablePk = pk
                  tableSk = sk
                  indexPk = sk
                  indexSk = pk
                  binaryData = "hello"
                  boolData = true }

            let data2 = { data1 with indexSk = "something new" }

            let settings1 = { SubscriberBehaviour.defaultOptions with delay = TimeSpan.FromSeconds(0.1) |> RunAsynchronously }
            let settings2 = { SubscriberBehaviour.defaultOptions with delay = RunSynchronously }

            // act
            let record = System.Collections.Generic.List<_>()
            use subscription =
                let sub = asFunc2 (fun x _ ->
                    record.Add(struct (DateTimeOffset.UtcNow, x))
                    ValueTask.CompletedTask)

                Subscriptions.AddSubscription(host, table.name, sub, behaviour = settings1)

            let put1 = client.PutItemAsync(table.name, TableItem.asAttributes data1)
            host.SetStreamBehaviour (ValueSome writer) table.name subscription.SubscriberId settings2
            do! client.PutItemAsync(table.name, TableItem.asAttributes data2) |> Io.ignoreTask
            do! put1 |> Io.ignoreTask
            do! host.AwaitAllSubscribers (ValueSome writer) CancellationToken.None

            // assert
            let adds =
                record
                |> Seq.sortBy fstT
                |> Seq.collect (sndT >> _.Records)
                |> Seq.map (_.Dynamodb.NewImage["IndexSk"].S)
                |> List.ofSeq

            Assert.Equal<string>([pk; "something new"], adds)

        }
