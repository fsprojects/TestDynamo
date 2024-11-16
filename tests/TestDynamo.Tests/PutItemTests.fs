
namespace TestDynamo.Tests

open System
open System.Linq
open System.Threading.Tasks
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Api.FSharp
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Requests.Queries
open Tests.Table
open Tests.Utils
open Xunit
open Amazon.DynamoDBv2
open Xunit.Abstractions
open TestDynamo.Utils
open RequestItemTestUtils
open TestDynamo.Client.ItemMapper
open Tests.Loggers

type DataFailureType =
    | None = 1
    | LoadsOfNestingWithErrors1 = 2
    | LoadsOfNestingWithErrors2 = 3
    | DuplicateInStringSet = 4
    | DuplicateInNumberSet = 5
    | DuplicateInBinarySet = 6
    | EmptyMapName = 7
    | EmptyPropName = 8
    | NullPartitionKey = 9
    | NullSortKey = 10
    | EmptyPartitionKey = 11
    | EmptySortKey = 12
    | NullIndexPartitionKey = 13
    | NullIndexSortKey = 14
    | EmptyIndexPartitionKey = 15
    | EmptyIndexSortKey = 16
    | InvalidNumber = 17
    | LoadsOfNesting = 18
    | EmptyStringSet = 19
    | EmptyNumberSet = 20
    | EmptyBinarySet = 21
    | InvalidDataTypeIndexSortKey = 22

type DataFailureTypeCases() =
    inherit EnumValues<DataFailureType>()

type PutItemTests(output: ITestOutputHelper) =

    let random = randomBuilder output

    static let table =
        let client = buildTempClient ()
        let client = client.Client

        let t = sprintf "%s%i" (nameof PutItemTests) (uniqueId())
        let req =
            TableBuilder.empty
            |> TableBuilder.withTableName t
            |> TableBuilder.withAttribute "TablePk" "S"
            |> TableBuilder.withAttribute "TableSk" "N"
            |> TableBuilder.withAttribute "IndexPk" "N"
            |> TableBuilder.withAttribute "IndexSk" "S"
            |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")
            |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" (ValueSome "IndexSk") false

        let table = TableBuilder.req req |> client.CreateTableAsync
        task {
            try
                // act
                let! _ = table
                return t
            finally
                client.Dispose()
        }

    static let allStringsTable =
        let client = buildClient (OutputCollector())
        let client = client.Client

        let t = sprintf "%s%i" (nameof PutItemTests) (uniqueId())
        let req =
            TableBuilder.empty
            |> TableBuilder.withTableName t
            |> TableBuilder.withAttribute "TablePk" "S"
            |> TableBuilder.withAttribute "TableSk" "S"
            |> TableBuilder.withAttribute "IndexPk" "S"
            |> TableBuilder.withAttribute "IndexSk" "S"
            |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")
            |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" (ValueSome "IndexSk") false

        let table = TableBuilder.req req |> client.CreateTableAsync
        task {
            try
                // act
                let! _ = table
                return t
            finally
                client.Dispose()
        }

    let get index partitionKey sortKey (client: IAmazonDynamoDB) =
        task {
            let! t = table

            let keyCondition =
                ValueOption.map (fun struct (name, _) -> $"{fstT partitionKey} = :p AND {name} = :s") sortKey
                |> ValueOption.defaultValue $"{fstT partitionKey} = :p"

            let index =
                ValueOption.map (QueryBuilder.setIndexName) index
                |> ValueOption.defaultValue id

            let skValue =
                ValueOption.map (fun struct (_, v) -> QueryBuilder.setExpressionAttrValues ":s" v) sortKey
                |> ValueOption.defaultValue id

            let q1 =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName t
                |> index
                |> QueryBuilder.setKeyConditionExpression keyCondition
                |> QueryBuilder.setExpressionAttrValues ":p" (sndT partitionKey)
                |> skValue
                |> QueryBuilder.queryRequest

            let! result = client.QueryAsync q1
            return result.Items
        }

    let assertOne index partitionKey sortKey (client: IAmazonDynamoDB) =
        task {
            let! x = get index partitionKey sortKey client
            return Assert.Single x
        }

    let assertZero index partitionKey sortKey (client: IAmazonDynamoDB) =
        task {
            let! x = get index partitionKey sortKey client
            return Assert.Empty x
        }

    let asBatchReq (req: PutItemRequest) =
        let put = WriteRequest()
        put.PutRequest <- PutRequest()
        put.PutRequest.Item <- req.Item

        let req2 = BatchWriteItemRequest()
        req2.RequestItems <- Dictionary()
        req2.RequestItems.Add(
            req.TableName,
            [put] |> Enumerable.ToList)

        req2

    let maybeBatch batch (client: IAmazonDynamoDB) (req: PutItemRequest) =
        if batch
        then asBatchReq req |> client.BatchWriteItemAsync |> Io.ignoreTask
        else client.PutItemAsync req |> Io.ignoreTask

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``PutItem table, partition and sort keys, creates successfully`` batch =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = table

            let pk1 = (Guid.NewGuid()).ToString()
            let pk1V = ItemBuilder.buildItemAttribute "S" pk1
            let pk2 = (uniqueId()).ToString()
            let pk2V = ItemBuilder.buildItemAttribute "N" pk2

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" pk2
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            do! maybeBatch batch client (ItemBuilder.asPutReq item)

            // assert
            let! _ = assertOne ValueNone (struct ("TablePk", pk1V)) ValueNone client
            let! _ = assertOne (ValueSome "TheIndex") (struct ("IndexPk", pk2V)) ValueNone client

            return ()
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``PutItem table, missing index partition key, creates successfully`` batch =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = table

            let pk1 = (Guid.NewGuid()).ToString()
            let pk1V = ItemBuilder.buildItemAttribute "S" pk1

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            do! maybeBatch batch client (ItemBuilder.asPutReq item)

            // assert
            let! _ = assertOne ValueNone (struct ("TablePk", pk1V)) ValueNone client

            return ()
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``PutItem table, missing index sort key, creates successfully`` batch =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = table

            let pk1 = (Guid.NewGuid()).ToString()
            let pk1V = ItemBuilder.buildItemAttribute "S" pk1
            let pk2 = (uniqueId()).ToString()
            let pk2V = ItemBuilder.buildItemAttribute "N" pk2

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" pk2
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            do! maybeBatch batch client (ItemBuilder.asPutReq item)

            // assert
            let! _ = assertOne ValueNone (struct ("TablePk", pk1V)) ValueNone client
            let! _ = assertZero (ValueSome "TheIndex") (struct ("IndexPk", pk2V)) ValueNone client

            return ()
        }

    [<Fact>]
    let ``PutItem table, missing partition key, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = table

            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" pk2
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item))

            assertError output "Missing partition key" e
        }

    [<Fact>]
    let ``PutItem table, missing sort key, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = table

            let pk1 = (Guid.NewGuid()).ToString()
            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "IndexPk" "N" pk2
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item))

            assertError output "Missing sort key" e
        }

    [<Fact>]
    let ``PutItem table, invalid partition key type, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = table

            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "N" "9"
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" pk2
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item))

            assertError output "to have type" e
        }

    [<Fact>]
    let ``PutItem table, invalid sort key type, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = table

            let pk1 = (Guid.NewGuid()).ToString()
            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" "x"
                |> ItemBuilder.withAttribute "TableSk" "S" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" pk2
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item))

            assertError output "to have type" e
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``PutItem, table and index, item is replicated`` ``table has sk`` ``index has sk`` batch =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableSk = if ``table has sk`` then ValueSome "TableSk" else ValueNone
            let indexSk = if ``index has sk`` then ValueSome "IndexSk" else ValueNone
            let tableName = sprintf "%s%i" (nameof PutItemTests) (uniqueId())
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> (ValueOption.map (flip TableBuilder.withAttribute "N") tableSk |> ValueOption.defaultValue id)
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> (ValueOption.map (flip TableBuilder.withAttribute "S") indexSk |> ValueOption.defaultValue id)
                |> TableBuilder.withKeySchema "TablePk" tableSk
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" indexSk false

            let! _ = TableBuilder.req req |> client.CreateTableAsync

            let pk1 = (Guid.NewGuid()).ToString()
            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" pk2
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            do! maybeBatch batch client (ItemBuilder.asPutReq item)

            let scan =
                ScanBuilder.empty
                |> ScanBuilder.withTableName tableName

            let! tableResult = client.ScanAsync(ScanBuilder.req scan)
            let! indexResult = client.ScanAsync(ScanBuilder.withIndexName "TheIndex" scan |> ScanBuilder.req)

            // assert
            Assert.Equal(1, tableResult.Items.Count)
            Assert.Equal(1, indexResult.Items.Count)
        }

    [<Theory>]
    [<ClassData(typedefof<DataFailureTypeCases>)>]
    let ``PutItem, test attribute types`` ``failure type`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! table = allStringsTable
            let tablePk = $"{IncrementingId.next()}"
            let tableSk = $"{IncrementingId.next()}"
            let indexPk = $"{IncrementingId.next()}"
            let indexSk = $"{IncrementingId.next()}"

            let validItem =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table
                |> ItemBuilder.withAttribute "TablePk" "S" tablePk
                |> ItemBuilder.withAttribute "TableSk" "S" tableSk
                |> ItemBuilder.withAttribute "IndexPk" "S" indexPk
                |> ItemBuilder.withAttribute "IndexSk" "S" indexSk
                |> ItemBuilder.withAttribute "Str" "S" $"StrVal"
                |> ItemBuilder.withAttribute "EmptyStr" "S" $""
                |> ItemBuilder.withAttribute "Num" "N" $"10"
                |> ItemBuilder.withAttribute "True" "BOOL" $"true"
                |> ItemBuilder.withAttribute "False" "BOOL" $"false"
                |> ItemBuilder.withAttribute "Bin" "UTF8" $"Hello"
                |> ItemBuilder.withNullAttribute "NL"
                |> ItemBuilder.withMapAttribute "Mp" [("v1", ("S", "hi")); ("v1.1", ("S", "")); ("v2", ("UTF8", "something"))]
                |> ItemBuilder.withMapAttribute "EmptyMp" []
                |> ItemBuilder.withListAttribute "Lst" [("NULL", "null"); ("BOOL", "true")]
                |> ItemBuilder.withListAttribute "EmptyLst" []
                |> ItemBuilder.withSetAttribute "StrS" "SS" ["v1"; "v2"; ""]
                |> ItemBuilder.withSetAttribute "NumS" "NS" ["-88"; "99.55"]
                |> ItemBuilder.withSetAttribute "BinS" "BS" ["val1"; "val2"]

            let struct (valid, putItemF, err) = 
                match ``failure type`` with
                | DataFailureType.None -> struct (true, id, "")
                | DataFailureType.EmptyStringSet -> struct (false, ItemBuilder.withSetAttribute "StrS" "SS" [], "Empty set not supported")
                | DataFailureType.EmptyNumberSet -> struct (false, ItemBuilder.withSetAttribute "NumS" "NS" [], "Empty set not supported")
                | DataFailureType.EmptyBinarySet -> struct (false, ItemBuilder.withSetAttribute "BinS" "BS" [], "Empty set not supported")
                | DataFailureType.DuplicateInStringSet -> struct (false, ItemBuilder.withSetAttribute "StrS" "SS" ["x";"x"], "Duplicate value in String set")
                | DataFailureType.DuplicateInNumberSet -> struct (false, ItemBuilder.withSetAttribute "NumS" "NS" ["1";"1"], "Duplicate value in Number set")
                | DataFailureType.DuplicateInBinarySet -> struct (false, ItemBuilder.withSetAttribute "BinS" "BS" ["x";"x"], "Duplicate value in Binary set")
                | DataFailureType.EmptyPropName -> struct (false, ItemBuilder.withAttribute "" "N" "99", "Item has map or property attribute with null or empty name")
                | DataFailureType.EmptyMapName -> struct (false, ItemBuilder.withMapAttribute "Mp" [("", ("S", "hi"))], "Item has map or property attribute with null or empty name: \"Mp\"")
                | DataFailureType.NullPartitionKey -> struct (false, ItemBuilder.withAttribute "TablePk" "S" null, "Unknown attribute type for")
                | DataFailureType.EmptyPartitionKey -> struct (false, ItemBuilder.withAttribute "TablePk" "S" "", "Empty string for key TablePk is not permitted")
                | DataFailureType.NullSortKey -> struct (false, ItemBuilder.withAttribute "TableSk" "S" null, "Unknown attribute type for")
                | DataFailureType.EmptySortKey -> struct (false, ItemBuilder.withAttribute "TableSk" "S" "", "Empty string for key TableSk is not permitted")
                | DataFailureType.NullIndexPartitionKey -> struct (false, ItemBuilder.withAttribute "IndexPk" "S" null, "Unknown attribute type for")
                | DataFailureType.EmptyIndexPartitionKey -> struct (false, ItemBuilder.withAttribute "IndexPk" "S" "", "Empty string for key IndexPk is not permitted")
                | DataFailureType.NullIndexSortKey -> struct (false, ItemBuilder.withAttribute "IndexSk" "S" null, "Unknown attribute type for")
                | DataFailureType.EmptyIndexSortKey -> struct (false, ItemBuilder.withAttribute "IndexSk" "S" "", "Empty string for key IndexSk is not permitted")
                | DataFailureType.InvalidDataTypeIndexSortKey -> struct (false, ItemBuilder.withAttribute "IndexSk" "N" "8", "Expected attribute \"IndexSk\" to have type: String, got type Number")
                | DataFailureType.InvalidNumber -> struct (false, ItemBuilder.withAttribute "" "N" "abc", "not in a correct format")
                | DataFailureType.LoadsOfNesting ->

                    let mapHead = buildDeeplyNestedMap() |> fstT
                    let listHead = buildDeeplyNestedList() |> fstT

                    struct (true, ItemBuilder.addAttribute "LotsOfNesting1" mapHead >> ItemBuilder.addAttribute "LotsOfNesting2" listHead, "")

                | DataFailureType.LoadsOfNestingWithErrors1 ->

                    let mapHead = DynamoAttributeValue()
                    let mapTail = mapHead |> addMapWith0Prop |> addListWith0Element |> addMapWith0Prop
                    mapTail.N <- "Err"

                    struct (false, ItemBuilder.addAttribute "LotsOfNesting1" mapHead, "was not in a correct format")

                | DataFailureType.LoadsOfNestingWithErrors2 ->

                    let mapHead = DynamoAttributeValue()
                    let mapTail = mapHead |> addMapWith0Prop |> addListWith0Element |> addMapWith0Prop
                    mapTail.M <- Dictionary<string, DynamoAttributeValue>()
                    mapTail.M[""] <- DynamoAttributeValue()
                    mapTail.M[""].S <- "x"
                    mapTail.IsMSet <- true

                    struct (false, ItemBuilder.addAttribute "LotsOfNesting1" mapHead, "Item has map or property attribute with null or empty name: \"LotsOfNesting1.0.[].0\"")
                | x -> invalidOp $"{x}"

            let putItem = putItemF validItem
            let put () =
                task {
                    do! client.PutItemAsync (ItemBuilder.asPutReq putItem) |> Io.ignoreTask
                }

            let get () =
                task {
                    let! items =  client.QueryAsync(
                        QueryBuilder.empty ValueNone
                        |> QueryBuilder.setTableName table
                        |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                        |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute "S" tablePk)
                        |> QueryBuilder.setExpressionAttrValues ":s" (ItemBuilder.buildItemAttribute "S" tableSk)
                        |> QueryBuilder.queryRequest)

                    let output =
                        match items.Items |> List.ofSeq with
                        | [] -> ValueNone
                        | [x] -> ValueSome x
                        | _ -> invalidOp "too many"

                    return output
                }

            // act
            if valid
            then
                do! put()
            else
                let! e = Assert.ThrowsAnyAsync(fun () -> put())
                assertError output err e

            // assert
            let! item = get()
            match item, valid with
            | ValueNone, true -> invalidOp "Item not found"
            | ValueSome _, false -> invalidOp "Item should not have been found"
            | ValueNone, false -> ()
            | ValueSome x, true ->

                assertDynamoDbItems struct ([ putItem.attrs |> CSharp.toDictionary id id ], [x], true)
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``PutItem twice, table and index, item is replaced and replicated`` (``table has sk``, ``index has sk``, batch) =
        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableSk = if ``table has sk`` then ValueSome "TableSk" else ValueNone
            let indexSk = if ``index has sk`` then ValueSome "IndexSk" else ValueNone
            let tableName = sprintf "%s%i" (nameof PutItemTests) (uniqueId())
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> (ValueOption.map (flip TableBuilder.withAttribute "N") tableSk |> ValueOption.defaultValue id)
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> (ValueOption.map (flip TableBuilder.withAttribute "S") indexSk |> ValueOption.defaultValue id)
                |> TableBuilder.withKeySchema "TablePk" tableSk
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" indexSk true

            let! _ = TableBuilder.req req |> client.CreateTableAsync

            let pk1 = $"id-{uniqueId()}"
            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" pk2
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            let modifiedItem =
                ItemBuilder.withAttribute "TablePk" "S" pk1 item
                |> ItemBuilder.withAttribute "Something" "N" "99"

            // act
            do! maybeBatch batch client (ItemBuilder.asPutReq item)
            do! maybeBatch batch client (ItemBuilder.asPutReq modifiedItem)

            let scan =
                ScanBuilder.empty
                |> ScanBuilder.withTableName tableName

            let! tableResult = client.ScanAsync(ScanBuilder.req scan)
            let! indexResult = client.ScanAsync(ScanBuilder.withIndexName "TheIndex" scan |> ScanBuilder.req)

            // assert
            Assert.Equal(1, tableResult.Items.Count)
            Assert.Equal(1, indexResult.Items.Count)

            Assert.Equal("99", (tableResult.Items[0]["Something"]).N)
            Assert.Equal("99", (indexResult.Items[0]["Something"]).N)
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``PutItem, table and index, index pk key missing, item not replicated`` (``table has sk``, ``index has sk``, batch) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableSk = if ``table has sk`` then ValueSome "TableSk" else ValueNone
            let indexSk = if ``index has sk`` then ValueSome "IndexSk" else ValueNone
            let tableName = sprintf "%s%i" (nameof PutItemTests) (uniqueId())
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> (ValueOption.map (flip TableBuilder.withAttribute "N") tableSk |> ValueOption.defaultValue id)
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> (ValueOption.map (flip TableBuilder.withAttribute "S") indexSk |> ValueOption.defaultValue id)
                |> TableBuilder.withKeySchema "TablePk" tableSk
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" indexSk false

            let! _ = TableBuilder.req req |> client.CreateTableAsync

            let pk1 = (Guid.NewGuid()).ToString()
            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            do! maybeBatch batch client (ItemBuilder.asPutReq item)

            let scan =
                ScanBuilder.empty
                |> ScanBuilder.withTableName tableName

            let! tableResult = client.ScanAsync(ScanBuilder.req scan)
            let! indexResult = client.ScanAsync(ScanBuilder.withIndexName "TheIndex" scan |> ScanBuilder.req)

            // assert
            Assert.Equal(1, tableResult.Items.Count)
            Assert.Equal(0, indexResult.Items.Count)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``PutItem, table and index, index sk key missing, item not replicated`` (``table has sk``, batch) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableSk = if ``table has sk`` then ValueSome "TableSk" else ValueNone
            let tableName = sprintf "%s%i" (nameof PutItemTests) (uniqueId())
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> (ValueOption.map (flip TableBuilder.withAttribute "N") tableSk |> ValueOption.defaultValue id)
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> TableBuilder.withAttribute "IndexSk" "S"
                |> TableBuilder.withKeySchema "TablePk" tableSk
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" (ValueSome "IndexSk") false

            let! _ = TableBuilder.req req |> client.CreateTableAsync

            let pk1 = (Guid.NewGuid()).ToString()
            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" "7"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            do! maybeBatch batch client (ItemBuilder.asPutReq item)

            let scan =
                ScanBuilder.empty
                |> ScanBuilder.withTableName tableName

            let! tableResult = client.ScanAsync(ScanBuilder.req scan)
            let! indexResult = client.ScanAsync(ScanBuilder.withIndexName "TheIndex" scan |> ScanBuilder.req)

            // assert
            Assert.Equal(1, tableResult.Items.Count)
            Assert.Equal(0, indexResult.Items.Count)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``PutItem, table and index, duplicate item on index, item replicated`` (``table has sk``, ``index has sk``) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableSk = if ``table has sk`` then ValueSome "TableSk" else ValueNone
            let indexSk = if ``index has sk`` then ValueSome "IndexSk" else ValueNone
            let tableName = sprintf "%s%i" (nameof PutItemTests) (uniqueId())
            let req =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> (ValueOption.map (flip TableBuilder.withAttribute "N") tableSk |> ValueOption.defaultValue id)
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> (ValueOption.map (flip TableBuilder.withAttribute "S") indexSk |> ValueOption.defaultValue id)
                |> TableBuilder.withKeySchema "TablePk" tableSk
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" indexSk false

            let! _ = TableBuilder.req req |> client.CreateTableAsync

            let pk1 = $"id-{uniqueId()}"
            let pk2 = (uniqueId()).ToString()

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "TablePk" "S" pk1
                |> ItemBuilder.withAttribute "TableSk" "N" "8"
                |> ItemBuilder.withAttribute "IndexPk" "N" "7"
                |> ItemBuilder.withAttribute "IndexSk" "S" "UU"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            // act
            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)
            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.withAttribute "TablePk" "S" "X" item |> ItemBuilder.dynamoDbAttributes)

            let scan =
                ScanBuilder.empty
                |> ScanBuilder.withTableName tableName

            let! tableResult = client.ScanAsync(ScanBuilder.req scan)
            let! indexResult = client.ScanAsync(ScanBuilder.withIndexName "TheIndex" scan |> ScanBuilder.req)

            // assert
            Assert.Equal(2, tableResult.Items.Count)
            Assert.Equal(2, indexResult.Items.Count)
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Put item, with various condition expression scenarios`` ``item exists`` ``condition matches`` ``use legacy inputs`` =

        let conditionTargetsSome = ``item exists`` = ``condition matches``
        let success = ``item exists`` = conditionTargetsSome

        task {
            use writer = new TestLogger(output)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClient.createClient ValueNone ValueNone (ValueSome host)
            let table = Tables.get true true tables
            let struct (pk, struct (sk, item)) = randomItem table.hasSk random

            let pk =
                if ``item exists``
                then pk
                else $"override{uniqueId()}"

            let itemOverride =
                Map.add "TablePk" (Model.AttributeValue.String pk) item
                |> Map.add "TablePk_Copy" (Model.AttributeValue.String "override")

            let keys =
                let keyCols = if table.hasSk then ["TablePk"; "TableSk"] else ["TablePk"]
                itemOverride
                |> Map.filter (fun k _ -> List.contains k keyCols)
                |> itemToDynamoDb

            let req = PutItemRequest()
            req.TableName <- table.name
            req.Item <- itemToDynamoDb itemOverride
            if ``use legacy inputs``
            then
                req.Expected <- Dictionary<_, _>()
                req.Expected.Add(
                    "TableSk",
                    let c = ExpectedAttributeValue()
                    c.ComparisonOperator <-
                        if conditionTargetsSome then ComparisonOperator.NOT_NULL else ComparisonOperator.NULL
                    c)

                req.Expected.Add(
                    "TablePk",
                    let c = ExpectedAttributeValue()
                    c.ComparisonOperator <- ComparisonOperator.NE

                    let attr = DynamoAttributeValue()
                    attr.S <- "XX"
                    c.AttributeValueList <- MList<_>([attr])
                    c)

            else
                req.ConditionExpression <- 
                    if conditionTargetsSome
                    then "attribute_exists(#attr) AND TablePk <> :v"
                    else "attribute_not_exists(#attr) AND TablePk <> :v"
                req.ExpressionAttributeNames <-
                    Map.add "#attr" "TableSk" Map.empty
                    |> CSharp.toDictionary id id

                req.ExpressionAttributeValues <-
                    Map.add ":v" (Model.AttributeValue.String "XX") Map.empty
                    |> itemToDynamoDb

            req.ReturnValues <- ReturnValue.ALL_OLD

            // act
            if success
            then
                let! response = client.PutItemAsync(req)
                if ``item exists``
                then
                    Assert.Equal(pk, response.Attributes["TablePk"].S)
                    Assert.Equal(sk, response.Attributes["TableSk"].N |> decimal)
                    Assert.Equal(pk, response.Attributes["TablePk_Copy"].S)

                    let! x = client.GetItemAsync(table.name, keys)
                    Assert.True(x.Item["TablePk_Copy"].S = "override")
                else Assert.Empty(response.Attributes)
            else
                let! e = Assert.ThrowsAnyAsync(fun _ -> client.PutItemAsync(req))
                assertError output "ConditionalCheckFailedException" e

                let! x = client.GetItemAsync(table.name, keys)
                if ``item exists``
                then Assert.True(x.Item["TablePk_Copy"].S <> "override")
                else Assert.True(x.Item.Count = 0)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Batch write item, invalid region, throws`` ``global`` ``invalid region``: Task<unit> =

        task {
            // arrange
            use writer = new TestLogger(output)
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            use dHost = new GlobalDatabase(host.BuildCloneData())
            let tableName = (Tables.get true true tables).name
            use client =
                if ``global``
                then TestDynamoClient.createGlobalClient ValueNone (ValueSome host.Id) ValueNone (ValueSome dHost)
                else TestDynamoClientBuilder.Create(writer)

            let req =
                let r = BatchWriteItemRequest()
                let k = WriteRequest()
                k.DeleteRequest <- DeleteRequest()
                k.DeleteRequest.Key <- Dictionary<_, _>()
                r.RequestItems.Add(
                    if ``invalid region``
                        then $"arn:aws:dynamodb:invalid-region:{Settings.DefaultAwsAccountId}:table/{tableName}"
                        else $"arn:aws:dynamodb:{host.Id}:999999:table/{tableName}"
                    ,
                    [k] |> Enumerable.ToList)
                r

            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> client.BatchWriteItemAsync(req) |> Io.ignoreTask)
            match struct (``global``, ``invalid region``) with
            | struct (true, false)
            | struct (false, false) -> "Invalid aws account id 999999 in ARN"
            | struct (true, true) -> "No resources have been created in DB region invalid-region"
            | struct (false, true) -> "Some update table requests only work on GlobalDatabases"
            |> flip (assertError output) e
        }

    [<Fact>]
    let ``Batch write item, one put, one delete, executes both`` (): Task<unit> =

        task {
            // arrange
            use writer = new TestLogger(output)
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            use client = TestDynamoClientBuilder.Create(host)
            let tableName = (Tables.get true true tables).name
            let struct (putPk, struct (putSk, putItem)) = randomItem true random
            let struct (deletePk, struct (deleteSk, _)) = randomFilteredItem (fstT >> (<>)putPk) true random

            let fromKeys pk sk =
                Map.add "TablePk" (TestDynamo.Model.AttributeValue.String pk) Map.empty
                |> Map.add "TableSk" (TestDynamo.Model.AttributeValue.Number sk)

            let put = WriteRequest()
            put.PutRequest <- PutRequest()
            put.PutRequest.Item <-
                putItem
                |> Map.add "Blabla" (TestDynamo.Model.AttributeValue.Number 3344M)  
                |> itemToDynamoDb

            let delete = WriteRequest()
            delete.DeleteRequest <- DeleteRequest()
            delete.DeleteRequest.Key <-
                fromKeys deletePk deleteSk
                |> itemToDynamoDb

            let req = BatchWriteItemRequest()
            req.RequestItems <- Dictionary()
            req.RequestItems.Add(
                tableName,
                [put; delete] |> Enumerable.ToList)

            // act
            do! client.BatchWriteItemAsync req |> Io.ignoreTask

            // assert
            let! deleted = client.GetItemAsync(tableName, fromKeys deletePk deleteSk |> itemToDynamoDb)
            let! putted = client.GetItemAsync(tableName, fromKeys putPk putSk |> itemToDynamoDb)

            Assert.True(deleted.Item = null || deleted.Item.Count = 0)
            Assert.True(putted.Item["Blabla"].N = "3344")
        }
