
namespace DynamoDbInMemory.Tests

open System.Linq
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory
open DynamoDbInMemory.Client
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open DynamoDbInMemory.Data.Monads.Operators
open Utils
open DynamoDbInMemory.Client.ItemMapper
open RequestItemTestUtils
open DynamoDbInMemory.Model
open DynamoDbInMemory.Api
open Tests.Loggers

type DeleteItemTests(output: ITestOutputHelper) =

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
        let client = buildClient(ValueSome output)

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

    let maybeBatch batch (client: IInMemoryDynamoDbClient) (req: DeleteItemRequest) =
        if batch
        then asBatchReq req |> client.BatchWriteItemAsync |> Io.fromTask |%|> (asLazy ValueNone)
        else client.DeleteItemAsync req |> Io.fromTask |%|> ValueSome

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Delete item, with pk, deletes from table and index`` ``table has sk`` ``index has sk`` batch =

        task {
            use writer = new TestLogger(output)
            use writer = new TestLogger(output)
            let writer = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = InMemoryDynamoDbClient.Create(host)
            let table = Tables.get ``table has sk`` ``index has sk`` tables

            let tablePk = $"{IncrementingId.next()}"
            let tableSk = (IncrementingId.next()).Value |> decimal
            let indexPk = $"{IncrementingId.next()}"
            let indexSk = $"{IncrementingId.next()}"
            let tablePk2 = $"{IncrementingId.next()}"
            let tableSk2 = (IncrementingId.next()).Value |> decimal

            let item1 =
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.withAttribute "TablePk" "S" tablePk
                |> ItemBuilder.withAttribute "TableSk" "N" $"{tableSk}"
                |> ItemBuilder.withAttribute "IndexPk" "N" indexPk
                |> ItemBuilder.withAttribute "IndexSk" "S" indexSk
                |> ItemBuilder.withAttribute "RandomData" "S" "item 1"

            let item2 =
                item1
                |> ItemBuilder.withAttribute "RandomData" "S" "item 2"
                |> if ``table has sk``
                    then ItemBuilder.withAttribute "TableSk" "N" $"{tableSk2}"
                    else ItemBuilder.withAttribute "TablePk" "S" tablePk2

            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item1,
                ItemBuilder.dynamoDbAttributes item1)
            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item2,
                ItemBuilder.dynamoDbAttributes item2)

            let keys1 =
                let keyCols = if table.hasSk then ["TablePk"; "TableSk"] else ["TablePk"]
                ItemBuilder.dynamoDbAttributes item1
                |> Seq.filter (fun x -> List.contains x.Key keyCols)
                |> Enumerable.ToDictionary

            let req = DeleteItemRequest()
            req.TableName <- ItemBuilder.tableName item1
            req.Key <- keys1
            req.ReturnValues <- ReturnValue.ALL_OLD

            // act
            let! response = maybeBatch batch client req

            // assert
            let getItems filters =
                let table = client.GetTable table.name
                table.GetIndexes()
                |> Seq.cast<IDebugIndex>
                |> Collection.prepend table
                |> Seq.map (fun idx -> idx.GetValues filters |> List.ofSeq)
                |> List.ofSeq

            let shouldHave =
                if ``table has sk``
                    then [struct ("TablePk", String tablePk); struct ("TableSk", Number tableSk2)]
                    else [struct ("TablePk", String tablePk2)]
                |> getItems

            let shouldNotHave =
                if ``table has sk``
                    then [struct ("TablePk", String tablePk); struct ("TableSk", Number tableSk)]
                    else [struct ("TablePk", String tablePk)]
                |> getItems
                |> List.concat

            let assertShouldHave indexValues =
                Assert.Collection(indexValues, (fun (x: DebugItem) ->
                    let data = Seq.filter (fstT >> ((=) "RandomData")) x |> Seq.head |> sndT
                    Assert.Equal(String "item 2", data)))

            Assert.Equal(0, List.length shouldNotHave)
            Assert.Collection(shouldHave, assertShouldHave, assertShouldHave)
            response
            ?|> (fun r -> Assert.True(r.Attributes.Count > 2))
            |> ValueOption.defaultValue ()

            // random double dispose of host
            host.Dispose()
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Delete item, with various condition expression scenarios`` ``item exists`` ``condition matches`` rev = // rev is a test for the ll2 compiler
        
        let conditionTargetsSome = ``item exists`` = ``condition matches``
        let success = ``item exists`` = conditionTargetsSome
        
        task {
            use writer = new TestLogger(output)
            let writer = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)
            
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = InMemoryDynamoDbClient.Create(host)
            let table = Tables.get true true tables
            let struct (pk, struct (sk, item)) = randomItem table.hasSk random
            
            let pk =
                if ``item exists``
                then pk
                else $"override{uniqueId()}"
            
            let itemOverride =
                Map.add "TablePk" (Model.AttributeValue.String pk) item

            let keys =
                let keyCols = if table.hasSk then ["TablePk"; "TableSk"] else ["TablePk"]
                itemOverride
                |> Map.filter (fun k _ -> List.contains k keyCols)
                |> itemToDynamoDb
                
            let expression =
                [
                    if conditionTargetsSome then "attribute_exists(#attr)" else "attribute_not_exists(#attr)" 
                    "TablePk <> :v"
                ]
                |> if rev then List.rev else id
                |> Str.join " AND "
                
            let req = DeleteItemRequest()
            req.TableName <- table.name
            req.Key <- keys
            req.ConditionExpression <- expression
            req.ExpressionAttributeNames <-
                Map.add "#attr" "TablePk" Map.empty
                |> CSharp.toDictionary id id
            req.ExpressionAttributeValues <-
                Map.add ":v" (Model.AttributeValue.String "XX") Map.empty
                |> itemToDynamoDb
            req.ReturnValues <- ReturnValue.ALL_OLD

            // act
            if success
            then
                let! response = client.DeleteItemAsync(req)
                if ``item exists``
                then
                    Assert.Equal(pk, response.Attributes["TablePk"].S)
                    Assert.Equal(sk, response.Attributes["TableSk"].N |> decimal)
                else Assert.Empty(response.Attributes)
                
                let! x = client.GetItemAsync(table.name, keys)
                Assert.Empty(x.Item)
            else
                let! e = Assert.ThrowsAnyAsync(fun _ -> client.DeleteItemAsync(req))
                assertError output "ConditionalCheckFailedException" e

                let! x = client.GetItemAsync(table.name, keys)
                if ``item exists``
                then Assert.NotEmpty(x.Item)
                else Assert.True(x.Item.Count = 0)
        }

    [<Theory>]
    [<InlineData(true, false, false)>]
    [<InlineData(true, true, false)>]
    [<InlineData(true, false, true)>]
    [<InlineData(false, false, false)>]
    [<InlineData(false, true, true)>]
    [<InlineData(false, false, true)>]
    let ``Delete item, with invalid keys, throws error`` ``table has sk`` ``include pk`` ``include sk`` =

        task {
            // arrange
            use writer = new TestLogger(output)
            let writer = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = InMemoryDynamoDbClient.Create(host)
            let table = Tables.get ``table has sk`` true tables

            let tablePk = $"{IncrementingId.next()}"
            let tableSk = IncrementingId.next()

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

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> client.DeleteItemAsync(
                ItemBuilder.tableName item,
                keys))

            match ``table has sk``, ``include pk``, ``include sk`` with
            | false, true, true -> assertError output "Found non key attributes" e
            |_ -> assertError output "Could not find" e
        }
