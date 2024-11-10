
namespace TestDynamo.Tests

open System
open Amazon.DynamoDBv2.Model
open TestDynamo
open Tests.Items
open Tests.Requests.Queries
open Tests.Table
open Tests.Utils
open Xunit
open Amazon.DynamoDBv2
open Xunit.Abstractions
open TestDynamo.Utils

type CreateTableTests(output: ITestOutputHelper) =

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

    let localIndexQuery tableName indexName (client: IAmazonDynamoDB) =
        task {
            let q1 =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tableName
                |> QueryBuilder.setIndexName indexName
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND IndexSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":s" (ItemBuilder.buildItemAttribute "S" "UU")
                |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute "S" "x")
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

    let localIndexAssert projectAttributes (result: QueryResponse) =
        // assert
        let item = Assert.Single result.Items
        Assert.Equal("x", item["TablePk"].S)

        if projectAttributes then
            Assert.Equal("Else", item["Something"].S)
            Assert.Equal("Else2", item["AnotherThing"].S)
            Assert.Equal("8", item["TableSk"].N)

        Assert.Equal("UU", item["IndexSk"].S)

        let k = if projectAttributes then 6 else 2
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
        |%|> basicAssert

    let indexAddAndAssert tableName indexName hasSortKey projectAttributes client =
        basicAdd tableName client
        %>>= fun _ -> indexQuery tableName indexName hasSortKey client
        |%|> indexAssert projectAttributes hasSortKey

    let localIndexAddAndAssert tableName indexName projectAttributes client =
        basicAdd tableName client
        %>>= fun _ -> localIndexQuery tableName indexName client
        |%|>localIndexAssert projectAttributes

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Create table, partition and sort keys, creates successfully`` (simpleInput: bool) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "TableSk" "N"
                |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")

            // act
            let! _ = createTable client table simpleInput

            // assert
            do! basicAddAndAssert tableName true client

            // random double dispose of client
            client.Dispose()
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Create table, partition keys only, creates successfully`` (simpleInput: bool) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withKeySchema "TablePk" ValueNone

            // act
            let! response = createTable client table simpleInput

            // assert
            do! basicAddAndAssert tableName false client
            Assert.Equal($"arn:aws:dynamodb:default-region:123456789012:table/{tableName}", response.TableDescription.TableArn)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Create table, missing key attribute, throws exception`` (missingSortKey: bool, simpleInput: bool) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> if not missingSortKey then TableBuilder.withAttribute "TableSk" "N" else id
                |> if missingSortKey then TableBuilder.withAttribute "TablePk" "S" else id
                |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")

            // act
            let! e = Assert.ThrowsAnyAsync(fun () -> createTable client table simpleInput)

            // assert
            assertError output "Cannot find key attribute" e
            assertNoTable tableName
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Create table, superfluous attribute, throws exception`` (simpleInput: bool) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "TableSk" "N"
                |> TableBuilder.withAttribute "TooMany" "S"
                |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> createTable client table simpleInput)

            // assert
            assertError output "TooMany" e
            assertNoTable tableName
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Create table, 2 key attributes the same, throws exception`` (simpleInput: bool) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withKeySchema "TablePk" (ValueSome "TablePk")

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> createTable client table simpleInput)

            // assert
            assertError output "Cannot use the a single attribute for partition and sort keys" e
            assertNoTable tableName
        }

    [<Fact>]
    let ``Create table, 2 index attributes the same, throws exception`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "IndexPk" "S"
                |> TableBuilder.withKeySchema "TablePk" ValueNone
                |> TableBuilder.withSimpleGsi "Ix" "IndexPk" (ValueSome "IndexPk") true

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> createTable client table false)

            // assert
            assertError output "Cannot use the a single attribute for partition and sort keys \"IndexPk\"" e
            assertNoTable tableName
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Create table, GSIs, creates successfully`` (hasSortKey: bool, projectAttributes: bool) =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let sortKey = if hasSortKey then ValueSome "IndexSk" else ValueNone

            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName

                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> (ValueOption.map (flip TableBuilder.withAttribute "S") sortKey |> ValueOption.defaultValue id)

                |> TableBuilder.withKeySchema "TablePk" ValueNone
                |> TableBuilder.withSimpleGsi "Idx1" "IndexPk" sortKey projectAttributes

            // act
            let! response = createTable client table false

            // assert
            Assert.Equal(1, response.TableDescription.GlobalSecondaryIndexes.Count)
            Assert.Equal(0, response.TableDescription.LocalSecondaryIndexes.Count)
            do! indexAddAndAssert tableName "Idx1" hasSortKey projectAttributes client
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Create table, LSIs, creates successfully`` projectAttributes =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName

                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "IndexSk" "S"

                |> TableBuilder.withKeySchema "TablePk" ValueNone
                |> TableBuilder.withSimpleLsi "Idx1" "TablePk" (ValueSome "IndexSk") projectAttributes

            // act
            let! response = createTable client table false

            // assert
            Assert.Equal(1, response.TableDescription.LocalSecondaryIndexes.Count)
            Assert.Equal(0, response.TableDescription.GlobalSecondaryIndexes.Count)
            do! localIndexAddAndAssert tableName "Idx1" projectAttributes client
        }

    [<Fact>]
    let ``Create table, LSI with no sort key, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let! e =
                Assert.ThrowsAnyAsync(fun _ ->
                    TableBuilder.empty
                    |> TableBuilder.withTableName tableName

                    |> TableBuilder.withAttribute "TablePk" "S"

                    |> TableBuilder.withKeySchema "TablePk" ValueNone
                    |> TableBuilder.withSimpleLsi "Idx1" "TablePk" ValueNone false
                    |> flip (createTable client) false
                    |> Io.ignoreTask)

            // assert
            assertError output "Sort key is mandatory for local secondary index" e
        }

    [<Fact>]
    let ``Create table, LSI with invalid partition key, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let tableName = (Guid.NewGuid()).ToString()
            let! e =
                Assert.ThrowsAnyAsync(fun _ ->
                    TableBuilder.empty
                    |> TableBuilder.withTableName tableName

                    |> TableBuilder.withAttribute "TablePk" "S"
                    |> TableBuilder.withAttribute "IndexPk" "N"
                    |> TableBuilder.withAttribute "IndexSk" "S"

                    |> TableBuilder.withKeySchema "TablePk" ValueNone
                    |> TableBuilder.withSimpleLsi "Idx1" "IndexPk" (ValueSome "IndexSk") false
                    |> flip (createTable client) false
                    |> Io.ignoreTask)

            // assert
            assertError output "Partition key must be the same as table partition key for local secondary index" e
        }

    [<Theory>]
    [<InlineData("Local")>]
    [<InlineData("NoSk")>]
    [<InlineData("Global")>]
    let ``Create table, with custom index projections, creates successfully`` indexType =

        let struct (hasSortKey, isLocal) =
            match indexType with
            | "Global" -> struct (true, false)
            | "NoSk" -> struct (false, false)
            | "Local" -> struct (true, true)
            | x -> invalidOp x

        let client = buildClient output

        let client = client.Client
        task {
            // arrange
            let sortKey = if hasSortKey then ValueSome "IndexSk" else ValueNone

            let tableName = (Guid.NewGuid()).ToString()
            let table =
                TableBuilder.empty
                |> TableBuilder.withTableName tableName

                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "TableSk" "N"
                |> if isLocal
                    then id
                    else TableBuilder.withAttribute "IndexPk" "N"
                |> (ValueOption.map (flip TableBuilder.withAttribute "S") sortKey |> ValueOption.defaultValue id)

                |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")
                |> if isLocal
                    then TableBuilder.withComplexLsi "Idx1" "TablePk" sortKey ["AnotherThing"]
                    else TableBuilder.withComplexGsi "Idx1" "IndexPk" sortKey ["AnotherThing"]

            // act
            let! response = createTable client table false

            // assert
            Assert.Equal(
                struct (ProjectionType.INCLUDE.Value, ["AnotherThing"]),
                response.TableDescription.GlobalSecondaryIndexes
                |> Seq.filter (_.IndexName >> ((=)"Idx1"))
                |> Seq.map (fun x -> struct (x.Projection.ProjectionType.Value, List.ofSeq x.Projection.NonKeyAttributes))
                |> Collection.concat2 (
                    response.TableDescription.LocalSecondaryIndexes
                    |> Seq.filter (_.IndexName >> ((=)"Idx1"))
                    |> Seq.map (fun x -> struct (x.Projection.ProjectionType.Value, List.ofSeq x.Projection.NonKeyAttributes)))
                |> Seq.head)

            let! result =
                basicAdd tableName client
                %>>= fun _ ->
                    if isLocal
                    then localIndexQuery tableName "Idx1" client
                    else indexQuery tableName "Idx1" hasSortKey client

            let item = Assert.Single result.Items
            if not isLocal then Assert.Equal("55", item["IndexPk"].N)
            if hasSortKey then Assert.Equal("UU", item["IndexSk"].S)

            Assert.Equal("x", item["TablePk"].S)
            Assert.Equal("8", item["TableSk"].N)
            Assert.Equal("Else2", item["AnotherThing"].S)

            let c = if hasSortKey && not isLocal then 5 else 4
            Assert.Equal(c, item.Count)
        }
