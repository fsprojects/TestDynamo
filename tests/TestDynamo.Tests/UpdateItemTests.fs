
namespace TestDynamo.Tests

open System.Text
open System.Text.Json
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Client
open TestDynamo.Model
open Tests.Items
open Tests.Loggers
open Tests.Requests.Queries
open Tests.Table
open Tests.Utils
open Xunit
open Amazon.DynamoDBv2
open Xunit.Abstractions
open TestDynamo.Utils
open TestDynamo.Client.ItemMapper
open TestDynamo.Tests.RequestItemTestUtils

type UpdateSameAttributeFlags () =
    inherit EightFlags(UpdateSameAttributeFlags.filter)

    with
    static member private filter x =
        UpdateSameAttributeFlags.exactly2Flags x && UpdateSameAttributeFlags.secondFlagIsOnlyTriggeredWithFirst x

    static member private exactly2Flags = Seq.filter id >> Seq.length >> (=)2

    static member private secondFlagIsOnlyTriggeredWithFirst =

        Collection.window 2
        >> Seq.map List.ofSeq
        >> Seq.forall (function
            | [false; true] -> false
            | _ -> true)

type UpdateModifyKeyAttributeFlags() =
    inherit FourOneHotFlags([|0|])

type UpdateFailureType =
    | None = 1
    | TooManyExpressionAttributeNames = 2
    | TooManyExpressionAttributeValues = 3
    | MissingExpressionAttributeName = 4
    | MissingExpressionAttributeValue = 5

type UpdateFailureTypeCases() =
    inherit EnumValues<UpdateFailureType>()

type UpdateItemTests(output: ITestOutputHelper) =

    let random = randomBuilder output

    static let table =
        let client = buildClient (OutputCollector() |> ValueSome)

        let t = sprintf "%s%i" (nameof UpdateItemTests) (uniqueId())
        let req =
            TableBuilder.empty
            |> TableBuilder.withTableName t
            |> TableBuilder.withAttribute "TablePk" "N"
            |> TableBuilder.withAttribute "TableSk" "N"
            |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")

        let table = TableBuilder.req req |> client.CreateTableAsync
        task {
            try
                // act
                let! _ = table
                return t
            finally
                client.Dispose()
        }

    let put query (client: IAmazonDynamoDB) =
        task {
            let! t = table
            let q = ItemBuilder.dynamoDbAttributes query
            do!
                client.PutItemAsync(t, q)
                |> Io.ignoreTask

            let queryBase =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName t
                |> QueryBuilder.setUpdateKey (
                    Map.add "TablePk" (AttributeValue.Number ((q["TablePk"]).N |> decimal)) Map.empty
                    |> Map.add "TableSk" (AttributeValue.Number ((q["TableSk"]).N |> decimal)))

            return queryBase
        }

    let updateRaw query (client: IAmazonDynamoDB) =
        task {
            let! t = table
            do!
                client.UpdateItemAsync query
                |> Io.ignoreTask

            let! x = client.GetItemAsync(t, query.Key)
            return x.Item |> itemFromDynamodb "$"
        }

    let update = QueryBuilder.updateRequest >> updateRaw

    let updateExpectErrorAndAssertNotModified query (client: IAmazonDynamoDB) =
        task {
            let! t = table
            let q = QueryBuilder.updateRequest query
            let! before = client.GetItemAsync(t, q.Key)

            let! e = Assert.ThrowsAnyAsync (fun _ ->
                client.UpdateItemAsync q
                |> Io.ignoreTask)

            let! after = client.GetItemAsync(t, q.Key)
            Assert.Equal<Map<string, AttributeValue>>(after.Item |> itemFromDynamodb "$", before.Item |> itemFromDynamodb "$")

            return e
        }

    let expectHashMap = function
        | HashMap x -> x
        | _ -> invalidOp "expeccted hash map"

    let expectList = function
        | AttributeList (CompressedList x) -> x
        | _ -> invalidOp "expeccted list"

    [<Fact>]
    let ``Update, smoke tests`` () =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "RemoveMe" "S" "rem me"
                |> ItemBuilder.withAttribute "AddMe" "N" "5"
                |> ItemBuilder.withAttribute "DeleteMe" "SS" "[\"X\"]"

            let! updateBase = put req client
            let xVal = AttributeValue.String "1234"

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET ((SetMe) = (:set)) REMOVE (RemoveMe) ADD (AddMe) (:add) DELETE (DeleteMe) (:del)"
                |> QueryBuilder.setExpressionAttrValues ":set" xVal
                |> QueryBuilder.setExpressionAttrValues ":add" (AttributeValue.Number 1M)
                |> QueryBuilder.setExpressionAttrValues ":del" (AttributeSet.fromStrings [AttributeValue.String "Y"] |> AttributeValue.HashSet)
                |> flip update client

            // assert
            let expected =
                Map.add "TablePk" (AttributeValue.Number (pk |> decimal)) Map.empty
                |> Map.add "TableSk" (AttributeValue.Number (sk |> decimal))
                |> Map.add "SetMe" xVal
                |> Map.add "AddMe" (AttributeValue.Number 6M) 
                |> Map.add "DeleteMe" (AttributeSet.fromStrings [AttributeValue.String "X"] |> AttributeValue.HashSet)

            Assert.Equal<Map<string, AttributeValue>>(expected, itemAfterUpdate)
        }

    [<Theory>]
    [<InlineData("SET")>]
    [<InlineData("REMOVE")>]
    [<InlineData("ADD")>]
    [<InlineData("DELETE")>]
    let ``Update, with reserved update verbs as properties, behaves correctly`` verb =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "RemoveMe" "S" "rem me"
                |> ItemBuilder.withAttribute "AddMe" "N" "5"
                |> ItemBuilder.withAttribute "DeleteMe" "SS" "[\"X\"]"

            let! updateBase = put req client
            let xVal = AttributeValue.String "1234"

            // act
            // assert
            let! e = 
                updateBase
                |> QueryBuilder.setUpdateExpression $"SET {verb} = :set"
                |> QueryBuilder.setExpressionAttrValues $":set" xVal
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "SET expression must have at least 1 clause" e
        }

    [<Fact>]
    let ``Update, AttributeUpdates, smoke tests`` () =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "AddMe" "N" "5"
                |> ItemBuilder.withAttribute "DeleteMe" "SS" "[\"X\"]"

            let setVal = AttributeValue.String "1234"
            let addVal = AttributeValue.Number 1M
            let deleteVal = AttributeSet.fromStrings [AttributeValue.String "Y"] |> AttributeValue.HashSet
            let! updateBase = put req client

            let updateReq =
                let ub = QueryBuilder.updateRequest updateBase
                ub.AttributeUpdates <- Dictionary()
                let add action name value =
                    ub.AttributeUpdates.Add(
                        name,
                        let u = AttributeValueUpdate()
                        u.Action <- AttributeAction.FindValue(action)
                        u.Value <- attributeToDynamoDb value
                        u)

                add "PUT" "SetMe" setVal
                add "ADD" "AddMe" addVal
                add "DELETE" "DeleteMe" deleteVal

                ub

            // act
            let! itemAfterUpdate = updateRaw updateReq client

            // assert
            let expected =
                Map.add "TablePk" (AttributeValue.Number (pk |> decimal)) Map.empty
                |> Map.add "TableSk" (AttributeValue.Number (sk |> decimal))
                |> Map.add "SetMe" setVal
                |> Map.add "AddMe" (AttributeValue.Number 6M) 
                |> Map.add "DeleteMe" (AttributeSet.fromStrings [AttributeValue.String "X"] |> AttributeValue.HashSet)

            Assert.Equal<Map<string, AttributeValue>>(expected, itemAfterUpdate)
        }

    [<Theory>]
    [<ClassData(typedefof<FourOneHotFlags>)>]
    let ``Update, clause with no values, throws`` ``set missing`` ``add missing`` ``remove missing`` ``delete missing`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "RemoveMe" "S" "rem me"
                |> ItemBuilder.withAttribute "AddMe" "N" "5"
                |> ItemBuilder.withAttribute "DeleteMe" "SS" "[\"X\"]"

            let! updateBase = put req client
            let xVal = AttributeValue.String "1234"

            let expression =
                [
                    if ``set missing`` then "SET"
                    else "SET SetMe = :set"
                    if ``add missing`` then "ADD"
                    else "ADD AddMe :add"
                    if ``remove missing`` then "REMOVE"
                    else "REMOVE RemoveMe"
                    if ``delete missing`` then "DELETE"
                    else "DELETE DeleteMe :del"
                ] |> RequestItemTestUtils.randomSort random
                |> Str.join " " 

            // act
            // assert
            let! err =
                updateBase
                |> QueryBuilder.setUpdateExpression expression
                |> if ``set missing`` then id else QueryBuilder.setExpressionAttrValues ":set" xVal
                |> if ``add missing`` then id else QueryBuilder.setExpressionAttrValues ":add" (AttributeValue.Number 1M)
                |> if ``delete missing`` then id else QueryBuilder.setExpressionAttrValues ":del" (AttributeSet.fromStrings [AttributeValue.String "Y"] |> AttributeValue.HashSet)
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "Error compiling expression" err
        }

    [<Fact>]
    let ``Update, item does not exists, puts item`` () =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let! t = table
            let req =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName t
                |> QueryBuilder.setUpdateKey (
                    Map.add "TablePk" (AttributeValue.Number (uniqueId() |> decimal)) Map.empty
                    |> Map.add "TableSk" (AttributeValue.Number (uniqueId() |> decimal)))
                |> QueryBuilder.setUpdateExpression "SET XX = :x"
                |> QueryBuilder.setExpressionAttrValues ":x" (AttributeValue.Number 1M)
                |> QueryBuilder.updateRequest

            // act
            do!
                req
                |> client.UpdateItemAsync
                |> Io.ignoreTask

            // assert
            let! after = client.GetItemAsync(t, req.Key)
            Assert.Equal("1", after.Item["XX"].N)
        }

    [<Fact>]
    let ``Update, invalid query, throws`` () =
        task {
            use client = buildClient (ValueSome output)

            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk

            let! updateBase = put req client

            // act
            let! e =
                updateBase
                |> QueryBuilder.setUpdateExpression "x = :x"
                |> QueryBuilder.setExpressionAttrValues ":x" (AttributeValue.String "1234")
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "is not a valid upate expression" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, SET, multiple nested values`` ``root has value`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if ``root has value``
                    then
                        ItemBuilder.withAttribute "xx" "M" """[["k", "S", "v1"]]"""
                        >> ItemBuilder.withAttribute "yy" "L" """[["S", "v2"]]"""
                    else id

            let! updateBase = put req client
            let xVal = AttributeValue.String "1234"
            let yVal = AttributeValue.String "4321"

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET xx.yy = :x, yy[3] = :y"
                |> QueryBuilder.setExpressionAttrValues ":x" xVal
                |> QueryBuilder.setExpressionAttrValues ":y" yVal
                |> flip update client

            // assert
            let xx =
                Map.add "yy" xVal Map.empty
                |> if ``root has value`` then Map.add "k" (AttributeValue.String "v1") else id
            let yy =
                [yVal]
                |> if ``root has value`` then Collection.prependL (AttributeValue.String "v2") else id

            Assert.Equal(4, Map.count itemAfterUpdate)
            Assert.Equal<Map<string,AttributeValue>>(Map.find "xx" itemAfterUpdate |> expectHashMap, xx)
            Assert.Equal(Map.find "yy" itemAfterUpdate |> expectList, yy)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, SET, with if_not_exists, sets correctly`` exists =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let attr1 = AttributeValue.String "1234"
            let attr2 = AttributeValue.String "4321"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if exists
                    then ItemBuilder.withAttribute "Attr" "S" "1234"
                    else id

            let! updateBase = put req client

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET Attr = if_not_exists(Attr, :x)"
                |> QueryBuilder.setExpressionAttrValues ":x" attr2
                |> flip update client

            // assert
            Assert.Equal(3, Map.count itemAfterUpdate)
            Assert.Equal(Map.find "Attr" itemAfterUpdate, if exists then attr1 else attr2)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, SET, with list_append, sets correctly`` exists =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let listData = [|AttributeValue.Number 4321M|]
            let data = AttributeValue.AttributeList (AttributeListType.CompressedList listData)

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if exists
                    then ItemBuilder.withAttribute "yy" "L" """[["S", "v0"]]"""
                    else id

            let! updateBase = put req client

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET Attr = list_append(yy, :x)"
                |> QueryBuilder.setExpressionAttrValues ":x" data
                |> flip update client

            // assert
            Assert.Equal((if exists then 4 else 3), Map.count itemAfterUpdate)
            Assert.Equal(
                Map.find "Attr" itemAfterUpdate,
                if exists then AttributeValue.AttributeList (AttributeListType.CompressedList (Array.concat [[|AttributeValue.String "v0"|]; listData;])) else data)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Update, SET, with list_append missing attributes, sets correctly`` ``1 exists`` ``2 exists`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if ``1 exists``
                    then ItemBuilder.withAttribute "l1" "L" """[["S", "v1"]]"""
                    else id
                |> if ``2 exists``
                    then ItemBuilder.withAttribute "l2" "L" """[["S", "v2"]]"""
                    else id

            let! updateBase = put req client

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET Attr = list_append(l1, l2)"
                |> flip update client

            // assert
            let struct (expectedCount, vals) =
                if ``1 exists`` && ``2 exists`` then struct (5, AttributeValue.AttributeList (AttributeListType.CompressedList [|AttributeValue.String "v1"; AttributeValue.String "v2"|]) |> ValueSome)
                elif ``1 exists`` then struct (4, AttributeValue.AttributeList (AttributeListType.CompressedList [|AttributeValue.String "v1"; |]) |> ValueSome)
                elif ``2 exists`` then struct (4, AttributeValue.AttributeList (AttributeListType.CompressedList [|AttributeValue.String "v2"|]) |> ValueSome)
                else struct (2, ValueNone)

            Assert.Equal(expectedCount, Map.count itemAfterUpdate)
            Assert.Equal(MapUtils.tryFind "Attr" itemAfterUpdate, vals)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, SET, with list_append non list, throws`` lIsNonList =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if lIsNonList
                    then
                        ItemBuilder.withAttribute "l1" "L" """[["S", "v1"]]"""
                        >> ItemBuilder.withAttribute "l2" "S" "xx"
                    else 
                        ItemBuilder.withAttribute "l2" "L" """[["S", "v1"]]"""
                        >> ItemBuilder.withAttribute "l1" "S" "xx"

            let! updateBase = put req client

            // act
            // assert
            let! e = 
                updateBase
                |> QueryBuilder.setUpdateExpression "SET Attr = list_append(l1, l2)"
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "Arguments to list_append function must be lists" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, SET, with +/-, sets correctly`` plus =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let xVal = AttributeValue.Number 100M

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Attr" "N" "5"
                |> ItemBuilder.withAttribute "Attr1" "N" "5"

            let! updateBase = put req client

            // act
            let! itemAfterUpdate =
                updateBase
                |> if plus
                    then QueryBuilder.setUpdateExpression "SET Attr = Attr + :x"
                    else QueryBuilder.setUpdateExpression "SET Attr = Attr1 - :x"
                |> QueryBuilder.setExpressionAttrValues ":x" xVal
                |> flip update client

            // assert
            Assert.Equal(4, Map.count itemAfterUpdate)
            Assert.Equal((if plus then AttributeValue.Number 105M else AttributeValue.Number -95M), Map.find "Attr" itemAfterUpdate)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, SET, combined arithmetic and if_not_exists`` exists =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let xVal = AttributeValue.Number 100M

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if exists then ItemBuilder.withAttribute "Attr" "N" "5" else id
                |> ItemBuilder.withAttribute "Attr1" "N" "10"

            let! updateBase = put req client

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET Attr = if_not_exists(Attr, Attr1) + :x"
                |> QueryBuilder.setExpressionAttrValues ":x" xVal
                |> flip update client

            // assert
            Assert.Equal(4, Map.count itemAfterUpdate)
            Assert.Equal((if exists then AttributeValue.Number 105M else AttributeValue.Number 110M), Map.find "Attr" itemAfterUpdate)
        }

    [<Fact>]
    let ``Update, SET, multiple clauses`` () =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let xVal = AttributeValue.Number 100M
            let yVal = AttributeValue.HashSet (AttributeSet.fromNumbers [AttributeValue.Number 20M])

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Attr1" "N" "10"
                |> ItemBuilder.withAttribute "Attr2" "S" "20"

            let! updateBase = put req client

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET Attr1 = :x, Attr2 = :y"
                |> QueryBuilder.setExpressionAttrValues ":x" xVal
                |> QueryBuilder.setExpressionAttrValues ":y" yVal
                |> flip update client

            // assert
            let expected =
                Map.add "TablePk" (AttributeValue.Number (pk |> decimal)) Map.empty
                |> Map.add "TableSk" (AttributeValue.Number (sk |> decimal))
                |> Map.add "Attr1" xVal 
                |> Map.add "Attr2" yVal 
            Assert.Equal<Map<string, AttributeValue>>(expected, itemAfterUpdate)
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Update, SET, with +/-, with invalid attr, throws`` plus add exists =
        if not plus && add then Io.retn () |> _.AsTask() |> Io.ignoreTask
        elif not exists && add then Io.retn () |> _.AsTask() |> Io.ignoreTask   // not an exception
        else

        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let xVal = AttributeValue.Number 100M

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if exists
                    then ItemBuilder.withAttribute "Attr" "S" "5"
                    else id

            let! updateBase = put req client

            // act
            let! e =
                updateBase
                |> if add then QueryBuilder.setUpdateExpression "ADD Attr :x"
                   elif plus then QueryBuilder.setUpdateExpression "SET Attr = Attr + :x"
                   else QueryBuilder.setUpdateExpression "SET Attr = Attr - :x"
                |> QueryBuilder.setExpressionAttrValues ":x" xVal
                |> flip updateExpectErrorAndAssertNotModified client

            // assert
            assertError output (if add then "Invalid ADD update expression" else "Both operands of a") e
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Update, SET, with +/-, with invalid value, throws`` plus add =
        if not plus && add then Io.retn () |> _.AsTask() |> Io.ignoreTask
        else

        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let xVal = AttributeValue.String "100M"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Attr" "N" "5"

            let! updateBase = put req client

            // act
            let! e =
                updateBase
                |> if add then QueryBuilder.setUpdateExpression "ADD Attr :x"
                   elif plus then QueryBuilder.setUpdateExpression "SET Attr = Attr + :x"
                   else QueryBuilder.setUpdateExpression "SET Attr = Attr - :x"
                |> QueryBuilder.setExpressionAttrValues ":x" xVal
                |> flip updateExpectErrorAndAssertNotModified client

            // assert
            assertError output (if add then "Invalid ADD update expression" else "Both operands of a") e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, REMOVE, multiple nested values`` ``root has value`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if ``root has value``
                    then
                        ItemBuilder.withAttribute "xx" "M" """[["yy", "S", "v1"]]"""
                        >> ItemBuilder.withAttribute "yy" "L" """[["S", "v0"], ["S", "v1"], ["S", "v2"]]"""
                    else id

            let! updateBase = put req client

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "REMOVE xx.yy, yy[1]"
                |> flip update client

            // assert
            if not ``root has value``
            then Assert.Equal(2, Map.count itemAfterUpdate)
            else
                let yy = [AttributeValue.String "v0"; AttributeValue.String "v2"]
                Assert.Equal(4, Map.count itemAfterUpdate)
                Assert.Equal<Map<string,AttributeValue>>(Map.find "xx" itemAfterUpdate |> expectHashMap, Map.empty)
                Assert.Equal(Map.find "yy" itemAfterUpdate |> expectList, yy)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, ADD numbers, sets correctly`` ``attr exists`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            let xVal = AttributeValue.Number 100M

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> if ``attr exists`` then ItemBuilder.withAttribute "Attr" "N" "5" else id

            let! updateBase = put req client

            // act
            let! itemAfterUpdate =
                updateBase
                |> QueryBuilder.setUpdateExpression "ADD Attr :x"
                |> QueryBuilder.setExpressionAttrValues ":x" xVal
                |> flip update client

            // assert
            Assert.Equal(3, Map.count itemAfterUpdate)
            Assert.Equal(AttributeValue.Number (if ``attr exists`` then 105M else 100M), Map.find "Attr" itemAfterUpdate)
        }

    [<Theory>]
    [<InlineData("All")>]
    [<InlineData("String")>]
    [<InlineData("Number")>]
    [<InlineData("Binary")>]
    let ``Update, ADD sets, processes correctly`` ``set type`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let aBin = Encoding.UTF8.GetBytes("a")
            let bBin = Encoding.UTF8.GetBytes("b")
            let bsSet = AttributeSet.fromBinary [AttributeValue.Binary aBin; AttributeValue.Binary [|5uy|]]
            let ssSet = AttributeSet.fromStrings [AttributeValue.String "a"; AttributeValue.String "c"]
            let nsSet = AttributeSet.fromNumbers [AttributeValue.Number 1M; AttributeValue.Number 3M]

            let bs = bsSet |> HashSet
            let ss = ssSet |> HashSet
            let ns = nsSet |> HashSet

            let ssExpected =
                ssSet
                |> tpl (AttributeSet.fromStrings [AttributeValue.String "b"])
                |> AttributeSet.tryUnion
                |> Maybe.expectSome
                |> HashSet

            let nsExpected =
                nsSet
                |> tpl (AttributeSet.fromNumbers [AttributeValue.Number 2M])
                |> AttributeSet.tryUnion
                |> Maybe.expectSome
                |> HashSet

            let bsExpected =
                bsSet
                |> tpl (AttributeSet.fromBinary [AttributeValue.Binary bBin])
                |> AttributeSet.tryUnion
                |> Maybe.expectSome
                |> HashSet

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Strings" "SS" "[\"a\", \"b\"]"
                |> ItemBuilder.withAttribute "Numbers" "NS" "[\"1\", \"2\"]"
                |> ItemBuilder.withAttribute "Binaries" "BS" "[\"a\", \"b\"]"

            let! updateBase = put req client

            let addStrings =
                QueryBuilder.setUpdateExpression "ADD Strings :str"
                >> QueryBuilder.setExpressionAttrValues ":str" ss
            let addNumbers =
                QueryBuilder.setUpdateExpression "ADD Numbers :num"
                >> QueryBuilder.setExpressionAttrValues ":num" ns
            let addBin =
                QueryBuilder.setUpdateExpression "ADD Binaries :bin"
                >> QueryBuilder.setExpressionAttrValues ":bin" bs
            let addAll =
                addStrings >> addNumbers >> addBin
                >> QueryBuilder.setUpdateExpression "ADD Binaries :bin, Strings :str,Numbers :num"

            // act
            let! itemAfterUpdate =
                updateBase
                |>
                    match ``set type`` with
                    | "String" -> addStrings
                    | "Number" -> addNumbers
                    | "Binary" -> addBin
                    | "All" -> addAll
                    | _ -> invalidOp ""
                |> flip update client

            // assert
            let assertString _ = Assert.Equal(ssExpected, Map.find "Strings" itemAfterUpdate)
            let assertNumber _ = Assert.Equal(nsExpected, Map.find "Numbers" itemAfterUpdate)
            let assertBin _ = Assert.Equal(bsExpected, Map.find "Binaries" itemAfterUpdate)

            match ``set type`` with
            | "String" -> assertString()
            | "Number" -> assertNumber()
            | "Binary" -> assertBin()
            | "All" ->
                assertString()
                assertNumber()
                assertBin()
            | _ -> invalidOp ""
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, ADD not top level attr, throws`` ``is number`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let ssSet = AttributeSet.fromStrings [AttributeValue.String "a"; AttributeValue.String "c"]
            let ss = ssSet |> HashSet

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Nested1" "L" (JsonSerializer.Serialize([["SS"; "[\"X\"]"]]))
                |> ItemBuilder.withAttribute "Nested2" "M" (JsonSerializer.Serialize([["TheNumber"; "N"; "4"]]))

            let! updateBase = put req client

            let addSet =
                QueryBuilder.setUpdateExpression "ADD Nested1[0] :str"
                >> QueryBuilder.setExpressionAttrValues ":str" ss
            let addNumber =
                QueryBuilder.setUpdateExpression "ADD Nested2.TheNumber :num"
                >> QueryBuilder.setExpressionAttrValues ":num" (AttributeValue.Number 1M)

            // act
            // assert
            let! e = 
                updateBase
                |> if ``is number``
                    then addNumber
                    else addSet
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "ADD update expression can only be used on top-level attributes, not nested attributes" e
        }

    [<Theory>]
    [<ClassData(typedefof<UpdateModifyKeyAttributeFlags>)>]
    let ``Update, try modify key attribute, throws`` ``is pk`` set remove add =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Val" "S" "X"

            let! updateBase = put req client

            let updateProp = if ``is pk`` then "TablePk" else "TableSk"
            let struct (updateExpr, requiresValue) =
                if set then struct ($"SET {updateProp} = :num", true)
                elif remove then struct ($"REMOVE {updateProp}", false)
                elif add then struct ($"ADD {updateProp} :num", true)
                else invalidOp "Invalid test inputs"

            // act
            // assert
            let! e = 
                updateBase
                |> QueryBuilder.setUpdateExpression updateExpr
                |> if requiresValue
                    then QueryBuilder.setExpressionAttrValues ":num" (AttributeValue.Number 77M)
                    else id
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "Key attributes cannot be updated" e
            assertError output updateProp e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, DELETE not top level attr, throws`` ``is nested list`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let ssSet = AttributeSet.fromStrings [AttributeValue.String "a"; AttributeValue.String "c"]
            let ss = ssSet |> HashSet

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Nested1" "L" (JsonSerializer.Serialize([["SS"; "[\"X\"]"]]))
                |> ItemBuilder.withAttribute "Nested2" "M" (JsonSerializer.Serialize([["TheSet"; "SS"; "[\"X\"]"]]))

            let! updateBase = put req client

            // act
            // assert
            let! e = 
                updateBase
                |> if ``is nested list``
                    then QueryBuilder.setUpdateExpression "DELETE Nested1[0] :str"
                    else QueryBuilder.setUpdateExpression "DELETE Nested2.TheSet :str"
                |> QueryBuilder.setExpressionAttrValues ":str" ss
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "DELETE update expression can only be used on top-level attributes, not nested attributes" e
        }

    [<Theory>]
    [<InlineData("All")>]
    [<InlineData("String")>]
    [<InlineData("Number")>]
    [<InlineData("Binary")>]
    let ``Update, DELETE, processes correctly`` ``set type`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let aBin = Encoding.UTF8.GetBytes("a")
            let bBin = Encoding.UTF8.GetBytes("b")
            let bsSet = AttributeSet.fromBinary [AttributeValue.Binary aBin; AttributeValue.Binary [|5uy|]]
            let ssSet = AttributeSet.fromStrings [AttributeValue.String "a"; AttributeValue.String "c"]
            let nsSet = AttributeSet.fromNumbers [AttributeValue.Number 1M; AttributeValue.Number 3M]

            let bs = bsSet |> HashSet
            let ss = ssSet |> HashSet
            let ns = nsSet |> HashSet

            let ssExpected =
                AttributeSet.fromStrings [AttributeValue.String "b"] |> HashSet
            let nsExpected =
                AttributeSet.fromNumbers [AttributeValue.Number 2M] |> HashSet
            let bsExpected =
                AttributeSet.fromBinary [AttributeValue.Binary bBin] |> HashSet

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Strings" "SS" "[\"a\", \"b\"]"
                |> ItemBuilder.withAttribute "Numbers" "NS" "[\"1\", \"2\"]"
                |> ItemBuilder.withAttribute "Binaries" "BS" "[\"a\", \"b\"]"

            let! updateBase = put req client

            let deleteStrings =
                QueryBuilder.setUpdateExpression "DELETE Strings :str"
                >> QueryBuilder.setExpressionAttrValues ":str" ss
            let deleteNumbers =
                QueryBuilder.setUpdateExpression "DELETE Numbers :num"
                >> QueryBuilder.setExpressionAttrValues ":num" ns
            let deleteBin =
                QueryBuilder.setUpdateExpression "DELETE Binaries :bin"
                >> QueryBuilder.setExpressionAttrValues ":bin" bs
            let deleteAll =
                deleteStrings >> deleteNumbers >> deleteBin
                >> QueryBuilder.setUpdateExpression "DELETE Binaries :bin, Strings :str,Numbers :num"

            // act
            let! itemAfterUpdate =
                updateBase
                |>
                    match ``set type`` with
                    | "String" -> deleteStrings
                    | "Number" -> deleteNumbers
                    | "Binary" -> deleteBin
                    | "All" -> deleteAll
                    | _ -> invalidOp ""
                |> flip update client

            let assertString _ = Assert.Equal(ssExpected, Map.find "Strings" itemAfterUpdate)
            let assertNumber _ = Assert.Equal(nsExpected, Map.find "Numbers" itemAfterUpdate)
            let assertBinary _ = Assert.Equal(bsExpected, Map.find "Binaries" itemAfterUpdate)

            // assert
            match ``set type`` with
            | "String" -> assertString()
            | "Number" -> assertNumber()
            | "Binary" -> assertBinary()
            | "All" -> 
                assertString()
                assertNumber()
                assertBinary()
            | _ -> invalidOp ""
        }

    [<Theory>]
    [<ClassData(typedefof<UpdateSameAttributeFlags>)>]
    let ``Update, same attribute twice, throws`` ``set 1`` ``set 2`` ``remove 1`` ``remove 2`` ``delete 1`` ``delete 2`` ``add 1`` ``add 2`` =

        let asInt = function | true -> 1 | false -> 0

        let totalSets = asInt ``set 1`` + asInt ``set 2``
        let totalAdds = asInt ``add 1`` + asInt ``add 2``
        let totalRemoves = asInt ``remove 1`` + asInt ``remove 2``
        let totalDeletes = asInt ``delete 1`` + asInt ``delete 2``

        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let expr =
                [ struct ("SET", [1..totalSets] |> List.map (asLazy "X = :val") |> Str.join ",")
                  struct ("ADD", [1..totalAdds] |> List.map (asLazy "X :val") |> Str.join ",")
                  struct ("REMOVE", [1..totalRemoves] |> List.map (asLazy "X") |> Str.join ",")
                  struct ("DELETE", [1..totalDeletes] |> List.map (asLazy "X :val") |> Str.join ",") ]
                |> Seq.map (function
                    | struct (_, "") -> ValueNone
                    | x, y -> ValueSome $"{x} {y}")
                |> Maybe.traverse
                |> RequestItemTestUtils.randomSort random
                |> Str.join " "

            let ss =
                AttributeSet.fromStrings [AttributeValue.String "a"; AttributeValue.String "c"]
                |> HashSet

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "X" "SS" "[\"a\", \"b\"]"

            let! updateBase = put req client

            // act
            // assert
            let! e = 
                updateBase
                |> QueryBuilder.setUpdateExpression expr
                |> QueryBuilder.setExpressionAttrValues ":val" ss
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "An attribute or attribute path was updated or projected multiple times" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, sibling attributes updated twice but nested, updates correctly`` ``nested list`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let x = AttributeValue.Number 22M

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk

            let! updateBase = put req client

            // act
            let! result = 
                updateBase
                |> if ``nested list``
                    then QueryBuilder.setUpdateExpression $"SET P[0] = :x, P[1] = :x"
                    else QueryBuilder.setUpdateExpression $"SET P.X = :x, P.Y = :x"
                |> QueryBuilder.setExpressionAttrValues ":x" x
                |> flip update client

            // assert
            let expected =
                Map.add "TablePk" (AttributeValue.Number (pk |> decimal)) Map.empty
                |> Map.add "TableSk" (AttributeValue.Number (sk |> decimal))
                |> if ``nested list``
                    then Map.add "P" ([|x; x|] |> CompressedList |> AttributeValue.AttributeList)
                    else Map.add "P" ([struct ("X", x); struct ("Y", x)] |> MapUtils.ofSeq |> AttributeValue.HashMap)

            Assert.Equal<Map<string, AttributeValue>>(expected, result)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Update, super deep nesting, updates correctly`` ``nested list`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let x = AttributeValue.Number 22M

            let listAttr =
                [|
                    Map.empty
                    |> Map.add "V1" (
                        [|
                            AttributeValue.String "hi2"

                            Map.empty
                            |> Map.add "V2" (AttributeValue.String "hi")
                            |> HashMap
                        |]
                        |> CompressedList
                        |> AttributeList
                    )
                    |> Map.add "V11" (AttributeValue.String "hi")
                    |> HashMap
                |]
                |> CompressedList
                |> AttributeList

            let mapAttr =
                Map.empty
                |> Map.add "V0" listAttr
                |> HashMap

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withRawAttribute "TheList" listAttr
                |> ItemBuilder.withRawAttribute "TheMap" mapAttr

            let! updateBase = put req client

            // act
            let! result = 
                updateBase
                |> if ``nested list``
                    then QueryBuilder.setUpdateExpression "SET TheList[0].V1[1].V2 = :x REMOVE TheList[0].V1[0]"
                    else QueryBuilder.setUpdateExpression "SET TheMap.V0[0].V1[1] = :x REMOVE TheMap.V0[0].V11"
                |> QueryBuilder.setExpressionAttrValues ":x" x
                |> flip update client

            // assert
            let expected =
                Map.add "TablePk" (AttributeValue.Number (pk |> decimal)) Map.empty
                |> Map.add "TableSk" (AttributeValue.Number (sk |> decimal))
                |> if ``nested list``
                    then Map.add "P" ([|x; x|] |> CompressedList |> AttributeValue.AttributeList)
                    else Map.add "P" ([struct ("X", x); struct ("Y", x)] |> MapUtils.ofSeq |> AttributeValue.HashMap)

            if ``nested list``
            then
                Assert.Equal<AttributeValue>(x, result["TheList"].AsList[0].AsMap["V1"].AsList[0].AsMap["V2"])
                Assert.Equal(1, result["TheList"].AsList[0].AsMap["V1"].AsList.Count)
            else
                Assert.Equal<AttributeValue>(x, result["TheMap"].AsMap["V0"].AsList[0].AsMap["V1"].AsList[1])
                Assert.Equal(1, result["TheMap"].AsMap["V0"].AsList[0].AsMap.Count)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Update, same attribute twice but nested, throws`` ``at root`` ``nested list`` =

        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let x =
                if ``nested list``
                then [||] |> CompressedList |> AttributeValue.AttributeList
                else Map.empty |> AttributeValue.HashMap

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk

            let root = if ``at root`` then "" else "P."
            let! updateBase = put req client

            // act
            // assert
            let! e = 
                updateBase
                |> if ``nested list``
                    then QueryBuilder.setUpdateExpression $"SET {root}X = :x, {root}X[0] = :x"
                    else QueryBuilder.setUpdateExpression $"SET {root}X = :x, {root}X.Y = :x"
                |> QueryBuilder.setExpressionAttrValues ":x" x
                |> flip updateExpectErrorAndAssertNotModified client

            assertError output "An attribute or attribute path was updated or projected multiple times" e
        }

    [<Theory>]
    [<ClassData(typedefof<UpdateFailureTypeCases>)>]
    let ``Update, problems with query inputs, throws`` failure =

        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let x = AttributeValue.Number 22M

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk

            let! updateBase = put req client
            let updateRequest =
                updateBase
                |> QueryBuilder.setUpdateExpression $"SET #x = :x"
                |>  match failure with
                    | UpdateFailureType.MissingExpressionAttributeName -> id
                    | _ -> QueryBuilder.setExpressionAttrName $"#x" "x"
                |>  match failure with
                    | UpdateFailureType.TooManyExpressionAttributeNames -> QueryBuilder.setExpressionAttrName $"#y" "y"
                    | _ -> id
                |>  match failure with
                    | UpdateFailureType.MissingExpressionAttributeValue -> id
                    | _ -> QueryBuilder.setExpressionAttrValues ":x" x
                |>  match failure with
                    | UpdateFailureType.TooManyExpressionAttributeValues -> QueryBuilder.setExpressionAttrName $"#y" "y"
                    | _ -> id

            // act
            // assert
            match failure with
            | UpdateFailureType.None ->
                do! update updateRequest client |> Io.ignoreTask
            | _ ->
                let! e = updateExpectErrorAndAssertNotModified updateRequest client

                match failure with
                | UpdateFailureType.MissingExpressionAttributeValue
                | UpdateFailureType.MissingExpressionAttributeName -> "Cannot find"
                | UpdateFailureType.TooManyExpressionAttributeValues
                | UpdateFailureType.TooManyExpressionAttributeNames -> "were not used"
                | _ -> invalidOp ""
                |> flip (assertError output) e
        }

    [<Theory>]
    [<InlineData(null)>]
    [<InlineData("ALL_NEW")>]
    [<InlineData("ALL_OLD")>]
    [<InlineData("NONE")>]
    [<InlineData("UPDATED_NEW")>]
    [<InlineData("UPDATED_OLD")>]
    let ``Update, with projection, behaves correctly`` ``return vaulues`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let oldValue = AttributeValue.String "old"
            let newValue = AttributeValue.String "new"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "Val1" "S" "old"

            let! updateBase = put req client

            // act
            let! response =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET Val1 = :set"
                |> QueryBuilder.setExpressionAttrValues ":set" newValue
                |> if ``return vaulues`` = null
                    then id
                    else QueryBuilder.setReturnValues (ReturnValue.FindValue(``return vaulues``))
                |> QueryBuilder.updateRequest
                |> client.UpdateItemAsync

            // assert
            let expected =
                Map.empty
                |>
                    match ``return vaulues`` with
                    | "ALL_NEW"
                    | "ALL_OLD" ->
                        Map.add "TablePk" (AttributeValue.Number (pk |> decimal))
                        >> Map.add "TableSk" (AttributeValue.Number (sk |> decimal))
                    | "UPDATED_NEW"
                    | "UPDATED_OLD"
                    | "NONE"
                    | null -> id
                    | x -> invalidOp x
                |>
                    match ``return vaulues`` with
                    | "UPDATED_NEW"
                    | "ALL_NEW" -> Map.add "Val1" newValue
                    | "UPDATED_OLD"
                    | "ALL_OLD" -> Map.add "Val1" oldValue
                    | "NONE"
                    | null -> id
                    | x -> invalidOp x

            let actual =
                response.Attributes
                |> itemFromDynamodb "$"

            Assert.Equal<Map<string, AttributeValue>>(expected, actual)
        }

    [<Theory>]
    [<InlineData("UPDATED_NEW")>]
    [<InlineData("UPDATED_OLD")>]
    let ``Update, with projection, special case "Set x.#prop = :p", behaves correctly`` ``return vaulues`` =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"

            let oldValue = AttributeValue.String "old"
            let newValue = AttributeValue.String "new"

            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "#prop" "S" oldValue.AsString

            let! updateBase = put req client

            // act
            let! response =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET #prop = :prop"
                |> QueryBuilder.setExpressionAttrValues ":prop" newValue
                |> QueryBuilder.setExpressionAttrName "#prop" "#prop"
                |> QueryBuilder.setReturnValues (ReturnValue.FindValue(``return vaulues``))
                |> QueryBuilder.updateRequest
                |> client.UpdateItemAsync

            // assert
            let expected =
                match ``return vaulues`` with
                | "UPDATED_NEW" -> newValue
                | "UPDATED_OLD" -> oldValue
                | x -> invalidOp x

            let actual =
                response.Attributes
                |> itemFromDynamodb "$"

            Assert.Equal(expected, actual["#prop"])
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Update, with condition, behaves correctly`` ``item exists`` ``condition matches`` =
        
        let conditionTargetsSome = ``item exists`` = ``condition matches``
        let success = ``item exists`` = conditionTargetsSome
        
        task {
            use writer = new TestLogger(output)
            let writer = ValueSome (writer :> Microsoft.Extensions.Logging.ILogger)
            
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use host = cloneHost writer
            let client = TestDynamoClient.Create(host)
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
                if conditionTargetsSome then "attribute_exists(#attr) AND TablePk <> :v" else "attribute_not_exists(#attr) AND TablePk <> :v"
                
            let req = UpdateItemRequest()
            req.TableName <- table.name
            req.Key <- keys
            req.ConditionExpression <- expression
            req.ExpressionAttributeNames <-
                Map.add "#attr" "TablePk" Map.empty
                |> CSharp.toDictionary id id
            req.UpdateExpression <- "SET xxx = :y"
            req.ExpressionAttributeValues <-
                Map.add ":v" (Model.AttributeValue.String "XX") Map.empty
                |> Map.add ":y" (Model.AttributeValue.String "pp")
                |> itemToDynamoDb
            req.ReturnValues <- ReturnValue.ALL_OLD

            // act
            if success
            then
                let! response = client.UpdateItemAsync(req)
                if ``item exists``
                then
                    Assert.Equal(pk, response.Attributes["TablePk"].S)
                    Assert.Equal(sk, response.Attributes["TableSk"].N |> decimal)
                else Assert.Empty(response.Attributes)
                
                let! x = client.GetItemAsync(table.name, keys)
                Assert.Equal("pp", x.Item["xxx"].S)
            else
                let! e = Assert.ThrowsAnyAsync(fun _ -> client.UpdateItemAsync(req))
                assertError output "ConditionalCheckFailedException" e

                let! x = client.GetItemAsync(table.name, keys)
                if ``item exists``
                then Assert.False(x.Item.ContainsKey("xxx"))
                else Assert.True(x.Item.Count = 0)
        }

    //[<Fact(Skip = "tmp")>]
    [<Fact>]
    let ``Exploratory: What happens if you delete all items in a set?"`` () =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            
            let reqBase =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                
            let expected = ItemBuilder.attributes reqBase

            let req =
                reqBase
                |> ItemBuilder.withAttribute "ASet" "SS" """["v1"]"""

            let! updateBase = put req client

            // act
            let! actual =
                updateBase
                |> QueryBuilder.setUpdateExpression "DELETE ASet :x"
                |> QueryBuilder.setExpressionAttrValues ":x" (AttributeSet.fromStrings [AttributeValue.String "v1"] |> AttributeValue.HashSet)
                |> flip update client

            Assert.Equal<Map<string, AttributeValue>>(expected, actual)
        }

    [<Fact>]
    let ``Exploratory: Is this valid? "SET x[123] = :p" if x is list of 1 element`` () =
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            
            let reqBase =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                
            let expected =
                ItemBuilder.attributes reqBase
                |> Map.add "AList" (CompressedList [|AttributeValue.String "v1";AttributeValue.String "v2"|] |> AttributeList)

            let req =
                reqBase
                |> ItemBuilder.withAttribute "AList" "L" """[["S", "v1"]]"""

            let! updateBase = put req client

            // act
            let! actual =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET AList[123] = :x"
                |> QueryBuilder.setExpressionAttrValues ":x" (AttributeValue.String "v2")
                |> flip update client

            Assert.Equal<Map<string, AttributeValue>>(expected, actual)
        }
        
    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Exploratory: What happens here? "SET x.y = :p" if x is not a map (or list)`` ``is list`` deep = 
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            
            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "AVal" "N" "2"
                |> ItemBuilder.withAttribute "DeepVal" "M" """[["AVal", "N", "2"]]"""
                
            let! updateBase = put req client

            // act
            let! e =
                updateBase
                |> if ``is list`` && deep
                   then QueryBuilder.setUpdateExpression "SET DeepVal.AVal[0] = :x"
                   elif ``is list`` then QueryBuilder.setUpdateExpression "SET AVal[0] = :x"
                   elif deep then QueryBuilder.setUpdateExpression "SET DeepVal.AVal.C = :x"
                   else QueryBuilder.setUpdateExpression "SET AVal.C = :x"
                |> QueryBuilder.setExpressionAttrValues ":x" (AttributeValue.String "v2")
                |> flip updateExpectErrorAndAssertNotModified client
                
            assertError output "Cannot set" e
        }
        
    [<Fact>]
    let ``Exploratory: What happens here? "SET x = x + :p" if x has no value`` () = 
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            
            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                
            let! updateBase = put req client

            // act
            let! e =
                updateBase
                |> QueryBuilder.setUpdateExpression "SET x = x + :x"
                |> QueryBuilder.setExpressionAttrValues ":x" (AttributeValue.Number 55M)
                |> flip updateExpectErrorAndAssertNotModified client
                
            assertError output "Both operands of a + operation must be numbers" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Exploratory: Is this valid? "SET val1 = :v1, val2 = val1"`` ``terms reversed`` = 
        task {
            use client = buildClient (ValueSome output)

            // arrange
            let pk = $"{uniqueId()}"
            let sk = $"{uniqueId()}"
            
            let req =
                ItemBuilder.empty
                |> ItemBuilder.withAttribute "TablePk" "N" pk
                |> ItemBuilder.withAttribute "TableSk" "N" sk
                |> ItemBuilder.withAttribute "val1" "N" "55"
                
            let expected =
                ItemBuilder.attributes req
                |> Map.add "val1" (AttributeValue.Number 66M)
                |> Map.add "val2" (AttributeValue.Number 55M)
                
            let! updateBase = put req client
            
            let terms =
                [
                    "val1 = :x"
                    "val2 = val1"
                ]
                |> if ``terms reversed`` then List.rev else id
                |> Str.join ", "

            // act
            let! actual =
                updateBase
                |> QueryBuilder.setUpdateExpression $"SET {terms}"
                |> QueryBuilder.setExpressionAttrValues ":x" (AttributeValue.Number 66M)
                |> flip update client
                
            Assert.Equal<Map<string, AttributeValue>>(expected, actual)
        }