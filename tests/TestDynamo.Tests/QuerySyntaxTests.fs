namespace TestDynamo.Tests

open System.Text
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open TestDynamo
open TestDynamo.Utils
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open TestDynamo.Data.Monads.Operators

type QuerySyntaxTests(output: ITestOutputHelper) =

    let random = randomBuilder output

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Query, on partition key only, returns correct items`` (``has sort key``: bool) forward =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get ``has sort key`` true tables

            let struct (pk, expected) =
                randomPartition tab.hasSk random
                |> mapSnd (
                    Seq.map sndT
                    >> if forward then id else Seq.rev)

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setForwards forward
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            if tab.hasSk then Assert.True((Seq.length expected) > 1)
            else Assert.Equal(1, Seq.length expected)
            Assert.Equal(Seq.length expected, result.Items.Count)

            assertItems (expected, result.Items, true)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Query, with BETWEEN, returns correct items`` ``swap eq condition`` ``use aliases`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) =
                randomPartition tab.hasSk random
                |> mapSnd Seq.head
                
            let hsh = if ``use aliases`` then "#" else ""

            // act
            let execute l r =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> if ``swap eq condition``
                   then QueryBuilder.setKeyConditionExpression $"{hsh}TablePk = :p AND {hsh}TableSk BETWEEN :l AND :r"
                   else QueryBuilder.setKeyConditionExpression $":p = {hsh}TablePk AND {hsh}TableSk BETWEEN :l AND :r"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":l" (Number l)
                |> QueryBuilder.setExpressionAttrValues ":r" (Number r)
                |> if ``use aliases``
                   then
                       QueryBuilder.setExpressionAttrName "#TablePk" "TablePk"
                       >> QueryBuilder.setExpressionAttrName "#TableSk" "TableSk"
                   else id
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.fromTask
                |%|> (fun x ->
                    x.Items
                    |> Maybe.Null.toOption
                    ?|? MList()
                    |> Seq.filter (fun x -> x["TablePk"].S = pk && x["TableSk"].N = (sk.ToString()))
                    |> Seq.isEmpty
                    |> not)
                
                
            let! r1 = execute sk sk
            let! r2 = execute (sk - 1M) (sk + 1M)
            let! r3 = execute (sk + 1M) sk
            let! r4 = execute sk (sk - 1M)

            // assert
            Assert.True(r1)
            Assert.True(r2)
            Assert.False(r3)
            Assert.False(r4)
        }

    [<Theory>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Query, with begins_with, returns correct items``  ``use aliases`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (_, struct (_, expected)) =
                randomPartition tab.hasSk random
                |> mapSnd Seq.head
                
            let pk = (Map.find "IndexPk" expected).N
            let sk = (Map.find "IndexSk" expected).S
            let hsh = if ``use aliases`` then "#" else ""

            // act
            let execute bw =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setIndexName "TheIndex"
                |> QueryBuilder.setKeyConditionExpression $"{hsh}IndexPk = :p AND begins_with({hsh}IndexSk, :bw)"
                |> QueryBuilder.setExpressionAttrValues ":p" (Number pk)
                |> QueryBuilder.setExpressionAttrValues ":bw" (String bw)
                |> if ``use aliases``
                   then
                       QueryBuilder.setExpressionAttrName "#IndexPk" "IndexPk"
                       >> QueryBuilder.setExpressionAttrName "#IndexSk" "IndexSk"
                   else id
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.fromTask
                |%|> (fun x ->
                    x.Items
                    |> Maybe.Null.toOption
                    ?|? MList()
                    |> Seq.filter (fun x -> x["IndexPk"].N = pk.ToString() && x["IndexSk"].S = sk)
                    |> Seq.isEmpty
                    |> not)
                
                
            let! r1 = execute (sk.Substring(0, 1))
            let! r2 = execute sk
            let! r3 = execute $"x{sk}"
            let! r4 = execute $"{sk}x"

            // assert
            Assert.True(r1)
            Assert.True(r2)
            Assert.False(r3)
            Assert.False(r4)
        }

    [<Theory>]
    [<InlineData("JOIN")>]
    [<InlineData("join")>]
    [<InlineData("RaNk")>]
    let ``Query, with reserved word, throws`` word =

        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! e = Assert.ThrowsAnyAsync(fun () -> 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression $"{word} = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"{word} = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            // assert
            Assert.Contains($"Cannot use reserved word {word}", e.ToString())
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Query, on partition and sort key, returns correct items`` forward =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, expected)) = randomItem tab.hasSk random

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setForwards forward
                |> QueryBuilder.queryRequest |> client.QueryAsync

            // assert
            Assert.Equal(1, result.Items.Count)
            assertItems (Seq.singleton expected, result.Items, true)
        }

    [<Fact>]
    let ``Query, with invalid partition operator, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let query =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk < :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest

            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> client.QueryAsync query)
            assertError output "Invalid query TablePk < :p" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Query, with partition key reuse, throws`` ``use alias`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            let struct (pk, struct (_, _)) = randomItem tab.hasSk random

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> 
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> if ``use alias``
                    then QueryBuilder.setKeyConditionExpression "#TablePk = :p AND #TablePk = :p"
                    else QueryBuilder.setKeyConditionExpression "TablePk = :p AND TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> if ``use alias``
                    then QueryBuilder.setExpressionAttrName "#TablePk" "TablePk"
                    else id
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            assertError output "Found multiple partition key queries" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Query, with sort key reuse, throws`` ``use alias`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if ``use alias``
                    then QueryBuilder.setKeyConditionExpression "TablePk = :p AND #TableSk = :s AND #TableSk = :s"
                    else QueryBuilder.setKeyConditionExpression "TablePk = :p AND  TableSk = :s AND  TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if ``use alias``
                    then QueryBuilder.setExpressionAttrName "#TableSk" "TableSk"
                    else id
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            Assert.Contains("Found multiple sort key queries", e.ToString())
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Query, on index partition key only, returns correct items`` (``has sort key``: bool) forwards =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true ``has sort key`` tables

            let struct (pk, expected) =
                randomIndexPartition random
                |> mapSnd (Seq.map sndT)

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setIndexName "TheIndex"
                |> QueryBuilder.setKeyConditionExpression "IndexPk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (Number pk)
                |> QueryBuilder.setForwards forwards
                |> QueryBuilder.queryRequest |> client.QueryAsync

            // assert
            Assert.True((Seq.length expected) > 1)
            Assert.Equal(Seq.length expected, result.Items.Count)

            assertItems (expected, result.Items, false)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Query, on index partition and sort key, returns correct items`` forward =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (struct (pk, sk), expected) = randomIndexItem random

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setIndexName "TheIndex"
                |> QueryBuilder.setKeyConditionExpression "IndexPk = :p AND IndexSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (Number pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (String sk)
                |> QueryBuilder.setForwards forward
                |> QueryBuilder.queryRequest |> client.QueryAsync

            // assert
            Assert.True((Seq.length expected) > 1)
            Assert.Equal(Seq.length expected, result.Items.Count)

            assertItems (expected, result.Items, false)
        }

    [<Fact>]
    let ``Query, on index with invalid partition operator, fails`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (struct (pk, sk), _) = randomIndexItem random

            // act
            let query =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setIndexName "TheIndex"
                |> QueryBuilder.setKeyConditionExpression "IndexPk < :p AND IndexSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (Number pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (String sk)
                |> QueryBuilder.queryRequest

            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> client.QueryAsync query)
            assertError output "Invalid query IndexPk < :p" e
        }

    [<Theory>]
    [<InlineData(true, true, false)>]
    [<InlineData(true, true, true)>]
    [<InlineData(true, false, false)>]
    [<InlineData(true, false, true)>]
    let ``Query, compare operators, returns correct items`` ``on index`` less inclusive =

        let op =
            match struct (less, inclusive) with
            | true, true -> "<="
            | true, false -> "<"
            | false, true -> ">="
            | false, false -> ">"

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, items') =
                if ``on index`` then randomIndexPartition random |> mapFst _.ToString()
                else
                    randomPartition true random
                    |> mapSnd (Collection.mapFst _.ToString())

            let items = items' |> Seq.groupBy fstT |> Seq.map structTpl |> List.ofSeq
            Assert.True((Seq.length items) > 2)

            let pivot = if less then 2 else 1
            let struct (rangeFrom, rangeTo) =
                let modifier = if inclusive then 0 else 1
                if less
                then struct (0, pivot - modifier)
                else struct (pivot + modifier, 10000)

            let sk =  items |> Seq.skip pivot |> Seq.head |> fstT
            let expected =
                items
                |> Seq.skip rangeFrom
                |> Seq.truncate (rangeTo + 1 - rangeFrom)
                |> Seq.collect sndT
                |> Seq.map sndT
                |> List.ofSeq

            output.WriteLine($"Pivot: {pivot}, rangeFrom: {rangeFrom}, rangeTo: {rangeTo}, expected: {List.length expected}")

            let struct (struct (pkName, pkType), struct (skName, skType), setIndex) =
                if ``on index``
                then struct (struct ("IndexPk", "N"), struct ("IndexSk", "S"), QueryBuilder.setIndexName "TheIndex")
                else struct (struct ("TablePk", "S"), struct ("TableSk", "N"), id)

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> setIndex
                |> QueryBuilder.setKeyConditionExpression $"{pkName} = :p AND {skName} {op} :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute pkType (pk.ToString()))
                |> QueryBuilder.setExpressionAttrValues ":s" (ItemBuilder.buildItemAttribute skType sk)
                |> QueryBuilder.queryRequest |> client.QueryAsync

            // assert
            Assert.Equal(Seq.length expected, result.Items.Count)
            Assert.Equal(Seq.length expected, result.Items.Count)

            assertItems (expected :> _ seq, result.Items, not ``on index``)
        }

    [<Fact>]
    let ``Query, with superfluous attr query values, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression $"TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute "S" "77")
                |> QueryBuilder.setExpressionAttrValues ":s" (ItemBuilder.buildItemAttribute "S" "77")
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            Assert.Contains("Expression attribute names or values were not used: :s", e.ToString())
        }

    [<Fact>]
    let ``Query, with superfluous attr query name, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression $"TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute "S" "77")
                |> QueryBuilder.setExpressionAttrName "#pp" "uu"
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            Assert.Contains("Expression attribute names or values were not used: #pp", e.ToString())
        }

    [<Theory>]
    [<InlineData(true, false)>]
    [<InlineData(true, true)>]
    [<InlineData(false, false)>]
    [<InlineData(false, true)>]
    let ``Query, BETWEEN, returns correct items`` bottomRange ``on index`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, items') =
                if ``on index`` then randomIndexPartition random |> mapFst _.ToString()
                else
                    randomPartition true random
                    |> mapSnd (Collection.mapFst _.ToString())

            let items = items' |> Seq.groupBy fstT |> Seq.map structTpl |> (sndT |> Seq.map |> mapSnd |> Seq.map) |> Array.ofSeq
            Assert.True((Array.length items) > 2)

            let expected' =
                items
                |> if bottomRange then Seq.take 2 else Seq.skip 1
                |> Array.ofSeq

            let struct (from, ``to``) =
                struct (Array.head expected' |> fstT, Array.last expected' |> fstT)

            let expected = Seq.collect sndT expected'
            let struct (struct (pkName, pkType), struct (skName, skType), setIndex) =
                if ``on index``
                then struct (struct ("IndexPk", "N"), struct ("IndexSk", "S"), QueryBuilder.setIndexName "TheIndex")
                else struct (struct ("TablePk", "S"), struct ("TableSk", "N"), id)

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> setIndex
                |> QueryBuilder.setKeyConditionExpression $"{pkName} = :p AND {skName} BETWEEN :s1 AND :s2"
                |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute pkType pk)
                |> QueryBuilder.setExpressionAttrValues ":s1" (ItemBuilder.buildItemAttribute skType from)
                |> QueryBuilder.setExpressionAttrValues ":s2" (ItemBuilder.buildItemAttribute skType ``to``)
                |> QueryBuilder.queryRequest |> client.QueryAsync

            // assert
            Assert.Equal(Seq.length expected, result.Items.Count)
            assertItems (expected, result.Items, not ``on index``)
        }

    [<Theory>]
    [<InlineData("0", 0)>]
    [<InlineData("1", 2)>]
    [<InlineData("11", 1)>]
    [<InlineData("2", 2)>]
    [<InlineData("22", 1)>]
    [<InlineData("3", 0)>]
    let ``Query, begins_with, returns correct items`` startsWith expectedCount =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get false true tables
            let pk = (IncrementingId.next()).Value |> decimal |> ((*)123m)

            let baseItem =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tab.name
                |> ItemBuilder.withAttribute "IndexPk" "N" (pk.ToString())

            let items' =
                [1..2]
                |> Seq.collect (sprintf "%i%i" >> flip Seq.map [1..2])
                |> Seq.map (fun sk ->
                    baseItem
                    |> ItemBuilder.withAttribute "TablePk" "S" $"id-{IncrementingId.next()}"
                    |> ItemBuilder.withAttribute "IndexSk" "S" sk
                    |> tpl sk)
                |> List.ofSeq

            let! _ =
                items'
                |> Collection.mapSnd (ItemBuilder.asPutReq)
                |> Seq.map (sndT >> client.PutItemAsync)
                |> Task.WhenAll

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setIndexName "TheIndex"
                |> QueryBuilder.setKeyConditionExpression $"IndexPk = :p AND begins_with(IndexSk, :s)"
                |> QueryBuilder.setExpressionAttrValues ":p" (Number pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (String startsWith)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            Assert.Equal(expectedCount, result.Items.Count)
        }

    [<Theory>]
    // [<InlineData(true)>]
    [<ClassData(typeof<OneFlag>)>]
    let ``Query, begins_with, binary, returns correct items`` filter =

        task {
            use client = buildClientWithlogLevel LogLevel.Trace output 
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = tables.``table, pBytes, sBytes, IpBytes, IsBytes``
            let key = "TheNewPk_TheNewPk_TheNewPk_TheNewPk_TheNewPk_TheNewPk"
            let keyBytes = Encoding.UTF8.GetBytes key
            let partialKeyBytes = Array.take 3 keyBytes
            let invalidKeyBytes = Array.concat [|partialKeyBytes; keyBytes|]

            do!
                ItemBuilder.empty
                |> ItemBuilder.withTableName tab.name
                |> ItemBuilder.withAttribute "TablePk" "UTF8" key
                |> ItemBuilder.withAttribute "TableSk" "UTF8" key
                |> ItemBuilder.asPutReq
                |> client.PutItemAsync
                |> Io.ignoreTask

            // act
            let execute sk =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if filter
                    then QueryBuilder.setFilterExpression $"TablePk = :p AND begins_with(TableSk, :s)"
                    else QueryBuilder.setKeyConditionExpression $"TablePk = :p AND begins_with(TableSk, :s)"
                |> QueryBuilder.setExpressionAttrValues ":p" (Binary keyBytes)
                |> QueryBuilder.setExpressionAttrValues ":s" (Binary sk)
                |> if filter
                    then
                        QueryBuilder.scanRequest
                        >> client.ScanAsync
                        >> Io.fromTask
                        >> Io.map _.Items
                    else
                        QueryBuilder.queryRequest
                        >> client.QueryAsync
                        >> Io.fromTask
                        >> Io.map _.Items

            let! result1 = execute partialKeyBytes
            let! result2 = execute invalidKeyBytes

            // assert
            Assert.Equal(1, result1.Count)
            Assert.Equal(0, result2.Count)
        }

    [<Fact>]
    let ``Query, with invalid attribute name characters, throws`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            let struct (pk, struct (_, _)) = randomItem tab.hasSk random

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> 
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p" 
                |> QueryBuilder.setFilterExpression "1x = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk) 
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            assertError output "Cannot parse token at position" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Query, with invalid expression attribute name characters, throws`` ``err in filter`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            let struct (pk, struct (_, _)) = randomItem tab.hasSk random

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> 
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> if ``err in filter``
                    then QueryBuilder.setKeyConditionExpression "#good = :p"
                    else QueryBuilder.setKeyConditionExpression "#-bad = :p"
                |> if ``err in filter``
                    then QueryBuilder.setFilterExpression "#-bad = :p"
                    else QueryBuilder.setFilterExpression "#good = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> if ``err in filter``
                    then QueryBuilder.setExpressionAttrName "#good" "TablePk" >> QueryBuilder.setExpressionAttrName "#-bad" "ABC"
                    else QueryBuilder.setExpressionAttrName "#-bad" "TablePk" >> QueryBuilder.setExpressionAttrName "#good" "ABC"
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            assertError output "Cannot parse token at position" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Query, with invalid expression attribute value characters, throws`` ``err in filter`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            let struct (pk, struct (_, _)) = randomItem tab.hasSk random

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () -> 
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> if ``err in filter``
                    then QueryBuilder.setKeyConditionExpression "TablePk = :p"
                    else QueryBuilder.setKeyConditionExpression "TablePk = :-p"
                |> if ``err in filter``
                    then QueryBuilder.setFilterExpression "XX = :-p"
                    else QueryBuilder.setFilterExpression "XX = :p"
                |> QueryBuilder.setExpressionAttrValues ":-p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            assertError output "Cannot parse token at position" e
        }

    [<Theory>]
    [<InlineData("-2")>]
    [<InlineData("-0002")>]
    [<InlineData("-2.000")>]
    let ``Query on various numeric representations`` number =

        task {
            
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client

            let tab = Tables.get true true tables
            let baseReq =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String "1")
                |> QueryBuilder.queryRequest

            baseReq.ExpressionAttributeValues.Add(
                ":s",
                let attr = DynamoAttributeValue()
                attr.N <- number
                attr)

            // act    
            let! response = client.QueryAsync baseReq

            // assert
            Assert.Equal(1, response.Count <!> 0)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Query, with filter expression, returns correct items`` (``on index``: bool) forward =

        task {
            use client = buildClient output

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, expected') =
                 if ``on index`` then
                    randomIndexPartition random
                    |> mapSnd (
                        Seq.map sndT
                        >> if forward then id else Seq.rev
                        >> List.ofSeq)
                    |> mapFst _.ToString()
                 else
                    randomPartition tab.hasSk random
                    |> mapSnd (
                        Seq.map sndT
                        >> if forward then id else Seq.rev
                        >> List.ofSeq)

            Assert.True(List.length expected' > 2)
            let expected =
                Seq.mapi tpl expected'
                |> Seq.filter (fun struct (i, _) -> i = 0 || i = 2)
                |> Seq.map sndT
                |> List.ofSeq

            let fstBinary = expected |> List.head |> Map.find "BinaryData"
            let sndBinary = expected |> List.last |> Map.find "BinaryData"

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if ``on index`` then QueryBuilder.setIndexName "TheIndex" else id
                |> QueryBuilder.setKeyConditionExpression (if ``on index`` then "IndexPk = :p" else "TablePk = :p")
                |> QueryBuilder.setFilterExpression "BinaryData = :b1 OR BinaryData = :b2"
                |> QueryBuilder.setExpressionAttrValues ":p" (if ``on index`` then Number (decimal pk) else String pk)
                |> QueryBuilder.setExpressionAttrValues ":b1" fstBinary
                |> QueryBuilder.setExpressionAttrValues ":b2" sndBinary
                |> QueryBuilder.setForwards forward
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            if tab.hasSk then Assert.True((Seq.length expected) > 1)
            else Assert.Equal(1, Seq.length expected)
            Assert.Equal(Seq.length expected, result.Items.Count)

            Assert.True(result.ScannedCount <!> 0 > result.Items.Count)
            assertItems (expected, result.Items, not ``on index``)
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Query, on binary data partition key only, returns correct items`` ``use index`` forward =
        task {
            use client = buildClient output

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = tables.``table, pBytes, sBytes, IpBytes, IsBytes``

            let struct (pk, expected) =
                if ``use index``
                then randomBinaryIndexPartition random
                else randomBinaryPartition random
                |> mapSnd (
                    Seq.map sndT
                    >> if forward then id else Seq.rev)

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if ``use index`` then QueryBuilder.setIndexName "TheIndex" else id
                |> if ``use index``
                    then QueryBuilder.setKeyConditionExpression "IndexPk = :p"
                    else QueryBuilder.setKeyConditionExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" pk
                |> QueryBuilder.setForwards forward
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            Assert.True((Seq.length expected) > 1)
            Assert.Equal(Seq.length expected, result.Items.Count)

            assertItems (expected, result.Items, not ``use index``)
        }

    let buildBracketsTestCase hasPartition hasSort1 hasSort2 =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (p, struct (s1, s2)) =
                randomPartition (hasSort1 || hasSort2) random
                |> mapSnd (
                    List.ofSeq
                    >> tpl ([hasSort1; hasSort2] |> Seq.filter id |> Seq.length)
                    >> function
                        | _, (head1, _)::(head2, _)::_ -> struct (head1, head2)
                        | 1, (head1, _)::_ when hasSort1 -> struct (head1, 0M)
                        | 1, (head1, _)::_ when hasSort2 -> struct (0M, head1)
                        | 0, _ -> struct (0M, 0M)
                        | need, has -> invalidOp $"Need {need} elements ({List.length has})")

            let result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if hasPartition then QueryBuilder.setExpressionAttrValues ":p" (String p) else id
                |> if hasSort1 then QueryBuilder.setExpressionAttrValues ":s1" (Number s1) else id
                |> if hasSort2 then QueryBuilder.setExpressionAttrValues ":s2" (Number s2) else id

            return result
        }

    let buildExecuteTestCase hasPartition hasSort1 hasSort2 query =

        task {
            use client = buildClient output

            // arrange
            let! baseQ = buildBracketsTestCase hasPartition hasSort1 hasSort2

            // act
            // assert
            let! _ =
                baseQ
                |> QueryBuilder.setKeyConditionExpression query
                |> QueryBuilder.queryRequest |> client.Client.QueryAsync

            ()
        }

    [<Fact>]
    let ``Query, single pk query with brackets, smoke test only`` () =

        buildExecuteTestCase true false false
        <| "(TablePk = :p)"

    [<Fact>]
    let ``Query, single pk query with double brackets, smoke test only`` () =

        buildExecuteTestCase true false false
        <| "((TablePk = :p))"

    [<Fact>]
    let ``Query, single pk query with two sets of brackets, smoke test only`` () =

        buildExecuteTestCase true false false
        <| "((TablePk) = (:p))"

    [<Fact>]
    let ``Query, pk and sk query with brackets, smoke test only`` () =

        buildExecuteTestCase true true false
        <| "(TablePk = :p AND TableSk > :s1)"

    [<Fact>]
    let ``Query, pk and sk query with multiple brackets, smoke test only`` () =

        buildExecuteTestCase true true false
        <| "((TablePk = :p) AND (TableSk > :s1))"

    [<Fact>]
    let ``Query, pk and sk query with multiple brackets (2), smoke test only`` () =

        buildExecuteTestCase true true false
        <| "(TablePk) = (   :p) AND (   TableSk) > (:s1)"

    [<Fact>]
    let ``Query, pk and 2sk query with multiple brackets, smoke test only`` () =

        buildExecuteTestCase true true true
        <| "(TablePk) = (   :p) AND ((   TableSk) BETWEEN (:s1) AND :s2)"