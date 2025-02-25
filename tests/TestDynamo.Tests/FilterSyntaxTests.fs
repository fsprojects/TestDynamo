namespace TestDynamo.Tests

open System
open System.IO
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Utils
open Microsoft.Extensions.Logging
open Tests.ClientLoggerContainer
open Tests.Table
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open Tests.Loggers

// warning FS3511: This state machine is not statically compilable.
// A 'let rec' occured in the resumable code specification. An alternative dynamic
// implementation will be used, which may be slower. Consider adjusting your
// code to ensure this state machine is statically compilable, or else suppress this warning
#nowarn "3511"

// tests filter part of query and scan
type FilterSyntaxTests(output: ITestOutputHelper) =

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

    let random = randomBuilder output

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Filter, =, <>, NOT, returns correct items`` ``on index`` neq invert =

        let filterExp = if neq then "TablePk <> :p" else "TablePk = :p"
        let struct (filterExp, neq) =
            if invert then struct ($"NOT {filterExp}", not neq)
            else struct (filterExp, neq)

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true ``on index`` tables

            let struct (pk, expected) =
                randomPartition tab.hasSk random
                |> mapSnd (Seq.map sndT)

            let expected =
                if neq then
                    groupedItems tab.hasSk
                    |> fstT
                    |> Seq.filter (fstT >> ((<>)pk))
                    |> Seq.collect sndT
                    |> Seq.map (sndT >> TableItem.asItem)
                else expected

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setFilterExpression filterExp
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.scanRequest
                |> client.ScanAsync

            // assert
            if neq || tab.hasSk then Assert.True((Seq.length expected) > 1)
            else Assert.Equal(1, Seq.length expected)
            Assert.Equal(Seq.length expected, result.Count <!> 0)

            Assert.True(result.ScannedCount <!> 0 > result.Items.Count)
            Assert.Equal(Seq.length (allItems tab.hasSk), result.ScannedCount <!> 0)
            assertItems (expected, result.Items, true)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Filter, NOT on property, returns correct items`` twice =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (trueVals, falseVals) =
                allItems tab.hasSk
                |> Seq.map TableItem.asItem
                |> Seq.filter (Map.containsKey "BoolData")
                |> Collection.partition (Map.find "BoolData" >> function | Boolean x -> x | _ -> invalidOp "X")

            Assert.True(List.length trueVals > 0)
            Assert.True(List.length falseVals > 0)

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setFilterExpression (if twice then "NOT NOT BoolData" else "NOT BoolData")
                |> QueryBuilder.scanRequest
                |> client.ScanAsync

            // assert
            let expected = if twice then  trueVals else falseVals
            assertItems (expected, result.Items, true)
        }

    [<Theory>]
    [<InlineData(true, 0)>]
    [<InlineData(true, 1)>]
    [<InlineData(false, 0)>]
    let ``Filter, contains on list, returns correct result`` ``does contain`` index =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random
            let find =
                match struct (``does contain``, index) with
                | true, i -> (Map.find "ListData" data) |> function | AttributeList x -> AttributeListType.find i x | _ -> invalidOp "XX"
                | false, _ -> "INVALID" |> String 

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "contains(ListData, :l)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":l" find
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            let expected = if ``does contain`` then  [data] else []
            assertItems (expected, result.Items, true)
        }

    [<Fact>]
    let ``Filter, contains, LHS is a value, smoke test`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random
            let find = (Map.find "IndexSk" data) |> function | String x -> x + "abc" | _ -> invalidOp "XX"

            // act
            let! x =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "contains(:f, IndexSk)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":f" (String find)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            Assert.Equal(1, x.Count <!> 0)
        }

    [<Theory>]
    [<InlineData("SSData", 2, true)>]
    [<InlineData("SSData", -1, false)>]
    [<InlineData("NSData", 2, true)>]
    [<InlineData("NSData", -1, false)>]
    [<InlineData("BSData", 2, true)>]
    [<InlineData("MapData", -1, false)>]
    [<InlineData("MapData", 2, true)>]
    [<InlineData("ListData", -1, false)>]
    [<InlineData("ListData", 2, true)>]
    [<InlineData("BinaryData", -1, false)>]
    [<InlineData("BinaryData", 1, true)>]
    [<InlineData("LongerString", -1, false)>]
    [<InlineData("LongerString", 23, true)>]
    [<InlineData(":p", -1, false)>]
    [<InlineData(":p", 1, true)>]   // this case might be flaky if pk is variable size
    let ``Filter, size, returns correct result`` ``property name`` size ``size is correct`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, data)) = randomFilteredItem (fstT >> _.Length >> (=)1) tab.hasSk random

            // hack for the way test data is built. Correct size might be 1 or 2
            let size =
                if ``property name`` = "BinaryData" && ``size is correct``
                then Map.find "BinaryData" data |> function | Binary x -> Array.length x | x -> x.ToString() |> invalidOp
                else size

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setFilterExpression $"size({``property name``}) = :expected"
                |> QueryBuilder.setExpressionAttrValues ":expected" (size |> decimal |> Number)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            let expected = if ``size is correct`` then [data] else []
            assertItems (expected, result.Items, true)
        }

    [<Theory>]
    [<InlineData("TablePk_Copy", true)>]
    [<InlineData("TablePk_Copy", false)>]
    [<InlineData("TableSk_Copy", true)>]
    [<InlineData("TableSk_Copy", false)>]
    [<InlineData("IndexPk_Copy", true)>]
    [<InlineData("IndexPk_Copy", false)>]
    [<InlineData("BinaryData", true)>]
    [<InlineData("BinaryData", false)>]
    [<InlineData("IndexSk_Copy", true)>]
    [<InlineData("IndexSk_Copy", false)>]
    [<InlineData("BoolData", true)>]
    [<InlineData("BoolData", false)>]
    [<InlineData("NullData", true)>]
    [<InlineData("NullData", false)>]
    [<InlineData("LongerString", true)>]
    [<InlineData("LongerString", false)>]
    [<InlineData("ListData", true)>]
    [<InlineData("ListData", false)>]
    [<InlineData("MapData", true)>]
    [<InlineData("MapData", false)>]
    [<InlineData("SSData", true)>]
    [<InlineData("SSData", false)>]
    [<InlineData("NSData", true)>]
    [<InlineData("NSData", false)>]
    [<InlineData("BSData", true)>]
    [<InlineData("BSData", false)>]
    let ``Filter, IN, returns correct result`` ``test prop`` ``does contain`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random
            let fullPropList = MapUtils.toSeq data
            let inList =
                fullPropList
                // table/index pks are not part of test, as they cannot be included in filter expression
                // they also have the same value as their _Copy props, making tests more complex 
                |> Seq.filter (fun struct (name, _) -> name <> "TablePk" && name <> "TableSk" && name <> "IndexPk" && name <> "IndexSk")
                |> if ``does contain`` then id else Seq.filter (fstT >> ((<>)``test prop``))
                |> randomSort random
                |> Seq.mapi tpl

            let struct (inVals, inProps) =
                inList
                |> Collection.partition (fstT >> flip (%)2 >> ((=)0))

            let inValueList = inVals |> List.map (sndT >> sndT)
            let inValueProp = inProps |> List.map (sndT >> fstT)

            let inNameList =
                inValueList
                |> List.mapi (fun i _ -> $":p{i}")

            let inAttrs =
                inValueList
                |> List.mapi tpl
                |> flip (List.fold (fun s struct (i, x) -> QueryBuilder.setExpressionAttrValues $":p{i}" x s))

            // act
            let cma = ", "
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"{``test prop``} IN ({Str.join cma inNameList}, {Str.join cma inValueProp})"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> inAttrs
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            let expected = if ``does contain`` then  [data] else []
            assertItems (expected, result.Items, true)
        }

    [<Theory>]
    [<InlineData("X IN (Y)")>]
    [<InlineData(":p IN (Y)")>]
    [<InlineData("Y IN (:p)")>]
    [<InlineData("X.Y IN (Y.X)")>]
    [<InlineData("X[0] IN (Y[1])")>]
    [<InlineData("size(X) IN (attribute_exists(Y))")>]
    let ``More complex Filter, IN, smoke tests`` ``in expression`` =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random

            // act
            // assert
            do!
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression ``in expression``
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask
        }

    [<Theory>]
    [<InlineData(1, 1)>]
    [<InlineData(2, 1)>]
    [<InlineData(3, 1)>]
    [<InlineData(4, 1)>]
    [<InlineData(2, 10)>]
    [<InlineData(3, 10)>]
    let ``Max filter size experiment`` ``parenthesis anchored at start`` ``parenthesis count`` =

        task {
            // arrange
            // use output = new FileWriter("""C:\Dev\TestDynamo\out.log""")
            use logger = new TestLogger(output, LogLevel.Warning)
            let client = buildClientFromLogger logger
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            let openP = [1..``parenthesis count``] |> Seq.map (asLazy "(") |> Str.join ""
            let closeP = [1..``parenthesis count``] |> Seq.map (asLazy ")") |> Str.join ""

            let rec nestCenter = function
                | []  -> invalidOp "empty"
                | [x] -> x
                | x::y::tail ->
                    let struct (before, after) =
                        let lTail = List.length tail
                        List.mapi tpl tail
                        |> Collection.partition (fun struct (i, _) -> i < lTail)
                        |> mapFst (List.map sndT)
                        |> mapSnd (List.map sndT)
                    before@($"{openP}{x} AND {y}{closeP}"::after) |> nestCenter

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random
            let folder =
                match ``parenthesis anchored at start`` with
                // no parenthesis
                | 1 -> Str.join " OR "
                // parenthesis anchored at start
                | 2 -> Seq.fold (fun s x -> $"{openP}{s} OR {x}{closeP}") "pX<>:p"
                // parenthesis anchored at end
                | 3 -> Seq.fold (fun s x -> $"{openP}{x} OR {s}{closeP}") "pX<>:p"
                // parenthesis anchored in center
                | 4 -> List.ofSeq >> nestCenter 
                | x -> invalidOp (x.ToString())

            let expr =
                // [0..10]
                [0..300]
                |> Seq.map (asLazy "p<>:p")
                |> folder

            // act
            // assert
            try
                do!
                    QueryBuilder.empty ValueNone
                    |> QueryBuilder.setTableName tab.name
                    |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                    |> QueryBuilder.setFilterExpression expr
                    |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                    |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                    |> QueryBuilder.queryRequest
                    |> client.QueryAsync
                    |> Io.ignoreTask

                // not important if it throws or not. Just need to
                // test for stack overflow
            with
            | e ->
                flip (assertAtLeast1Error output) e
                <| [ "Maximum number of operators has been reached"
                     "Max expression size is 4KB" ]
        }

    [<Theory>]
    [<InlineData(100, true)>]
    [<InlineData(101, false)>]
    let ``Filter, IN, with a lot of values, behaves as expected`` ``value count`` ``should succeed`` =
        task {
            // arrange
            // this test has a lot of logging. Turn of tracing
            use logger = new TestLogger(output, LogLevel.Warning)
            let client = buildClientFromLogger (logger)

            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            let ins =
                [1..``value count``]
                |> Seq.map (fun _ -> ":p")
                |> Str.join ","
                |> sprintf "Prop in (%s)"

            // act
            let execute _ =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression ins
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask

            if ``should succeed`` then do! execute ()
            else
                let! e = Assert.ThrowsAnyAsync(execute)
                assertError output "IN expression supports max 100 inputs" e
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Filter, with indexed property, processes successfully`` ``named attribute value`` found =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get false false tables
            let struct (pk, struct (_, data)) = randomItem tab.hasSk random
            let listData = Map.find "ListData" data |> function
                | AttributeList (CompressedList [|_; x|]) -> x
                | _ -> invalidOp ""

            // act
            let! result =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":l" (if found then listData else (String "invalid"))
                |> if ``named attribute value``
                    then  QueryBuilder.setFilterExpression $"ListData[1] = :l"
                    else  (QueryBuilder.setFilterExpression $"#ListData[1] = :l" >> QueryBuilder.setExpressionAttrName "#ListData" "ListData")
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            if found then Assert.Equal(1, result.Count <!> 0)
            else Assert.Equal(0, result.Count <!> 0)
        }

    [<Fact>]
    let ``Scan filter can contain non primary key attributes`` () =
        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables

            // act
            // assert
            do!
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setFilterExpression $"TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String "123456")
                |> QueryBuilder.scanRequest
                |> client.ScanAsync
                |> Io.ignoreTask
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Query filter can only contain non primary key attributes`` control ``use alias`` ``part of path`` =
        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random
            let suffix1 = if control then "_Copy" else ""
            let suffix2 = if ``part of path`` then ".X" else ""

            let act () =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> if ``use alias``
                    then QueryBuilder.setFilterExpression $"#Tpk{suffix2} = :p"
                    else QueryBuilder.setFilterExpression $"TablePk{suffix1}{suffix2} = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if ``use alias`` then QueryBuilder.setExpressionAttrName "#Tpk" $"TablePk{suffix1}" else id
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask

            // act
            if control then do! act()
            else
                let! e = Assert.ThrowsAnyAsync(fun () ->act())

                // assert
                output.WriteLine(e.ToString())
                Assert.Contains($"Cannot use key attribute TablePk in filter expression", e.ToString())
        }

    [<Theory>]
    [<InlineData("attribute_not_exists")>]
    [<InlineData("attribute_exists")>]
    let ``Filter, attribute_exists with non attr, throws`` func =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"{func}(:p)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            // assert
            Assert.Contains($"Operand for {func} must be an attribute", e.ToString())
        }

    [<Theory>]
    [<InlineData(null)>]
    [<InlineData("M")>]
    [<InlineData("L")>]
    [<InlineData("SS")>]
    [<InlineData("NS")>]
    [<InlineData("BS")>]
    let ``Filter, map, list and hash set equality`` ``modify prop`` =

        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use h = cloneHostUnlogged()
            use client = TestDynamoClientBuilder.Create(h)
            let tab = Tables.get true true tables
            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random
            let! item =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            let buildDeeplyNestedMap =
                buildDeeplyNestedMap
                >> mapSnd (fun tail ->
                    tail.S <- null
                    maybeSetProperty "IsMSet" tail true
                    tail.M <- Dictionary<string, DynamoAttributeValue>()
                    maybeSetProperty "IsMSet" tail true

                    tail.M["SS"] <- DynamoAttributeValue()
                    maybeSetProperty "IsSSSet" tail.M["SS"] true
                    tail.M["SS"].SS <- System.Collections.Generic.List<string>()
                    maybeSetProperty "IsSSSet" tail.M["SS"] true
                    tail.M["SS"].SS.Add("123")
                    tail.M["SS"].SS.Add("456")

                    tail.M["NS"] <- DynamoAttributeValue()
                    maybeSetProperty "IsNSSet" tail.M["NS"] true
                    tail.M["NS"].NS <- System.Collections.Generic.List<string>()
                    maybeSetProperty "IsNSSet" tail.M["NS"] true
                    tail.M["NS"].NS.Add("123")

                    tail.M["BS"] <- DynamoAttributeValue()
                    maybeSetProperty "IsBSSet" tail.M["BS"] true
                    tail.M["BS"].BS <- System.Collections.Generic.List<MemoryStream>()
                    maybeSetProperty "IsBSSet" tail.M["BS"] true
                    tail.M["BS"].BS.Add(new MemoryStream([| 123uy |]))
                    tail)
                >> fstT

            let newMap = buildDeeplyNestedMap()
            item.Items[0].Add("NewItem", newMap)
            let! _ =  client.PutItemAsync(
                tab.name,
                item.Items[0])

            let isEqual =
                match ``modify prop`` with
                | null -> true
                | "M" ->
                    let attr = AttributeValue()
                    attr.S <- "hi"
                    newMap.M["0"].L[0].M.Add("x", attr)
                    false
                | "L" ->
                    let attr = AttributeValue()
                    attr.S <- "hi"
                    newMap.M["0"].L.Add(attr)
                    false
                | "SS" ->
                    newMap.M["0"].L[0].M["0"].M["SS"].SS.RemoveAt(0)
                    false
                | "NS" ->
                    newMap.M["0"].L[0].M["0"].M["NS"].NS.Add("77")
                    false
                | "BS" ->
                    newMap.M["0"].L[0].M["0"].M["BS"].BS.Add(new MemoryStream([| 33uy |]))
                    false
                | x -> invalidOp x

            // act
            let! response = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"NewItem = :ni"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":ni" (attributeFromDynamodb newMap)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            if isEqual then Assert.Equal(1, response.Items.Count)
            else Assert.Equal(0, response.Items.Count)
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Filter, ".", finds items`` exists testProp aliased =

        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use h = cloneHostUnlogged()
            use client = TestDynamoClientBuilder.Create(h)
            let tab = Tables.get true true tables
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random
            let! item =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            item.Items[0].Add("NewItem", DynamoAttributeValue())
            let nested = item.Items[0]["NewItem"] |> addMapWithProp "nested" |> addMapWithProp "boolVal"
            maybeSetProperty "IsBOOLSet" nested true
            nested.BOOL <- !<> exists
            maybeSetProperty "IsBOOLSet" nested true

            let value = DynamoAttributeValue()
            value.N <- "6"
            (item.Items[0]["NewItem"]).M["nested"].M.Add("numVal", value)

            let! _ =  client.PutItemAsync(
                tab.name,
                item.Items[0])

            let queryNumber = if exists then 6M else 7M

            // act
            let! response = 
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression (
                    if testProp && aliased then "NewItem.#nested.numVal = :n"
                    elif testProp then "NewItem.nested.numVal = :n"
                    elif aliased then "NewItem.#nested.boolVal"
                    else "NewItem.nested.boolVal")
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if testProp then QueryBuilder.setExpressionAttrValues ":n" (Number queryNumber) else id
                |> if aliased
                    then QueryBuilder.setExpressionAttrName "#nested" "nested"
                    else id
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            if exists then Assert.Equal(1, response.Items.Count)
            else Assert.Equal(0, response.Items.Count)

            // random double dispose of host
            h.Dispose()
        }

    [<Fact>]
    let ``Filter, "." on unknown invalid map, finds nothing`` () =

        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! response = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "NullData.Nested.Val"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            Assert.Equal(0, response.Items.Count)
        }

    [<Fact>]
    let ``Filter, [i] on root, throws error`` () =

        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! e = Assert.ThrowsAnyAsync (fun _ ->
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "[1] = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            // assert
            assertError output "Cannot parse" e
        }

    [<Fact>]
    let ``Filter, "." with LHS is expr attr value, throws`` () =

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
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "NullData.:p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            // assert
            output.WriteLine(e.ToString())
            Assert.Contains("Invalid expression, expected form: x.y", e.ToString())
        }

    [<Fact>]
    let ``Filter, '.' with LHS is function call, throws`` () =

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
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "NullData.Nested.contains(XX, p)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask)

            // assert
            output.WriteLine(e.ToString())
            Assert.Contains("Invalid expression, expected form: x.y", e.ToString())
        }

    [<Fact>]
    let ``Filter, with invalid attribute name characters, throws`` () =

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
                |> QueryBuilder.setFilterExpression "1x = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk) 
                |> QueryBuilder.scanRequest
                |> client.ScanAsync
                |> Io.ignoreTask)

            assertError output "Cannot parse token at position" e
        }

    [<Fact>]
    let ``Filter, with invalid expression attribute name characters, throws`` () =

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
                |> QueryBuilder.setFilterExpression "#-bad = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrName "#-bad" "ABC"
                |> QueryBuilder.scanRequest
                |> client.ScanAsync
                |> Io.ignoreTask)

            assertError output "Cannot parse token at position" e
        }

    [<Fact>]
    let ``Scan, with invalid expression attribute value characters, throws`` () =

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
                |> QueryBuilder.setFilterExpression "XX = :-p"
                |> QueryBuilder.setExpressionAttrValues ":-p" (String pk)
                |> QueryBuilder.scanRequest
                |> client.ScanAsync
                |> Io.ignoreTask)

            assertError output "Cannot parse token at position" e
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Filter, "." on known non map table scalar, throws`` ``with exp att name`` ``looks like scalar but isn't`` =

        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            let req =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if ``with exp att name`` && ``looks like scalar but isn't``
                    then QueryBuilder.setFilterExpression "A.#placeholder   .    Nested.Val"
                   elif ``with exp att name``
                    then QueryBuilder.setFilterExpression "#placeholder.A.Nested.Val"
                   elif ``looks like scalar but isn't``
                    then QueryBuilder.setFilterExpression "A.IndexPk   .   Nested.Val"
                    else QueryBuilder.setFilterExpression "IndexPk.A.Nested.Val"
                |> if ``with exp att name`` then QueryBuilder.setExpressionAttrName "#placeholder" "IndexPk" else id
                |> QueryBuilder.queryRequest

            // act
            if ``looks like scalar but isn't`` then
                do! client.QueryAsync req |> Io.ignoreTask
            else
                let! e = Assert.ThrowsAnyAsync(fun () -> 
                    req
                    |> client.QueryAsync
                    |> Io.ignoreTask)

                // assert
                output.WriteLine(e.ToString())
                Assert.Contains("Table scalar attribute IndexPk is not a map", e.ToString())
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Filter, "." on known non list table scalar, throws`` ``with exp att name`` ``looks like scalar but isn't`` =

        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            let req =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if ``with exp att name`` && ``looks like scalar but isn't``
                    then QueryBuilder.setFilterExpression "A.#placeholder[0]"
                   elif ``with exp att name``
                    then QueryBuilder.setFilterExpression "#placeholder[0]"
                   elif ``looks like scalar but isn't``
                    then QueryBuilder.setFilterExpression "A.IndexPk[0]"
                    else QueryBuilder.setFilterExpression "IndexPk[0]"
                |> if ``with exp att name`` then QueryBuilder.setExpressionAttrName "#placeholder" "IndexPk" else id
                |> QueryBuilder.queryRequest

            // act
            if ``looks like scalar but isn't`` then
                do! client.QueryAsync req |> Io.ignoreTask
            else
                let! e = Assert.ThrowsAnyAsync(fun () -> 
                    req
                    |> client.QueryAsync
                    |> Io.ignoreTask)

                // assert
                output.WriteLine(e.ToString())
                Assert.Contains("Table scalar attribute IndexPk is not a list", e.ToString())
        }

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Filter, [1] on known non list table scalar, throws`` ``with exp att name`` ``looks like scalar but isn't`` =

        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            let req =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if ``with exp att name`` && ``looks like scalar but isn't``
                    then QueryBuilder.setFilterExpression "A.#placeholder   [1]   .Nested.Val"
                   elif ``with exp att name``
                    then QueryBuilder.setFilterExpression "#placeholder[1].A.Nested.Val"
                   elif ``looks like scalar but isn't``
                    then QueryBuilder.setFilterExpression "A.IndexPk   [1]   .Nested.Val"
                    else QueryBuilder.setFilterExpression "IndexPk[1].A.Nested.Val"
                |> if ``with exp att name`` then QueryBuilder.setExpressionAttrName "#placeholder" "IndexPk" else id
                |> QueryBuilder.queryRequest

            // act
            if ``looks like scalar but isn't`` then
                do! client.QueryAsync req |> Io.ignoreTask
            else
                let! e = Assert.ThrowsAnyAsync(fun () -> 
                    req
                    |> client.QueryAsync
                    |> Io.ignoreTask)

                // assert
                output.WriteLine(e.ToString())
                Assert.Contains("Table scalar attribute IndexPk is not a list", e.ToString())
        }

    [<Theory>]
    [<InlineData("JOIN")>]
    [<InlineData("join")>]
    [<InlineData("RaNk")>]
    let ``Filter, with reserved word, throws`` word =

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
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
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
    let ``Scan, with superfluous attr query values, throws`` ``has filter`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client
            let tab = Tables.get true true tables

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if ``has filter`` then QueryBuilder.setFilterExpression "X" else id
                |> QueryBuilder.setExpressionAttrValues ":s" (ItemBuilder.buildItemAttribute "S" "77")
                |> QueryBuilder.scanRequest
                |> client.ScanAsync
                |> Io.ignoreTask)

            Assert.Contains("Expression attribute names or values were not used: :s", e.ToString())
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Scan, with superfluous attr query name, throws`` ``has filter`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let client = client.Client
            let tab = Tables.get true true tables

            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> if ``has filter`` then QueryBuilder.setFilterExpression "X" else id 
                |> QueryBuilder.setExpressionAttrName "#pp" "uu"
                |> QueryBuilder.scanRequest
                |> client.ScanAsync
                |> Io.ignoreTask)

            Assert.Contains("Expression attribute names or values were not used: #pp", e.ToString())
        }

    [<Fact>]
    let ``Filter, "." on query attribute value, throws`` () =
        task {
            // arrange - make sure tables are added to common host
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! e = Assert.ThrowsAnyAsync(fun () -> 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression ":y.BoolValue"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":y" (HashMap (Map.add "BoolValue" (Boolean true) Map.empty))
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync
                |> Io.ignoreTask)

            // assert
            assertError output "Invalid expression, expected form: x.y" e
        }

    [<Fact>]
    let ``Filter, begins_with RHS is number, throws`` () =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"begins_with(LongerString, :n)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":n" (Number 666M)
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync
                |> Io.ignoreTask)

            // assert
            Assert.Contains("Invalid right operand for begins_with", e.ToString())
        }

    [<Theory>]
    [<InlineData("=", true, true)>]
    [<InlineData("<>", true, false)>]
    [<InlineData("<=", false, true)>]
    [<InlineData("<", false, true)>]
    [<InlineData(">=", true, true)>]
    [<InlineData(">", true, false)>]
    let ``Filter, operators with LHS and RHS the same, throws`` operator ``lhs aliased`` ``rhs aliased`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            let lh =  if ``lhs aliased`` then "#" else ""
            let rh =  if ``rhs aliased`` then "#" else ""

            // act
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression $"TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"{lh}Attr1 {operator} {rh}Attr1"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if  ``lhs aliased`` || ``rhs aliased``
                    then QueryBuilder.setExpressionAttrName "#Attr1" "Attr1"
                    else id
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync
                |> Io.ignoreTask)

            // assert
            assertError output $"LHS and RHS of operation {operator} cannot be the same value" e
        }

    [<Theory>]
    [<InlineData("contains", true, true)>]
    [<InlineData("begins_with", true, false)>]
    [<InlineData("contains", false, true)>]
    [<InlineData("begins_with", false, false)>]
    let ``Filter, functions with LHS and RHS the same, throws`` func ``l aliased`` ``r aliased`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            let lh = if ``l aliased`` then "#" else ""
            let rh = if ``r aliased`` then "#" else ""

            // act
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"{func}({lh}Attr1, {rh}Attr1)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if ``l aliased`` || ``r aliased``
                    then QueryBuilder.setExpressionAttrName "#Attr1" "Attr1"
                    else id
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync
                |> Io.ignoreTask)

            // assert
            assertError output $"LHS and RHS of operation {func} cannot be the same value" e
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Filter, contains on binary set, returns correct result`` ``does contain`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "contains(BSData, :b)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":b" (if ``does contain`` then (Map.find "BinaryData" data) else [|111uy|] |> Binary)
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            let expected = if ``does contain`` then  [data] else []
            assertItems (expected, result.Items, true)

            // random double dispose of client
            (client :> IDisposable).Dispose()
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Filter, contains on number set, returns correct result`` ``does contain`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "contains(NSData, :n)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":n" ((if ``does contain`` then sk else -666M) |> Number)
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            let expected = if ``does contain`` then  [data] else []
            assertItems (expected, result.Items, true)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Filter, contains on string set, returns correct result`` ``does contain`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "contains(SSData, :str)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":str" ((if ``does contain`` then pk else "invalid") |> String)
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            let expected = if ``does contain`` then  [data] else []
            assertItems (expected, result.Items, true)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Filter, contains on string, returns correct result`` ``does contain`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output

            let struct (pk, struct (sk, data)) = randomItem tab.hasSk random

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression "contains(LongerString, :str)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":str" ((if ``does contain`` then "is a longer" else "invalid") |> String)
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            let expected = if ``does contain`` then  [data] else []
            assertItems (expected, result.Items, true)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Filter, or, returns correct items`` ``on index`` =
        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true ``on index`` tables
            use client = buildClient output

            let struct (struct (pk1, expected1), struct (pk2, expected2)) =
                randomPartitions 2 tab.hasSk random
                |> List.ofSeq
                |> function
                    | [x; y] -> struct (x, y)
                    | _ -> invalidOp "Expected 2"

            let expected =
                Seq.concat [expected1; expected2]
                |> Seq.map sndT
                |> Seq.sortBy (Map.find "TablePk")

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setFilterExpression "TablePk = :p1 OR TablePk = :p2"
                |> QueryBuilder.setExpressionAttrValues ":p1" (String pk1)
                |> QueryBuilder.setExpressionAttrValues ":p2" (String pk2)
                |> QueryBuilder.scanRequest
                |> client.Client.ScanAsync

            // assert
            if tab.hasSk then Assert.True((Seq.length expected) > 1)
            else Assert.Equal(1, Seq.length expected)
            Assert.Equal(Seq.length expected, result.Count <!> 0)

            Assert.True(result.ScannedCount <!> 0 > result.Items.Count)
            Assert.Equal(Seq.length (allItems tab.hasSk), result.ScannedCount <!> 0)
            assertItems (expected, result.Items, not ``on index``)
        }

    [<Theory>]
    [<InlineData("BoolData", "attribute_exists", true, true)>]
    [<InlineData("BoolData", "attribute_not_exists", false, true)>]
    [<InlineData("invalid", "attribute_exists", false, true)>]
    [<InlineData("invalid", "attribute_not_exists", true, true)>]
    [<InlineData("BoolData", "attribute_exists", true, false)>]
    [<InlineData("BoolData", "attribute_not_exists", false, false)>]
    [<InlineData("invalid", "attribute_exists", false, false)>]
    [<InlineData("invalid", "attribute_not_exists", true, false)>]
    let ``Filter, attribute exists, works correctly`` ``attribute name`` fn exists aliased =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random
            let h = if aliased then "#" else ""

            // act
            let! result =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"{fn}({h}{``attribute name``})"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> if aliased
                    then QueryBuilder.setExpressionAttrName $"{h}{``attribute name``}" ``attribute name``
                    else id
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            if exists then Assert.Equal(1, result.Items.Count)
            else Assert.Equal(0, result.Items.Count)
        }

    [<Theory>]
    [<InlineData("TablePk_Copy", "S", true)>]
    [<InlineData("TableSk_Copy", "N", true)>]
    [<InlineData("BinaryData", "B", true)>]
    [<InlineData("BoolData", "BOOL", true)>]
    [<InlineData("NullData", "NULL", true)>]
    [<InlineData("ListData", "L", true)>]
    [<InlineData("MapData", "M", true)>]
    [<InlineData("SSData", "SS", true)>]
    [<InlineData("NSData", "NS", true)>]
    [<InlineData("BSData", "BS", true)>]
    [<InlineData("BoolData", "S", false)>]
    [<InlineData("invalid", "S", false)>]
    let ``Filter, attribute_type, works correctly`` ``attribute name`` ``test type`` correct =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"attribute_type({``attribute name``}, :t)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":t" (String ``test type``)
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            if correct then Assert.Equal(1, result.Items.Count)
            else Assert.Equal(0, result.Items.Count)
        }

    [<Fact>]
    let ``Filter, attribute_type with invalid arg, throws`` () =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"attribute_type(Something, :t)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.setExpressionAttrValues ":t" (String "invalid")
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync
                |> Io.ignoreTask)

            // assert
            Assert.Contains("Invalid attribute type parameter \"invalid\"", e.ToString())
        }

    [<Fact>]
    let ``Filter, attribute_type with non parameter type, throws`` () = 
        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            let tab = Tables.get true true tables
            use client = buildClient output
            let struct (pk, struct (sk, _)) = randomItem tab.hasSk random

            // act
            let! e = Assert.ThrowsAnyAsync(fun () ->
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression $"attribute_type(Something, SomethingElse)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync
                |> Io.ignoreTask)

            // assert
            assertError output "Right operand for attribute_type must be an expression attribute value" e
        }

    [<Theory>]
    [<ClassData(typedefof<FiveFlags>)>]
    let ``Filter, AND, OR and () tests`` ``negate 1`` ``negate 2`` ``negate 3`` ``parenthesis 1`` ``parenthesis 2`` =

        // build all possible queries of the form x AND y OR z
        // execute in both dynamodb and f# code
        // verify that results match
        let rec randomTrueItem hasSk i =
            if i > 100 then invalidOp "???"

            let item = randomItem hasSk random
            if item |> sndT |> sndT |> Map.find "BoolData" |> function | Boolean x -> x | _ -> invalidOp "X"
            then item
            else randomTrueItem hasSk (i + 1)

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let tab = Tables.get true true tables

            let struct (pk, struct (sk, _)) = randomTrueItem tab.hasSk 1

            let struct (filter, expectedResult) =
                let eq1 = if ``negate 1`` then "<>" else "="
                let condition1 = ``negate 1`` |> not
                let eq2 = if ``negate 2`` then "<>" else "="
                let condition2 = ``negate 2`` |> not
                let eq3 = if ``negate 3`` then "NOT " else ""
                let condition3 = ``negate 3`` |> not

                match struct (``parenthesis 1``, ``parenthesis 2``) with
                | true, true -> struct (
                    $"(TablePk_Copy {eq1} :p AND TableSk_Copy {eq2} :s OR {eq3}BoolData)",
                    (condition1 && condition2 || condition3))
                | true, false -> struct (
                    $"(TablePk_Copy {eq1} :p AND TableSk_Copy {eq2} :s) OR {eq3}BoolData",
                    (condition1 && condition2) || condition3)
                | false, true -> struct (
                    $"TablePk_Copy {eq1} :p AND (TableSk_Copy {eq2} :s OR {eq3}BoolData)",
                    condition1 && (condition2 || condition3))
                | false, false -> struct (
                    $"TablePk_Copy {eq1} :p AND TableSk_Copy {eq2} :s OR {eq3}BoolData",
                    condition1 && condition2 || condition3)

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                // query part will isolate from other records, negate powerful OR and ensure that result is either 1 or 0
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p AND TableSk = :s"
                |> QueryBuilder.setFilterExpression filter
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setExpressionAttrValues ":s" (Number sk)
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            if expectedResult then Assert.Equal(1, Seq.length result.Items)
            else Assert.Equal(0, Seq.length result.Items)
        }

    [<Theory>]
    [<ClassData(typedefof<ThreeFlags>)>]
    let ``Filter, <,<=,>,>=, returns correct items`` ``on index`` lt inclusive =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let tab = Tables.get true true tables

            let allItemsGrouped =
                allItems tab.hasSk
                |> Seq.groupBy (TableItem.asItem >> Map.find "TablePk")
                |> Seq.map structTpl
                |> Seq.sortBy fstT
                |> Array.ofSeq

            Assert.True(Array.length allItemsGrouped > 2)

            let target = Array.length allItemsGrouped / 2 |> Array.get allItemsGrouped |> fstT
            let struct (op, itemFilter) =
                match struct (lt, inclusive) with
                | true, true -> "<=", (fun struct (x, _) -> x <= target) |> Array.takeWhile
                | true, false -> "<", (fun struct (x, _) -> x < target) |> Array.takeWhile
                | false, true -> ">=", (fun struct (x, _) -> x < target) |> Array.skipWhile
                | false, false -> ">", (fun struct (x, _) -> x <= target) |> Array.skipWhile

            output.WriteLine($"Executing op \"{op}\"")

            let expected =
                allItemsGrouped
                |> itemFilter
                |> Seq.collect sndT
                |> Seq.map TableItem.asItem
                |> Array.ofSeq

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if ``on index`` then QueryBuilder.setIndexName "TheIndex" else id
                |> QueryBuilder.setFilterExpression $"TablePk {op} :p"
                |> QueryBuilder.setExpressionAttrValues ":p" target
                |> QueryBuilder.scanRequest
                |> client.Client.ScanAsync

            // assert
            Assert.True((Seq.length expected) > 1)
            Assert.Equal(Seq.length expected, result.Count <!> 0)

            Assert.True(result.ScannedCount <!> 0 > result.Items.Count)
            Assert.Equal(Seq.length (allItems tab.hasSk), result.ScannedCount <!> 0)
            assertItems (expected, result.Items, not ``on index``)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Filter, begins_with, returns correct items`` ``on index`` =

        let expectString =function
            | AttributeValue.String str -> str
            | _ -> invalidOp "Expect string"

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let tab = Tables.get true true tables

            let allItemsGrouped =
                allItems tab.hasSk
                |> Seq.groupBy (TableItem.asItem >> Map.find "IndexSk" >> expectString >> fun x -> x[0].ToString())
                |> Seq.map structTpl
                |> Seq.sortBy fstT
                |> Array.ofSeq

            Assert.True(Array.length allItemsGrouped > 1)

            let target = Array.head allItemsGrouped |> fstT
            let expected =
                allItemsGrouped
                |> Seq.filter (function
                    | struct (x, _) -> x.StartsWith(target))
                |> Seq.collect sndT
                |> Seq.map TableItem.asItem
                |> Array.ofSeq

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if ``on index`` then QueryBuilder.setIndexName "TheIndex" else id
                |> QueryBuilder.setFilterExpression $"begins_with(IndexSk, :p)"
                |> QueryBuilder.setExpressionAttrValues ":p" (String target)
                |> QueryBuilder.scanRequest
                |> client.Client.ScanAsync

            // assert
            Assert.True((Seq.length expected) > 1)
            Assert.Equal(Seq.length expected, result.Count <!> 0)

            Assert.True(result.ScannedCount <!> 0 > result.Items.Count)
            Assert.Equal(Seq.length (allItems tab.hasSk), result.ScannedCount <!> 0)
            assertItems (expected, result.Items, false)
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Filter, between, returns correct items`` ``on index`` =

        task {
            // arrange
            let! tables = sharedTestData ValueNone // (ValueSome output)
            use client = buildClient output
            let tab = Tables.get true true tables

            let allItemsGrouped =
                allItems tab.hasSk
                |> Seq.groupBy (TableItem.asItem >> Map.find "TableSk")
                |> Seq.map structTpl
                |> Seq.sortBy fstT
                |> Array.ofSeq

            Assert.True(Array.length allItemsGrouped > 2)

            let target1 = allItemsGrouped |> Array.head |> fstT
            let target2 = allItemsGrouped |> Array.tail |> Array.head |> fstT
            let expected =
                allItemsGrouped
                |> Seq.filter (function
                    | struct (x, _) -> x >= target1 && x <= target2)
                |> Seq.collect sndT
                |> Seq.map TableItem.asItem
                |> Array.ofSeq

            // act
            let! result =
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName tab.name
                |> if ``on index`` then QueryBuilder.setIndexName "TheIndex" else id
                |> QueryBuilder.setFilterExpression "TableSk BETWEEN :p1 AND :p2 AND TableSk >= :p1"
                |> QueryBuilder.setExpressionAttrValues ":p1" target1
                |> QueryBuilder.setExpressionAttrValues ":p2" target2
                |> QueryBuilder.scanRequest
                |> client.Client.ScanAsync

            // assert
            Assert.True((Seq.length expected) > 1)
            Assert.Equal(Seq.length expected, result.Count <!> 0)

            Assert.True(result.ScannedCount <!> 0 > result.Items.Count)
            Assert.Equal(Seq.length (allItems tab.hasSk), result.ScannedCount <!> 0)
            assertItems (expected, result.Items, false)
        }
