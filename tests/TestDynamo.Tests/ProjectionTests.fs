namespace TestDynamo.Tests

open Amazon.DynamoDBv2
open TestDynamo
open TestDynamo.Client
open TestDynamo.Utils
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open TestDynamo.Client.ItemMapper
open Tests.Loggers

type ProjectionTests(output: ITestOutputHelper) =

    let random = randomBuilder output

    [<Fact>]
    let ``Query on table, with project count, returns no records`` () =

        task {
            use client = buildClient (ValueSome output)

            let! data = sharedTestData ValueNone //(ValueSome output)
            let table = Tables.get true true data
            use client = buildClient(ValueSome output)
            let struct (pk, _) = randomItem true random

            // arrange
            // act
            let! result = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setSelect Select.COUNT
                |> QueryBuilder.queryRequest
                |> client.QueryAsync

            // assert
            Assert.Equal(0, result.Items.Count)
            Assert.True(result.Count > 0)
        }

    [<Fact>]
    let ``Scan on table, with project SPECIFIC_ATTRIBUTES but no projection, returns empty records`` () =

        task {
            use client = buildClient (ValueSome output)

            let! data = sharedTestData ValueNone //(ValueSome output)
            let table = Tables.get true true data
            use client = buildClient(ValueSome output)
            let struct (pk, _) = randomItem true random

            // arrange
            // act
            let! result = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setFilterExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setSelect Select.SPECIFIC_ATTRIBUTES
                |> QueryBuilder.scanRequest
                |> client.ScanAsync

            // assert
            Assert.True(result.Items.Count > 0)

            for r in result.Items do
                Assert.Equal(0, r.Count)
        }

    let projectionTest projection alterReq =

        task {
            let! data = sharedTestData ValueNone //(ValueSome output)
            let table = Tables.get true true data
            use logger = new TestLogger(output)
            let client = TestDynamoClient.Create(cloneHost (ValueSome logger))
            let pk = $"id-{IncrementingId.next()}"
            let sk = (IncrementingId.next()).Value |> decimal
            let iPk = (IncrementingId.next()).Value |> decimal
            let iSk = $"id-{IncrementingId.next()}"

            let struct (map, _) = buildDeeplyNestedMap()
            map.M["Branch"] <-
                let attr = DynamoAttributeValue()
                attr.S <- "MapBranch"
                attr

            map.M["0"].L.Add(
                let attr = DynamoAttributeValue()
                attr.S <- "MapBranch2"
                attr)

            let struct (list, _) = buildDeeplyNestedList()
            list.L.Add(
                let attr = DynamoAttributeValue()
                attr.S <- "ListBranch"
                attr)
            list.L[0].M["Branch"] <-
                let attr = DynamoAttributeValue()
                attr.S <- "ListBranch2"
                attr

            do!
                ItemBuilder.empty
                |> ItemBuilder.withTableName table.name
                |> ItemBuilder.withAttribute "TablePk" "S" pk
                |> ItemBuilder.withAttribute "TableSk" "N" (sk.ToString())
                |> ItemBuilder.withAttribute "IndexPk" "N" (iPk.ToString())
                |> ItemBuilder.withAttribute "IndexSk" "S" iSk
                |> ItemBuilder.withAttribute "Something" "S" "Else"
                |> ItemBuilder.addAttribute "TheMap" map
                |> ItemBuilder.addAttribute "TheList" list
                |> ItemBuilder.asPutReq
                |> client.PutItemAsync
                |> Io.ignoreTask

            // arrange
            // act
            let! result = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setFilterExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setSelect Select.SPECIFIC_ATTRIBUTES
                |> QueryBuilder.setProjectionExpression projection
                |> alterReq
                |> QueryBuilder.scanRequest
                |> client.ScanAsync

            // assert
            Assert.Equal(1, result.Items.Count)
            let output =
                result.Items[0]
                |> itemFromDynamodb "$"
                |> tpl struct (struct (pk, sk), struct (iPk, iSk))

            return output
        }

    let theMap =
        String "EndMap"
        |> flip (Map.add "0") Map.empty |> HashMap
        |> Array.singleton
        |> flip Array.append [|String "MapBranch2"|]
        |> CompressedList
        |> AttributeList
        |> flip (Map.add "0") Map.empty
        |> Map.add "Branch" (String "MapBranch") |> HashMap

    let theList =
        String "EndList"
        |> Array.singleton
        |> CompressedList
        |> AttributeList
        |> flip (Map.add "0") Map.empty
        |> Map.add "Branch" (String "ListBranch2")
        |> HashMap
        |> Array.singleton
        |> flip Array.append [|String "ListBranch"|]
        |> CompressedList
        |> AttributeList

    [<Fact>]
    let ``Scan on table, with root props, projects correctly`` () =

        task {
            // arrange
            // act
            let! struct (struct (struct (pk, _), _), result) = projectionTest "TablePk,TheMap,TheList" id

            // assert
            Assert.Collection(
                result |> MapUtils.toSeq |> Seq.sortBy fstT,
                (fun struct (k, v) ->
                    Assert.Equal("TablePk", k)
                    Assert.Equal(String pk, v)),
                (fun struct (k, v) ->
                    Assert.Equal("TheList", k)
                    Assert.Equal(theList, v)),
                (fun struct (k, v) ->
                    Assert.Equal("TheMap", k)
                    Assert.Equal(theMap, v)))
        }

    [<Fact>]
    let ``Scan on table, with nested props, projects correctly`` () =

        task {
            // arrange
            // act
            let! struct (_, result) =
                projectionTest "TheMap.Branch,TheList[1]" id

            // assert
            Assert.Collection(
                result |> MapUtils.toSeq |> Seq.sortBy fstT,
                (fun struct (k, v) ->
                    Assert.Equal("TheList", k)
                    Assert.Equal(AttributeList (CompressedList [|String "ListBranch"|]), v)),
                (fun struct (k, v) ->
                    Assert.Equal("TheMap", k)
                    Assert.Equal(HashMap (Map.add "Branch" (String "MapBranch") Map.empty), v)))
        }

    [<Fact>]
    let ``Scan on table, with double nested props, projects correctly`` () =

        task {
            // arrange
            // act
            let! struct (_, result) =
                projectionTest "TheMap.#zero[1],TheList[0].Branch" (QueryBuilder.setExpressionAttrName "#zero" "0")

            // assert
            Assert.Collection(
                result |> MapUtils.toSeq |> Seq.sortBy fstT,
                (fun struct (k, v) ->
                    Assert.Equal("TheList", k)
                    Assert.Equal(AttributeList (CompressedList [|(Map.add "Branch" (String "ListBranch2") Map.empty) |> HashMap|]), v)),
                (fun struct (k, v) ->
                    Assert.Equal("TheMap", k)
                    Assert.Equal(HashMap (Map.add "0" (AttributeList (CompressedList [|String "MapBranch2"|])) Map.empty), v)))
        }

    [<Fact>]
    let ``Scan on table, with duplicate prop, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionTest "TheMap, TheMap" id)
            assertError output "An attribute or attribute path was updated or projected multiple times" e 
        }

    [<Fact>]
    let ``Scan on table, with duplicate half nested prop, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionTest "TheMap, TheMap.TheBranch" id)
            assertError output "An attribute or attribute path was updated or projected multiple times" e 
        }

    [<Fact>]
    let ``Scan on table, with duplicate nested prop, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionTest "TheMap.TheBranch, TheMap.TheBranch" id)
            assertError output "An attribute or attribute path was updated or projected multiple times" e 
        }

    [<Fact>]
    let ``Scan on table, with duplicate numeric prop, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionTest "TheList[1],TheList[1]" id)
            assertError output "An attribute or attribute path was updated or projected multiple times" e 
        }

    [<Fact>]
    let ``Scan on table, with table scalar accesso, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionTest "TablePk.x" id)
            assertError output "Table scalar attribute TablePk is not a map" e 
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Scan on table, numeric prop ordering, projects correctly`` rev =

        task {
            // arrange
            // act
            let! struct (_, result) =
                if rev then projectionTest "TheList[0],TheList[1]" id
                else projectionTest "TheList[1],TheList[0]" id

            // assert
            Assert.Collection(
                result |> MapUtils.toSeq |> Seq.sortBy fstT,
                (fun struct (k, v) ->
                    Assert.Equal("TheList", k)
                    Assert.Equal(theList, v)))
        }

    [<Fact>]
    let ``Scan on table, full map specified in parts, projects correctly`` () =

        task {
            // arrange
            // act
            let! struct (_, result) =
                projectionTest "TheMap.#zero,TheMap.Branch" (QueryBuilder.setExpressionAttrName "#zero" "0")

            // assert
            Assert.Collection(
                result |> MapUtils.toSeq |> Seq.sortBy fstT,
                (fun struct (k, v) ->
                    Assert.Equal("TheMap", k)
                    Assert.Equal(theMap, v)))
        }

    [<Fact>]
    let ``Scan on table, with unknown props, projects correctly`` () =

        task {
            // arrange
            // act
            let! struct (struct (struct (pk, _), _), result) = projectionTest "TablePk,Unknown1,TheMap.Unknown2,TheList[123]" id

            // assert
            Assert.Collection(
                result |> MapUtils.toSeq |> Seq.sortBy fstT,
                (fun struct (k, v) ->
                    Assert.Equal("TablePk", k)
                    Assert.Equal(String pk, v)))
        }