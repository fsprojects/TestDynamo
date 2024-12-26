namespace TestDynamo.Tests

open Amazon.DynamoDBv2
open TestDynamo
open TestDynamo.Utils
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions
open RequestItemTestUtils
open TestDynamo.Model
open Tests.Loggers

type ProjectionTests(output: ITestOutputHelper) =

    let random = randomBuilder output

    [<Fact>]
    let ``Query on table, with project count, returns no records`` () =

        task {
            use client = buildClient output
            let client = client.Client

            let! data = sharedTestData ValueNone //(ValueSome output)
            let table = Tables.get true true data
            use client = buildClient output
            let client = client.Client
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
            use client = buildClient output
            let client = client.Client

            let! data = sharedTestData ValueNone //(ValueSome output)
            let table = Tables.get true true data
            use client = buildClient output
            let client = client.Client
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

    let prepareProjectionTest projection projectAsAttributes =

        task {
            let! data = sharedTestData ValueNone //(ValueSome output)
            let table = Tables.get true true data
            let logger = new TestLogger(output)
            let client = new ClientContainer(cloneHost logger, logger, true)
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
                |> client.Client.PutItemAsync
                |> Io.ignoreTask

            let struct (setProjection, setAttributesToGet) =
                if not projectAsAttributes then QueryBuilder.setProjectionExpression projection, id
                else id, projection.Split(",") |> Seq.map (_.Trim()) |> List.ofSeq |> QueryBuilder.setAttributesToGet

            // arrange
            // act
            return
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setFilterExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String pk)
                |> QueryBuilder.setSelect Select.SPECIFIC_ATTRIBUTES
                |> setProjection
                |> setAttributesToGet
                |> tpl struct (struct (pk, sk), struct (iPk, iSk))
                |> tpl client
        }

    let projectionScanTest projection projectAsAttributes alterReq =

        task {
            let! struct (client, struct (keys, queryBuilder)) = prepareProjectionTest projection projectAsAttributes
            use _ = client

            // arrange
            // act
            let! result = 
                queryBuilder
                |> alterReq
                |> QueryBuilder.scanRequest
                |> client.Client.ScanAsync

            // assert
            Assert.Equal(1, result.Items.Count)
            let output =
                result.Items[0]
                |> itemFromDynamodb
                |> tpl keys

            return output
        }

    let projectionQueryTest projection projectAsAttributes alterReq =

        task {
            let! struct (client, struct(struct(struct (pk, _), _) & keys, queryBuilder)) = prepareProjectionTest projection projectAsAttributes
            use _ = client

            // arrange
            // act
            let! result = 
                queryBuilder
                |> QueryBuilder.setKeyConditionExpression (QueryBuilder.getFilterExpression queryBuilder |> Maybe.expectSome)
                |> QueryBuilder.removeFilterExpression
                |> alterReq
                |> QueryBuilder.queryRequest
                |> client.Client.QueryAsync

            // assert
            Assert.Equal(1, result.Items.Count)
            let output =
                result.Items[0]
                |> itemFromDynamodb
                |> tpl keys

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

    [<Theory>]
    [<ClassData(typedefof<TwoFlags>)>]
    let ``Scan or query on table, with root props, projects correctly`` ``is scan`` ``project as attributes to get`` =

        task {
            // arrange
            let testF =
                if ``is scan`` then projectionScanTest
                else projectionQueryTest

            // act
            let! struct (struct (struct (pk, _), _), result) = testF "TablePk,TheMap,TheList" ``project as attributes to get`` id

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
                projectionScanTest "TheMap.Branch,TheList[1]" false id

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
                projectionScanTest "TheMap.#zero[1],TheList[0].Branch" false (QueryBuilder.setExpressionAttrName "#zero" "0")

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
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionScanTest "TheMap, TheMap" false id)
            assertError output "An attribute or attribute path was updated or projected multiple times" e 
        }

    [<Fact>]
    let ``Scan on table, with duplicate half nested prop, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionScanTest "TheMap, TheMap.TheBranch" false id)
            assertError output "An attribute or attribute path was updated or projected multiple times" e 
        }

    [<Fact>]
    let ``Scan on table, with duplicate nested prop, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionScanTest "TheMap.TheBranch, TheMap.TheBranch" false id)
            assertError output "An attribute or attribute path was updated or projected multiple times" e 
        }

    [<Fact>]
    let ``Scan on table, with duplicate numeric prop, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionScanTest "TheList[1],TheList[1]" false id)
            assertError output "An attribute or attribute path was updated or projected multiple times" e 
        }

    [<Fact>]
    let ``Scan on table, with table scalar accesso, throws`` () =

        task {
            // arrange
            // act
            // assert
            let! e = Assert.ThrowsAnyAsync(fun _ -> projectionScanTest "TablePk.x" false id)
            assertError output "Table scalar attribute TablePk is not a map" e 
        }

    [<Theory>]
    [<ClassData(typedefof<OneFlag>)>]
    let ``Scan on table, numeric prop ordering, projects correctly`` rev =

        task {
            // arrange
            // act
            let! struct (_, result) =
                if rev then projectionScanTest "TheList[0],TheList[1]" false id
                else projectionScanTest "TheList[1],TheList[0]" false id

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
                projectionScanTest "TheMap.#zero,TheMap.Branch" false (QueryBuilder.setExpressionAttrName "#zero" "0")

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
            let! struct (struct (struct (pk, _), _), result) = projectionScanTest "TablePk,Unknown1,TheMap.Unknown2,TheList[123]" false id

            // assert
            Assert.Collection(
                result |> MapUtils.toSeq |> Seq.sortBy fstT,
                (fun struct (k, v) ->
                    Assert.Equal("TablePk", k)
                    Assert.Equal(String pk, v)))
        }