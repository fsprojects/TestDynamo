
namespace TestDynamo.Tests

open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Api.FSharp
open TestDynamo.Client
open TestDynamo.Model
open TestDynamo.Utils
open Tests.Items
open Tests.Requests.Queries
open Tests.Utils
open Xunit
open Xunit.Abstractions

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue

type SmokeTests(output: ITestOutputHelper) =

    [<Fact>]
    let ``Create table, put item, query smoke tests`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = addTable client false

            let item =
                ItemBuilder.empty
                |> ItemBuilder.withTableName tableName
                |> ItemBuilder.withAttribute "PartitionKey" "S" "x"
                |> ItemBuilder.withAttribute "SortKey" "N" "8"
                |> ItemBuilder.withAttribute "Something" "S" "Else"

            let q1 =
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName tableName
                |> QueryBuilder.setKeyConditionExpression "PartitionKey = :p AND SortKey = :s"
                |> QueryBuilder.setExpressionAttrValues ":p" (ItemBuilder.buildItemAttribute "S" "x")
                |> QueryBuilder.setExpressionAttrValues ":s" (ItemBuilder.buildItemAttribute "N" "8")
                |> QueryBuilder.queryRequest

            // act
            let! _ = client.PutItemAsync(
                ItemBuilder.tableName item,
                ItemBuilder.dynamoDbAttributes item)

            let! result = client.QueryAsync q1

            // assert
            let item = Assert.Single result.Items
            Assert.Equal("x", item["PartitionKey"].S)
            Assert.Equal("Else", item["Something"].S)
            Assert.Equal("8", item["SortKey"].N)

            Assert.Equal(3, item.Count)
        }

    [<Fact>]
    let ``Create table, describe table smoke test`` () =

        task {
            use client = buildClient output
            let client = client.Client

            // arrange
            let! tableName = addTable client false
            
            // act
            let! t = client.DescribeTableAsync(tableName)

            // assert
            Assert.Equal("PartitionKey", t.Table.KeySchema[0].AttributeName)
            Assert.Equal("HASH", t.Table.KeySchema[0].KeyType.Value)
            Assert.Equal("SortKey", t.Table.KeySchema[1].AttributeName)
            Assert.Equal("RANGE", t.Table.KeySchema[1].KeyType.Value)
        }

    [<Fact>]        
    let ``addCancellationTokenNetStandard tests`` () =
        let doAndReturn7 () =
            task {
                do! Task.Delay(100)
                return 7
            }
            
        task {
            let! v1 = doAndReturn7() |> Io.addCancellationTokenNetStandard CancellationToken.None
            Assert.Equal(7, v1)
            
            use cts = new CancellationTokenSource(10)
            let! e = Assert.ThrowsAnyAsync(fun _ -> doAndReturn7() |> Io.addCancellationTokenNetStandard cts.Token |> Io.ignoreTask)
            assertError output "A task was canceled" e
        }

    [<Fact>]
    let ``List tables smoke test`` () =

        task {
            use client = TestDynamoClient.CreateClient()

            // arrange
            let! tableName1 = addTable client false
            let! tableName2 = addTable client false
            
            let tableNames = [|tableName1; tableName2|] |> Array.sort
            
            // act
            let! t = client.ListTablesAsync()

            // assert
            Assert.Equal(2, t.TableNames.Count)
            
            
            // act
            let! t = client.ListTablesAsync("0000000")

            // assert
            Assert.Equal(2, t.TableNames.Count)
            
            
            // act
            let! t = client.ListTablesAsync(1)

            // assert
            Assert.Equal(1, t.TableNames.Count)
            Assert.Equal(tableNames[0], t.TableNames[0])
            
            
            // act
            let! t = client.ListTablesAsync(t.LastEvaluatedTableName, 10)

            // assert
            Assert.Equal(1, t.TableNames.Count)
            Assert.Equal(tableNames[1], t.TableNames[0])
            
            
            // act
            let! t = client.ListTablesAsync(tableNames[0], 1)

            // assert
            Assert.Equal(1, t.TableNames.Count)
            Assert.Equal(tableNames[1], t.TableNames[0])
        }

    [<Fact>]
    let ``List global tables smoke test`` () =
        
        let addGlobalTable client =
            task {
                let! tableName = addTable client true
                let req = CreateGlobalTableRequest()
                req.GlobalTableName <- tableName
                req.ReplicationGroup <- System.Collections.Generic.List()
                req.ReplicationGroup.Add(Replica())
                req.ReplicationGroup[0].RegionName <- "r2"
                do! client.CreateGlobalTableAsync req |> Io.ignoreTask
                return tableName
            }

        task {
            use host = new GlobalDatabase()
            use client = TestDynamoClient.createGlobalClient ValueNone (ValueSome {regionId = "r1" }) (ValueSome host)

            // arrange
            let! tableName1 = addGlobalTable client
            let! tableName2 = addGlobalTable client
            
            let tableNames = [|tableName1; tableName2|] |> Array.sort
            
            // act
            let req = ListGlobalTablesRequest()
            let! t = client.ListGlobalTablesAsync req

            // assert
            Assert.Equal(2, t.GlobalTables.Count)
            
            
            // act
            let req = ListGlobalTablesRequest()
            req.ExclusiveStartGlobalTableName <- "000000000000"
            let! t = client.ListGlobalTablesAsync req

            // assert
            Assert.Equal(2, t.GlobalTables.Count)
            
            
            // act
            let req = ListGlobalTablesRequest()
            req.Limit <- 1
            let! t = client.ListGlobalTablesAsync req

            // assert
            Assert.Equal(1, t.GlobalTables.Count)
            Assert.Equal(tableNames[0], t.GlobalTables[0].GlobalTableName)
            
            
            // act
            let req = ListGlobalTablesRequest()
            req.Limit <- 10
            req.ExclusiveStartGlobalTableName <- t.LastEvaluatedGlobalTableName
            let! t = client.ListGlobalTablesAsync req

            // assert
            Assert.Equal(1, t.GlobalTables.Count)
            Assert.Equal(tableNames[1], t.GlobalTables[0].GlobalTableName)
            
            
            // act
            let req = ListGlobalTablesRequest()
            req.Limit <- 1
            req.ExclusiveStartGlobalTableName <- tableNames[0]
            let! t = client.ListGlobalTablesAsync req

            // assert
            Assert.Equal(1, t.GlobalTables.Count)
            Assert.Equal(tableNames[1], t.GlobalTables[0].GlobalTableName)
        }

    [<Fact>]
    let ``Create global table, describe global table smoke test`` () =

        task {
            // arrange
            use commonHost = new GlobalDatabase()
            use client = TestDynamoClient.createGlobalClient ValueNone (ValueSome {regionId = "r1" }) (ValueSome commonHost)

            let! tableName = addTable client true
            let req = CreateGlobalTableRequest()
            req.GlobalTableName <- tableName
            req.ReplicationGroup <- System.Collections.Generic.List()
            req.ReplicationGroup.Add(Replica())
            req.ReplicationGroup[0].RegionName <- "r2"
            do! client.CreateGlobalTableAsync req |> Io.ignoreTask
            
            // act
            let r =
                let rr = DescribeGlobalTableRequest()
                rr.GlobalTableName <- tableName
                rr
            let! t = client.DescribeGlobalTableAsync(r)

            // assert
            Assert.Equal(tableName, t.GlobalTableDescription.GlobalTableName)
            Assert.Equal(2, t.GlobalTableDescription.ReplicationGroup.Count)
            Assert.Equal("r1", t.GlobalTableDescription.ReplicationGroup[0].RegionName)
            Assert.Equal("r2", t.GlobalTableDescription.ReplicationGroup[1].RegionName)
        }

    [<Fact>]
    let ``Create local table, describe as global, fails`` () =

        task {
            // arrange
            use commonHost = new GlobalDatabase()
            use client = TestDynamoClient.createGlobalClient ValueNone (ValueSome {regionId = "r1" }) (ValueSome commonHost)
            let! tableName = addTable client true
            
            let r =
                let rr = DescribeGlobalTableRequest()
                rr.GlobalTableName <- tableName
                rr
            
            // act
            let! err = Assert.ThrowsAnyAsync(fun _ -> client.DescribeGlobalTableAsync(r))
            
            // assert
            assertError output "is not a global table" err
        }

    [<Theory>]
    [<InlineData(0)>]
    [<InlineData(1)>]
    [<InlineData(2)>]
    [<InlineData(4)>]
    [<InlineData(5)>]
    let ``A bunch of comparer tests`` alterABit =
        let buildMap1 alterABit =
            Map.empty
            |> Map.add "P1" AttributeValue.Null
            |> Map.add "P2" (
                Map.empty
                |> Map.add "P3" (AttributeValue.String (if alterABit = 1 then "xy" else "xz"))
                |> Map.add "P4" (AttributeValue.AttributeList (Maybe.traverse [
                    AttributeSet.create [
                        AttributeValue.Number 1M; AttributeValue.Number (if alterABit = 2 then 7M else 8M)
                    ] |> AttributeValue.HashSet |> ValueSome

                    AttributeSet.create [
                        AttributeValue.String "1"; AttributeValue.String (if alterABit = 3 then "7" else "8")
                    ] |> AttributeValue.HashSet |> ValueSome

                    AttributeSet.create [
                        AttributeValue.Binary [|1uy|]; AttributeValue.Binary [|if alterABit = 4 then 7uy else 8uy|]
                    ] |> AttributeValue.HashSet |> ValueSome

                    if alterABit = 5 then AttributeValue.Null |> ValueSome else ValueNone
                ] |> Array.ofSeq |> CompressedList))
                |> AttributeValue.HashMap)
            |> AttributeValue.HashMap

        let map1 = buildMap1 0
        let map2 = buildMap1 alterABit

        match alterABit with
        | 0 ->
            Assert.True((map1 = map2))
            Assert.Equal(0, compare map1 map2)
            Assert.Equal(map1.GetHashCode(), map2.GetHashCode())
        | _ ->
            Assert.False((map1 = map2))
            Assert.NotEqual(0, compare map1 map2)
            Assert.NotEqual(map1.GetHashCode(), map2.GetHashCode())

    [<Fact>]
    let ``Memoization tests`` () =
        // arrange
        let fn = memoize (ValueSome (2, 4)) id (fun (x: int) -> obj()) >> sndT

        // act1, assert1
        let req1 = fn 1
        let req2 = fn 2
        let req3 = fn 3
        let req4 = fn 4

        Assert.Equal(req1, fn 1)
        Assert.Equal(req2, fn 2)
        Assert.Equal(req3, fn 3)
        Assert.Equal(req4, fn 4)

        // act2, assert2
        let req5 = fn 5
        Assert.Equal(req4, fn 4)
        Assert.Equal(req5, fn 5)
        Assert.NotEqual(req1, fn 1)
        Assert.NotEqual(req2, fn 2)
        Assert.NotEqual(req3, fn 3)
