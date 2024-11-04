namespace TestDynamo.Tests

open System.Text
open System.Threading.Tasks
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
open Tests.Loggers

type PagingTests(output: ITestOutputHelper) =

    let random = randomBuilder output

    static let tablePk = nameof PagingTests
    static let indexKey = (-98765).ToString()
    static let sharedTestData =

        let rec put table (client: ITestDynamoClient): int -> Task = function
            | x when x <= 99 -> task { return () } |> Io.ignoreTask
            | i ->
                let t1 =
                    let first50 = ((i <= 149).ToString().ToLower())
                    client.PutItemAsync(
                        table,
                        ItemBuilder.empty
                        |> ItemBuilder.withAttribute "TablePk" "S" tablePk
                        |> ItemBuilder.withAttribute "TableSk" "N" (i.ToString())
                        |> ItemBuilder.withAttribute "IndexPk" "N" indexKey
                        |> ItemBuilder.withAttribute "IndexSk" "S" first50
                        |> ItemBuilder.withAttribute "IsEven" "BOOL" ((i % 2 = 0).ToString().ToLower())
                        |> ItemBuilder.withAttribute "First50" "BOOL" first50
                        |> ItemBuilder.dynamoDbAttributes)
                    |> Io.ignoreTask

                let t2 = put table client (i - 1)
                Task.WhenAll([|t1; t2|])

        let collector = OutputCollector()
        let execution =
            task {
                use writer = new TestLogger(collector)

                // make sure host is initialized
                let! data = sharedTestData (ValueSome collector)
                let host = cloneHost writer
                use client = TestDynamoClient.Create(host, (writer :> Microsoft.Extensions.Logging.ILogger))
                let tab = Tables.get true true data
                do! put tab.name client 199

                return struct (tab, host)
            }

        fun output ->
            task {
                let! t = execution

                match output with
                | ValueSome x -> collector.Emit x
                | ValueNone -> ()

                return t
            }

    let rec executePagedQuery (client: ITestDynamoClient) req: Task<struct (Dictionary<_, _> list list * int)> =
        task {
            let! results = client.QueryAsync req
            Assert.Equal(results.Count, results.Items.Count)

            if results.LastEvaluatedKey.Count = 0
            then return List.singleton (results.Items |> List.ofSeq) |> flip tpl results.ScannedCount
            else
                req.ExclusiveStartKey <- results.LastEvaluatedKey
                let! struct (next, nextScanned) = executePagedQuery client req
                return (results.Items |> List.ofSeq)::next |> flip tpl (results.ScannedCount + nextScanned)
        }

    let rec executePagedScan (client: ITestDynamoClient) req: Task<struct (Dictionary<_, _> list list * int)> =
        task {
            let! results = client.ScanAsync req
            Assert.Equal(results.Count, results.Items.Count)

            if results.LastEvaluatedKey.Count = 0
            then return List.singleton (results.Items |> List.ofSeq) |> flip tpl results.ScannedCount
            else
                req.ExclusiveStartKey <- results.LastEvaluatedKey
                let! struct (next, nextScanned) = executePagedScan client req
                return (results.Items |> List.ofSeq)::next |> flip tpl (results.ScannedCount + nextScanned)
        }

    [<Theory>]
    [<InlineData(true, null)>]
    [<InlineData(false, null)>]
    [<InlineData(true, "even")>]
    [<InlineData(false, "even")>]
    [<InlineData(true, "first")>]
    [<InlineData(false, "first")>]
    [<InlineData(true, "second")>]
    [<InlineData(false, "second")>]
    let ``Query on table, with paging, pages correctly`` forward  ``filter type`` =

        let rec verifySequential increment count next = function
            | (head1: int)::(head2::_ & tail) ->
                Assert.Equal<int>(next head1, head2)
                verifySequential increment (count + increment) next tail
            | _::_ -> count + increment
            | [] -> count

        task {
            use writer = new TestLogger(output)

            // arrange
            let! struct (table, host) = sharedTestData ValueNone // (ValueSome output)
            use client = TestDynamoClient.Create(host, (writer :> Microsoft.Extensions.Logging.ILogger))
            // act
            let! struct (resultPages, resultScanned) = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setKeyConditionExpression "TablePk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (String tablePk)
                |> match ``filter type`` with
                   | null -> id
                   | "even" -> QueryBuilder.setFilterExpression "IsEven"
                   | "first" -> QueryBuilder.setFilterExpression "First50"
                   | "second" -> QueryBuilder.setFilterExpression "NOT First50"
                   | x -> invalidOp x
                |> QueryBuilder.setForwards forward
                |> QueryBuilder.setLimit 6
                |> QueryBuilder.queryRequest
                |> executePagedQuery client

            // assert
            let pageCount = List.length resultPages
            let struct (increment, pageSize, totalCount) =
                match ``filter type`` with
                | null -> struct (1, 6, 100)
                | "even" -> struct (2, 6, 50)
                | "first" -> struct (1, 6, 50)
                | "second" -> struct (1, 6, 50)
                | x -> invalidOp x

            resultPages
            |> List.mapi tpl
            |> List.fold (fun _ struct(i, x) ->

                Assert.NotEmpty(x)
                match i with
                | i when i = pageCount - 1 -> Assert.True(List.length x <= pageSize)
                | _ -> Assert.Equal(pageSize, List.length x)) ()

            let count =                
                resultPages
                |> List.collect id
                |> List.map (fun x -> int x["TableSk"].N)
                |> verifySequential 1 0 (if forward then ((+)increment) else flip (-)increment)

            Assert.Equal(count, totalCount)
            Assert.Equal(100, resultScanned)
        }

    [<Theory>]
    [<InlineData(null)>]
    [<InlineData("even")>]
    [<InlineData("first")>]
    [<InlineData("second")>]
    let ``Scan on table, with paging, pages correctly``  ``filter type`` =

        task {
            use writer = new TestLogger(output)

            // arrange
            let! struct (table, host) = sharedTestData ValueNone // (ValueSome output)
            use client = TestDynamoClient.Create(host, (writer :> Microsoft.Extensions.Logging.ILogger))
            // act
            let! struct (resultPages, resultScanned) = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> match ``filter type`` with
                   | null -> id
                   | "even" -> QueryBuilder.setFilterExpression "IsEven"
                   | "first" -> QueryBuilder.setFilterExpression "First50"
                   | "second" -> QueryBuilder.setFilterExpression "NOT First50"
                   | x -> invalidOp x
                |> QueryBuilder.setLimit 6
                |> QueryBuilder.scanRequest
                |> executePagedScan client

            // assert
            let pageCount = List.length resultPages
            let struct (pageSize, totalCount) =
                match ``filter type`` with
                | null -> struct (6, ValueNone)
                | "even" -> struct (6, ValueSome 50)
                | "first" -> struct (6, ValueSome 50)
                | "second" -> struct (6, ValueSome 50)
                | x -> invalidOp x

            resultPages
            |> List.mapi tpl
            |> List.fold (fun _ struct(i, x) ->
                Assert.NotEmpty(x)
                match i with
                | i when i = pageCount - 1 -> Assert.True(List.length x <= pageSize)
                | _ -> Assert.Equal(pageSize, List.length x)) ()

            let count = Seq.map Seq.length resultPages |> Seq.sum 
            match totalCount with
            | ValueSome tc -> Assert.Equal(count, tc)
            | ValueNone -> Assert.Equal(count, resultScanned)
        }

    [<Theory>]
    [<InlineData("Query-backwards", true, false, false)>]
    [<InlineData("Query-backwards", false, true, false)>]
    [<InlineData("Query-backwards", false, false, true)>]
    [<InlineData("Query", true, false, false)>]
    [<InlineData("Query", false, true, false)>]
    [<InlineData("Query", false, false, true)>]
    [<InlineData("Scan", true, false, false)>]
    [<InlineData("Scan", false, true, false)>]
    [<InlineData("Scan", false, false, true)>]
    let ``Query and scan on packed index, with min paging, returns 2 pages`` opType limit maxScanItems maxPageSizeBytes  =

        task {
            use writer = new TestLogger(output)
            let struct (query, forwards) =
                match opType with
                | "Query-backwards" -> struct (true, false)
                | "Query" -> struct (true, true)
                | "Scan" -> struct (false, true)
                | x -> invalidOp x

            // arrange
            let! struct (table, host) = sharedTestData ValueNone // (ValueSome output)
            use client = TestDynamoClient.Create(host, (writer :> Microsoft.Extensions.Logging.ILogger))
            client.SetScanLimits
                { maxScanItems = if maxScanItems then 1 else 100000
                  maxPageSizeBytes = if maxPageSizeBytes then 1 else 100000 }

            let req = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setIndexName "TheIndex"
                |> if query
                    then QueryBuilder.setKeyConditionExpression "IndexPk = :p"
                    else QueryBuilder.setFilterExpression "IndexPk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (Number (decimal indexKey))
                |> if query then QueryBuilder.setForwards forwards else id
                |> if limit then QueryBuilder.setLimit 6 else id

            // act
            let! struct (resultPages, resultScanned) =
                if query
                then QueryBuilder.queryRequest req |> executePagedQuery client
                else QueryBuilder.scanRequest req |> executePagedScan client

            // assert
            let results =
                List.map List.length resultPages
                |> List.sort

            Assert.Equal<int>([0;50;50], results)
            Assert.Equal(100, resultScanned)
        }

    [<Theory>]
    [<InlineData("Query-backwards", true, false, false)>]
    [<InlineData("Query-backwards", false, true, false)>]
    [<InlineData("Query-backwards", false, false, true)>]
    [<InlineData("Query", true, false, false)>]
    [<InlineData("Query", false, true, false)>]
    [<InlineData("Query", false, false, true)>]
    [<InlineData("Scan", true, false, false)>]
    [<InlineData("Scan", false, true, false)>]
    [<InlineData("Scan", false, false, true)>]
    let ``Query and scan on packed index, with min paging + 1, returns 1 page`` opType limit maxScanItems maxPageSizeBytes  =

        let singleItemSize tableName (client: ITestDynamoClient) =
            client.GetTable tableName
            |> _.GetValues()
            |> Seq.map _.InternalItem
            |> Seq.filter (
                Item.attributes
                >> MapUtils.tryFind "TablePk"
                >> ValueOption.map ((=) (String tablePk))
                >> ValueOption.defaultValue false)
            |> Seq.map Item.size
            |> Seq.head

        task {
            use writer = new TestLogger(output)
            let struct (query, forwards) =
                match opType with
                | "Query-backwards" -> struct (true, false)
                | "Query" -> struct (true, true)
                | "Scan" -> struct (false, true)
                | x -> invalidOp x

            // arrange
            let! struct (table, host) = sharedTestData ValueNone // (ValueSome output)
            use client = TestDynamoClient.Create(host, (writer :> Microsoft.Extensions.Logging.ILogger))
            client.SetScanLimits
                { maxScanItems = if maxScanItems then 51 else 100000
                  maxPageSizeBytes = if maxPageSizeBytes then (singleItemSize table.name client |> float |> (*) 50.1 |> int) else 100000 }

            let req = 
                QueryBuilder.empty (ValueSome random)
                |> QueryBuilder.setTableName table.name
                |> QueryBuilder.setIndexName "TheIndex"
                |> if query
                    then QueryBuilder.setKeyConditionExpression "IndexPk = :p"
                    else QueryBuilder.setFilterExpression "IndexPk = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (Number (decimal indexKey))
                |> if query then QueryBuilder.setForwards forwards else id
                |> if limit then QueryBuilder.setLimit 51 else id

            // act
            let! struct (resultPages, resultScanned) =
                if query
                then QueryBuilder.queryRequest req |> executePagedQuery client
                else QueryBuilder.scanRequest req |> executePagedScan client

            // assert
            let results =
                List.map List.length resultPages
                |> List.sort

            Assert.Equal<int>([0;100], results)
            Assert.Equal(100, resultScanned)
        }

    [<Fact>]
    let ``Various size calculation smoke tests`` () =
        // NOTE: values are not 100% accurate due to round to nearest 10

        // arrange
        let baseItem =
            Map.add "X1" (String "abcdefghi") Map.empty

        Assert.Equal(20, Item.create "" baseItem |> Item.size)

        let sizeDiff expected item =
            let bI = Item.create "" baseItem
            let i = Item.create "" item

            Assert.Equal(expected, Item.size i - Item.size bI)

        // N
        Number 123456789M
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 10

        // S
        String "123456789M"
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 10

        // B
        Binary (Encoding.UTF8.GetBytes "123456789M")
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 10

        // skipping bool and null as values are too small for rounding error

        // quad nested map
        HashMap baseItem
        |> (flip (Map.add "D4") baseItem >> HashMap)
        |> (flip (Map.add "D3") baseItem >> HashMap)
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 50

        // quad nested list
        HashMap baseItem
        |> (Array.singleton >> CompressedList >> AttributeList)
        |> (Array.singleton >> CompressedList >> AttributeList)
        |> (Array.singleton >> CompressedList >> AttributeList)
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 20

        // quad list
        [|HashMap baseItem;HashMap baseItem;HashMap baseItem;HashMap baseItem|]
        |> CompressedList 
        |> AttributeList
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 60

        // numbers
        [|
            Number 0M
            Number -0M
            Number 12345678M
            Number -12345678M
            Number 1234.5678M
            Number -1234.5678M
            Number 0.5678M
            Number -0.5678M
        |]
        |> CompressedList
        |> AttributeList
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 60

        // NS
        [|
            Number 0M
            Number 12345678M
            Number -12345678M
            Number 1234.5678M
            Number -1234.5678M
            Number 0.5678M
            Number -0.5678M
        |]
        |> Seq.ofArray
        |> AttributeSet.create
        |> HashSet
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 60

        // SS
        [|
            String "0"
            String "12345678"
            String "-12345678"
            String "1234.5678"
            String "-1234.5678"
            String "0.5678"
            String "-0.5678"
        |]
        |> Seq.ofArray
        |> AttributeSet.create
        |> HashSet
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 50

        // BS
        [|
            Binary (Encoding.UTF8.GetBytes "0")
            Binary (Encoding.UTF8.GetBytes  "12345678")
            Binary (Encoding.UTF8.GetBytes  "-12345678")
            Binary (Encoding.UTF8.GetBytes  "1234.5678")
            Binary (Encoding.UTF8.GetBytes  "-1234.5678")
            Binary (Encoding.UTF8.GetBytes  "0.5678")
            Binary (Encoding.UTF8.GetBytes  "-0.5678")
        |]
        |> Seq.ofArray
        |> AttributeSet.create
        |> HashSet
        |> (flip (Map.add "D2") baseItem)
        |> sizeDiff 50