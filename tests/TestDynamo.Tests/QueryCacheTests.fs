
namespace TestDynamo.Tests

open System.Text.RegularExpressions
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Microsoft.Extensions.Logging
open TestDynamo
open Tests.ClientLoggerContainer
open Tests.Items
open Tests.Requests.Queries
open Tests.Table
open Tests.Utils
open Xunit
open Xunit.Abstractions
open TestDynamo.Utils
open Tests.Loggers

type QueryCacheTests(output: ITestOutputHelper) =

    [<Fact>]
    let ``Big cache test: 1. create table; 2. do query+filter+projection; 3. do different query; 4. do different filter; 5. do different projection; 6. alter table and do query`` () =

        let execute (f: AmazonDynamoDBClient -> Task) =
            task {
                use recorder = new TestLogger(output, LogLevel.Debug)
                use client = TestDynamoClientBuilder.Create(commonHost, recorder)
                recorder.Record true

                do! f client
                let rcd = recorder.RecordedStrings()
                recorder.Disable()

                return rcd
            }

        let assertRecord count exact record records =
            task {
                let! records = records
                // rx removes log prefix
                let filter = if exact then Regex(@"^\s*\d+\s*\>\s*" + Regex.Escape(record) + "$") else Regex(Regex.Escape(record))
                let found = Seq.filter (fun (x: string) -> filter.IsMatch x) records |> List.ofSeq
                Assert.True((List.length found = count), $"{record}, exact: {exact}, expected count: {count}, actual {found}")

                return records
            }

        let executeQuery =
            let rec executeQuery recurse filterCompiler projectionCompiler queryExp filterExp projectionExp query =

                // invert meaning of inputs (inputs mean "was compiled", instead of "cache hit")
                let oppString = not >> _.ToString().ToLower()
                let filterCompiler = ValueOption.map oppString filterCompiler
                let projectionCompiler = ValueOption.map oppString projectionCompiler
                let queryExp = oppString queryExp
                let filterExp = oppString filterExp
                let projectionExp = oppString projectionExp

                task {
                    do!
                        execute query
                        |> (
                            ValueOption.map (fun x -> assertRecord 1 true $"Pre-built filter compiler cache hit: {x}") filterCompiler
                            |> ValueOption.defaultValue (assertRecord 0 false "Pre-built filter compiler cache hit"))
                        |> (
                            ValueOption.map (fun x -> assertRecord 1 true $"Pre-built projection compiler cache hit: {x}") projectionCompiler
                            |> ValueOption.defaultValue (assertRecord 0 false "Pre-built projection compiler cache hit"))
                        |> assertRecord 1 true $"Pre-compiled index opener cache hit: {queryExp}"
                        |> assertRecord 1 true $"Pre-compiled filter expression cache hit: {filterExp}"
                        |> assertRecord 1 true $"Pre-compiled projection expression cache hit: {projectionExp}"
                        |> Io.ignoreTask

                    if recurse then
                        output.WriteLine("NEGATIVE")
                        do! executeQuery false ValueNone ValueNone false false false query
                    return ()
                }

            executeQuery true

        task {
            use setupRecorder = new TestLogger(output)
            let setupClient = TestDynamoClientBuilder.Create(commonHost, setupRecorder)
            // Arrange
            let table = nameof QueryCacheTests
            let pk = $"{table}Pk"
            let sk = $"{table}Sk"
            let indexName = $"{table}Index"
            let indexPk = $"{table}IndexPk"

            let baseReq = 
                QueryBuilder.empty ValueNone
                |> QueryBuilder.setTableName (nameof QueryCacheTests)
                |> QueryBuilder.setKeyConditionExpression $"{pk} = :p"
                |> QueryBuilder.setFilterExpression "Something = :p"
                |> QueryBuilder.setSelect Select.SPECIFIC_ATTRIBUTES
                |> QueryBuilder.setProjectionExpression "Something"
                |> QueryBuilder.setExpressionAttrValues ":p" (Model.AttributeValue.String "123")

            let baseQuery (client: AmazonDynamoDBClient) = 
                baseReq
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask

            let newIndexQuery (client: AmazonDynamoDBClient) = 
                baseReq
                |> QueryBuilder.setIndexName indexName
                |> QueryBuilder.setKeyConditionExpression $"{indexPk} = :p" 
                |> QueryBuilder.setFilterExpression "Something = :p"
                |> QueryBuilder.setExpressionAttrValues ":p" (Model.AttributeValue.String "123")
                |> QueryBuilder.setProjectionExpression "SomethingElseElse"
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask

            let keyCondChangedQuery (client: AmazonDynamoDBClient) = 
                baseReq
                |> QueryBuilder.setKeyConditionExpression $"{pk} = :p AND {sk} = :p"
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask

            let projectionExprChangedQuery (client: AmazonDynamoDBClient) = 
                baseReq
                |> QueryBuilder.setProjectionExpression "SomethingElse"
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask

            let keyfilterChangedQuery (client: AmazonDynamoDBClient) = 
                baseReq 
                |> QueryBuilder.setFilterExpression "SomethingElse = :p"
                |> QueryBuilder.queryRequest
                |> client.QueryAsync
                |> Io.ignoreTask

            output.WriteLine("PART 1")
            do!
                execute
                    (fun client ->
                        TableBuilder.empty
                        |> TableBuilder.withTableName (nameof QueryCacheTests)
                        |> TableBuilder.withAttribute pk "S"
                        |> TableBuilder.withAttribute sk "S"
                        |> TableBuilder.withKeySchema pk (ValueSome sk)
                        |> TableBuilder.req
                        |> client.CreateTableAsync
                        |> Io.ignoreTask)
                |> assertRecord 1 false "Adding table"
                |> Io.ignoreTask

            // add an item to ensure filters are actually used on opened index
            let! _ =
                ItemBuilder.empty
                |> ItemBuilder.withTableName (nameof QueryCacheTests)
                |> ItemBuilder.withAttribute pk "S" "123"
                |> ItemBuilder.withAttribute sk "S" "123"
                |> ItemBuilder.withAttribute indexPk "S" "123"
                |> ItemBuilder.asPutReq
                |> setupClient.PutItemAsync
                |> Io.ignoreTask

            output.WriteLine("PART 2")
            do!
                baseQuery
                |> executeQuery (ValueSome true) (ValueSome true) true true true

            output.WriteLine("PART 3")
            do!
                keyCondChangedQuery
                |> executeQuery ValueNone ValueNone true false false

            output.WriteLine("PART 4")
            do!
                keyfilterChangedQuery
                |> executeQuery (ValueSome false) ValueNone false true false

            output.WriteLine("PART 5")
            do!
                projectionExprChangedQuery
                |> executeQuery ValueNone (ValueSome false) false false true

            output.WriteLine("PART 6")
            do!
                TableBuilder.empty
                |> TableBuilder.withTableName table
                |> TableBuilder.withSimpleGsi indexName indexPk ValueNone false
                |> TableBuilder.withAttribute indexPk "S"
                |> TableBuilder.updateReq
                |> setupClient.UpdateTableAsync
                |> Io.ignoreTask

            do!
                baseQuery
                // only filter and projection are re-compiled when indexes change
                |> executeQuery (ValueSome true) (ValueSome true) false true true

            output.WriteLine("PART 6.2")
            do!
                newIndexQuery
                |> executeQuery (ValueSome true) (ValueSome false) true true true
        }