namespace TestDynamo.Client

open System
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Runtime
open Microsoft.Extensions.Logging
open TestDynamo
open TestDynamo.Client.MultiClientOperations
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Api.FSharp

type Stream = System.IO.Stream

module private ObjPipelineInterceptorUtils = 
    let cast<'a when 'a :> AmazonWebServiceResponse> (x: 'a) = x :> AmazonWebServiceResponse

type ObjPipelineInterceptor(
    database: Either<ApiDb, struct (GlobalDatabase * DatabaseId)>,
    artificialDelay: TimeSpan,
    defaultLogger: ILogger voption) =

    let db =
        match database with
        | Either1 x -> x
        | Either2 struct (x: GlobalDatabase, id) -> x.GetDatabase defaultLogger id

    let parent =
        match database with
        | Either1 _ -> ValueNone
        | Either2 struct (x, id) -> ValueSome struct (x, id)
        
    let parentDdb = ValueOption.map fstT parent

    let mutable artificialDelay = artificialDelay

    let loggerOrDevNull = ValueOption.defaultValue Logger.notAnILogger defaultLogger

    static let defaultScanSizeLimits =
        { maxScanItems = Settings.ScanSizeLimits.DefaultMaxScanItems
          maxPageSizeBytes = Settings.ScanSizeLimits.DefaultMaxPageSizeBytes } : ExpressionExecutors.Fetch.ScanLimits

    let mutable scanSizeLimits = defaultScanSizeLimits

    let mutable awsAccountId = Settings.DefaultAwsAccountId
    
    static let asTask (x: ValueTask<'a>) = x.AsTask()

    static let delay' (delay: TimeSpan) (c: CancellationToken) =
        task {
            do! Task.Delay(delay, c).ConfigureAwait(false)
        }
    
    static let taskify delay (c: CancellationToken) x =
        match delay with
        | d when d < TimeSpan.Zero -> notSupported "Delay time must be greater than or equal to 0"
        | d when d = TimeSpan.Zero -> ValueTask<'a>(result = x)
        | d -> delay' d c |> ValueTask |> Io.normalizeVt |%|> (asLazy x)

    let execute mapIn f mapOut c: _ -> ValueTask<AmazonWebServiceResponse> =
        taskify artificialDelay c >> Io.map (mapIn >> f defaultLogger >> mapOut db.Id >> ObjPipelineInterceptorUtils.cast)

    let executeAsync mapIn f mapOut c =
        taskify artificialDelay c >> Io.bind (mapIn >> f defaultLogger) >> Io.map (mapOut db.Id >> ObjPipelineInterceptorUtils.cast)

    static let notSupported ``member`` = ``member`` |> sprintf "%s member is not supported" |> NotSupportedException |> raise

    static let describeRequiredTable (db: Api.FSharp.Database) (logger: ILogger voption) (name: string) =
        db.TryDescribeTable logger name |> ValueOption.defaultWith (fun _ -> clientError $"Table {name} not found on database {db.Id}")

    static let maybeUpdateTable (db: Api.FSharp.Database) (logger: ILogger voption) (name: string) (req: UpdateSingleTableData voption) =
        match req with
        | ValueNone -> describeRequiredTable db logger name
        | ValueSome req -> db.UpdateTable logger name req
        
    static let taskFactory = TaskFactory(
        CancellationToken.None,
        TaskCreationOptions.None,
        TaskContinuationOptions.None,
        TaskScheduler.Default)
    
    let mutable awsLogger = Unchecked.defaultof<Amazon.Runtime.Internal.Util.ILogger>
    let mutable innerHandler = Unchecked.defaultof<IPipelineHandler>
    let mutable outerHandler = Unchecked.defaultof<IPipelineHandler>
    
    let invoke operation cancellationToken: AmazonWebServiceRequest -> ValueTask<AmazonWebServiceResponse> = function
        | :? BatchGetItemRequest as request ->
            let update = MultiClientOperations.BatchGetItem.batchGetItem database
            request
            |> execute (GetItem.Batch.inputs1 awsAccountId db.Id) update GetItem.Batch.output cancellationToken
        | :? BatchWriteItemRequest as request ->
            let update = MultiClientOperations.BatchWriteItem.batchPutItem database
            request
            |> execute (PutItem.BatchWrite.inputs1 awsAccountId db.Id) update PutItem.BatchWrite.output cancellationToken
        | :? CreateGlobalTableRequest as request ->
            let update logger =
                let t = maybeUpdateTable db logger
                let dt = parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.createGlobalTable awsAccountId parentDdb db.Id t dt

            flip (execute id update (asLazy id)) request cancellationToken
        | :? CreateTableRequest as request ->
            request
            |> execute CreateTable.inputs1 db.AddTable (CreateTable.output awsAccountId) cancellationToken
        | :? DeleteItemRequest as request ->
            request
            |> execute DeleteItem.inputs1 db.Delete DeleteItem.output cancellationToken
        | :? DeleteTableRequest as request ->
            request.TableName
            |> executeAsync id db.DeleteTable (DeleteTable.output awsAccountId) cancellationToken
        | :? DescribeGlobalTableRequest as request ->
            let cluster =
                parent
                |> ValueOption.map fstT
                |> ValueOption.defaultWith (fun _ -> notSupported "This operation is only supported on clients which have a global database")
                
            if cluster.IsGlobalTable defaultLogger db.Id request.GlobalTableName |> not
            then clientError $"{request.GlobalTableName} in {db.Id} is not a global table"
                
            request.GlobalTableName
            |> execute id db.DescribeTable (DescribeTable.Global.output awsAccountId (ValueSome cluster) GlobalTableStatus.ACTIVE) cancellationToken
        | :? DescribeTableRequest as request ->
            request.TableName
            |> execute id db.DescribeTable (DescribeTable.Local.output awsAccountId) cancellationToken
        | :? GetItemRequest as request ->
            execute GetItem.inputs1 db.Get GetItem.output cancellationToken request
        | :? ListGlobalTablesRequest as request ->
            let cluster =
                parent
                |> ValueOption.map fstT
                |> ValueOption.defaultWith (fun _ -> notSupported "This operation is only supported on clients which have a global database")
                
            execute DescribeTable.Global.List.inputs cluster.ListGlobalTables (DescribeTable.Global.List.output request.Limit) cancellationToken request
        | :? ListTablesRequest as request ->
            let limit = DescribeTable.List.getLimit request
            execute DescribeTable.List.inputs1 db.ListTables (DescribeTable.List.output limit) cancellationToken request
        | :? PutItemRequest as request ->
            execute PutItem.inputs1 db.Put PutItem.output cancellationToken request
        | :? QueryRequest as request ->
            execute (Query.inputs1 scanSizeLimits) db.Query Query.output cancellationToken request
        | :? ScanRequest as request ->
            execute (Scan.inputs1 scanSizeLimits) db.Query Scan.output cancellationToken request
        | :? TransactGetItemsRequest as request ->
            request
            |> execute GetItem.Transaction.inputs1 db.Gets GetItem.Transaction.output cancellationToken
        | :? TransactWriteItemsRequest as request ->
            request
            |> execute TransactWriteItems.inputs1 db.TransactWrite TransactWriteItems.output cancellationToken
        | :? UpdateGlobalTableRequest as request ->
            let update logger =
                let local = maybeUpdateTable db logger
                let ``global`` = parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.updateGlobalTable awsAccountId parentDdb db.Id local ``global``

            flip (execute id update (asLazy id)) request cancellationToken
        | :? UpdateItemRequest as request ->
            request
            |> execute (UpdateItem.inputs1 loggerOrDevNull) db.Update UpdateItem.output cancellationToken
        | :? UpdateTableRequest as request ->
            let update logger =
                let local = maybeUpdateTable db logger
                let dt = parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.updateTable awsAccountId parentDdb db.Id local dt
                
            flip (execute id update (asLazy id)) request cancellationToken
        | x -> operation |> sprintf "%s operation is not supported" |> NotSupportedException |> raise
    
    member _.ProcessingDelay
        with get () = artificialDelay
        and set value = artificialDelay <- value
        
    member _.Database = db
    member _.GlobalDatabase = parent ?|> fstT
    member _.SetScanLimits limits = scanSizeLimits <- limits
    member _.AwsAccountId
        with get () = awsAccountId
        and set value = awsAccountId <- value
    
    interface IPipelineHandler with
    
        member _.Logger
            with get () = awsLogger
            and set value = awsLogger <- value
        member _.InnerHandler
            with get () = innerHandler
            and set value = innerHandler <- value
        member _.OuterHandler
            with get () = outerHandler
            and set value = outerHandler <- value
            
        member _.InvokeSync(executionContext: IExecutionContext) =
            let result =
                invoke
                    (executionContext.RequestContext.OriginalRequest.GetType()).Name
                    executionContext.RequestContext.CancellationToken
                    executionContext.RequestContext.OriginalRequest
                |%|> fun r -> executionContext.ResponseContext.Response <- r
                
            if not result.IsCompletedSuccessfully
            // TODO: test
            then taskFactory.StartNew(fun _ -> result.AsTask()).Unwrap().GetAwaiter().GetResult()
            else ()
            
        member _.InvokeAsync<'a when 'a : (new : unit -> 'a) and 'a :> AmazonWebServiceResponse>(executionContext: IExecutionContext) =
            invoke
                (executionContext.RequestContext.OriginalRequest.GetType()).Name
                executionContext.RequestContext.CancellationToken
                executionContext.RequestContext.OriginalRequest
            |%|> fun r ->
                let r = r  |> box :?> 'a
                executionContext.ResponseContext.Response <- r
                r
            |> asTask

// type HttpPipelineInterceptor() =
//     
//     let mutable logger = Unchecked.defaultof<Amazon.Runtime.Internal.Util.ILogger>
//     let mutable inner = Unchecked.defaultof<IPipelineHandler>
//     let mutable outer = Unchecked.defaultof<IPipelineHandler>
//     interface IPipelineHandler with
//     
//         member _.Logger
//             with get () = logger
//             and set value = logger <- value
//         member _.InnerHandler
//             with get () = inner
//             and set value = inner <- value
//         member _.OuterHandler
//             with get () = outer
//             and set value = outer <- value
//             
//         member _.InvokeSync(executionContext: IExecutionContext) =
//             
//             let content = System.Text.Encoding.UTF8.GetBytes """{
// }"""
//
//             let reqId = Guid.NewGuid().ToString()
//             let eq (x: string) y = x.Equals(y, StringComparison.OrdinalIgnoreCase)
//
//             let headers (name: string) =
//                 if name = null then ValueNone
//                 elif eq name "Content-Length" then ValueSome (content.Length.ToString())
//                 elif eq name "Content-Encoding" then ValueSome "UTF-8"
//                 elif eq name "x-amzn-RequestId" then
//                     let tt = executionContext.RequestContext
//                     let oo = executionContext.ResponseContext
//                     let rrr = Encoding.UTF8.GetString(executionContext.RequestContext.Request.Content)
//                     ValueSome reqId
//                 else ValueNone
//             
//             executionContext.ResponseContext.HttpResponse <-
//                 { new IWebResponseData with
//                     member _.ContentLength = content.Length
//                     member _.ContentType = "application/json"
//                     member _.StatusCode = HttpStatusCode.OK
//                     member _.IsSuccessStatusCode = true
//                     member _.GetHeaderNames() = invalidOp "GetHeaderNames"
//                     member _.IsHeaderPresent headerName = headers headerName |> ValueOption.isSome
//                     member _.GetHeaderValue headerName = headers headerName |> Maybe.expectSomeErr "Cannot find header %s" headerName
//                     member _.ResponseBody =
//                         { new IHttpResponseBody with
//                              member _.Dispose() = ()
//                              member _.OpenResponse() = new MemoryStream(content)
//                              member _.OpenResponseAsync() = Task.FromResult<Stream>(new MemoryStream(content))}}
//                 
//             ()
//             
//         member _.InvokeAsync<'a when 'a : (new : unit -> 'a) and 'a :> AmazonWebServiceResponse>(executionContext: IExecutionContext) = Unchecked.defaultof<Task<'a>>
        
