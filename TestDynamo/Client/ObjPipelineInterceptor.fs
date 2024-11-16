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

/// <summary>
/// A request interceptor which can be used to override or polyfill client functionality
/// </summary>
[<AllowNullLiteral>]
type IRequestInterceptor =
    /// <summary>
    /// Intercept a request. If this method returns a non-null object, the object will
    /// be used as the response of the request. If null is returned, then the request will continue
    /// as normal
    /// </summary>
    abstract member Intercept: database: ApiDb -> request: obj -> CancellationToken -> ValueTask<obj>

type ObjPipelineInterceptor(
    database: Either<ApiDb, struct (GlobalDatabase * DatabaseId)>,
    artificialDelay: TimeSpan,
    interceptor: IRequestInterceptor voption,
    defaultLogger: ILogger voption,
    disposeDatabase) =

    static let noInterceptorResult = ValueTask<obj>(result = null).Preserve()
    static let noInterceptor =
        { new IRequestInterceptor with
            member _.Intercept _ _ _ = noInterceptorResult }

    let interceptor = interceptor ?|? noInterceptor
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

    let execute overrideDelay mapIn f mapOut c: _ -> ValueTask<AmazonWebServiceResponse> =
        taskify (overrideDelay ?|? artificialDelay) c >> Io.map (mapIn >> f defaultLogger >> mapOut db.Id >> ObjPipelineInterceptorUtils.cast)

    let executeAsync overrideDelay mapIn f mapOut c =
        taskify (overrideDelay ?|? artificialDelay) c >> Io.bind (mapIn >> f defaultLogger) >> Io.addCancellationToken c >> Io.map (mapOut db.Id >> ObjPipelineInterceptorUtils.cast)

    static let notSupported ``member`` = ``member`` |> sprintf "%s member is not supported" |> NotSupportedException |> raise

    static let describeRequiredTable (db: Api.FSharp.Database) (logger: ILogger voption) (name: string) =
        db.TryDescribeTable logger name |> ValueOption.defaultWith (fun _ -> clientError $"Table {name} not found on database {db.Id}")

    static let maybeUpdateTable (db: Api.FSharp.Database) (logger: ILogger voption) (name: string) (req: UpdateSingleTableData voption) =
        match req with
        | ValueNone -> describeRequiredTable db logger name
        | ValueSome req -> db.UpdateTable logger name req

    let mutable awsLogger = Unchecked.defaultof<Amazon.Runtime.Internal.Util.ILogger>
    let mutable innerHandler = Unchecked.defaultof<IPipelineHandler>
    let mutable outerHandler = Unchecked.defaultof<IPipelineHandler>

    let invoke'' overrideDelay cancellationToken: AmazonWebServiceRequest -> ValueTask<AmazonWebServiceResponse> = function
        | :? BatchGetItemRequest as request ->
            let update = MultiClientOperations.BatchGetItem.batchGetItem database
            request
            |> execute overrideDelay (GetItem.Batch.inputs awsAccountId db.Id) update GetItem.Batch.output cancellationToken
        | :? BatchWriteItemRequest as request ->
            let update = MultiClientOperations.BatchWriteItem.batchPutItem database
            request
            |> execute overrideDelay (PutItem.BatchWrite.inputs awsAccountId db.Id) update PutItem.BatchWrite.output cancellationToken
        | :? CreateGlobalTableRequest as request ->
            let update logger =
                let t = maybeUpdateTable db logger
                let dt = parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.createGlobalTable awsAccountId parentDdb db.Id t dt

            flip (execute overrideDelay id update (asLazy id)) request cancellationToken
        | :? CreateTableRequest as request ->
            request
            |> execute overrideDelay CreateTable.inputs db.AddTable (CreateTable.output awsAccountId) cancellationToken
        | :? DeleteItemRequest as request ->
            request
            |> execute overrideDelay DeleteItem.inputs db.Delete DeleteItem.output cancellationToken
        | :? DeleteTableRequest as request ->
            request.TableName
            |> executeAsync overrideDelay id db.DeleteTable (DeleteTable.output awsAccountId) cancellationToken
        | :? DescribeGlobalTableRequest as request ->
            let cluster =
                parent
                |> ValueOption.map fstT
                |> ValueOption.defaultWith (fun _ -> notSupported "This operation is only supported on clients which have a global database")

            if cluster.IsGlobalTable defaultLogger db.Id request.GlobalTableName |> not
            then clientError $"{request.GlobalTableName} in {db.Id} is not a global table"

            request.GlobalTableName
            |> execute overrideDelay id db.DescribeTable (DescribeTable.Global.output awsAccountId (ValueSome cluster) GlobalTableStatus.ACTIVE) cancellationToken
        | :? DescribeTableRequest as request ->
            request.TableName
            |> execute overrideDelay id db.DescribeTable (DescribeTable.Local.output awsAccountId) cancellationToken
        | :? GetItemRequest as request ->
            execute overrideDelay GetItem.inputs db.Get GetItem.output cancellationToken request
        | :? ListGlobalTablesRequest as request ->
            let cluster =
                parent
                |> ValueOption.map fstT
                |> ValueOption.defaultWith (fun _ -> notSupported "This operation is only supported on clients which have a global database")

            execute overrideDelay DescribeTable.Global.List.inputs cluster.ListGlobalTables (DescribeTable.Global.List.output request.Limit) cancellationToken request
        | :? ListTablesRequest as request ->
            let limit = DescribeTable.List.getLimit request
            execute overrideDelay DescribeTable.List.inputs db.ListTables (DescribeTable.List.output limit) cancellationToken request
        | :? PutItemRequest as request ->
            execute overrideDelay PutItem.inputs db.Put PutItem.output cancellationToken request
        | :? QueryRequest as request ->
            execute overrideDelay (Query.inputs scanSizeLimits) db.Query Query.output cancellationToken request
        | :? ScanRequest as request ->
            execute overrideDelay (Scan.inputs scanSizeLimits) db.Query Scan.output cancellationToken request
        | :? TransactGetItemsRequest as request ->
            request
            |> execute overrideDelay GetItem.Transaction.inputs db.Gets GetItem.Transaction.output cancellationToken
        | :? TransactWriteItemsRequest as request ->
            request
            |> execute overrideDelay TransactWriteItems.inputs db.TransactWrite TransactWriteItems.output cancellationToken
        | :? UpdateGlobalTableRequest as request ->
            let update logger =
                let local = maybeUpdateTable db logger
                let ``global`` = parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.updateGlobalTable awsAccountId parentDdb db.Id local ``global``

            flip (execute overrideDelay id update (asLazy id)) request cancellationToken
        | :? UpdateItemRequest as request ->
            request
            |> execute overrideDelay (UpdateItem.inputs loggerOrDevNull) db.Update UpdateItem.output cancellationToken
        | :? UpdateTableRequest as request ->
            let update logger =
                let local = maybeUpdateTable db logger
                let dt = parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.updateTable awsAccountId parentDdb db.Id local dt

            flip (execute overrideDelay id update (asLazy id)) request cancellationToken
        | x -> x.GetType().Name |> sprintf "%s operation is not supported" |> NotSupportedException |> raise

    let invoke' overrideDelay cancellationToken req =
        let interceptorResult = interceptor.Intercept db req cancellationToken
        if interceptorResult = Unchecked.defaultof<ValueTask<_>>
            then noInterceptorResult
            else interceptorResult
        |%>>= function
            | null -> 
                cancellationToken.ThrowIfCancellationRequested()
                invoke'' overrideDelay cancellationToken req
            | :? AmazonWebServiceResponse as r -> ValueTask<_>(result = r)
            | _ -> invalidOp $"Intercept response must be null or inherit from {nameof AmazonWebServiceRequest}"

    let invoke = invoke' ValueNone

    let invokeWithoutDelay = invoke' (ValueSome TimeSpan.Zero)

    member _.ProcessingDelay
        with get () = artificialDelay
        and set value = artificialDelay <- value

    member _.Database = db
    member _.GlobalDatabase = parent ?|> fstT
    member _.SetScanLimits limits = scanSizeLimits <- limits
    member _.AwsAccountId
        with get () = awsAccountId
        and set value = awsAccountId <- value

    // exposed to test specific edge case
    member _.InvokeSync useDelay request c =
        let result = (if useDelay then invoke else invokeWithoutDelay) c request

        if not result.IsCompletedSuccessfully
        then result.AsTask().ConfigureAwait(false).GetAwaiter().GetResult()
        else result.Result

    interface IDisposable with
        member _.Dispose() =
            if disposeDatabase
            then
                database
                |> Either.map1Of2 _.Dispose()
                |> Either.map2Of2 (fstT >> _.Dispose())
                |> Either.reduce

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

        member this.InvokeSync(executionContext: IExecutionContext) =
            executionContext.ResponseContext.Response <- 
                this.InvokeSync
                    false
                    executionContext.RequestContext.OriginalRequest
                    executionContext.RequestContext.CancellationToken

        member _.InvokeAsync<'a when 'a : (new : unit -> 'a) and 'a :> AmazonWebServiceResponse>(executionContext: IExecutionContext) =
            invoke
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
