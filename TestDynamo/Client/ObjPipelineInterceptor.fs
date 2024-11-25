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

    type ObjPipelineInterceptorState =
        { db: ApiDb
          parent: struct (GlobalDatabase * DatabaseId) voption
          artificialDelay: TimeSpan
          loggerOrDevNull: ILogger
          defaultLogger: ILogger voption
          scanSizeLimits: ExpressionExecutors.Fetch.ScanLimits
          awsAccountId: AwsAccountId }

    let private cast<'a when 'a :> AmazonWebServiceResponse> (x: 'a) = x :> AmazonWebServiceResponse

    let asTask (x: ValueTask<'a>) = x.AsTask()

    let private delay' (delay: TimeSpan) (c: CancellationToken) =
        task {
            do! Task.Delay(delay, c).ConfigureAwait(false)
        }

    let private taskify delay (c: CancellationToken) x =
        match delay with
        | d when d < TimeSpan.Zero -> notSupported "Delay time must be greater than or equal to 0"
        | d when d = TimeSpan.Zero -> ValueTask<'a>(result = x)
        | d -> delay' d c |> ValueTask |> Io.normalizeVt |%|> (asLazy x)

    let private execute state overrideDelay mapIn f mapOut c: _ -> ValueTask<AmazonWebServiceResponse> =
        taskify (overrideDelay ?|? state.artificialDelay) c >> Io.map (mapIn >> f state.defaultLogger >> mapOut state.db.Id >> cast)

    let private executeAsync state overrideDelay mapIn f mapOut c =
        taskify (overrideDelay ?|? state.artificialDelay) c >> Io.bind (mapIn >> f state.defaultLogger) >> Io.addCancellationToken c >> Io.map (mapOut state.db.Id >> cast)

    let private notSupported ``member`` = ``member`` |> sprintf "%s member is not supported" |> NotSupportedException |> raise

    let private describeRequiredTable (db: Api.FSharp.Database) (logger: ILogger voption) (name: string) =
        db.TryDescribeTable logger name |> ValueOption.defaultWith (fun _ -> clientError $"Table {name} not found on database {db.Id}")

    let private maybeUpdateTable (db: Api.FSharp.Database) (logger: ILogger voption) (name: string) (req: UpdateSingleTableData voption) =
        match req with
        | ValueNone -> describeRequiredTable db logger name
        | ValueSome req -> db.UpdateTable logger name req

    let private parentDdb state = state.parent ?|> fstT

    let private eitherDatabase state = state.parent ?|> Either2 ?|? Either1 state.db

    let private invoke' state overrideDelay (c: CancellationToken): AmazonWebServiceRequest -> ValueTask<AmazonWebServiceResponse> =
        c.ThrowIfCancellationRequested()
        function
        | :? BatchGetItemRequest as request ->
            let update = MultiClientOperations.BatchGetItem.batchGetItem (eitherDatabase state)
            request
            |> execute state overrideDelay (GetItem.Batch.inputs state.awsAccountId state.db.Id) update GetItem.Batch.output c
        | :? BatchWriteItemRequest as request ->
            let update = MultiClientOperations.BatchWriteItem.batchPutItem (eitherDatabase state)
            request
            |> execute state overrideDelay (PutItem.BatchWrite.inputs state.awsAccountId state.db.Id) update PutItem.BatchWrite.output c
        | :? CreateGlobalTableRequest as request ->
            let update logger =
                let t = maybeUpdateTable state.db logger
                let dt = state.parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.createGlobalTable state.awsAccountId (parentDdb state) state.db.Id t dt

            flip (execute state overrideDelay id update (asLazy id)) request c
        | :? CreateTableRequest as request ->
            request
            |> execute state overrideDelay CreateTable.inputs state.db.AddTable (CreateTable.output state.awsAccountId) c
        | :? DeleteItemRequest as request ->
            request
            |> execute state overrideDelay DeleteItem.inputs state.db.Delete DeleteItem.output c
        | :? DeleteTableRequest as request ->
            request.TableName
            |> executeAsync state overrideDelay id state.db.DeleteTable (DeleteTable.output state.awsAccountId) c
        | :? DescribeGlobalTableRequest as request ->
            let cluster =
                state.parent
                |> ValueOption.map fstT
                |> ValueOption.defaultWith (fun _ -> notSupported "This operation is only supported on clients which have a global database")

            if cluster.IsGlobalTable state.defaultLogger state.db.Id request.GlobalTableName |> not
            then clientError $"{request.GlobalTableName} in {state.db.Id} is not a global table"

            request.GlobalTableName
            |> execute state overrideDelay id state.db.DescribeTable (DescribeTable.Global.output state.awsAccountId (ValueSome cluster) GlobalTableStatus.ACTIVE) c
        | :? DescribeTableRequest as request ->
            request.TableName
            |> execute state overrideDelay id state.db.DescribeTable (DescribeTable.Local.output state.awsAccountId) c
        | :? GetItemRequest as request ->
            execute state overrideDelay GetItem.inputs state.db.Get GetItem.output c request
        | :? ListGlobalTablesRequest as request ->
            let cluster =
                state.parent
                |> ValueOption.map fstT
                |> ValueOption.defaultWith (fun _ -> notSupported "This operation is only supported on clients which have a global database")

            execute state overrideDelay DescribeTable.Global.List.inputs cluster.ListGlobalTables (DescribeTable.Global.List.output request.Limit) c request
        | :? ListTablesRequest as request ->
            let limit = DescribeTable.List.getLimit request
            execute state overrideDelay DescribeTable.List.inputs state.db.ListTables (DescribeTable.List.output limit) c request
        | :? PutItemRequest as request ->
            execute state overrideDelay PutItem.inputs state.db.Put PutItem.output c request
        | :? QueryRequest as request ->
            execute state overrideDelay (Query.inputs state.scanSizeLimits) state.db.Query Query.output c request
        | :? ScanRequest as request ->
            execute state overrideDelay (Scan.inputs state.scanSizeLimits) state.db.Query Scan.output c request
        | :? TransactGetItemsRequest as request ->
            request
            |> execute state overrideDelay GetItem.Transaction.inputs state.db.Gets GetItem.Transaction.output c
        | :? TransactWriteItemsRequest as request ->
            request
            |> execute state overrideDelay TransactWriteItems.inputs state.db.TransactWrite TransactWriteItems.output c
        | :? UpdateGlobalTableRequest as request ->
            let update logger =
                let local = maybeUpdateTable state.db logger
                let ``global`` = state.parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.updateGlobalTable state.awsAccountId (parentDdb state) state.db.Id local ``global``

            flip (execute state overrideDelay id update (asLazy id)) request c
        | :? UpdateItemRequest as request ->
            request
            |> execute state overrideDelay (UpdateItem.inputs state.loggerOrDevNull) state.db.Update UpdateItem.output c
        | :? UpdateTableRequest as request ->
            let update logger =
                let local = maybeUpdateTable state.db logger
                let dt = state.parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.updateTable state.awsAccountId (parentDdb state) state.db.Id local dt

            flip (execute state overrideDelay id update (asLazy id)) request c
        | x -> x.GetType().Name |> sprintf "%s operation is not supported" |> NotSupportedException |> raise

    let private castToAmazonWebServiceResponse (x: obj) =
        try
            x :?> AmazonWebServiceResponse
        with
        | :? InvalidCastException as e ->
            InvalidOperationException($"Intercepted responses must inherit from {nameof AmazonWebServiceResponse}", e) |> raise

    let invoke = flip invoke' ValueNone

    let invokeWithoutDelay = flip invoke' (ValueSome TimeSpan.Zero)

type ObjPipelineInterceptor(
    database: Either<ApiDb, struct (GlobalDatabase * DatabaseId)>,
    artificialDelay: TimeSpan,
    defaultLogger: ILogger voption,
    disposeDatabase) =

    let mutable state =
        { db =
            match database with
            | Either1 x -> x
            | Either2 struct (x: GlobalDatabase, id) -> x.GetDatabase defaultLogger id
          parent =
            match database with
            | Either1 _ -> ValueNone
            | Either2 struct (x, id) -> ValueSome struct (x, id)
          artificialDelay = artificialDelay
          defaultLogger = defaultLogger 
          loggerOrDevNull = ValueOption.defaultValue Logger.notAnILogger defaultLogger
          scanSizeLimits =
            { maxScanItems = Settings.ScanSizeLimits.DefaultMaxScanItems
              maxPageSizeBytes = Settings.ScanSizeLimits.DefaultMaxPageSizeBytes }
          awsAccountId = Settings.DefaultAwsAccountId }: ObjPipelineInterceptorUtils.ObjPipelineInterceptorState

    let mutable awsLogger = Unchecked.defaultof<Amazon.Runtime.Internal.Util.ILogger>
    let mutable innerHandler = Unchecked.defaultof<IPipelineHandler>
    let mutable outerHandler = Unchecked.defaultof<IPipelineHandler>

    member _.ProcessingDelay
        with get () = state.artificialDelay
        and set value = state <- { state with artificialDelay = value }

    member _.Database = state.db
    member _.GlobalDatabase = state.parent ?|> fstT
    member _.SetScanLimits limits = state <- { state with scanSizeLimits = limits }
    member _.AwsAccountId
        with get () = state.awsAccountId
        and set value = state <- { state with awsAccountId = value }

    // exposed to test specific edge case
    member _.InvokeSync useDelay request c =
        (if useDelay then ObjPipelineInterceptorUtils.invoke else ObjPipelineInterceptorUtils.invokeWithoutDelay) state c request
        |> Io.execute

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
            ObjPipelineInterceptorUtils.invoke
                state
                executionContext.RequestContext.CancellationToken
                executionContext.RequestContext.OriginalRequest
            |%|> fun r ->
                let r = r :?> 'a
                executionContext.ResponseContext.Response <- r
                r
            |> ObjPipelineInterceptorUtils.asTask
