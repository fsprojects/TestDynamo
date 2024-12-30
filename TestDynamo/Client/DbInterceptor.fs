namespace TestDynamo.Client

open System
open System.Threading
open System.Threading.Tasks
open Amazon.Runtime
open Microsoft.Extensions.Logging
open TestDynamo
open TestDynamo.Data.Monads.Operators
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.GenericMapper
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Api.FSharp

type ApiDb = TestDynamo.Api.FSharp.Database
type AmazonWebServiceResponse = Amazon.Runtime.AmazonWebServiceResponse
type Stream = System.IO.Stream

module DbInterceptorUtils =

    type DbInterceptorState =
        { db: ApiDb
          parent: struct (GlobalDatabase * DatabaseId) voption
          artificialDelay: TimeSpan
          loggerOrDevNull: ILogger
          defaultLogger: ILogger voption
          scanSizeLimits: ExpressionExecutors.Fetch.ScanLimits
          awsAccountId: AwsAccountId }

    // let private cast<'a when 'a :> AmazonWebServiceResponse> (x: 'a) = x :> AmazonWebServiceResponse

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

    let private execute state overrideDelay mapIn f mapOut c: _ -> ValueTask<obj> =
        taskify (overrideDelay ?|? state.artificialDelay) c >> Io.map (mapIn >> f state.defaultLogger >> mapOut state.db.Id >> box)

    let private executeAsync state overrideDelay mapIn f mapOut c =
        taskify (overrideDelay ?|? state.artificialDelay) c >> Io.bind (mapIn >> f state.defaultLogger) >> Io.addCancellationToken c >> Io.map (mapOut state.db.Id >> box)

    let private notSupported ``member`` = ``member`` |> sprintf "%s member is not supported" |> NotSupportedException |> raise

    let private describeRequiredTable (db: Api.FSharp.Database) (logger: ILogger voption) (name: string) =
        db.TryDescribeTable logger name |> ValueOption.defaultWith (fun _ -> ClientError.clientError $"Table {name} not found on database {db.Id}")

    let private maybeUpdateTable (db: Api.FSharp.Database) (logger: ILogger voption) (name: string) (req: UpdateSingleTableData voption) =
        match req with
        | ValueNone -> describeRequiredTable db logger name
        | ValueSome req -> db.UpdateTable logger name req

    let private parentDdb state = state.parent ?|> fstT

    let private eitherDatabase state = state.parent ?|> Either2 ?|? Either1 state.db

    let invokeFromMapped state overrideDelay (c: CancellationToken): obj -> ValueTask<obj> =
        c.ThrowIfCancellationRequested()
        function
        | :? BatchGetItemRequest<AttributeValue> as request ->
            let update = MultiClientOperations.BatchGetItem.batchGetItem (eitherDatabase state)
            request
            |> execute state overrideDelay (Item.Batch.Get.input state.awsAccountId state.db.Id) update Item.Batch.Get.output c
        | :? BatchWriteItemRequest<AttributeValue> as request ->
            let update = MultiClientOperations.BatchWriteItem.batchPutItem (eitherDatabase state)
            request
            |> execute state overrideDelay (Item.Batch.Write.input state.awsAccountId state.db.Id) update Item.Batch.Write.output c
        | :? CreateGlobalTableRequest as request ->
            let update logger =
                let t = maybeUpdateTable state.db logger
                let dt = state.parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.createGlobalTable state.awsAccountId (parentDdb state) state.db.Id t dt

            flip (execute state overrideDelay id update (asLazy id)) request c
        | :? CreateTableRequest as request ->
            request
            |> execute state overrideDelay Table.Local.Create.input state.db.AddTable (Table.Local.Create.output state.awsAccountId) c
        | :? DeleteItemRequest<AttributeValue> as request ->
            request
            |> execute state overrideDelay Item.Delete.input state.db.Delete Item.Delete.output c
        | :? DeleteTableRequest as request ->
            request.TableName
            |> ClientError.expectSomeClientErr "TableName property is required%s" ""
            |> executeAsync state overrideDelay id state.db.DeleteTable (Table.Local.Delete.output state.awsAccountId) c
        | :? DescribeGlobalTableRequest as request ->
            let cluster =
                state.parent
                |> ValueOption.map fstT
                |> ValueOption.defaultWith (fun _ -> notSupported "This operation is only supported on clients which have a global database")

            let name =
                request.GlobalTableName
                |> ClientError.expectSomeClientErr "GlobalTableName property is required%s" ""

            if cluster.IsGlobalTable state.defaultLogger state.db.Id name |> not
            then ClientError.clientError $"{request.GlobalTableName} in {state.db.Id} is not a global table"

            name
            |> execute state overrideDelay id state.db.DescribeTable (Table.Global.Describe.output state.awsAccountId (ValueSome cluster) GlobalTableStatus.ACTIVE) c
        | :? DescribeTableRequest as request ->
            request.TableName
            |> ClientError.expectSomeClientErr "TableName property is required%s" ""
            |> execute state overrideDelay id state.db.DescribeTable (Table.Local.Describe.output state.awsAccountId) c
        | :? GetItemRequest<AttributeValue> as request ->
            execute state overrideDelay Item.Get.input state.db.Get Item.Get.output c request
        | :? ListGlobalTablesRequest as request ->
            let struct (_, _, limit) & inputs = Table.Global.List.input request

            let cluster =
                state.parent
                |> ValueOption.map fstT
                |> ValueOption.defaultWith (fun _ -> notSupported "This operation is only supported on clients which have a global database")

            execute state overrideDelay id cluster.ListGlobalTables (Table.Global.List.output limit) c inputs
        | :? ListTablesRequest as request ->
            let inputs = Table.Local.List.input request
            let limit = inputs |> sndT
            execute state overrideDelay id state.db.ListTables (Table.Local.List.output limit) c inputs
        | :? PutItemRequest<AttributeValue> as request ->
            request
            |> execute state overrideDelay Item.Put.input state.db.Put Item.Put.output c
        | :? QueryRequest<AttributeValue> as request ->
            execute state overrideDelay (Query.input state.scanSizeLimits) state.db.Query Query.output c request
        | :? ScanRequest<AttributeValue> as request ->
            execute state overrideDelay (Scan.input state.scanSizeLimits) state.db.Query Scan.output c request
        | :? TransactGetItemsRequest<AttributeValue> as request ->
            request
            |> execute state overrideDelay Item.Transact.Get.input state.db.Gets Item.Transact.Get.output c
        | :? TransactWriteItemsRequest<AttributeValue> as request ->
            request
            |> execute state overrideDelay Item.Transact.Write.input state.db.TransactWrite Item.Transact.Write.output c
        | :? UpdateGlobalTableRequest as request ->
            let update logger =
                let local = maybeUpdateTable state.db logger
                let ``global`` = state.parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.updateGlobalTable state.awsAccountId (parentDdb state) state.db.Id local ``global``

            flip (execute state overrideDelay id update (asLazy id)) request c
        | :? UpdateItemRequest<AttributeValue> as request ->
            request
            |> execute state overrideDelay (Item.Update.input state.loggerOrDevNull) state.db.Update Item.Update.output c
        | :? UpdateTableRequest as request ->
            let update logger =
                let local = maybeUpdateTable state.db logger
                let dt = state.parent ?|> (fun struct (p, id) -> p.UpdateTable id logger)
                MultiClientOperations.UpdateTable.updateTable state.awsAccountId (parentDdb state) state.db.Id local dt

            flip (execute state overrideDelay id update (asLazy id)) request c
        | x -> x.GetType().Name |> sprintf "%s operation is not supported. You can add an interceptor to implement out of scope use cases" |> NotSupportedException |> raise

    let private castToAmazonWebServiceResponse (x: obj) =
        try
            x :?> AmazonWebServiceResponse
        with
        | :? InvalidCastException as e ->
            InvalidOperationException($"Intercepted responses must inherit from {nameof AmazonWebServiceResponse}", e) |> raise

    let private invoke' state overrideDelay c x =
        DtoMappers.fromDtoObj x
        |> invokeFromMapped state overrideDelay c
        |%|> DtoMappers.fromDtoObj

    let invoke = flip invoke' ValueNone

    let invokeWithoutDelay = flip invoke' (ValueSome TimeSpan.Zero)

module DbCommandExecutor =
    let execute state overrideDelay c input =
        DbInterceptorUtils.invokeFromMapped state overrideDelay c input
        

type DbInterceptor(
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
          awsAccountId = Settings.DefaultAwsAccountId }: DbInterceptorUtils.DbInterceptorState

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
        (if useDelay then DbInterceptorUtils.invoke else DbInterceptorUtils.invokeWithoutDelay) state c request
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
                :?> Amazon.Runtime.AmazonWebServiceResponse

        member _.InvokeAsync<'a when 'a : (new : unit -> 'a) and 'a :> AmazonWebServiceResponse>(executionContext: IExecutionContext) =
            DbInterceptorUtils.invoke
                state
                executionContext.RequestContext.CancellationToken
                executionContext.RequestContext.OriginalRequest
            |%|> fun r ->
                let r = r :?> 'a
                executionContext.ResponseContext.Response <- r
                r
            |> DbInterceptorUtils.asTask
