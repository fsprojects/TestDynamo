namespace TestDynamo.Model

open System.Threading
open Microsoft.FSharp.Core
open System
open System.Threading.Tasks
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.Data.Monads
open TestDynamo.Model.ExpressionExecutors.Fetch
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model
open System.Runtime.CompilerServices

type CreateTableData =
    { tableConfig: TableConfig
      createStream: bool }

type private DatabaseInfo =
    { /// <summary>Identifies the database within a cluster</summary>
      databaseId: DatabaseId
      /// <summary>Identifies the database within the application</summary>
      uniqueIdentifier: IncrementingId

      /// <summary>A cache of idempotency keys, transact write results and a cache invalidation times to prevent request replay</summary>
      transactWriteResultCache: Map<string, struct (Map<string,Map<string,AttributeValue> list> * DateTimeOffset)>

      /// <summary>
      /// A place to stash any errors which were not handled on a stream or stream subscriber
      /// before it was deleted
      /// </summary>
      errorsFromDeletedStreams: ValueTask<SubscriberError list> }

/// <summary>
/// A composite of info and data. These properties
/// are split into multiple types to reduce allocations when
/// Items are written to the table with copy on write, while keeping
/// struct size namagable
/// </summary>
[<Struct; IsReadOnly>]
type private DatabaseState =
    { tables: DatabaseTables
      streams: TableStreams
      info: DatabaseInfo }
    with
    override this.ToString() = this.info.databaseId.ToString()

/// <summary>
/// A composite of info and data. These properties
/// are split into multiple types to reduce allocations when
/// Items are written to the table with copy on write, while keeping
/// struct size namagable
/// </summary>
[<Struct; IsReadOnly>]
type GetResponseData =
    { items: Map<string, AttributeValue> array
      scannedCount: int
      /// <summary>Sum of the individual sizes of items</summary>
      itemSizeBytes: int
      unevaluatedKeys: Map<string, AttributeValue> array }

[<Struct; IsReadOnly>]
type UpdateSingleTableData =
    { updateTableData: UpdateTableData
      streamConfig: UpdateStreamArgs voption }

    with
    // this empty value is important. It tells the sysem to ignore certain errors around corruption due to synchronization
    static member empty =
        { updateTableData = UpdateTableData.empty
          streamConfig = ValueNone }

type DatabaseClone =
    { initialState: DatabaseTables
      streamsEnabled: string list }

    with
    static member empty =
        { initialState = DatabaseTables.empty
          streamsEnabled = [] }

    /// <summary>Create a new DatabaseCloneData with only the required tables in it</summary>
    member this.ExtractTables(tableNames: string seq) =
        let tn = Array.ofSeq tableNames

        { initialState =
               DatabaseTables.empty
               |> flip (Array.fold (flip (fun name ->
                   (DatabaseTables.getTable name this.initialState
                   |> flip (DatabaseTables.addClonedTable name) Logger.empty)))) tn
          streamsEnabled =
              this.streamsEnabled
              |> List.filter (flip Array.contains tn)}

type SynchronizationResult =
    | Ingested
    | FullCircle
    | TableNotFound
    | SynchronizationError of SynchronizationException

type private NonFunctioning =
    | Disposed of d: struct (DatabaseId * IncrementingId)
    | SyncError of struct (DatabaseState * SynchronizationException)

/// <summary>
/// A single region Database containing Tables and Streams
/// </summary>
[<Struct; IsReadOnly>]
type Database =
    private
    | Db of s: DatabaseState
    | NonFunctioning of NonFunctioning

    override this.ToString() =
        match this with
        | Db x -> x.ToString()
        | NonFunctioning (Disposed (dbId, globalId)) -> $"DISPOSED {dbId}/{globalId}"
        | NonFunctioning (SyncError (x, _)) -> $"SYNC ERR {x}"

/// <summary>
/// A composite of a Table and it's Stream info
/// </summary>
type TableDetails =
    { table: Table
      streamSubscribers: Stream voption }

    with

    member this.name = Table.name this.table
    member this.createdDate = Table.getCreatedDate this.table
    member this.itemCount = Table.itemCount this.table
    member this.arn regionId = Table.getArn regionId this.table
    member this.hasDeletionProtection = Table.hasDeletionProtection this.table
    member this.attributes = Table.attributeNames this.table
    member this.indexes = Table.indexes this.table
    member this.primaryIndex = Table.primaryIndex this.table
    member this.streamArn regionId = this.streamSubscribers ?|> (Stream.arn regionId)
    member this.streamLabel = this.streamSubscribers ?|> Stream.label
    member this.streamEnabled = ValueOption.isSome this.streamSubscribers

    // will return an empty list if stream not enabled
    member this.streamConfigs =
        this.streamSubscribers
        ?|> Stream.listSubscriberConfig
        |> ValueOption.defaultValue []

[<RequireQualifiedAccess>]
module Database =

    /// <summary>Does not dispose of any resources. Only marks the DB as disposed</summary>
    let createDisposed = function
        | NonFunctioning (Disposed _) & x -> x
        | Db { info = { databaseId = x; uniqueIdentifier = y } }
        | NonFunctioning (SyncError ({ info = { databaseId = x; uniqueIdentifier = y } }, _)) -> struct (x, y) |> Disposed |> NonFunctioning

    let private noErrors = (ValueTask<SubscriberError list> []).Preserve()

    module private Execute =

        let logOperation x = DatabaseLogger.logOperation "Database" x
        let logOperationAsync x = DatabaseLogger.logOperationAsync "Database" x
        let logOperationHalfAsync x = DatabaseLogger.logOperationHalfAsync "Database" x

        let inline traceArgs f logger x =
            Logger.traceFn1 "Input: %A" logger x
            |> f logger
            |> Logger.traceFn1 "Output: %A" logger

        let inline tupleSecondArg' f x struct (y, z) = f x y z
        let inline logAndInline logOperation f =
            tupleSecondArg' f
            |> traceArgs
            |> logOperation
            |> OptimizedClosures.FSharpFunc<_, _, _>.Adapt

        let private transactWriteExpired (createdDate: DateTimeOffset) =
            createdDate < DateTimeOffset.Now - Settings.TransactWriteSettings.ClientRequestTokenTTL

        let private purgeTransactWriteCache' (dbInfo: DatabaseState) =
            let struct (firstExpired, expiredItems) =
                dbInfo.info.transactWriteResultCache
                |> MapUtils.toSeq
                |> Seq.filter (sndT >> sndT >> transactWriteExpired)
                |> Seq.map fstT
                |> Collection.tryHeadAndReturnUnmodified

            match firstExpired with
            | ValueNone -> dbInfo
            | ValueSome _ ->
                let altered =
                    Seq.fold (fun s x -> Map.remove x s) dbInfo.info.transactWriteResultCache expiredItems

                { dbInfo with info.transactWriteResultCache = altered }

        let private purgeTransactWriteCache = function
            | Db x -> purgeTransactWriteCache' x |> Db
            | NonFunctioning (SyncError (x, err)) -> purgeTransactWriteCache' x |> flip tpl err |> SyncError |> NonFunctioning
            | x -> x

        let private dbMap logOperation executeOnSyncError name f =

            let fLogged = logAndInline (logOperation name) f
            let exe request logger database =
                try
                    match database with
                    | Db x -> fLogged.Invoke(logger, struct(request, x)) |> mapSnd Db
                    | NonFunctioning (SyncError struct (db, err)) when executeOnSyncError ->
                        fLogged.Invoke(logger, struct(request, db))
                        |> mapSnd (flip tpl err >> SyncError >> NonFunctioning)
                    | NonFunctioning (Disposed x) -> invalidOp $"Database {x} has been disposed"
                    | NonFunctioning (SyncError struct (_, e)) -> raise e
                    |> mapSnd purgeTransactWriteCache
                with
                | e ->
                    Logger.debug1 "Error in %s operation" name logger

                    if Settings.Logging.LogDatabaseExceptions
                    then Logger.log1 "%O" e logger
                    reraise()

            exe

        let private dbBind logOperation executeOnSyncError name f =

            let fLogged = logAndInline (logOperation name) f
            let exe request logger database =
                try
                    match database with
                    | Db x -> fLogged.Invoke(logger, struct (request, struct (x, ValueNone)))
                    | NonFunctioning (SyncError struct (db, err)) when executeOnSyncError ->
                        fLogged.Invoke(logger, struct (request, struct (db, ValueSome err)))
                    | NonFunctioning (Disposed x) -> invalidOp $"Database {x} has been disposed"
                    | NonFunctioning (SyncError struct (_, e)) -> raise e
                    |> mapSnd purgeTransactWriteCache
                with
                | e ->
                    match struct (Settings.Logging.LogDatabaseExceptions, e.GetType().Name = "AmazonDynamoDBException") with
                    | false, true -> ()
                    | _ ->
                        Logger.debug1 "Error in %s operation" name logger

                        if Settings.Logging.LogDatabaseExceptions
                        then Logger.log1 "%O" e logger

                    reraise()

            exe

        let private emptyDatabaseState =
            { tables = DatabaseTables.empty
              streams = TableStreams.empty
              info =
                  {
                    databaseId = { regionId = "empty-database" }
                    uniqueIdentifier = IncrementingId.next()
                    transactWriteResultCache = Map.empty 
                    errorsFromDeletedStreams = noErrors } }

        let private query' isNonMutatingDescribeOperation name f =
            let f' = f >>>> (emptyDatabaseState |> flip tpl) 
            dbMap logOperation isNonMutatingDescribeOperation name f' >>>> fstT

        /// <summary>A query is a non mutating request for data</summary>
        let query name = query' false name

        /// <summary>A query is a non mutating request for information. It should not return specific pieces of data</summary>
        let describe name = query' true name

        /// <summary>A command is non mutating request for data</summary>
        let command name = dbMap logOperation false name

        /// <summary>A command is non mutating request for data</summary>
        let commandHalfAsync name = dbMap logOperationHalfAsync false name

        // /// <summary>A command is non mutating request for data</summary>
        // let commandHalfAsync name = dbMap logOperationHalfAsync false name

        /// <summary>A synchronize call is type of command which is able to change the database type, and which will be executed on sync error databases</summary>
        let synchronize name =
            let inline exe f x =
                match x with
                | NonFunctioning (Disposed _) & x -> struct (Ingested, x)
                | x -> f x

            dbBind logOperation true name >>>> exe

    /// <summary>Identifies the database within a cluster</summary>
    let databaseId = function
        | NonFunctioning (Disposed (x, _))
        | Db { info = { databaseId = x } }
        | NonFunctioning (SyncError ({ info = { databaseId = x } }, _)) -> x

    /// <summary>Identifies the database within the application</summary>
    let uniqueId = function
        | NonFunctioning (Disposed (_, x))
        | Db { info = { uniqueIdentifier = x } }
        | NonFunctioning (SyncError ({ info = { uniqueIdentifier = x } }, _)) -> x

    let hasTable name ignoreDisposed logger = function
        | NonFunctioning (Disposed _) when ignoreDisposed -> false
        | x ->
            Execute.describe
                "HAS TABLE"
                (fun logger name db -> DatabaseTables.tryGetTable name db.tables |> ValueOption.isSome)
                logger
                name
                x

    let private print' (db: DatabaseState) =

        DatabaseTables.listTables db.tables
        |> Seq.map (
            tplDouble
            >> mapFst (flip DatabaseTables.getTable db.tables
                >> fun t -> sprintf "      %s: count %i" (Table.name t) (Table.itemCount t))
            >> mapSnd (
                flip TableStreams.tryGetStream db.streams
                >> ValueOption.bind (
                    Stream.listSubscribers
                    >> Seq.map toString
                    >> Str.join ", "
                    >> function | "" -> ValueNone | x -> ValueSome $"        Subscribers {x}")
                >> ValueOption.defaultValue "      With no subscribers")
            >> tplToList
            >> Str.join "\n")
        |> Collection.atLeast1 "    No tables found"
        |> Collection.prepend "    Tables"
        |> Str.join "\n"

    let print = function
        | NonFunctioning (Disposed (id, gId)) -> $"DB {id}/{gId}\n  DISPOSED"
        | NonFunctioning (SyncError struct (db, e)) -> $"DB {db.info.databaseId}/{db.info.databaseId}\n  SYNC ERROR FROM {e.FromDb}\n{print' db}"
        | Db db -> $"DB {db.info.databaseId}/{db.info.uniqueIdentifier}\n{print' db}"

    let build =
        fun logger struct (databaseId, initialState, streamEnabledTables: string list) ->
            Logger.log1 "Database id %O" databaseId logger

            let tableNames = DatabaseTables.listTables initialState |> List.ofSeq

            let streams =
                streamEnabledTables
                |> Seq.filter (flip DatabaseTables.tryGetTable initialState >> ValueOption.isSome)
                |> Seq.fold (fun s k -> TableStreams.addStream k true logger s) TableStreams.empty
                |> TableStreams.ensureStreams tableNames

            { tables = initialState
              streams = streams
              info =
                  {
                    databaseId = databaseId
                    transactWriteResultCache = Map.empty
                    uniqueIdentifier = IncrementingId.next()
                    errorsFromDeletedStreams = noErrors } }
            |> Db
        |> Execute.logOperation "BUILD DATABASE"

    let listTables =
        Execute.describe "LIST TABLES" (fun logger (struct (start: string voption, limit) & args) db ->
            let predicate =
                start
                ?|> (compare >>> (flip (<) 0))
                ?|? asLazy true

            DatabaseTables.listTables db.tables
            |> Seq.sort
            |> Seq.filter predicate
            |> Seq.map (fun n ->
                { table = DatabaseTables.getTable n db.tables
                  streamSubscribers = TableStreams.tryGetStream n db.streams }
                |> tpl n)
            |> Seq.truncate limit)

    let private describeTable' name db =
        DatabaseTables.tryGetTable name db.tables
        ?|> (fun table ->
            { table = table
              streamSubscribers = TableStreams.tryGetStream name db.streams })

    let describeTable =
        Execute.describe "DESCRIBE TABLE" (asLazy describeTable')

    let query =
        Execute.query "QUERY" (fun logger (req: FetchInput) db ->
            db.tables
            |> DatabaseTables.getTable req.tableName
            |> tplDouble
            |> mapFst (
                Table.getIndex req.indexName
                >> Maybe.defaultWith (fun name -> ClientError.clientError $"Invalid index {name}") req.indexName
                >> Logger.logFn1 "Using index %A" logger)
            |> mapSnd Table.attributeNames
            |> uncurry (executeFetch req logger))

    let private concatStreamErrors err1 err2 =
        Io.retn Collection.concat2
        |> Io.apply err1
        |> Io.apply err2
        |%|> List.ofSeq

    let private tryRecover logger deletedSubscriberId (err: SynchronizationException) state =
        if deletedSubscriberId <> err.SubscriberId
        then
            Logger.debug1 "Removing from sync error DB. DB still corrupted from subscriber %i" err.SubscriberId.Value logger
            NonFunctioning (SyncError (state, err))
        else
            let db = Db state
            Logger.log1 "Database %O has recovered from synchronization error" db logger
            db

    type TableName = string
    type AddDeletionProtection = bool
    type RemoveSubscriberFn = Logger -> Database -> Database
    let private subscribeToStream' =
        let apply f (state: DatabaseState) =
            let struct (errs, streams) = f state
            { state with
                streams = streams
                info.errorsFromDeletedStreams = concatStreamErrors state.info.errorsFromDeletedStreams errs }

        let executeIfUnwrappable logger subscriberId f = function
            | NonFunctioning (Disposed _) & x ->
                Logger.debug0 "Database is disposed" logger
                x
            | Db state -> 
                Logger.debug0 "Removing from healthy DB" logger
                apply f state |> Db
            | NonFunctioning (SyncError (state, err)) ->
                let db = apply f state
                tryRecover logger subscriberId err state

        let removeSubscriber struct (remove, id) logger =
            executeIfUnwrappable logger id (fun db -> remove logger db.streams)

        fun logger struct (deletionProtection: AddDeletionProtection, table: TableName, behaviour, dataType, subscriber) db ->
            TableStreams.addSubscriber db.info.databaseId table behaviour dataType subscriber deletionProtection logger db.streams
            |> mapFst (
                tplDouble
                >> mapFst sndT
                >> mapSnd removeSubscriber)
            |> mapSnd (fun x -> {db with streams = x })

    let subscribeToStream = Execute.command "ADD SUBSCRIBER" subscribeToStream'

    let infiniteScan = { maxScanItems = Int32.MaxValue; maxPageSizeBytes = Int32.MaxValue }: ScanLimits

    let private validateCondition =

        let validateMessages' reqName =
            struct (
                $"An error occurred (ConditionalCheckFailedException) when calling the {reqName} operation: The conditional request failed",
                $"Reverting changes to {reqName} operation")

        let keySelecctor (x: string) = x
        let validateMessages = memoize (ValueSome struct (100, 200)) keySelecctor validateMessages' >> sndT

        fun struct (logger, reqName) result ->
            match result with
            | struct ({ conditionExpression = ValueSome _ }: ConditionAndProject<BasicReturnValues>, { items = [||] }: FetchOutput) ->
                let struct (err, log) = validateMessages reqName
                Logger.log0 log logger
                ClientError.clientError err
            | _, { items = items } -> items

    let private removeUnWanted = function
        | struct ({ returnValues = BasicReturnValues.None }, items) -> [||]
        | { returnValues = AllOld }, items -> items

    let private itemEmptyInput = Item.empty |> Seq.singleton |> QueryItems.ScannedItems
    let private noOutput = [||] |> asLazy

    let private conditionAndProjectPostProcessor reqName struct (req: ConditionAndProject<BasicReturnValues>, updateExpression) logger struct (item, table) =

        let index = Table.primaryIndex table
        let attrs = Table.attributeNames table

        let struct (queryExpression, outputModifier) =
            item
            ?|> (Seq.singleton >> QueryItems.ScannedItems)
            ?|> flip tpl id
            ?|? struct (itemEmptyInput, noOutput)

        Logger.trace1 "Items %O" queryExpression logger

        // still do filter even if there is no expression. This validates the other expression parts
        { tableName = req.tableName
          indexName = ValueNone
          queryExpression = queryExpression
          filterExpression = req.conditionExpression
          expressionAttrValues = req.expressionAttrValues
          expressionAttrNames = req.expressionAttrNames
          updateExpression = updateExpression
          limits = infiniteScan
          pageSize = Int32.MaxValue
          lastEvaluatedKey = ValueNone
          forwards = true
          // The request has basic return values of None or AllOld
          selectTypes = AllAttributes }
        |> flip1To4 executeConditionAndProject logger index attrs
        |> tpl req
        |> validateCondition struct (logger, reqName)
        |> outputModifier
        |> tpl req
        |> removeUnWanted

    let inline private publishChange logger streams data = TableStreams.onChange data logger streams

    let private buildSubscriberMessage logger db tableName synchronizationPath packet =
        let table = DatabaseTables.getTable tableName db.tables
        let listBuilder =
            ValueOption.map (flip NonEmptyList.prepend) synchronizationPath
            |> ValueOption.defaultValue NonEmptyList.singleton

        { data =
            { packet = packet
              tableArn = table |> Table.arnBuilder }
          correlationId = Logger.id logger
          tableName = tableName
          synchronizationPacketPath = listBuilder db.info.databaseId }
        |> ChangeDataCapture
        |> OnChange

    let private publishCdcPacket (logger: Logger) (db: DatabaseState) (eBrake: EmergencyBrake) tableName synchronizationPath =
        mapFst (fun x ->
            ({ request = buildSubscriberMessage logger db tableName synchronizationPath x
               emergencyBrake = eBrake }: EmergencyBrakeRequest<_>)
            |> publishChange logger db.streams)
        >> fun struct (s, t) -> { db with streams = s; tables = t }

    let private put' apiRequestName execute logger struct (synchronizationPath, updateExpression, args: EmergencyBrakeRequest<PutItemArgs<_>>) db =

        let tableName = args |> EmergencyBrakeRequest.map _.tableName

        execute args.request logger db.tables
        |> tplDouble
        |> mapFst (
            tplDouble
            >> mapFst (
                mapSnd (DatabaseTables.getTable args.request.tableName)
                >> mapFst (
                    _.changeResult
                    // read the deletions because we are scanning retrospectively
                    >> ChangeResults.deletedItems
                    >> Collection.tryHead)
                >> conditionAndProjectPostProcessor apiRequestName struct (args.request.conditionExpression, updateExpression) logger
                >> Collection.tryHead)
            >> mapSnd (publishCdcPacket logger db args.emergencyBrake args.request.tableName synchronizationPath))
        |> mapSnd fstT

    let private clientPut =
        let inline buildArgs conditionExpression x = struct (ValueNone, conditionExpression, x)
        let inline reformatResult struct (struct (x, y), z) = struct (struct (x, z), y)
        let buildPut struct (struct (reqName, opName), conditionExpression) =
            let f1 = put' reqName DatabaseTables.put |> flip
            let f2 = buildArgs conditionExpression >> f1 |> flip >>>> reformatResult
            Execute.command opName f2

        let keySelector (x: struct (struct (string * string) * string voption)) = x
        memoize (ValueSome (50, 100)) keySelector buildPut >> sndT

    let put =
        let inline reformatResult struct (struct (x, _), z) = struct (x, z)
        EmergencyBrakeRequest.noBrake >> clientPut struct (struct ("PutItem", "PUT"), ValueNone) >>>> reformatResult

    let private outputMapper struct (returnValues, projector, logger) changeResult =

        let inline mapLazyF (f: Lazy<_>) x =
            match x with
            | ValueSome x' -> f.Value x' |> ValueSome
            | ValueNone -> ValueNone

        let inline oldItem () =
            ChangeResults.deletedItems changeResult.changeResult
            |> Collection.tryHead

        let inline newItem () =
            ChangeResults.putItems changeResult.changeResult
            |> Collection.tryHead

        let inline debug x =
            Logger.debug1 "Projected updates %A" returnValues logger
            x

        match returnValues with
        | Basic _ -> id // basic projections are handled by the put' part of the operation
        | AllNew -> newItem() ?|> Item.attributes |> asLazy
        | UpdatedOld -> oldItem() |> mapLazyF projector |> asLazy
        | UpdatedNew -> newItem () |> mapLazyF projector |> asLazy
        >> debug

    let private mapReturnValues = function
        | Basic x -> x
        | _ -> None

    let private updatePut logger updateExpression args db =
        put' "UpdateItem" DatabaseTables.put logger struct (ValueNone, updateExpression, args) db

    type private UpdateResult =
        { updateProjector: Lazy<Item -> Map<string,AttributeValue>>
          cdcPacket: CdcPacket
          projectionFromPutOperation: Map<string,AttributeValue> voption } 

    let private noProjections = lazy(fun _ -> Map.empty)

    let private update' =
        let createItem logger attrs table =
            Logger.log0 "Applying mock put" logger 

            // ignore the table state from the put, just take the item
            let struct (result, _) = Table.put logger attrs table
            result.changeResult.OrderedChanges |> List.head |> _.Put.Value

        let getPart logger (args: UpdateItemArgs) db =
            let table = DatabaseTables.getTable args.tableName db.tables

            Table.get args.key table
            |> ValueOption.defaultWith (fun _ -> createItem logger args.key table)
            |> tpl table

        let putPart logger (args: EmergencyBrakeRequest<_>) struct (struct (table, item), db) =
            Logger.log0 "Found item, applying updates" logger

            let struct (updateResult, projector) = 
                args.request.updateExpression
                ?|> (fun updateExpr ->

                    let updateArgs =
                        { updateExpression = updateExpr
                          expressionAttrValues = args.request.conditionExpression.expressionAttrValues
                          expressionAttrNames = args.request.conditionExpression.expressionAttrNames }: ExpressionExecutors.Update.UpdateInput

                    let attributeNames = Table.attributeNames table
                    let tableKeys = Table.primaryIndex table |> Index.keyConfig
                    ExpressionExecutors.Update.executeUpdate updateArgs logger tableKeys attributeNames item)
                ?|? struct (Item.attributes item, noProjections)

            let putReq =
                EmergencyBrakeRequest.map (fun (args: UpdateItemArgs) ->
                    { item = updateResult
                      conditionExpression = ConditionAndProject.map mapReturnValues args.conditionExpression }: PutItemArgs<_>) args

            Logger.log0 "Updates applied, persisting" logger
            let struct (struct (putProjection, database), cdc) =
                updatePut logger args.request.updateExpression putReq db

            let output =
                { cdcPacket = cdc
                  updateProjector = projector
                  projectionFromPutOperation = putProjection } 

            struct (output, database)

        let compose logger args db =
            getPart logger args.request db
            |> flip tpl db
            |> putPart logger args

        Execute.command "UPDATE" compose

    let update args logger =
        EmergencyBrakeRequest.noBrake args
        |> flip update' logger
        >> mapFst (fun result ->
            let finalProjectionBuilder = outputMapper struct (args.conditionExpression.returnValues, result.updateProjector, logger)
            finalProjectionBuilder result.cdcPacket result.projectionFromPutOperation)

    let tryGetStreamSubscribers = Execute.describe "GET STREAM SUBSCRIBERS" (fun logger tableName db ->
        TableStreams.tryStreamSubscribers tableName db.streams)

    let private delete' execute logger struct (args: EmergencyBrakeRequest<DeleteItemArgs<_>>, synchronizationPath) db =
        execute args.request logger db.tables
        |> tplDouble
        |> mapFst (fun struct (cdcPacket, db) ->
            let table = DatabaseTables.getTable args.request.tableName db
            // read the deletions because we are scanning retrospectively
            let postProcessed = ChangeResults.deletedItems cdcPacket.changeResult |> Collection.tryHead
            conditionAndProjectPostProcessor "DeleteItem" struct (args.request.conditionExpression, ValueNone) logger struct (postProcessed, table)
            |> Collection.tryHead
            |> tpl cdcPacket)
        |> mapSnd (publishCdcPacket logger db args.emergencyBrake args.request.tableName synchronizationPath)

    let delete =
        let inline addNoSyncPath x = struct (x, ValueNone)
        EmergencyBrakeRequest.noBrake >> addNoSyncPath >> (delete' DatabaseTables.delete |> Execute.command "DELETE") >>>> mapFst sndT

    let clearTable =
        Execute.command "CLEAR TABLE" (fun logger tableName db ->
            DatabaseTables.clear tableName logger db.tables
            ?|> publishCdcPacket logger db EmergencyBrakeRequest.noBrakeValue tableName ValueNone
            ?|? db
            |> tpl ())

    let private getPostProcessor struct (getReq: GetItemArgs, logger) struct (getResponse: Item seq, table) =

        let index = Table.primaryIndex table
        let attrs = Table.attributeNames table

        // still do filter even if there is no expression. This validates the other expression parts
        ({ tableName = getReq.tableName
           indexName = ValueNone
           updateExpression = ValueNone 
           filterExpression = getReq.conditionExpression.conditionExpression
           expressionAttrValues = getReq.conditionExpression.expressionAttrValues
           expressionAttrNames = getReq.conditionExpression.expressionAttrNames
           queryExpression = ScannedItems getResponse
           limits = { infiniteScan with maxPageSizeBytes = getReq.maxPageSizeBytes }
           pageSize = Int32.MaxValue
           lastEvaluatedKey = ValueNone
           forwards = true
           selectTypes = getReq.conditionExpression.returnValues }: ExpressionExecutors.Fetch.FetchInput)
        |> flip1To4 ExpressionExecutors.Fetch.executeFetch logger index attrs

    let get =
        let finalMapper args (x: ExpressionExecutors.Fetch.FetchOutput) =
            { items = x.items
              scannedCount = x.scannedCount
              itemSizeBytes = x.scannedSize
              unevaluatedKeys =
                  if x.items.Length < args.keys.Length
                  then Array.skip x.items.Length args.keys
                  else Array.empty }

        let getItems logger args =
            args.keys
            |> Seq.map (
                tpl args.tableName
                >> flip DatabaseTables.get logger
                >> Reader.create)
            |> Reader.traverseSeq
            |> flip Reader.execute
            >> Maybe.traverse

        Execute.query "GET" (fun logger args db ->
            // there is a lot of correlation in this function that needs to be respected
            //  1. getItems must process keys in exact input order
            //  2. getPostProcessor first arg (ValueNone) specifies no filtering outsise of page limits
            //  3. finalMapper assumes the ordering to get not processed keys and that nothing was filtered out
            //      to get itemSizeBytes

            db.tables
            |> tplDouble
            |> mapFst (getItems logger args)
            |> mapSnd (DatabaseTables.getTable args.tableName)
            |> getPostProcessor struct (args, logger)
            |> finalMapper args)

    let private updateTable' logger struct (name, synchronizationPath, args) db =

        let struct (table, dbResult) = DatabaseTables.updateTable name args.updateTableData logger db.tables
        let listBuilder =
            ValueOption.map (flip NonEmptyList.prepend) synchronizationPath
            |> ValueOption.defaultValue NonEmptyList.singleton

        let synchronizationMessage =
            { data = args.updateTableData.schemaChange
              correlationId = Logger.id logger
              tableName = name
              synchronizationPacketPath = listBuilder db.info.databaseId }
            |> SchemaChange
            |> OnChange
            |> EmergencyBrakeRequest.noBrake

        let streamUpdateResult =
            // update streams
            args.streamConfig
            ?|> flip1To3 (TableStreams.updateStream name) logger db.streams
            |> ValueOption.defaultValue db.streams

            // publish changes to streams
            |> flip (publishChange logger) synchronizationMessage

        let tableDetails =
          { table = table
            streamSubscribers = TableStreams.tryGetStream name streamUpdateResult }

        let dbState = { db with tables = dbResult; streams = streamUpdateResult }
        struct (tableDetails, dbState)

    let updateTable =
        let inline addNoPath struct (x, y) = struct (x, ValueNone, y)
        addNoPath >> flip updateTable' |> flip |> Execute.command "UPDATE"

    /// <summary>
    /// Deleting tables must happen in 2 phases.
    /// First, delete the table, second wait for streams to complete final executions
    /// </summary>
    let tryDeleteTable =
        Execute.commandHalfAsync "DELETE TABLE" (fun logger name db ->

            let struct (table, dbResult) = DatabaseTables.tryDeleteTable name logger db.tables
            let struct (streamResult, streams) = TableStreams.deleteTable name logger db.streams
            let streamResult = streamResult.Preserve()

            // ignore any errors and just return something that can be awaited
            // errors will be stored with the database and recovered with "await" function call
            let awaiter =
                Io.recover (fun () -> streamResult |%|> ignore) ignoreTyped<exn>
                |%|> (asLazy (ValueOption.map (fun table ->
                    { table = table; streamSubscribers = TableStreams.tryGetStream name streams }) table))

            let errs = concatStreamErrors db.info.errorsFromDeletedStreams streamResult
            { db with tables = dbResult; streams = streams; info.errorsFromDeletedStreams = errs }
            |> tpl awaiter)

    let deleteTable name logger db =
        tryDeleteTable name logger db
        |> mapFst (Io.map (function
            | ValueSome x -> x
            | ValueNone -> ClientError.clientError $"Table \"{name}\" not found"))

    let addTable =
        Execute.command "ADD TABLE" (fun logger (args: CreateTableData) db ->
            let addTable = DatabaseTables.addTable args.tableConfig
            let addStream = TableStreams.addStream args.tableConfig.name args.createStream

            let tables = addTable logger db.tables
            let streams = addStream logger db.streams
            { db with
               tables = tables
               streams = streams }
            |> tpl
                { table = DatabaseTables.getTable args.tableConfig.name tables
                  streamSubscribers = TableStreams.tryGetStream args.tableConfig.name streams })

    let addClonedTable =
        Execute.command "CLONING TABLE" (fun logger struct (name, table, subscription: ClonedTableSubscriberOpts voption) db ->
            let addTable = DatabaseTables.addClonedTable name table

            let t = addTable logger db.tables
            let s =
                subscription
                ?|> (fun _ ->
                    TableStreams.addStream name true logger db.streams)
                |> ValueOption.defaultValue db.streams

            let state =
                { db with
                   tables = t
                   streams = s }

            let inline buildSubscriberArgs opts x = struct (opts.addDeletionProtection, name, opts.behaviour, opts.dataType, x)

            subscription
            ?|> (fun opts ->
                opts.subscriber
                |> buildSubscriberArgs opts
                |> flip (subscribeToStream' logger) state
                |> mapFst ValueSome)
            |> ValueOption.defaultValue struct (ValueNone, state))

    let importClone =
        Execute.command "IMPORT CLONE" (fun logger (args: DatabaseClone) db ->
            let tables =
                args.initialState
                |> DatabaseTables.listTables
                |> Seq.map (
                    tplDouble
                    >> mapSnd (flip DatabaseTables.getTable args.initialState))
                |> Seq.fold (fun s struct(name, data) ->
                    DatabaseTables.addClonedTable name data logger s) db.tables

            let streams =
                args.streamsEnabled
                |> List.fold (fun s x ->
                    TableStreams.addStream x true logger s) db.streams

            { db with tables = tables; streams = streams } |> tpl ())

    let buildCloneData =
        Execute.describe "BUILD CLONE INPUT" (fun logger () db ->
            { initialState = db.tables
              streamsEnabled = TableStreams.listEnabled db.streams |> List.ofSeq }) |> apply () 

    let setStreamBehaviour =
        Execute.command "SET STREAM BEHAVIOUR" (fun logger struct (tableName, subscriberId, behaviour) db ->
            let str = TableStreams.setBehaviour tableName subscriberId behaviour db.streams
            { db with streams = str } |> tpl ())
        >>>> sndT

    let awaitAllSubscribers =
        let detachDeletedSubscriberErrors c db =
            struct (Io.addCancellationToken c db.info.errorsFromDeletedStreams, {db with info.errorsFromDeletedStreams = noErrors})

        let mergeErrors struct (err1, struct (err2, x)) =
            Io.retn Collection.concat2
            |> Io.apply err1
            |> Io.apply err2
            |%|> List.ofSeq
            |> flip tpl x

        let rebuild struct (rebuild, struct (errs, db)) = struct (errs, rebuild db)

        // unwrap but more permissive of non functioning state
        //  * do nothing for disposed dbs
        //  * allow await on sync error databases   
        let unwrapForSubscriberAwait = function
            | Db x -> ValueSome struct (Db, x)
            | NonFunctioning (Disposed x) -> ValueNone
            | NonFunctioning (SyncError struct (db, err)) -> ValueSome struct (flip tpl err >> SyncError >> NonFunctioning, db)

        let noErrors = (ValueTask<_> []).Preserve()

        fun c logger ->

            let await =
                detachDeletedSubscriberErrors c
                >> mapSnd (fun db ->
                    TableStreams.await ValueNone logger db.streams
                    |> mapSnd (fun x -> { db with streams = x })
                    |> mapFst (Io.addCancellationToken c))
                >> mergeErrors

            tplDouble
            // path 1, disposed database, do nothing
            >> mapFst (tpl noErrors)
            // path 2, await subscribers
            >> mapSnd (
                unwrapForSubscriberAwait
                >> ValueOption.map (
                    mapSnd await
                    >> rebuild))
            >> uncurry ValueOption.defaultValue

    module Replication =

        [<Struct; IsReadOnly>]
        type private Change =
            | Del of d: DeleteItemArgs<struct (IncrementingId * Map<string, AttributeValue>)>
            | Put of PutItemArgs<Item>

            with
            member this.changeId =
                match this with
                | Del x -> (fstT x.key).Value
                | Put x -> (Item.internalId x.item).Value

        let private compareChange (x: Change) (y: Change) =
            x.changeId - y.changeId

        let private buildOrderedChanges (change: DatabaseSynchronizationPacket<TableCdcPacket>) =
            let condition = ConditionAndProject.fromTable change.tableName
            let changes =
                List.map (ChangeResult.asTpl >> function
                    | struct (id, Simple(Delete del)) -> { conditionExpression = condition; key = struct (id, Item.attributes del) } |> Del
                    | struct (_, Simple(Create put))
                    // ignore the DELETE part of this operation. It will be performed automatically
                    | struct (_, Replace (put, _)) -> {conditionExpression = condition; item = put } |> Put) change.data.packet.changeResult.OrderedChanges

            let attempts =
                List.map (
                    DeleteAttemptData.asDeleteRequest
                    >> fun x -> { key = x; conditionExpression = condition } |> Del) change.data.packet.changeResult.OrderedDeleteAttempts

            Collection.mergeSortedSeq compareChange changes attempts

        let private replicationPut =
            let inline reshapeInputs struct (x, y) = struct (x, ValueNone, EmergencyBrakeRequest.noBrake y)
            reshapeInputs >> flip (put' "DatabaseReplication" DatabaseTables.putReplicated) |> flip
        let private replicationDelete =
            let inline reshapeInputs struct (x, y) = struct (EmergencyBrakeRequest.noBrake x, y)
            reshapeInputs >> flip (delete' DatabaseTables.deleteReplicated) |> flip

        let private hasTable = describeTable' >>> ValueOption.isSome

        let private syncErrorDescriber = Logger.describable (fun struct (fromDb: DatabaseId, toDb: DatabaseId, subscriberId: IncrementingId) ->
            $"Database {toDb} is now corrupted with a synchronization error from database {fromDb}, subscriber {subscriberId}")

        let private buildCorrectDbType = 
            let build x =
                ValueOption.map (tpl x >> SyncError >> NonFunctioning)
                >> ValueOption.defaultValue (Db x)

            flip build

        let private synchronize' logger struct (streamSubscriberId, change) struct (db, syncErr) =

            match change with
            | ChangeDataCapture cdc ->
                buildOrderedChanges cdc
                |> Seq.fold (fun db -> function
                    | Put p -> replicationPut logger struct (ValueSome cdc.synchronizationPacketPath, p) db |> fstT |> sndT
                    | Del d -> replicationDelete logger struct (d, ValueSome cdc.synchronizationPacketPath) db |> sndT) db
                |> buildCorrectDbType syncErr
                |> tpl Ingested
            | SubscriberChange { data = SubscriberDeleted id} ->
                match syncErr with
                | ValueNone -> Db db
                | ValueSome err -> tryRecover logger id err db
                |> tpl Ingested
            | SchemaChange cdc ->
                try
                    let req ={ updateTableData = { schemaChange = cdc.data; deletionProtection = ValueNone }; streamConfig = ValueNone }

                    updateTable' logger struct (cdc.tableName, ValueSome cdc.synchronizationPacketPath, req) db
                    |> sndT
                    |> buildCorrectDbType syncErr
                    |> tpl Ingested
                with
                | e when Table.isItemNotValidOnIndexError e ->
                    let fromDb = DatabaseChange.instancePacketPath change |> NonEmptyList.unwrap |> List.head
                    let toDb = db.info.databaseId
                    syncErrorDescriber struct (fromDb, toDb, streamSubscriberId)
                    |> flip (Logger.error1 "%O") logger

                    let ee = SynchronizationException(fromDb, toDb, streamSubscriberId, AddedIndexConflict, e)
                    struct (SynchronizationError ee, struct (db, ee) |> SyncError |> NonFunctioning)

        let synchronize =

            Execute.synchronize "INGEST SYNC" (fun logger (struct (streamSubscriberId, change) & x) (struct (db, syncErr) & s) ->
                let logger = DatabaseChange.correlationId change |> flip Logger.correlate logger
                if DatabaseChange.instancePacketPath change |> NonEmptyList.unwrap |> List.length > 100
                then serverError "Circular replication detected"

                if DatabaseChange.instancePacketPath change |> NonEmptyList.unwrap |> List.contains db.info.databaseId
                then
                    Logger.log1 "Sync full circle %O" db.info.databaseId logger
                    struct (FullCircle, buildCorrectDbType syncErr db)
                else
                    Logger.log1 "Processing synchonization for %O" db.info.databaseId logger

                    if hasTable (DatabaseChange.tableName change) db |> not
                    then
                        Logger.log2 "Table %O not found in database %O" (DatabaseChange.tableName change) databaseId logger
                        struct (TableNotFound, buildCorrectDbType syncErr db)
                    else
                        synchronize' logger x s)

    module TransactWrite =
        type TransactWrites =
            { puts: PutItemArgs<Map<string,AttributeValue>> list
              deletes: DeleteItemArgs<Map<string,AttributeValue>> list
              updates: UpdateItemArgs list
              idempotencyKey: string voption
              conditionCheck: GetItemArgs list }

        let private singleKey = function
            | [|x|] -> x
            | xs -> invalidOp "Expected single key"

        let private validateWrites logger (writes: TransactWrites) (database: Database) =
            let totalCount =
                List.length writes.deletes
                + List.length writes.puts
                + List.length writes.conditionCheck
                + List.length writes.updates

            if totalCount > Settings.TransactWriteSettings.MaxItemsPerRequest
            then ClientError.clientError
                     ($"Maximum items in a transact write is {Settings.BatchItems.MaxBatchWriteItems}, got {totalCount}. "
                       + $"You can change this value by modifying {nameof Settings}.{nameof Settings.TransactWriteSettings}.{nameof Settings.TransactWriteSettings.MaxItemsPerRequest}")

            let affectedTables =
                [
                    writes.puts |> Seq.map _.tableName
                    writes.deletes |> Seq.map _.tableName
                    writes.updates |> Seq.map _.tableName
                    writes.conditionCheck |> Seq.map _.tableName
                ]
                |> Seq.concat
                |> Seq.distinct
                |> Seq.map (fun name -> describeTable name logger database |> Maybe.expectSomeErr "Cannot find table %s" name)
                |> Seq.fold (fun s table -> Map.add table.name table s) Map.empty

            let getKeys struct (tableName, attributes) =
                Map.find tableName affectedTables
                |> _.primaryIndex
                |> Index.keyConfig
                |> KeyConfig.validateAttributeKeys attributes
                |> function
                    | struct (AttributeLookupResult.HasValue x, ValueNone) -> struct (x, ValueNone)
                    | AttributeLookupResult.HasValue x, ValueSome (AttributeLookupResult.HasValue y) -> struct (x, ValueSome y)
                    | pp -> ClientError.clientError $"Invalid key specification for write request to table {tableName}"

            let tableModifications =    
                [
                    writes.puts |> Seq.map (fun x -> struct (x.tableName, x.item))
                    writes.deletes |> Seq.map (fun x -> struct (x.tableName, x.key))
                    writes.updates |> Seq.map (fun x -> struct (x.tableName, x.key))
                ]
                |> Seq.concat
                |> List.ofSeq

            let keyErrs =
                tableModifications
                |> Seq.map (fun struct (k, v) -> struct (k, struct (k, v)))
                |> Collection.concat2 (writes.conditionCheck |> Seq.map (fun x -> struct (x.tableName, struct (x.tableName, singleKey x.keys))))
                |> Collection.mapSnd getKeys
                |> Collection.groupBy id
                |> Collection.mapSnd Array.ofSeq
                |> Seq.filter (sndT >> Array.length >> flip (>) 1)
                |> Seq.map (fstT >> fun struct (table, struct (pk, sk)) ->
                    let skname = sk ?|> (AttributeValue.describe >> sprintf "/%O") ?|? ""
                    sprintf " * Duplicate request on item %s/%s%s" table (AttributeValue.describe pk) skname)
                |> Str.join "\n"

            match keyErrs with
            | "" -> ()
            | err -> sprintf "Error executing transact write\n%s" err |> ClientError.clientError

            tableModifications
            |> Collection.groupBy fstT
            |> Collection.mapSnd (Seq.map sndT >> List.ofSeq)
            |> MapUtils.ofSeq

        let private getForCondition =
            get
            >>>> function
                | { items = [||]; scannedCount = s } when s > 0 -> $"An error occurred (ConditionalCheckFailedException): The conditional request failed" |> ClientError.clientError
                | xs -> xs.itemSizeBytes

        let private validateItemSize = function
            | s when s > Settings.TransactWriteSettings.MaxRequestDataBytes ->
                $"The maximum size of a transact write is {Settings.TransactWriteSettings.MaxRequestDataBytes}B, attempted {s}B. "
                + $"You can change this value by modifying {nameof Settings}.{nameof Settings.TransactWriteSettings}.{nameof Settings.TransactWriteSettings.MaxRequestDataBytes}"
                |> ClientError.clientError
            | _ -> ()

        let private transactPut =
            let inline reformatResult struct (struct (_, y), z) = struct (y, z)
            EmergencyBrakeRequest.create
            >>> (
                clientPut struct (struct ("TransactPut", "TRANSACT PUT"), ValueNone)
                >>>> reformatResult)

        let private transactDelete =
            let inline addNoSyncPath x = struct (x, ValueNone)
            EmergencyBrakeRequest.create
            >>> (
                addNoSyncPath >> (delete' DatabaseTables.delete |> Execute.command "DELETE")
                >>>> mapFst fstT)

        let private transactUpdate =
            EmergencyBrakeRequest.create
            >>> (
                update' >>>> mapFst _.cdcPacket)
        type private IReplicationTransaction =
            inherit IDisposable
            abstract member Commit: unit -> unit
            abstract member Rollback: unit -> unit
            abstract member CompleteSignal: Task<bool>

        let private buildTransaction () =
            let tcs = TaskCompletionSource<bool>()
            let mutable operationCounter = 0

            let rollback disposing =
                if Interlocked.Increment(&operationCounter) = 1 
                then tcs.SetResult(false)
                elif not disposing then serverError "Transaction rollback error"

            { new IReplicationTransaction
                with
                member this.CompleteSignal = tcs.Task
                member this.Commit() =
                    if Interlocked.Increment(&operationCounter) = 1
                    then tcs.SetResult(true)
                    else serverError "Transaction commit error"
                member this.Rollback() = rollback false
                member this.Dispose() = rollback true }

        let private logCacheHit logger = function
            | ValueSome _ & x ->
                Logger.log0 "Cache hit" logger
                x
            | ValueNone & x ->
                Logger.log0 "Cache miss" logger
                x

        let private tryFindPreviousTransactWrite =
            Execute.query "IDEMPOTENCY CHECK" (fun logger idempotencyKey db ->
                Logger.log1 "Using idempotency key %s" idempotencyKey logger
                MapUtils.tryFind idempotencyKey db.info.transactWriteResultCache
                ?|> fstT
                |> logCacheHit logger)

        let private recordTransactWrite =
            Execute.command "IDEMPOTENCY RECORD" (fun logger struct (idempotencyKey, result) db ->
                Logger.log1 "Recording idempotency key %s" idempotencyKey logger
                { db with
                   info.transactWriteResultCache =
                       Map.add idempotencyKey struct (result, DateTimeOffset.Now) db.info.transactWriteResultCache }
                |> tpl ())
            >>>> sndT

        let private write' (writes: TransactWrites) logger db _ =

            let addSize acc (cdc: CdcPacket) =
                match ChangeResults.deletedItems cdc.changeResult |> List.ofSeq with
                | [] -> ChangeResults.putItems cdc.changeResult
                | xs -> xs
                |> Seq.map Item.size |> Seq.sum
                |> ((+)acc)

            use transction = buildTransaction ()
            let emergencyBrake () = ValueTask<_>(task = transction.CompleteSignal)
            let stats = validateWrites logger writes db
            let resultDb =
                struct (0, db)
                |> flip (List.fold (fun struct (cdc, db) ->
                    flip1To3 (transactPut (emergencyBrake())) logger db >> mapFst (addSize cdc))) writes.puts
                |> flip (List.fold (fun struct (cdc, db) ->
                    flip1To3 (transactDelete (emergencyBrake())) logger db >> mapFst (addSize cdc))) writes.deletes
                |> flip (List.fold (fun struct (cdc, db) ->
                    flip1To3 (transactUpdate (emergencyBrake())) logger db >> mapFst (addSize cdc))) writes.updates
                |> flip (List.fold (fun struct (cdc, db) ->
                    flip1To3 getForCondition logger db >> flip tpl db)) writes.conditionCheck
                |> mapFst validateItemSize
                |> sndT
                |> (writes.idempotencyKey
                    ?|> flip tpl stats
                    ?|> flip recordTransactWrite logger
                    ?|? id)

            transction.Commit()    
            struct (stats,  resultDb)

        let write (writes: TransactWrites) logger db =

            writes.idempotencyKey
            ?>>= flip1To3 tryFindPreviousTransactWrite logger db
            ?|> flip tpl db
            ?|>? write' writes logger db