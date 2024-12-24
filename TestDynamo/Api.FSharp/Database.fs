namespace TestDynamo.Api.FSharp

open System
open System.Threading
open System.Threading.Tasks
open TestDynamo
open TestDynamo.Data
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model
open Microsoft.Extensions.Logging
open System.Runtime.CompilerServices

type IStreamSubscriberDisposal =
    inherit IDisposable

    abstract member SubscriberId: IncrementingId

[<Struct; IsReadOnly>]
type private DatabaseState =
    { tables: DatabaseTables
      streams: TableStreams }

type DatabaseCloneData =
    { data: Model.DatabaseClone
      databaseId: DatabaseId }
    with
    static member empty databaseId =
        { data = Model.DatabaseClone.empty
          databaseId = databaseId }

    static member emptyDefault =
        DatabaseCloneData.empty DatabaseId.defaultId

/// <summary>
/// A mutable Database object containing Tables and Streams
/// </summary>
type Database private(logger: ILogger voption, cloneData: DatabaseCloneData) =

    let globalLogger = logger
    let iLoggerOrLoggerInput = ValueOption.map Either1

    let buildLogger localLogger =
        match localLogger with
        | ValueSome (Either1 w) -> DatabaseLogger.buildLogger globalLogger (ValueSome w)
        | ValueSome (Either2 l) -> l
        | ValueNone -> DatabaseLogger.buildLogger globalLogger ValueNone

    let buildOperationLogger operationName localLogger =
        let logger = buildLogger localLogger
        Logger.debug1 "[Api.Database] %s" operationName logger
        Logger.scope logger

    let state =
        let initialState = cloneData.data.initialState
        let initialStreamConfig = cloneData.data.streamsEnabled
        let databaseId = cloneData.databaseId

        let inputs = struct (databaseId, initialState, initialStreamConfig)

        MutableValue.create (Model.Database.build (buildLogger ValueNone) inputs)

    let mutable debugTables = lazy(List.empty)

    let buildDebugTable struct (name, table) =
        let table = LazyDebugTable(name, table.table)
        table.EagerLoad()

    let buildDebugTables logger h =
        Database.listTables struct (ValueNone, Int32.MaxValue) logger h
        |> Seq.map buildDebugTable
        |> List.ofSeq

    static let describable = Logger.describable (fun struct (databaseId: DatabaseId, uniqueId: IncrementingId, lockId: IncrementingId, threadId: int) ->
        $"{{databaseId = {databaseId}/{uniqueId.Value}, thread = {threadId}, lockId = {lockId.Value}}}") 

    let mutate logger f state =
        let lockId = IncrementingId.next()
        let lck = describable (Database.databaseId state, Database.uniqueId state, lockId, Thread.CurrentThread.ManagedThreadId)
        Logger.debug1 "Acquiring lock %O" lck logger

        try
            let struct (x, h) = f logger state
            debugTables <- lazy(buildDebugTables logger h)
            struct (x, h)
        finally
            Logger.debug1 "Releasing lock %i" lockId.Value logger

    let lockedAndLogged operation localLogger f =
        let struct (logger, d) = buildOperationLogger operation localLogger
        use _ = d

        MutableValue.mutate (mutate logger f) state

    let waitTooLong logger errorMessage iteration timespan =
        if timespan > Settings.DatabaseLockWaitTime then sprintf "%s (%A)" errorMessage timespan |> serverError

        Logger.debug2 "Acquire lock failed after %O, iteration %i. Trying again" timespan iteration logger
        Task.Delay(iteration * 10)
        |> ValueTask
        |> Io.normalizeVt

    let tryLockedAndLogged operation errorMessage localLogger f =
        let struct (logger, d) = buildOperationLogger operation localLogger
        use _ = d

        MutableValue.retryMutate (waitTooLong logger errorMessage) (mutate logger f) state

    let loggedGet operation localLogger f =
        let struct (logger, d) = buildOperationLogger operation localLogger
        use _ = d

        MutableValue.get state |> f logger

    let subscriberRemovalDisposable remove =
        let remove' logger state = remove logger state |> tpl ()

        { new IDisposable with
            member _.Dispose() =
                lockedAndLogged "REMOVE SUBSCRIBER" ValueNone remove' }

    static let noSubscriberChange = ValueTask<_>(()).Preserve()

    static let createSubscriberDisposal struct (id: IncrementingId, disposer: IDisposable) =
        { new IStreamSubscriberDisposal with
            member _.Dispose() = disposer.Dispose()
            override this.SubscriberId = id }

    // trigger an update to set debug tables
    do lockedAndLogged "INIT" ValueNone (asLazy (tpl ()))

    new() = new Database(ValueNone, DatabaseCloneData.empty DatabaseId.defaultId)

    new(cloneData: DatabaseCloneData) =
        new Database(ValueNone, cloneData)

    new(logger: ILogger) =
        new Database(ValueSome logger, DatabaseCloneData.empty DatabaseId.defaultId)

    new(logger: ILogger, cloneData: DatabaseCloneData) =
        new Database(ValueSome logger, cloneData)

    new(databaseId: DatabaseId) =
        let cloneData = DatabaseCloneData.empty databaseId
        new Database(ValueNone, cloneData)

    new(databaseId: DatabaseId, logger: ILogger) =
        new Database(ValueSome logger, DatabaseCloneData.empty databaseId)

    override this.ToString() = sprintf "Database %A" this.Id

    interface IDisposable with member this.Dispose() = this.Dispose()

    member _.Dispose() =
        lockedAndLogged "DISPOSE" ValueNone (fun logger db ->
            let id = Database.databaseId db
            Logger.log1 "Disposing database %O" id logger
            (state :> IDisposable).Dispose()
            Logger.log1 "Disposed database %O" id logger
            Database.createDisposed db
            |> tpl ())

    /// <summary>
    /// Get a list of DebugTables. All Tables, Indexes and Items will be enumerated  
    /// </summary>
    member _.DebugTables = debugTables.Value

    member _.DefaultLogger = globalLogger

    member _.Id = MutableValue.get state |> Database.databaseId

    /// <summary>
    /// Synchronize with changes from another database
    /// Try to ingest synchronously. If a lock cannot be found, defer to a different thread
    /// </summary>
    member internal _.Synchronize subscriberId change (c: CancellationToken) =
        tryLockedAndLogged "SYNC" "Could not acquire lock for data replication" ValueNone (Database.Replication.synchronize struct (subscriberId, change))

    member internal _.SubscribeToStream_Internal logger table struct (behaviour, dataType) deletionProtection (subscriber: SubscriberInputFn) =
        Database.subscribeToStream struct (deletionProtection, table, behaviour, dataType, subscriber)
        |> lockedAndLogged "SUBSCRIBE TO STREAM" logger
        |> mapSnd subscriberRemovalDisposable

    /// <summary>
    /// Add a callback which can subscribe to a Table Stream
    /// </summary>
    member this.SubscribeToStream logger table streamConfig (subscriber: DatabaseSynchronizationPacket<TableCdcPacket> -> CancellationToken -> ValueTask<Unit>) =

        let sub x c =
            match x with
            | { databaseChange = OnChange (SchemaChange _) }
            | { databaseChange = OnChange (SubscriberChange _) }
            | { databaseChange = TableDeleted _ } -> noSubscriberChange
            | { databaseChange = OnChange (ChangeDataCapture x') } ->
                subscriber x' c

        this.SubscribeToStream_Internal (logger ?|> Either1) table streamConfig false sub
        |> createSubscriberDisposal

    /// <summary>
    /// PUT an Item into the Database  
    /// </summary>
    member _.Put logger args =
        lockedAndLogged "PUT" (iLoggerOrLoggerInput logger) (Database.put args)

    /// <summary>
    /// Update the behaviour of a stream subscriber or an entire stream  
    /// </summary>
    member internal _.SetStreamBehaviour_Internal logger tableName subscriberId behaviour =
        lockedAndLogged "SET STREAM BEHAVIOUR" logger (Database.setStreamBehaviour (tableName, subscriberId, behaviour) >>> tpl ())

    /// <summary>
    /// Update the behaviour of a stream subscriber or an entire stream  
    /// </summary>
    member this.SetStreamBehaviour logger tableName subscriberId behaviour =
        // do not allow client to set behavior for subscribers that they do not have an id for
        this.SetStreamBehaviour_Internal (iLoggerOrLoggerInput logger) tableName (ValueSome subscriberId) behaviour

    /// <summary>
    /// Delete a table  
    /// </summary>
    member _.DeleteTable logger name =
        lockedAndLogged "DELETE TABLE" (iLoggerOrLoggerInput logger) (name |> Database.deleteTable)

    /// <summary>
    /// Update a table
    /// </summary>
    member _.UpdateTable logger name req =
        lockedAndLogged "UPDATE TABLE" (iLoggerOrLoggerInput logger) (Database.updateTable struct (name, req))

    /// <summary>
    /// Try to delete a table
    /// 
    /// Returns a task which will resolve when all stream subscribers are finished processing
    /// The task will not throw errors. Call AwaitAllSubscribers to consume any errors
    /// </summary>
    member _.TryDeleteTable logger name =
        lockedAndLogged "TRY DELETE TABLE" (iLoggerOrLoggerInput logger) (name |> Database.tryDeleteTable)

    /// <summary>
    /// Get the stream config for a table  
    /// </summary>
    member _.StreamsEnabled tableName =
        MutableValue.get state
        |> Database.describeTable tableName Logger.empty
        ?>>= _.streamSubscribers
        |> ValueOption.isSome

    /// <summary>
    /// DELETE an Item
    /// </summary>
    member _.Delete logger (args: DeleteItemArgs<_>) =
        lockedAndLogged "DELETE" (iLoggerOrLoggerInput logger) (Database.delete args)

    /// <summary>
    /// CLEAR table
    /// </summary>
    member _.ClearTable logger tableName =
        lockedAndLogged "CLEAR TABLE" (iLoggerOrLoggerInput logger) (Database.clearTable tableName)

    /// <summary>
    /// UPDATE an Item  
    /// </summary>
    member _.Update logger (args: UpdateItemArgs) =
        lockedAndLogged "UPDATE" (iLoggerOrLoggerInput logger) (Database.update args)

    /// <summary>
    /// Execute transact writes  
    /// </summary>
    member _.TransactWrite logger (args: Database.TransactWrite.TransactWrites) =
        lockedAndLogged "TRANSACT WRITE" (iLoggerOrLoggerInput logger) (Database.TransactWrite.write args)

    /// <summary>
    /// GET an Item  
    /// </summary>
    member _.Get logger (args: GetItemArgs) =
        loggedGet "GET" (iLoggerOrLoggerInput logger) (Database.get args)

    /// <summary>
    /// Get table details
    /// </summary>
    member _.TryDescribeTable logger name =
        loggedGet "TRY DESCRIBE TABLE" (iLoggerOrLoggerInput logger) (Database.describeTable name)

    /// <summary>
    /// Get table details
    /// </summary>
    member this.DescribeTable logger name =
        this.TryDescribeTable logger name |> Maybe.defaultWith (fun name -> ClientError.clientError $"Table {name} not found") name

    /// <summary>
    /// Get a table with all of its data
    /// </summary>
    member this.GetTable logger name =
        LazyDebugTable(name, (this.DescribeTable logger name).table)

    /// <summary>
    /// Get table details
    /// </summary>
    member this.ListTables logger (struct (start, limit) & args) =
        loggedGet "LIST TABLES" (iLoggerOrLoggerInput logger) (Database.listTables args)

    // temp method. Can be changed
    member internal _.DescribeTable_Internal logger name =
        loggedGet "DESCRIBE TABLE" logger (Database.describeTable name)

    /// <summary>
    /// Execute multiple GET requests transactionally  
    /// </summary>
    member _.Gets logger (args: GetItemArgs seq) =
        let get logger db = Seq.map (Database.get >> apply logger >> apply db) args
        loggedGet "GETS" (iLoggerOrLoggerInput logger) get

    /// <summary>
    /// GET Items from the database  
    /// </summary>
    member _.Query logger (req: ExpressionExecutors.Fetch.FetchInput) =
        loggedGet "QUERY" (iLoggerOrLoggerInput logger) (Database.query req)

    /// <summary>
    /// Get an Index if it exists  
    /// </summary>
    member _.TryGetStream tableName =
        MutableValue.get state |> Database.tryGetStreamSubscribers tableName Logger.empty

    /// <summary>
    /// Add a new table  
    /// </summary>
    member _.AddTable logger args =
        Database.addTable args |> lockedAndLogged "ADD TABLE" (iLoggerOrLoggerInput logger)

    /// <summary>
    /// Import some clone data. Cloned tables must not exist in the system already   
    /// </summary>
    member _.Import logger args =
        Database.importClone args |> lockedAndLogged "IMPORT CLONE" (iLoggerOrLoggerInput logger)

    /// <summary>
    /// Return some info about the database as a string
    /// </summary>
    member _.Print () =
        loggedGet "PRINT" ValueNone (asLazy Database.print)

    /// <summary>
    /// Add a table clone from an existing table  
    /// </summary>
    member this.AddClonedTable logger name table =
        this.AddClonedTable_Internal (iLoggerOrLoggerInput logger) name table ValueNone

    member internal _.AddClonedTable_Internal logger name table subscription =
        Database.addClonedTable struct (name, table, subscription)
        |> lockedAndLogged "ADD CLONED TABLE" logger
        ?|> mapSnd subscriberRemovalDisposable

    /// <summary>
    /// Build a clone of this Database which contains data and stream config
    /// Does not clone subscription callbacks to streams
    /// </summary>
    member this.Clone () =
        new Database(ValueNone, this.BuildCloneData())

    /// <summary>
    /// Build a clone of this Database which contains data and stream config
    /// Does not clone subscription callbacks to streams
    /// </summary>
    /// <param name="globalLogger">
    /// The logger to use in the newly created database
    /// </param>
    member this.Clone (globalLogger: ILogger) =
        new Database(ValueSome globalLogger, this.BuildCloneData())

    /// <summary>
    /// Build some data which can be used to clone this Database at a later state.
    /// The clone data is immutable and can be used to clone multiple other Databases
    /// Any Database created from this clone data will contain the data and stream config from this Database
    /// It will not contain subscription callbacks to streams
    /// </summary>
    member this.BuildCloneData () =
        { databaseId = this.Id
          data =  MutableValue.get state |> Database.buildCloneData Logger.empty }

    /// <summary>
    /// Return a task that will be completed when all subscribers have been completed and the system is at rest
    /// The returned task will re-throw any errors thrown by Stream subscriber callbacks
    /// </summary>
    member this.AwaitAllSubscribers logger c =
        this.AwaitAllSubscribers_Internal (iLoggerOrLoggerInput logger) c
        |%|> (function
            | [] -> ()
            | errs -> StreamException.streamExceptions errs |> raise)
        |> Io.deNormalizeVt

    member internal this.AwaitAllSubscribers_Internal logger c =
        lockedAndLogged "AWAIT SUBSCRIBERS" logger (Database.awaitAllSubscribers c)

    // quick fix for F# lookup error
    static member internal AwaitAllSubscribers_ExtHack (db: Database) logger c = db.AwaitAllSubscribers logger c