namespace TestDynamo.Api.FSharp

open System
open Microsoft.FSharp.Core
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators
open TestDynamo.Data
open TestDynamo.Data.BasicStructures
open TestDynamo.Model
open Microsoft.Extensions.Logging

type UpdateGlobalTableData =
    { replicaInstructions: CreateOrDelete<DatabaseId> list
      /// <summary>If true, and the source table does not have streams enabled, this will enable the streams correctly</summary>
      createStreamsForReplication: bool }

type UpdateTableData =
    { tableName: string
      globalTableData: UpdateGlobalTableData
      tableData: UpdateSingleTableData }
    
type GlobalDatabaseCloneData =
    { data: GlobalDatabaseClone }

/// <summary>
/// A mutable Global Database object containing Tables and Streams
/// 
/// Allows database tables to be relicated between Databases
/// Databases are created automatically when requested
/// </summary>
type GlobalDatabase private (initialDatabases: GlobalDatabaseCloneData, logger: ILogger voption) =

    static let validateError id =
        $" * Duplicate database in setup data {id}"
        
    do
        let err =
            initialDatabases.data.databases
            |> Collection.groupBy _.databaseId
            |> Collection.mapSnd (List.ofSeq)
            |> Seq.filter (sndT >> List.length >> (flip (>) 1))
            |> Seq.map fstT
            |> Seq.map validateError
            |> Str.join "\n"
            
        match err with
        | "" -> ()
        | e -> $"Invalid database input\n{e}" |> invalidOp
    
    let state: MutableValue<GlobalDatabaseState> =
        initialDatabases.data.databases
        |> GlobalDatabaseState.build logger
        |> MutableValue.createDisposable

    let defaultLogger = logger
    let buildLogger operationName localLogger =
        let logger = DatabaseLogger.buildLogger defaultLogger localLogger
        Logger.trace1 "[Api.GlobalDatabase] %s" operationName logger
        Logger.scope logger

    let buildDebugTables =
        Map.map (fun _ (x: TestDynamo.Api.FSharp.Database) -> x.DebugTables)

    let lockedAndLogged operationName localLogger f =
        MutableValue.mutate (fun state ->
            let struct (l, d) = buildLogger operationName localLogger
            use _ = d
            
            let struct (x, h) = f l state
            struct (x, h)) state

    let lockedAndLoggedMany operationName localLogger fs =
        fs
        |> List.fold (fun s f ->
            lockedAndLogged operationName localLogger f::s) []
        |> List.rev

    let loggedGet operationName localLogger f =
        let struct (l, d) = buildLogger operationName localLogger
        use _ = d
        
        MutableValue.get state |> f l

    let buildReplicationDisposable worker =
        let worker' logger = worker logger >> tpl ()

        { new IDisposable with
            member _.Dispose() =
                lockedAndLogged "DISPOSING REPLICATION" ValueNone worker' }

    // Create replications - needs to be done after initial state set
    do
        let createReplication =
            GlobalDatabaseState.createReplication true false defaultLogger buildReplicationDisposable
            >>>> tpl ()

        initialDatabases.data.replicationKeys
        |> List.map createReplication
        |> lockedAndLoggedMany "CREATING REPLICATION" ValueNone
        |> ignoreTyped<unit list>

    // trigger an update to set debug tables
    do lockedAndLogged "INIT" ValueNone (asLazy (tpl ()))

    new(initialDatabases: GlobalDatabaseCloneData, ?logger: ILogger) =
        new GlobalDatabase(initialDatabases, Maybe.fromRef logger)

    new(?logger: ILogger) =
        let data =
            { data = GlobalDatabaseClone.empty }
            
        new GlobalDatabase(data, Maybe.fromRef logger)

    new(cloneData: Api.FSharp.DatabaseCloneData, ?logger: ILogger) =
        let data =
            { data = { databases = [cloneData]; replicationKeys = [] } }
        new GlobalDatabase(data, Maybe.fromRef logger)

    interface IDisposable with member this.Dispose() = this.Dispose()

    member _.Dispose() =
        lockedAndLogged "DISPOSING" ValueNone (fun logger db ->
            Logger.log0 "Disposing global database" logger
            (state :> IDisposable).Dispose()
            Logger.log0 "Disposed global database" logger
            GlobalDatabaseState.Disposed
            |> tpl ())

    member _.DefaultLogger = defaultLogger
    
    /// <summary>
    /// Get a list of DebugTables. All Databases, Tables, Indexes and Items will be enumerated  
    /// </summary>
    member _.DebugTables = MutableValue.get state |> GlobalDatabaseState.databases |> buildDebugTables

    /// <summary>
    /// Get a database in a specific region.
    /// If the Database does not exist it will be created  
    /// </summary>
    member _.GetDatabase logger databaseId =
        GlobalDatabaseState.ensureDatabase defaultLogger databaseId
        |> lockedAndLogged "GET DATABASE" logger

    /// <summary>
    /// Try to get a database in a specific region.
    /// If the Database does not exist nothing will be returned  
    /// </summary>
    member _.TryGetDatabase databaseId =
        MutableValue.get state |> GlobalDatabaseState.tryDatabase databaseId

    /// <summary>
    /// List all databases that have been created so far  
    /// </summary>
    member _.GetDatabases () = MutableValue.get state |> GlobalDatabaseState.databases
    
    /// <summary>
    /// Describe a global table as a list of linked tables
    /// Optionally omit tables which replicate TO a specific DB 
    /// </summary>
    member _.TryDescribeGlobalTable logger dbId table =
        MutableValue.get state
        |> GlobalDatabaseState.getReplicatedTables logger dbId table
        |> List.ofSeq
        |> function
            | [] -> ValueNone
            | xs -> NonEmptyList.ofList xs |> ValueSome
    
    member this.ListGlobalTables logger struct (databaseId, start: string voption, limit) =
        let predicate =
            start
            ?|> fun s x -> compare s (fstT x) < 0
            ?|? asLazy true
            
        let dbIdFilter =
            databaseId
            ?|> (fun dbId ->
                Seq.filter (sndT >> NonEmptyList.unwrap >> List.filter (fstT >> (=) dbId) >> Seq.isEmpty >> not))
            ?|? id
                
        MutableValue.get state
        |> GlobalDatabaseState.listReplicatedTables
        |> Seq.map (tplDouble >> mapSnd (this.DescribeGlobalTable logger ValueNone))
        |> dbIdFilter
        |> Seq.filter predicate
        |> Seq.map sndT
        |> Seq.truncate limit

    /// <summary>
    /// Describe a global table as a list of linked tables
    /// Optionally omit tables which replicate TO a specific DB 
    /// </summary>
    member this.DescribeGlobalTable logger dbId table =
        this.TryDescribeGlobalTable logger dbId table
        |> function
            | ValueNone ->
                let fromDb = dbId ?|> sprintf " from database %A" ?|? ""
                clientError $"Table {table}{fromDb} is not a global table"
            | ValueSome xs -> xs

    member this.GetGlobalTable databaseId name =
        let db = this.GetDatabase defaultLogger databaseId

        db.TryDescribeTable defaultLogger name
        ?|> fun x -> LazyDebugTable(name, x.table)
        |> ValueOption.defaultWith (fun _ -> invalidArg (nameof name) $"Invalid table \"{name}\" on database \"{databaseId}\"")
    
    member this.IsGlobalTable logger =
        ValueSome
        >> this.TryDescribeGlobalTable logger
        >>> ValueOption.isSome
        
    member _.ListReplications logger rootsOnly =
        loggedGet "LIST REPLICATIONS" logger (fun _ db ->
            GlobalDatabaseState.listReplications true db)

    /// <summary>
    /// Build a clone of this GlobalDatabase which contains Databases with data, stream config and replication settings
    /// Does not clone subscription callbacks to streams
    /// </summary>
    member _.Clone logger =
        let data =
            { data = loggedGet "CLONE" logger GlobalDatabaseState.getCloneData } 

        new GlobalDatabase(initialDatabases = data, logger = logger)

    /// <summary>
    /// Build some data which can be used to clone this GlobalDatabase at a later state.
    /// The clone data is immutable and can be used to clone multiple other GlobalDatabases
    /// Any GlobalDatabase created from this clone data will contain the data, stream config and replication settings from this GlobalDatabase
    /// It will not contain subscription callbacks to streams
    /// </summary>
    member _.BuildCloneData logger =
        loggedGet "BUILD CLONE DATA" logger GlobalDatabaseState.getCloneData
        |> fun x -> { data = x; }
        
    /// <summary>
    /// Change how a replication propagates it's data
    /// </summary>
    member this.UpdateReplication logger replicationId behaviour twoWayUpdate =
        GlobalDatabaseState.updateReplication replicationId behaviour twoWayUpdate
        |> loggedGet "REPLICATION UPDATE" logger

    /// <summary>
    /// Update a table. This can be used to add replications
    /// </summary>
    member this.UpdateTable dbId logger tableName (args: UpdateGlobalTableData) =
        let struct (replicationSuccess, replicationFailure) =
            args.replicaInstructions
            |> List.map (function
                | Delete x ->
                    let name = $"Delete {x}/{tableName}"
                    try
                        { fromDb = dbId; toDb = x; tableName = tableName }
                        |> GlobalDatabaseState.removeReplication
                        >>> tpl ()
                        |> lockedAndLogged "DELETE REPLICATION" logger
                        |> tpl name
                        |> Either1
                    with
                    | e -> Either2 struct (name, e)
                | Create x ->
                    let name = $"Create {x}/{tableName}"
                    try
                        let key = { fromDb = dbId; toDb = x; tableName = tableName }
                        GlobalDatabaseState.createReplication false args.createStreamsForReplication defaultLogger buildReplicationDisposable key
                        >>> tpl ()
                        |> lockedAndLogged "CREATE REPLICATION" logger
                        |> tpl name
                        |> Either1
                    with
                    | e -> Either2 struct (name, e))
            |> Either.partition

        match struct (replicationFailure, replicationSuccess) with
        | [], _ ->
            let db = this.GetDatabase logger dbId
            let t = db.DescribeTable logger tableName
            { table = t.table; streamSubscribers = db.TryGetStream tableName }

        | fs, [] ->
            let struct (msg, err) = Collection.unzip fs |> mapFst (Str.join ", ")
            AggregateException($"Error modifying database replications: {msg}", err) |> raise

        | fs, ss ->
            let struct (errMsg, err) = Collection.unzip fs |> mapFst (Str.join ", ")
            let struct (sccMsg, _) = Collection.unzip ss |> mapFst (Str.join ", ")
            AggregateException($"Error modifying database replications. Some replications have succeeded ({sccMsg}) and other failed ({errMsg})", err) |> raise

    /// <summary>
    /// Return a task that will be completed when all subscribers have been completed and the system is at rest
    /// The returned task will throw any errors encountered in subscribers
    /// </summary>
    member _.AwaitAllSubscribers logger c =
        loggedGet "AWAIT ALL SUBSCRIBERS" logger (GlobalDatabaseState.awaitAllSubscribers c)
        |> Io.deNormalizeVt