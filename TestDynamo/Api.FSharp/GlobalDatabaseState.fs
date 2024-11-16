namespace TestDynamo.Api.FSharp

open System
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators
open TestDynamo.Data
open TestDynamo.Model
open Microsoft.FSharp.Core

type StreamViewType = Amazon.DynamoDBv2.StreamViewType

[<Struct; IsReadOnly>]
type DbReplicationKey =
    { tableName: string
      fromDb: DatabaseId
      toDb: DatabaseId }
    with
    static member rev x = { x with fromDb = x.toDb; toDb = x.fromDb }

[<Struct; IsReadOnly>]
type GlobalTableLeaf =
    { id: DatabaseId
      children: GlobalTableLeaf list }

type GlobalTableTree =
    { tableName: string
      root: GlobalTableLeaf }

    with

    static member private depthFirstSearch' f s tree =
        tree.children
        |> List.fold (GlobalTableTree.depthFirstSearch' f) (f s tree)

    static member depthFirstSearch f s tree = GlobalTableTree.depthFirstSearch' f s tree.root

    static member listIds =
        GlobalTableTree.depthFirstSearch (fun s x -> x.id::s) []

    static member private subTree' id tree =
        if tree.id = id then ValueSome tree
        else
            tree.children
            |> Seq.map (GlobalTableTree.subTree' id)
            |> Maybe.traverse
            |> Collection.tryHead

    static member subTree id tree =
        GlobalTableTree.subTree' id tree.root
        ?|> fun x -> { tableName = tree.tableName; root = x }

    static member databaseIds (tree: GlobalTableTree) =
        seq {
            yield tree.root.id
        }

    static member private build' (relationships: Map<DatabaseId, DatabaseId list>) id =
        let children =
            MapUtils.tryFind id relationships
            ?|? []
            |> List.map (GlobalTableTree.build' relationships)

        { id = id
          children = children }

    static member build tableName (replicationIds: DbReplicationKey seq) =

        let struct (nodeChildren, children) =
            replicationIds
            |> Seq.fold (
                fun x ->
                    mapFst (MapUtils.collectionMapAdd x.fromDb x.toDb)
                    >> mapSnd (Set.add x.toDb)
                |> flip) struct (Map.empty, Set.empty)

        let root =
            Map.keys nodeChildren
            |> Seq.filter (flip Set.contains children >> not)
            |> List.ofSeq

        match root with
        | [x] ->
            { tableName = tableName
              root = GlobalTableTree.build' nodeChildren x } |> ValueSome
        | [] when Map.isEmpty nodeChildren -> ValueNone    
        | _ -> serverError "Unable to find global table"

type GlobalDatabaseClone =
    { databases: Api.FSharp.DatabaseCloneData list
      replicationKeys: DbReplicationKey list }
    with

    static member empty = {databases = List.empty; replicationKeys = [] }

type private Replication =
    { secondaryReplication: DbReplicationKey voption
      dispose: IDisposable
      subscriberId: IncrementingId 
      id: IncrementingId }

    member this.isPrimary = this.secondaryReplication |> ValueOption.isSome
    interface IDisposable with member this.Dispose() = this.dispose.Dispose()

type private GlobalDatabaseData =
    { databases: Map<DatabaseId, TestDynamo.Api.FSharp.Database>
      replications: Map<DbReplicationKey, Replication> }

    interface IDisposable with
        member this.Dispose() =
            this.databases
            |> MapUtils.toSeq
            |> Seq.map sndT
            |> Seq.cast<IDisposable>
            |> Collection.concat2 (
                this.replications
                |> MapUtils.toSeq
                |> Seq.map sndT
                |> Seq.cast<IDisposable>)
            |> Seq.fold (fun s x ->
                x.Dispose()
                s) ()

/// <summary>
/// A collection of Databases used to model multiple regions in AWS
/// Allows database tables to be relicated between Databases
/// Databases are created automatically when requested
/// 
/// This construct contains both mutable and immutable data
/// </summary>
type GlobalDatabaseState =
    private
    | Dbs of GlobalDatabaseData
    | Disposed

    interface IDisposable with
        member this.Dispose() =
            match this with
            | Dbs x -> (x :> IDisposable).Dispose()
            | Disposed -> ()

type ReplicationDisposalFactory = (Logger -> GlobalDatabaseState -> GlobalDatabaseState) -> IDisposable
type ReplicationDisposalBuilder = ReplicationDisposalFactory -> IDisposable

[<Struct; IsReadOnly>]
type ReplicationIndexes =
    | ReplicateFromSource of string list
    | ReplicateAll
    /// <summary>If the replication table exists, keep its indexes. Otherwise don't replicate indexes</summary>
    | Ignore

type internal CreateReplicationArgs =
    { /// <summary>If true, no tables or indexes are copied, and only subscriber links are added</summary>
      linkExistingTables_internal: bool
      createStreamIfMissing: bool
      newDbDefaultLogger: ILogger voption
      disposeFactory: ReplicationDisposalFactory
      replicateIndexes: ReplicationIndexes
      dbReplicationKey: DbReplicationKey }
    
module private CreateReplication =

    type ApiDb = TestDynamo.Api.FSharp.Database

    let private noTask = ValueTask<unit>(()).Preserve()

    let private unwrap = function
        | Dbs x -> x
        | Disposed -> invalidOp "Global database has been disposed"

    module private DisposeOnTableDeletion =
        let create () = MutableValue.create struct (false, ValueNone)

        let disposalReady (d: IDisposable) = MutableValue.mutate (function
            | struct (_, ValueSome _) -> invalidOp "Disposal initialized twice"
            | struct (true, ValueNone) & x ->
                d.Dispose()
                struct ((), x)
            | struct (false, ValueNone) -> struct ((), struct (false, ValueSome d)))

        let private tableDeleted = MutableValue.mutate (function
            | struct (true, _) & x -> struct ((), x)
            | struct (false, ValueNone & x) -> struct ((), struct (true, x))
            | struct (false, ValueSome (d: IDisposable) & x) ->
                d.Dispose()
                struct ((), struct (true, x)))

        let processCdcResult disposer = function
            | Ingested
            | FullCircle -> ()
            | SynchronizationError e -> raise e
            | TableNotFound -> tableDeleted disposer

    let private tableNotFound = ValueTask<SynchronizationResult voption>(ValueSome TableNotFound).Preserve()
    let private noResult = ValueTask<SynchronizationResult voption>(ValueNone).Preserve()
    let private ingestChange disposer (db: ApiDb) tableName c =
        function 
        | { databaseChange = OnChange change; subscriberId = subscriberId } -> db.Synchronize subscriberId change c |%|> ValueSome
        | { databaseChange = TableDeleted x } when x = tableName -> tableNotFound
        | { databaseChange = TableDeleted _ } -> noResult
        >> Io.map (
            ValueOption.map (DisposeOnTableDeletion.processCdcResult disposer)
            >> ValueOption.defaultValue ())

    // process the queue and return a function that can process items on demand
    let processItemBuffer tableName handleError (destinationDb: TestDynamo.Api.FSharp.Database) disposer clonedDbVersion =
        let ingest = ingestChange disposer destinationDb tableName |> flip

        let rec processItemBuffer (queue: Queue<SubscriberMessage>) =
            match Queue.dequeue queue with
            | ValueNone, _ -> ingest
            | ValueSome { databaseChange = OnChange (ChangeDataCapture x)}, q when x.data.packet.dataVersion <= clonedDbVersion ->
                processItemBuffer q
            | ValueSome x, q ->
                Io.recover (fun () -> ingest x CancellationToken.None) handleError |> ignoreTyped<ValueTask<unit>>
                processItemBuffer q

        processItemBuffer

    let private optionalRemove replicationKey (replicationId: IncrementingId) replicationKeys =
        MapUtils.tryFind replicationKey replicationKeys
        ?>>= (function
            | x when x.id.Value = replicationId.Value -> Map.remove replicationKey replicationKeys |> ValueSome
            | _ -> ValueNone)
        |> ValueOption.defaultValue replicationKeys

    let private recordReplication primaryId struct (primarySubId, primarySub: IDisposable) secondaryId struct (secondarySubId, secondarySub: IDisposable) (state: GlobalDatabaseData) =

        let pRepId = IncrementingId.next()
        let sRepId = IncrementingId.next()

        let p = { id = pRepId; dispose = primarySub; secondaryReplication = ValueSome secondaryId; subscriberId = primarySubId }
        let s = { id = sRepId; dispose = secondarySub; secondaryReplication = ValueNone; subscriberId = secondarySubId }

        let pOut = optionalRemove primaryId pRepId
        let sOut = optionalRemove secondaryId sRepId

        let remove logger = unwrap >> fun state ->
            let rep = state.replications |> pOut |> sOut
            if Map.count state.replications = Map.count rep then
                Logger.log2 "Replications %O, %O have already been removed" primaryId secondaryId logger
                state |> Dbs
            else
                Logger.log2 "Removing replications %O, %O" primaryId secondaryId logger
                Disposable.disposeList [primarySub; secondarySub]

                { state with replications = rep } |> Dbs

        let newState =
            { state with
                replications =
                    Map.add primaryId p state.replications
                    |> Map.add secondaryId s } |> Dbs

        struct (remove, newState)

    let private replicationSubscriberConfig = struct (SubscriberBehaviour.defaultOptions, NewAndOldImages)
    let createLeftSubscription logReplication logger (left: ApiDb) { tableName = tableName; fromDb = fromDb; toDb = toDb } =

        // a buffer containing items which replicated between the time
        // of the table clone and the time of the subscriber attachment. When the buffer is
        // empty it is replaced with a subscriber function 
        let leftBuffer = Either1 Queue.empty |> MutableValue.create

        let logger' = logger |> Either2 |> ValueSome
        Logger.log2 "Buffering data from %O/%O" fromDb tableName logger

        let leftSubscription = left.SubscribeToStream_Internal logger' tableName replicationSubscriberConfig true (fun x c ->
            logReplication fromDb toDb x |> ignoreTyped<SubscriberMessage>
            MutableValue.mutate (function
                | Either1 q -> struct (noTask, Queue.enqueue x q |> Either1)
                | Either2 f -> struct (f x c, f |> Either2)) leftBuffer)

        Logger.log2 "Subscription added %O => %O" fromDb toDb logger
        struct (leftBuffer, leftSubscription)
        
    let private pruneIndexes tableExists retainIndexes table =
            
        let retainIndexes =
            match retainIndexes with
            | ReplicateFromSource x -> ValueSome x
            | ReplicateAll -> Table.indexes table |> Map.keys |> List.ofSeq |> ValueSome
            | Ignore when tableExists -> ValueNone
            | Ignore -> ValueSome []

        retainIndexes
        ?|> (fun retainIndexes ->
            let table =
                Table.indexes table
                |> Map.keys
                // delete indexes
                |> Seq.filter (flip List.contains retainIndexes >> not)
                |> Seq.fold (flip (Table.deleteIndex Logger.empty)) table
                
            let indexMissing =
                Table.indexes table
                |> flip Map.containsKey
                >> not
                |> flip List.filter retainIndexes
                |> Str.join ", "
                
            match indexMissing with
            | "" -> table
            | err ->
                let idx = Table.indexes table |> Map.keys |> List.ofSeq
                clientError $"Invalid index(es) {err}. Available index(es) are {idx}")
        ?|? table

    let createRightSubscription (args: CreateReplicationArgs) logReplication logger disposer (left: ApiDb) (right: ApiDb)  =
        
        let { tableName = tableName; fromDb = fromDb; toDb = toDb } = args.dbReplicationKey
        let rightSubscriber x c =
            logReplication toDb fromDb x
            |> ingestChange disposer left tableName c

        let logger' = logger |> Either2 |> ValueSome
        let leftTable =
            left.DescribeTable_Internal logger' tableName
            |> ValueOption.defaultWith (fun _ -> clientError $"Invalid table {tableName} in database {fromDb}")

        if args.linkExistingTables_internal then
            let subscriber = right.SubscribeToStream_Internal (Either2 logger |> ValueSome) tableName replicationSubscriberConfig true rightSubscriber
            Logger.log2 "Subscription added %O => %O" right.Id left.Id logger
            subscriber
        else
            Logger.log2 "Creating %O/%O" toDb tableName logger
            let clone =
                pruneIndexes false args.replicateIndexes leftTable.table
                |> right.AddClonedTable_Internal logger' tableName
            
            let subscriber =
                { dataType = sndT replicationSubscriberConfig
                  behaviour = fstT replicationSubscriberConfig
                  subscriber = rightSubscriber
                  addDeletionProtection = true }
                |> ValueSome
                |> clone
                |> Maybe.expectSome
                
            Logger.log2 "Subscription added %O => %O" right.Id left.Id logger
            subscriber

    let describeCdcResult = Logger.describable (fun struct (fromDb, toDb, tableName, added, removed) ->
        sprintf "SYNC - %O => %O, %s: added %i, removed %i" fromDb toDb tableName added removed)

    let describeSchemaChangeResult = Logger.describable (fun struct (fromDb, toDb, tableName, indexAdded, indexRemoved) ->
        sprintf "SYNC - %O => %O, %s: index(es) added %A, index(es) removed %A" fromDb toDb tableName indexAdded indexRemoved)
        
    let createReplication
        (args: CreateReplicationArgs)
        logger
        struct (left: TestDynamo.Api.FSharp.Database, struct (right: TestDynamo.Api.FSharp.Database, dbs)): struct (ReplicationDisposalBuilder * GlobalDatabaseState) =
            
        let { tableName = tableName; fromDb = leftId; toDb = rightId } = args.dbReplicationKey

        if leftId = rightId then clientError $"Cannot add replication from {leftId} => {rightId}"
        left.TryDescribeTable args.newDbDefaultLogger tableName
        ?|> ignoreTyped<TableDetails>
        |> ValueOption.defaultWith (fun _ -> clientError $"Cannot find table {leftId}/{tableName}")

        match struct (args.createStreamIfMissing, left.StreamsEnabled tableName) with
        | struct (_, true) -> ()
        | false, false ->
            clientError $"Replications must be added to tables with streams enabled"
        | true, false ->
            { updateTableData =
                {  deleteGsi = Set.empty
                   createGsi = Map.empty
                   attributes = []
                   deletionProtection = ValueNone }
              streamConfig = CreateStream |> ValueSome }
            |> left.UpdateTable (Logger.internalLogger logger |> ValueSome) tableName
            |> ignoreTyped<TableDetails>

        let logReplication =
            let replicationLogger = DatabaseLogger.buildLogger ValueNone args.newDbDefaultLogger

            fun fromDb toDb -> function
                | { databaseChange = OnChange (ChangeDataCapture packet) } & p ->
                    let struct (added, removed) = ChangeResults.modificationCounts packet.data.packet.changeResult
                    describeCdcResult struct (fromDb, toDb, tableName, added, removed)
                    |> flip (Logger.log1 "%O") replicationLogger

                    p
                | { databaseChange = OnChange (SchemaChange packet) } & p ->

                    let added = packet.data.createGsi |> Map.keys |> List.ofSeq
                    let removed = packet.data.deleteGsi |> List.ofSeq
                    describeSchemaChangeResult struct (fromDb, toDb, tableName, added, removed)
                    |> flip (Logger.log1 "%O") replicationLogger

                    p
                | { databaseChange = OnChange (SubscriberChange { data = SubscriberDeleted id }) } & p ->
                    Logger.log1 "Subscriber deleted %O" id replicationLogger
                    p
                | { databaseChange = TableDeleted t } & p ->
                    Logger.log1 "Table deleted %O" t replicationLogger
                    p

        // used to delete the subscriptions if a table has been deleted    
        let disposer = DisposeOnTableDeletion.create ()
        let struct (leftBuffer, leftSubscription) =
            createLeftSubscription logReplication logger left args.dbReplicationKey

        let logger' = logger |> Either2 |> ValueSome
        try
            let leftTable =
                left.DescribeTable_Internal logger' tableName
                |> ValueOption.defaultWith (fun _ -> clientError $"Invalid table {tableName} in database {leftId}")

            let rightSubscription = createRightSubscription args logReplication logger disposer left right
            try
                Logger.log2 "Streaming to %O/%O" rightId tableName logger

                // replace buffer queue with direct subscription                
                do
                    let buildConsumer q =
                        let err = flip (Logger.log1 "Error with replication %A") logger

                        Table.getVersion leftTable.table
                        |> flip (processItemBuffer tableName err right disposer) q

                    MutableValue.mutate (function
                        | Either2 f & f' -> struct ((), f')
                        | Either1 q -> struct ((), buildConsumer q |> Either2)) leftBuffer

                let subscriptionIdReversed =
                    { tableName = args.dbReplicationKey.tableName
                      fromDb = args.dbReplicationKey.toDb
                      toDb = args.dbReplicationKey.fromDb }

                match MapUtils.tryFind args.dbReplicationKey dbs.replications, MapUtils.tryFind subscriptionIdReversed dbs.replications with
                | ValueSome _, _
                | _, ValueSome _ ->
                    clientError $"Concurrency error. Attempting to add multiple replications between databases: {args.dbReplicationKey}"
                | ValueNone, ValueNone ->
                    recordReplication args.dbReplicationKey leftSubscription subscriptionIdReversed rightSubscription dbs
                    |> mapFst (fun dispose createDispose ->
                        let disposable = createDispose dispose
                        DisposeOnTableDeletion.disposalReady disposable disposer
                        disposable)
            with
            | e ->
                Disposable.handleCatch e [sndT rightSubscription]
                reraise()
        with
        | e ->
            let deleteDisposable =
                { new IDisposable with
                    member this.Dispose() =
                        right.TryDeleteTable args.newDbDefaultLogger tableName
                        |%|> (
                            ValueOption.map ignoreTyped<TableDetails>
                            >> ValueOption.defaultValue ())
                        // this should be safe. See comments for TryDeleteTable 
                        |> ignoreTyped<ValueTask<unit>>  }

            Disposable.handleCatch e [sndT leftSubscription; deleteDisposable]
            reraise()

    let removeReplication subscriptionId logger dbs =
        Logger.log1 "Removing replication %O" subscriptionId logger

        match MapUtils.tryFind subscriptionId dbs.replications with
        | ValueNone ->
            serverError $"Replication {subscriptionId} not found"
            Dbs dbs
        | ValueSome { secondaryReplication = ValueNone } ->
            clientError $"Replication {subscriptionId} is from a secondary table to a primary table. You must delete a replication on the same table that the replication was created on." logger
        | ValueSome ({ secondaryReplication = ValueSome secondary } & d) ->
            let secondaryD =
                match MapUtils.tryFind secondary dbs.replications with
                | ValueNone ->
                    Logger.log0 "Warning: unable to find secondary replication" logger
                    Disposable.nothingToDispose
                | ValueSome x ->
                    x

            Logger.log0 "Clearing subscriptions" logger
            Disposable.disposeList [d; secondaryD]

            Logger.log0 "Clearing registrations" logger
            { dbs with replications = dbs.replications |> Map.remove subscriptionId |> Map.remove secondary } |> Dbs

module GlobalDatabaseState =
    let logOperation x = DatabaseLogger.logOperation "GlobalDatabase" x
    let logOperationAsync x = DatabaseLogger.logOperationAsync "GlobalDatabase" x

    let newDatabase id _ = new TestDynamo.Api.FSharp.Database(databaseId = id)

    let private unwrap = function
        | Dbs x -> x
        | Disposed -> invalidOp "Global database has been disposed"

    let databases = unwrap >> _.databases

    let updateReplication replicationId behaviour twoWayUpdate =
        fun logger -> unwrap >> fun state ->

            Logger.debug1 "Key %A" replicationId logger

            let innerLogger = Either2 logger |> ValueSome
            [
                MapUtils.tryFind replicationId state.replications
                |> Maybe.expectSomeErr "Could not find replication %A" replicationId
                |> tpl replicationId.fromDb
                |> ValueSome

                if twoWayUpdate
                then
                    let repIdRev = DbReplicationKey.rev replicationId
                    MapUtils.tryFind repIdRev state.replications
                    |> Maybe.expectSomeErr "Could not find replication %A" repIdRev
                    |> tpl replicationId.toDb
                    |> ValueSome
                else ValueNone
            ]
            |> Maybe.traverse
            |> Collection.mapFst (
                flip MapUtils.tryFind state.databases
                >> Maybe.expectSomeErr "Could not find database %A" id)
            |> Collection.mapSnd _.subscriberId
            |> Seq.fold (fun s struct (db, subscriberId) ->
                Logger.debug2 "Setting for db %A, %i" replicationId subscriberId.Value logger
                db.SetStreamBehaviour_Internal innerLogger replicationId.tableName (ValueSome subscriberId) behaviour) ()
        |> logOperation "UPDATING REPLICATION"

    let getCloneData =
        fun logger -> unwrap >> fun state ->
            let dbs = Seq.map (fun (v: TestDynamo.Api.FSharp.Database) -> v.BuildCloneData()) state.databases.Values
            let repKeys =
                MapUtils.toSeq state.replications
                |> Seq.filter (fun struct (_, x) -> x.isPrimary)
                |> Seq.map (fun struct (k, _) -> k)

            { databases = List.ofSeq dbs
              replicationKeys = repKeys |> List.ofSeq } : GlobalDatabaseClone
        |> logOperation "CLONING"

    let tryDatabase databaseId =
        unwrap >> fun state -> MapUtils.tryFind databaseId state.databases

    let private ensureDatabase' defaultLogger databaseId logger =
        unwrap >> fun state ->
            match MapUtils.tryFind databaseId state.databases with
            | ValueSome x ->
                Logger.log1 "Found %O" databaseId logger
                struct (x, Dbs state)
            | ValueNone ->
                Logger.log1 "Not found %O, Creating" databaseId logger
                let db =
                    ValueOption.map (fun x -> new TestDynamo.Api.FSharp.Database(
                        logger = x, databaseId = databaseId)) defaultLogger
                    |> ValueOption.defaultWith (newDatabase databaseId)

                let dbs = {state with databases = Map.add databaseId db state.databases } |> Dbs
                struct (db, dbs)

    let ensureDatabase defaultLogger databaseId =
        ensureDatabase' defaultLogger databaseId
        |> logOperation "FINDING DATABASE"

    let awaitAllSubscribers (c: CancellationToken) =
        fun logger -> unwrap >> fun dbs ->

            let loggerArg = logger |> Either2 |> ValueSome
            let execute () =
                dbs.databases
                |> MapUtils.toSeq
                |> Seq.map (fun struct (_, db) -> db.AwaitAllSubscribers_Internal loggerArg c)
                |> List.ofSeq
                |> Io.traverse
                |%|> List.concat
                |%|> (function
                    | [] -> ()
                    | errs -> StreamException.streamExceptions errs |> raise)

            // waitForStabalizationTime (ms) is the amount of time to wait between AWAIT attempts
            // This is designed to wait for propagations which trigger more propagations, and even though the system
            // may be at rest, it also may be about to kick off another propagation
            let rec executeMany struct (waitForStabalizationTime: int, count) =
                c.ThrowIfCancellationRequested()
                if waitForStabalizationTime > 60_000 then serverError "Wait timeout"
                if waitForStabalizationTime < 1 then serverError "An unexpected error has occurred"

                let result = execute ()
                if result.IsCompleted
                then
                    Logger.debug1 "AWAIT cycle complete after %i attempts" count logger
                    result
                else
                    let threadId = Thread.CurrentThread.ManagedThreadId
                    task {
                        do! result
                        Logger.debug1 "Nothing in progress. Waiting for stabalization time %ims" waitForStabalizationTime logger
                        c.ThrowIfCancellationRequested()
                        do! Task.Delay(waitForStabalizationTime).ConfigureAwait(false)
                        c.ThrowIfCancellationRequested()
                        // non tail call recursion is capped by wait timeout above
                        do! executeMany (waitForStabalizationTime * 2, count + 1)
                        Logger.debug1 "Awaiter diverted off thread; from/to: %A" struct (threadId, Thread.CurrentThread.ManagedThreadId) logger

                        return ()
                    } |> ValueTask<unit>

            Logger.debug0 "Starting AWAIT cycle" logger
            executeMany struct (1, 1)
        |> logOperationAsync "AWAIT DATABASES"
    
    let internal createReplication (args: CreateReplicationArgs) =
        fun logger dbs ->
            let  {fromDb = fromDb; toDb = toDb; tableName = tableName} = args.dbReplicationKey
            Logger.log2 "Creating replication %O => %O" fromDb toDb logger 

            ensureDatabase' args.newDbDefaultLogger fromDb logger dbs
            |> mapSnd (ensureDatabase' args.newDbDefaultLogger toDb logger)
            |> mapSnd (mapSnd unwrap)
            |> CreateReplication.createReplication args logger
            |> mapFst (apply args.disposeFactory)    // mutable operation
            |> sndT
        |> logOperation "CREATE REPLICATION"

    let removeReplication id =
        let rem logger = unwrap >> CreateReplication.removeReplication id logger
        logOperation "REMOVE REPLICATION" rem

    /// <summary>Returns tables replicated to or from</summary>
    let isReplicationTable dbId tableName =
        unwrap >> (_.replications.Keys >> Seq.filter (fun k -> k.tableName = tableName && (k.fromDb = dbId || k.toDb = dbId)) >> Seq.isEmpty >> not)

    let findReplications tableName state =
        unwrap state |> (fun x ->
            x.replications
            |> MapUtils.toSeq
            |> Seq.filter (sndT >> _.isPrimary)
            |> Seq.map fstT
            |> Seq.filter (_.tableName >> ((=)tableName))
            |> GlobalTableTree.build tableName)

    /// <summary>Returns replicated tables, optionally filtered by a root table</summary>
    let getReplicatedTables logger dbId tableName state =

        let subTree =
            dbId
            ?|> GlobalTableTree.subTree
            ?|? ValueSome

        findReplications tableName state
        ?>>= subTree
        ?|> (
            GlobalTableTree.listIds
            >> Seq.map (fun k -> tryDatabase k state ?|> (tpl k))
            >> Maybe.traverse)
        ?|? Seq.empty
        |> Collection.mapSnd (fun db -> db.DescribeTable logger tableName |> _.table)

    /// <summary>Returns replicated tables, optionally filtered by a root table</summary>
    let listReplicatedTables =
        unwrap
        >> (
            _.replications
            >> Map.keys
            >> Seq.map _.tableName
            >> Seq.distinct
            >> Seq.sort)

    let private listReplications' = unwrap >> _.replications.Keys >> Collection.ofSeq

    let private listReplicationRoots' =
        unwrap >> (
            _.replications
            >> MapUtils.toSeq
            >> Seq.filter (sndT >> _.isPrimary)
            >> Seq.map fstT)

    let listReplications rootsOnly =
        if not rootsOnly then listReplications'
        else listReplicationRoots'

    let private disposeNothing _ = Disposable.nothingToDispose

    let private combineDisposeBuilders d1F d2F factory =
        Disposable.combine (d1F factory) (d2F factory)

    let build iLogger (initialDatabases: Api.FSharp.DatabaseCloneData list) =

        let logger = DatabaseLogger.buildLogger ValueNone iLogger

        let buildDb c =
            Logger.log1 "Creating database %A" c.databaseId logger
            match iLogger with
            | ValueSome l -> new TestDynamo.Api.FSharp.Database(l, c)
            | ValueNone -> new TestDynamo.Api.FSharp.Database(cloneData = c)

        let dbs =
            initialDatabases
            |> List.fold (fun s d -> buildDb d |> flip (Map.add d.databaseId) s) Map.empty

        Dbs { databases = dbs; replications = Map.empty }
