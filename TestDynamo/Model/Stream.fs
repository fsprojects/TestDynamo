namespace TestDynamo.Model

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open TestDynamo.Data
open TestDynamo.Data.BasicStructures
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators
open Microsoft.FSharp.Core

type Delay = TimeSpan

type SubscriberId = IncrementingId

/// <summary>
/// An emergency brake is a delay on a request
/// If the emergency brake returns false, the request is cancelled
/// </summary>
type EmergencyBrake = ValueTask<bool>

[<Struct; IsReadOnly>]
type EmergencyBrakeRequest<'request> =
    { request: 'request
      emergencyBrake: EmergencyBrake }

module EmergencyBrakeRequest =
    let noBrakeValue = ValueTask<_>(true).Preserve()
    let noBrake x = { request = x; emergencyBrake = noBrakeValue }
    let inline create emergencyBrake x = { request = x; emergencyBrake = emergencyBrake }
    let inline map f x = { request = f x.request; emergencyBrake = x.emergencyBrake }

[<Struct; IsReadOnly>]
type SubscriberChange =
    | SubscriberDeleted of SubscriberId

[<Struct; IsReadOnly>]
type TableCdcPacket =
    { packet: CdcPacket
      tableArn: struct(AwsAccountId * RegionId) -> string }

[<Struct; IsReadOnly>]
type DatabaseChange =
    | ChangeDataCapture of cdc: DatabaseSynchronizationPacket<TableCdcPacket>
    | SchemaChange of sch: DatabaseSynchronizationPacket<UpdateTableSchema>
    | SubscriberChange of DatabaseSynchronizationPacket<SubscriberChange>
    with
    static member tableName = function
        | ChangeDataCapture { tableName = x }
        | SubscriberChange { tableName = x }
        | SchemaChange { tableName = x } -> x

    static member instancePacketPath = function
        | ChangeDataCapture { synchronizationPacketPath = x }
        | SubscriberChange { synchronizationPacketPath = x }
        | SchemaChange { synchronizationPacketPath = x } -> x

    static member correlationId = function
        | ChangeDataCapture { correlationId = x }
        | SubscriberChange { correlationId = x }
        | SchemaChange { correlationId = x } -> x

type TableName = string

type SubscriberMessageData =
    // a message to tell the downstream to unsubscribe
    | TableDeleted of TableName
    | OnChange of DatabaseChange
    with
    static member getTableName = function
        | OnChange x -> DatabaseChange.tableName x
        | TableDeleted tableName -> tableName

    override this.ToString() =
        match this with
        | OnChange (ChangeDataCapture _ & x ) -> $"{nameof ChangeDataCapture} {DatabaseChange.tableName x}"
        | OnChange (SchemaChange _ & x ) -> $"{nameof SchemaChange} {DatabaseChange.tableName x}"
        | OnChange (SubscriberChange { data = SubscriberDeleted _ } & x) -> $"{nameof SubscriberDeleted} {DatabaseChange.tableName x}"
        | TableDeleted tableName -> $"{nameof TableDeleted} {tableName}"

type SubscriberMessage =
    { databaseChange: SubscriberMessageData
      streamConfig: StreamDataType
      subscriberId: SubscriberId }

/// <summary>
/// Settings for synchronous/asynchronous propagation and error handling
/// </summary>
type GlobalDataPropagationBehaviour =
    /// <summary>
    ///  1) Code propagates synchronously if possible. May propagate async if thread lock cannot be found on the current thread
    ///  2) Errors in subscribers are cached and propagated when awaited
    ///  3) Errors in subscribers are isolated.
    /// </summary>
    | RunSynchronously

    /// <summary>
    ///  1) Code propagates synchronously if possible. May propagate async if thread lock cannot be found on the current thread
    ///  2) Errors in subscribers propagate back to the DynamoDb client which executed the request 
    ///  3) Errors in subscribers are not isolated. An error in 1 subscriber might prevent another subscriber from operating
    /// </summary> 
    | RunSynchronouslyAndPropagateToEventTrigger

    /// <summary>
    ///  1) Code propagates asynchronously
    ///  2) Errors in subscribers are cached and propagated when awaited
    ///  3) Errors in subscribers are isolated.
    /// </summary>
    | RunAsynchronously of Delay

    override this.ToString() =
        match this with
        | RunSynchronously -> nameof RunSynchronously
        | RunSynchronouslyAndPropagateToEventTrigger -> nameof RunSynchronouslyAndPropagateToEventTrigger
        | RunAsynchronously delay -> $"{nameof RunAsynchronously} after {delay}"

type SubscriberBehaviour =
    { delay: GlobalDataPropagationBehaviour
      subscriberTimeout: TimeSpan }

    with
    static member defaultOptions =
        { delay = TimeSpan.FromMilliseconds(10) |> RunAsynchronously
          subscriberTimeout = TimeSpan.FromSeconds(float 30) }

    /// <summary>
    ///  1) Code propagates asynchronously
    ///  2) Errors in subscribers are cached and propagated when awaited
    ///  3) Errors in subscribers are isolated.
    /// </summary>
    static member RunSynchronously(timeout) =
        { subscriberTimeout = timeout; delay = RunSynchronously }

    /// <summary>
    ///  1) Code propagates synchronously if possible. May propagate async if thread lock cannot be found on the current thread
    ///  2) Errors in subscribers propagate back to the DynamoDb client which executed the request 
    ///  3) Errors in subscribers are not isolated. An error in 1 subscriber might prevent another subscriber from operating
    /// </summary>
    static member RunSynchronouslyAndPropagateToEventTrigger(timeout) =
        { subscriberTimeout = timeout; delay = RunSynchronouslyAndPropagateToEventTrigger }

    /// <summary>
    ///  1) Code propagates synchronously if possible. May propagate async if thread lock cannot be found on the current thread
    ///  2) Errors in subscribers are cached and propagated when awaited
    ///  3) Errors in subscribers are isolated.
    /// </summary>
    static member RunAsynchronously(timeout, [<Optional; DefaultParameterValue(TimeSpan())>] delay) =
        { subscriberTimeout = timeout; delay = RunAsynchronously delay }

[<IsReadOnly; Struct>]
type UpdateStreamArgs =
    | CreateStream
    | DeleteStream

type SubscriberError = (struct (SubscriberMessage * Exception))
type SubscriberInputFn = SubscriberMessage -> CancellationToken -> ValueTask<unit>

type ClonedTableSubscriberOpts =
    { dataType: StreamDataType
      behaviour: SubscriberBehaviour
      subscriber: SubscriberInputFn
      addDeletionProtection: bool }

type TimeInQueue = TimeSpan

/// <summary>
/// The input has a delay and is an option so that that emitter has a chance to pause and cancel
/// propagation. This is to facilitate transact writes and two phase commits
/// </summary>
type SubscriberFn = SubscriberBehaviour -> TimeInQueue -> Logger -> ValueTask<SubscriberMessage voption> -> ValueTask<SubscriberError voption>

/// <summary>
/// A callback which receives notifications when data changes
/// Models a dynamodb stream subscription
/// </summary>
[<Struct; IsReadOnly>]
type private StreamSubscriber =
    { id: SubscriberId
      callback: SubscriberFn
      dataType: StreamDataType
      behaviour: SubscriberBehaviour

      /// <summary>
      /// The current execution state of the subscriber
      /// If the task is not resolved, then there is currently a callback in progress
      /// If the task is resolved, contains all previous errors reported by the subscriber
      ///
      /// Previous state can be cleared with the StreamSubscriber.await function
      /// </summary>
      executionState: ValueTask<SubscriberError list>
      hasDeletionProtection: bool }

module private StreamSubscriber =

    let private addErrorLogging logger id (sub: 'a -> ValueTask<'b>) c =
        let err e = Logger.log2 "Subscriber %O failed\n%O" id e logger
        Io.onError (fun _ -> sub c) err

    let private delay time () =
        task {
            do! Task.Delay(delay = time).ConfigureAwait(false)
            return ()
        } |> ValueTask<unit>

    let private noDelay = ValueTask<unit>(()).Preserve() |> asLazy

    type private ErrorHandler = (SubscriberMessage -> ValueTask<unit>) -> SubscriberMessage -> ValueTask<struct (SubscriberMessage * exn) voption>
    let private errorHandling (f: SubscriberMessage -> ValueTask<unit>) x =

        let f' () = f x |%|> (fun _ -> ValueNone)
        Io.recover f' (fun e -> ValueSome struct (x, e))

    let private noErrorHandling (f: SubscriberMessage -> ValueTask<unit>) =
        f >> Io.map (fun _ -> ValueNone)

    let buildCts settings =
        if settings.subscriberTimeout = TimeSpan.Zero
        then struct (Disposable.nothingToDispose, CancellationToken.None)
        else
            let cts = new CancellationTokenSource(settings.subscriberTimeout)
            struct (cts, cts.Token)

    let private dataCancelled = ValueTask<_>(ValueNone).Preserve()

    let prepareSubscriberFunction behaviour subscriberId (subscriber: SubscriberInputFn): SubscriberFn =

        // just execute subscriber with cts.
        // the difficulties here are in not using the "task" workflow if not required
        // so as to stay on the same thread
        let execute' (errHandling: ErrorHandler) logger message =
            let mutable struct (dispose, c) = buildCts behaviour
            try
                let result =
                    flip subscriber c
                    |> addErrorLogging logger subscriberId
                    |> errHandling
                    <| message

                if result.IsCompleted then result
                else
                    let d = dispose
                    dispose <- null
                    task {
                        use d' = d
                        return! result.ConfigureAwait(false)
                    } |> ValueTask<SubscriberError voption>
            finally
                if dispose <> null then dispose.Dispose()

        let execute errHandling logger message =
            match message with
            | ValueNone ->
                Logger.log0 "Subscriber message cancelled" logger
                dataCancelled
            | ValueSome m -> execute' errHandling logger m

        fun behaviour timeSpentInQueue logger change ->
            let struct (errHandling, delay) =
                match behaviour with
                | { delay = RunAsynchronously d } when d = TimeSpan.Zero ->
                    Logger.debug0 "Propagating synchronously" logger
                    struct (errorHandling, noDelay)
                | { delay = RunAsynchronously d } ->
                    let adjustedDelay = d - timeSpentInQueue
                    Logger.debug0 "Propagating asynchronously" logger
                    if adjustedDelay <= TimeSpan.Zero
                    then struct (errorHandling, noDelay)
                    else struct (errorHandling, delay adjustedDelay)
                | { delay = RunSynchronouslyAndPropagateToEventTrigger } ->
                    Logger.debug0 "Propagating synchronously" logger
                    struct (errorHandling, noDelay)
                | { delay = RunSynchronously } ->
                    Logger.debug0 "Propagating synchronously" logger
                    struct (noErrorHandling, noDelay)

            Io.retn tpl
            <|%| delay ()
            <|%| change
            |%|> sndT
            |%>>= execute errHandling logger

    let private noErrors = (ValueTask<SubscriberError list> []).Preserve()
    let create dataType subscriber deletionProtection (behaviour: SubscriberBehaviour) =
        let id = IncrementingId.next()

        let sub = prepareSubscriberFunction behaviour id subscriber
        { id = id
          dataType = dataType
          callback = sub
          executionState = noErrors
          hasDeletionProtection = deletionProtection
          behaviour = behaviour }

    let noErrorsList = asLazy []

    /// <summary>
    /// Clear and return all errors
    /// Returned task can be awaited to block until all messages are processed
    /// </summary>
    let await logger subscriber =
        if subscriber.executionState.IsCompletedSuccessfully
        then struct (subscriber.executionState, noErrors)
        else
            let logComplete =
                if subscriber.executionState.IsCompleted
                then id
                else
                    Logger.log1 "Waiting for subscriber %O" subscriber.id logger

                    Io.map (fun errs ->
                        let errMsg =
                            match List.length errs with
                            | 0 -> ""
                            | 1 -> ", 1 error"
                            | e -> $", {e} errors"

                        Logger.log2 "Wait complete for subscriber %O%O" subscriber.id errMsg logger
                        errs)

            let queue = Io.map noErrorsList subscriber.executionState
            struct (subscriber.executionState |%|> List.rev |> logComplete, queue)
        |> mapSnd (fun queue -> { subscriber with executionState = queue.Preserve() })

    let inline private removePuts (x: DatabaseSynchronizationPacket<TableCdcPacket>) =
        { x with data.packet.changeResult =  ChangeResults.removePuts x.data.packet.changeResult }

    let inline private removeDeletes (x: DatabaseSynchronizationPacket<TableCdcPacket>) =
        { x with data.packet.changeResult =  ChangeResults.removeDeletes x.data.packet.changeResult }

    let private buildSubscriberMessageData =
        function
        | struct (_, TableDeleted _ & x)
        | struct (_, OnChange (SchemaChange _) & x)
        | struct (_, OnChange (SubscriberChange _) & x)
        | NewAndOldImages, x -> x
        | NewImage, OnChange (ChangeDataCapture change) -> removeDeletes change |> ChangeDataCapture |> OnChange
        | OldImage, OnChange (ChangeDataCapture change) -> removePuts change |> ChangeDataCapture |> OnChange
        | KeysOnly, OnChange (ChangeDataCapture change) ->
            let keysOnly x =
                KeyConfig.keyNames change.data.packet.changeResult.KeyConfig
                |> Array.ofSeq
                |> ValueSome
                |> flip (Item.reIndex (Item.itemName x)) x

            let changeResult = ChangeResults.mapChanges keysOnly change.data.packet.changeResult
            { change with
                data.packet.changeResult = changeResult } |> ChangeDataCapture |> OnChange

    let onChange logger changes subscriber =

        let message =
            changes
            |%|> ValueOption.map (fun changes ->
                { databaseChange = buildSubscriberMessageData struct(subscriber.dataType, changes)
                  streamConfig = subscriber.dataType
                  subscriberId = subscriber.id })

        let beforeWait = DateTime.UtcNow
        let newErrs =
            subscriber.executionState
            |%>>= fun failed ->
                let waitTime = DateTime.UtcNow - beforeWait
                subscriber.callback subscriber.behaviour waitTime logger message
                |%|> tpl failed
            |%|> function
                | failed, ValueNone -> failed
                | failed, ValueSome x -> x::failed

        { subscriber with executionState = newErrs.Preserve() }

type private StreamInfo =
    { tableName: string
      streamLabel: string
      arn: struct (AwsAccountId * RegionId) -> string }

[<Struct; IsReadOnly>]
type private StreamData =
    { subscribers: StreamSubscriber list
      info: StreamInfo }

type RegionId = string
type AwsAccountId = string

/// <summary>
/// A model of a dynamodb Stream.
/// A collection of subscribers with shared config
/// </summary>
[<Struct; IsReadOnly>]
type Stream =
    private
    | Str of StreamData

module Stream =
    let zeroToNone = function
        | x when x = TimeSpan.Zero -> ValueNone
        | x -> ValueSome x

    let streamLabelGen =
        let state = MutableValue.create Map.empty

        // label must be unique per table
        let generateUniqueDate' lastValue = 
            match DateTime.UtcNow with
            | x when x > lastValue -> x
            | x -> x.AddMilliseconds 1
            |> Some

        let generateUniqueDate =
            Option.map generateUniqueDate'
            >> Option.defaultValue (Some DateTime.UtcNow)

        let mutateValue table v =
            let v = Map.change table generateUniqueDate v
            struct ((Map.find table v).ToString("yyyy-MM-ddThh:mm:ss.fff"), v)

        fun tableName ->
            MutableValue.mutate (mutateValue tableName) state

    let empty tableName =
        let label = streamLabelGen tableName
        { subscribers = []
          info =
              { tableName = tableName
                streamLabel = label 
                arn = fun struct(awsAccount: AwsAccountId, region: RegionId) -> $"arn:aws:dynamodb:{region}:{awsAccount}:table/{tableName}/stream/{label}" } }
        |> Str

    let setBehaviour subscriberId behaviour (Str data) =
        let subscribers =
            subscriberId
            ?|> (fun s ->
                data.subscribers
                |> Collection.partition (_.id >> ((=)s)))
            ?|? struct (data.subscribers, [])
            |> mapFst ((function
                | _::_ & xs -> xs
                | [] & xs when ValueOption.isNone subscriberId -> xs
                | _ -> serverError $"Unable to find subscriber {subscriberId}")
                >> List.map (fun s -> {s with behaviour = behaviour }))
            |> uncurry Collection.concat2
            |> Seq.sortBy _.id.Value
            |> List.ofSeq

        Str { data with subscribers = subscribers }

    let arn location = function
        | Str x -> x.info.arn location

    let label = function
        | Str x -> x.info.streamLabel

    let hasDeletionProtection = function
        | Str x -> x.subscribers |> Seq.filter _.hasDeletionProtection |> Seq.isEmpty |> not

    let listSubscribers (Str x) =
        x.subscribers |> List.map _.id

    let listSubscriberConfig (Str x) =
        x.subscribers |> List.map _.dataType |> List.distinct

    let addSubscriber behaviour dataType (subscriber: SubscriberInputFn) deletionProtection = function
        | Str x ->
            let subscriber = StreamSubscriber.create dataType subscriber deletionProtection behaviour
            { x with subscribers = subscriber::x.subscribers } |> Str |> tpl subscriber.id

    let noErrors = ValueTask<SubscriberError seq>(Seq.empty).Preserve()

    /// <summary>
    /// Return a task that will be completed when all subscribers have been completed
    /// The returned task can be used for error handling within subscriber logic
    /// This is a mutation operation, so the new Stream is also returned
    /// </summary>
    let await logger idFilter = function
        | Str x ->
            x.subscribers
            |> Collection.foldBackL (fun struct (errAcc, subAcc) ->
                StreamSubscriber.await logger
                >> mapFst (Io.bind (Collection.concat2 >> flip Io.map errAcc))
                >> mapSnd (flip Collection.prependL subAcc)) struct (noErrors, [])
            |> mapFst (Io.map List.ofSeq)
            |> mapSnd (fun s -> Str {x with subscribers = s })

    let private applySubscribers logger = function
        | struct ({ request = OnChange (ChangeDataCapture change)}, xs) when ChangeResults.hasModificationsOrModifcationAttempts change.data.packet.changeResult |> not ->
            Logger.log1 "No changes to stream for \"%O\"" change.tableName logger
            xs
        | { request = OnChange (ChangeDataCapture change)}, ([] & xs) ->
            Logger.log1 "No subscribers to stream to for \"%O\"" change.tableName logger
            xs
        | { request = change; emergencyBrake = emergencyBrake}, subs ->
            Logger.log1 "Applying subscribers for \"%O\"" change logger
            let change' = emergencyBrake |%|> function | true -> ValueSome change | false -> ValueNone
            List.map (StreamSubscriber.onChange logger change') subs

    let onChange logger changes (Str stream) =
        { stream with subscribers = applySubscribers logger struct (changes, stream.subscribers) } |> Str

    let removeSubscriber logger databaseId (subscriberId: IncrementingId) = function
        | Str x ->
            let struct (subscribers, removed) =
                x.subscribers |> Collection.partition (_.id.Value >> ((<>)subscriberId.Value))

            let deleteMessageResults =
                match removed with
                | [] ->
                    Logger.debug1 "No subscribers found with id %O" subscriberId logger
                    []
                | xs ->
                    let packet =  
                        { tableName = x.info.tableName
                          synchronizationPacketPath = NonEmptyList.singleton databaseId
                          correlationId = Logger.id logger
                          data = subscriberId |> SubscriberDeleted } |> SubscriberChange |> OnChange |> EmergencyBrakeRequest.noBrake

                    applySubscribers logger struct (packet, removed)
                |> List.map _.executionState
                |> Io.traverse
                |%|> List.concat

            { x with subscribers = subscribers }
            |> Str
            |> tpl deleteMessageResults

/// <summary>
/// Streams for multiple Tables
/// </summary>
[<Struct; IsReadOnly>]
type TableStreams =
    private
    | Strs of Map<string, Stream voption>

module TableStreams =

    let logOperation x = DatabaseLogger.logOperation "Streams" x
    let logOperationHalfAsync x = DatabaseLogger.logOperationHalfAsync "Streams" x

    let empty = Map.empty |> Strs

    let private describeConfig = Logger.describable (function
        | struct (ValueNone, tablename: string) -> sprintf "Setting stream for table %A - No streams" tablename
        | (ValueSome (opts: SubscriberBehaviour), tablename) -> $"Setting stream for table \"{tablename}\" - timeout: {opts.subscriberTimeout}, behavior: {opts.delay}")

    let addStream tableName enabled =
        fun logger -> function
        | Strs x when Map.containsKey tableName x ->
            clientError $"Table \"{tableName}\" already has streams configured"
        | Strs x ->
            let str =
                if enabled then Stream.empty tableName |> ValueSome
                else ValueNone

            Map.add tableName str x
            |> Strs
        |> logOperation "ADD STREAM"

    let rec private ensureStreamMap map =
        function
        | [] -> map
        | head::tail ->
            match Map.containsKey head map with
            | true -> ensureStreamMap map tail
            | false ->
                Map.add head ValueNone map
                |> flip ensureStreamMap tail

    let ensureStreams streamNames = function
        | Strs x -> ensureStreamMap x streamNames |> Strs

    let tryGetStream streamName = function
        | Strs x -> MapUtils.tryFind streamName x ?>>= id

    let setBehaviour tableName subscriberId behaviour (Strs data) =
        MapUtils.tryFind tableName data
        ?>>= id
        |> Maybe.expectSomeErr "Cannot find stream for table %s" tableName
        |> Stream.setBehaviour subscriberId behaviour
        |> ValueSome
        |> flip (Map.add tableName) data
        |> Strs

    let private noErrors = ValueTask<SubscriberError seq>(Seq.empty).Preserve()
    let private noErrorsL = ValueTask<SubscriberError list>(List.empty).Preserve()

    let removeSubscriber databaseId table subscriberId =
        fun logger -> function
        | Strs streams & strs ->
            match MapUtils.tryFind table streams with
            | ValueNone
            | ValueSome ValueNone -> struct (noErrorsL,  strs)
            | ValueSome (ValueSome config) ->

                let result =
                    Stream.removeSubscriber logger databaseId subscriberId config
                    |> mapSnd (
                        ValueSome
                        >> flip (Map.add table) streams
                        >> Strs)

                Logger.log1 "Subscriber %i removed" subscriberId.Value logger
                result
        |> logOperationHalfAsync "REMOVE STREAM SUBSCRIBER"

    type RemoveSubscriberFn = Logger -> TableStreams -> struct (ValueTask<SubscriberError list> * TableStreams)
    let addSubscriber databaseId table behaviour dataType subscriber deletionProtection: _ -> _ -> struct (struct (RemoveSubscriberFn * _) * _) =
        fun logger -> function
        | Strs streams ->
            match MapUtils.tryFind table streams with
            | ValueNone -> clientError $"Cannot find table \"{table}\" on database \"{databaseId}\" for stream creation"
            | ValueSome ValueNone -> clientError $"Streams are disabled for table \"{table}\" on database \"{databaseId}\""
            | ValueSome (ValueSome config) ->

                Stream.addSubscriber behaviour dataType subscriber deletionProtection config
                |> mapFst (
                    Logger.logFn1 "Subscriber %O added" logger
                    >> tplDouble
                    >> mapFst (removeSubscriber databaseId table))
                |> mapSnd (
                    ValueSome
                    >> flip (Map.add table) streams
                    >> Strs)
        |> logOperation "ADD STREAM SUBSCRIBER"

    let onChange changeResult =
        fun logger -> function
        | Strs x ->
            Map.change (SubscriberMessageData.getTableName changeResult.request) (
                Option.map (ValueOption.map (Stream.onChange logger changeResult))) x
            |> Strs
        |> logOperation "ON DATA CHANGE"

    /// <summary>Returns any unhandled errors and a task to wait for streams to complete</summary>
    let deleteTable table =
        fun logger ->
            TableDeleted table
            |> EmergencyBrakeRequest.noBrake
            |> flip onChange logger
            >> function
                | Strs streams & s ->
                    MapUtils.tryFind table streams
                    ?|> (
                        ValueOption.map (Stream.await logger ValueNone >> fstT)
                        >> ValueOption.defaultValue noErrorsL)
                    ?|> (fun x ->
                        Map.remove table streams
                        |> Strs
                        |> tpl x)
                    |> ValueOption.defaultValue struct (noErrorsL, s)
        |> logOperationHalfAsync "REMOVING TABLE STREAM"

    type Enable = bool
    let updateStream tableName (req: UpdateStreamArgs) =
        fun logger -> function
        | Strs streams ->
            MapUtils.tryFind tableName streams
            ?|> (flip tpl req)
            ?|> (function
                // You receive a ValidationException if you:
                // 1) try to enable a stream on a table that already has a stream
                | ValueSome _, CreateStream ->
                    clientError $"Table \"{tableName}\" already has streams enabled"
                // 2) if you try to disable a stream on a table that doesn't have a stream.
                | ValueNone, DeleteStream ->
                    clientError $"Table \"{tableName}\" does not have a stream to disable"
                | ValueSome str, DeleteStream when Stream.hasDeletionProtection str ->
                    clientError $"Stream for table \"{tableName}\" cannot be removed. This might be because it is used for database replication"
                | ValueSome _, DeleteStream ->
                    Map.add tableName ValueNone streams |> Strs
                | ValueNone, CreateStream ->
                    let newV = Stream.empty tableName |> ValueSome
                    Map.add tableName newV streams |> Strs)
            |> ValueOption.defaultWith (fun _ ->
                clientError $"Cannot find table \"{tableName}\" for stream modification")
        |> logOperation "UPDATE STREAMS"

    /// <summary>Lists enabled streams along with distinct config of their subscribers</summary>
    let listEnabled = function
        | Strs x ->
            MapUtils.toSeq x
            |> Seq.filter (sndT >> ValueOption.isSome)
            |> Seq.map fstT

    let tryStreamSubscribers name = function
        | Strs x -> MapUtils.tryFind name x ?>>= id

    /// <summary>
    /// Return a task that will be completed when all subscribers have been completed
    /// The returned task can be used for error handling within subscriber logic
    /// Also return the Subscribers which can be awaited again, but with no error handling
    /// </summary>
    let await filter =
        fun logger -> function
        | Strs x ->
            let tableFilter = ValueOption.map fstT filter
            let idFilter = ValueOption.bind sndT filter

            tableFilter
            // get filtered table
            ?|> (fun f ->
                MapUtils.tryFind f x
                ?>>= id
                |> Maybe.toList
                |> Seq.ofList
                |> Seq.map (tpl f))

            // -or- use all items
            |> ValueOption.defaultWith (fun _ ->
                MapUtils.toSeq x
                |> Seq.map (fun struct (k, v) -> ValueOption.map (tpl k) v)
                |> Maybe.traverse)

            // await and combine all subscribers and errors together
            |> Seq.fold (fun struct (errAcc, map) struct (k, v) ->
                Stream.await logger idFilter v
                |> mapFst (Io.bind (fun subErr -> Io.map (Collection.concat2 subErr) errAcc))
                |> mapSnd (ValueSome >> flip (Map.add k) map)) (struct (noErrors, x))
            |> mapFst (Io.map List.ofSeq)
            |> mapSnd Strs
        |> logOperationHalfAsync "AWAIT SUBSCRIBERS"

type StreamException(error: SubscriberError) =
    inherit Exception($"Exception encountered processing stream\n{sndT error}", sndT error)

    static member streamExceptions = function
        | [err] -> StreamException(err) :> Exception
        | xs -> AggregateException(Seq.map (fun x -> StreamException(x) :> Exception) xs)
