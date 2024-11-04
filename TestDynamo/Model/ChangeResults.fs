namespace TestDynamo.Model

open System.Runtime.CompilerServices
open TestDynamo.Data.BasicStructures
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model
type private Math = System.Math

type PutAndDelete = (struct (Item * Item))

[<IsReadOnly; Struct>]
type ExplicitChangeResult =
    | Simple of c: CreateOrDelete<Item>
    | Replace of r: PutAndDelete
    with
    static member created = function
        | Simple(Create x) -> ValueSome x
        | Replace (x, _) -> ValueSome x
        | Simple(Delete _) -> ValueNone

    static member deleted = function
        | Simple(Create _) -> ValueNone
        | Replace (_, x) -> ValueSome x
        | Simple(Delete x) -> ValueSome x

[<IsReadOnly; Struct>]
type private ChangeResultData =
    { id: IncrementingId
      uniqueEventId: System.Guid
      result: ExplicitChangeResult }

/// <summary>
/// The result of a single change operation which can delete, put or replace an item
/// </summary>
[<IsReadOnly; Struct>]
type ChangeResult =
    private | Pcr of ChangeResultData
    with
    member this.Id = match this with | Pcr {id = x} -> x
    member this.UniqueEventId = match this with | Pcr {uniqueEventId = x} -> x
    member this.Result = match this with | Pcr {result = x} -> x
    member this.Put = ExplicitChangeResult.created this.Result
    member this.Deleted = ExplicitChangeResult.deleted this.Result

module ChangeResult =

    let inline id (x: ChangeResult) = x.Id
    let inline deleted (x: ChangeResult) = x.Deleted
    let inline put (x: ChangeResult) = x.Put
    let result (Pcr x) = x.result
    let asTpl (Pcr x) = struct (x.id, x.result)

    let asPutItem = function
        | Pcr { result = Simple(Delete _) } -> ValueNone
        | Pcr { result = Simple(Create x) } -> ValueSome x
        | Pcr { result = Replace (put, _) } -> ValueSome put

    let asPut = function
        | Pcr { result = Simple(Create _) } & x -> ValueSome x
        | Pcr { result = Simple(Delete _) } -> ValueNone
        | Pcr ({ result = Replace (put, _) } & x) -> {x with result = Simple(Create put) } |> Pcr |> ValueSome

    let asDeleteItem = function
        | Pcr { result = Simple(Create _) } -> ValueNone
        | Pcr { result = Simple(Delete x) } -> ValueSome x
        | Pcr { result = Replace (_, delete) } -> ValueSome delete

    let asDelete = function
        | Pcr { result = Simple(Create _) } & x -> ValueNone
        | Pcr { result = Simple(Delete _) } & x -> ValueSome x
        | Pcr ({ result = Replace (_, delete) } & x) -> {x with result = Simple(Delete delete) } |> Pcr |> ValueSome

    let mapItems f = function
        | Pcr ({ result = Simple(Create x) } & data) -> { data with result = Simple(Create (f x)) } |> Pcr
        | Pcr ({ result = Simple(Delete x) } & data) -> { data with result = Simple(Delete (f x)) } |> Pcr
        | Pcr ({ result = Replace (x, y) } & data) -> { data with result = (Replace (f x, f y)) } |> Pcr

    let ofPut put replaced =
        let result =
            replaced
            ?|> (tpl put >> Replace)
            |> ValueOption.defaultValue (Simple(Create put))
        { id = Item.internalId put
          uniqueEventId = System.Guid.NewGuid()
          result = result } |> Pcr

    let ofDelete id delete =
        { id = id
          uniqueEventId = System.Guid.NewGuid()
          result = Simple(Delete delete) } |> Pcr

    type PutAndDeleteCount = (struct (int * int))

    /// <summary>
    /// Returns negative number if the aggregate result is less items
    /// </summary>
    let modificationCounts: ChangeResult -> PutAndDeleteCount = function
        | Pcr { result = Simple(Create _) } -> struct (1, 0)
        | Pcr { result = Simple(Delete _) } -> struct (0, 1)
        | Pcr { result = Replace _ } -> struct (1, 1)

    /// <summary>
    /// Returns negative number if the aggregate result is less items
    /// </summary>
    let modificationCount = modificationCounts >> fun struct (p, d) -> p - d

/// <summary>
/// A delete attempt is a delete which failed because the item did not exist
/// This can happen if invalid keys are supplied, but also if data replication is slow and the
/// item has not been replicated yet
/// </summary>
[<Struct; IsReadOnly>]
type DeleteAttemptData =
    { deleteId: IncrementingId
      partitionKey: struct (string * AttributeValue)
      sortKey: struct (string * AttributeValue) voption }

    with
    static member asDeleteRequest attemptedDelete =
        attemptedDelete.sortKey
        ?|> (flip3To1 Map.add Map.empty |> uncurry)
        |> ValueOption.defaultValue Map.empty
        |> Map.add (fstT attemptedDelete.partitionKey) (sndT attemptedDelete.partitionKey)
        |> tpl attemptedDelete.deleteId

[<IsReadOnly; Struct>]
type private ChangeResultsData =
    { keyConfig: KeyConfig 
      orderedChanges: ChangeResult list
      orderedDeleteAttempts: DeleteAttemptData list }

/// <summary>
/// The data for the result of a modification to an Index
/// </summary>
[<IsReadOnly; Struct>]
type ChangeResults =
    private | Crs of ChangeResultsData
    with

    member this.KeyConfig = match this with Crs x -> x.keyConfig
    member this.OrderedChanges = match this with Crs x -> x.orderedChanges
    member this.OrderedDeleteAttempts = match this with Crs x -> x.orderedDeleteAttempts

module ChangeResults =

    let create keys (crs: ChangeResult list) (dels: DeleteAttemptData list) =
        { keyConfig = keys
          orderedChanges = List.sortBy ChangeResult.id crs
          orderedDeleteAttempts =  dels |> List.sortBy _.deleteId } |> Crs

    let ofPartitionChangeResults = flip3To1 create []

    let ofPutItems: KeyConfig -> Item seq -> ChangeResults =
        let create = flip ChangeResult.ofPut ValueNone
        fun keyConfig -> Seq.map create >> List.ofSeq >> ofPartitionChangeResults keyConfig

    let ofDeleteAttempts = flip create []

    let ofPartitionChangeResult keys = List.singleton >> ofPartitionChangeResults keys

    let modificationCounts: ChangeResults -> ChangeResult.PutAndDeleteCount =
        _.OrderedChanges
        >> List.fold (fun struct (put, del) x ->
            let struct (p, d) = ChangeResult.modificationCounts x
            (put + p, del + d)) struct (0, 0)

    /// <summary>
    /// Returns negative number if there are more deletes than puts
    /// </summary>
    let modificationCount = modificationCounts >> fun struct (p, d) -> p - d

    let removeDeletes (Crs x) =
        { x with
            orderedChanges = Seq.map ChangeResult.asPut x.orderedChanges |> Maybe.traverse |> List.ofSeq
            orderedDeleteAttempts = [] } |> Crs

    let deletedItems (Crs x) =
        x.orderedChanges |> Seq.map ChangeResult.asDeleteItem |> Maybe.traverse

    let putItems (Crs x) =
        x.orderedChanges |> Seq.map ChangeResult.asPutItem |> Maybe.traverse

    let removePuts (Crs x) =
        { x with
            orderedChanges = Seq.map ChangeResult.asDelete x.orderedChanges |> Maybe.traverse |> List.ofSeq } |> Crs

    let mapChanges f (Crs x) =
        { x with
            orderedChanges = List.map (ChangeResult.mapItems f) x.orderedChanges } |> Crs

    // experimenting with how a mutable accumulator "feels" like (as an optimization)
    let private mergeSort =
        let rec execute = function
            | struct (sortBy, acc: MList<_>, [], []) -> List.ofSeq acc
            | sortBy, acc, head::tail, ([] & ys) ->
                acc.Add(head)
                execute struct (sortBy, acc, tail, ys)
            | sortBy, acc, ([] & xs), head::tail ->
                acc.Add(head)
                execute struct (sortBy, acc, xs, tail)
            | sortBy, acc, xHead::xTail, (yHead::_) & yTail when compare (sortBy xHead) (sortBy yHead) <= 0 ->
                acc.Add(xHead)
                execute struct (sortBy, acc, xTail, yTail)
            | sortBy, acc, xTail, yHead::yTail ->
                acc.Add(yHead)
                execute struct (sortBy, acc, xTail, yTail)

        let inline ms sortBy struct (xs, ys) = execute struct (sortBy, MList<_>(List.length xs + List.length ys), xs, ys)
        ms

    let private sortChanges = mergeSort ChangeResult.id
    let private sortChangeAttempts: struct (DeleteAttemptData list * DeleteAttemptData list) -> DeleteAttemptData list =
        mergeSort _.deleteId

    let concat (Crs r1) (Crs r2) =
        match r1.keyConfig = r2.keyConfig with
        | false -> serverError "Keys must match"
        | true ->

            { keyConfig = r1.keyConfig
              orderedChanges = sortChanges struct(r1.orderedChanges, r2.orderedChanges)
              orderedDeleteAttempts = sortChangeAttempts struct(r1.orderedDeleteAttempts, r2.orderedDeleteAttempts) } |> Crs

    let empty keyConfig = {orderedChanges = []; orderedDeleteAttempts = []; keyConfig = keyConfig;} |> Crs

    let hasModifications (Crs x) = List.isEmpty x.orderedChanges |> not

    let hasModificationsOrModifcationAttempts (Crs x' & x) = hasModifications x || (List.isEmpty x'.orderedDeleteAttempts |> not)

    let addDeleteAttempt att = function
        | Crs ({ orderedDeleteAttempts = [] } & x) -> { x with orderedDeleteAttempts = [att] } |> Crs
        | Crs ({ orderedDeleteAttempts = atts } & x) -> { x with orderedDeleteAttempts = sortChangeAttempts struct ([att], x.orderedDeleteAttempts) } |> Crs

/// <summary>
/// A packet used for change data capture
/// </summary>
type CdcPacket =
    { changeResult: ChangeResults

      /// <summary>
      /// The version that the index is at after modification
      /// </summary>
      dataVersion: int }

module CdcPacket =
    let concat r1 r2 =
        { dataVersion = Math.Max(r1.dataVersion, r2.dataVersion)
          changeResult = ChangeResults.concat r1.changeResult r2.changeResult }

    let log logVersion logger cr =
        let count = List.length cr.changeResult.OrderedChanges
        let struct (logger, disposeScope) =
            if logVersion && count > 0
            then
                Logger.log1 "Version %d" cr.dataVersion logger
                Logger.scope logger
            elif count > 0 then Logger.scope logger
            else struct (logger, Disposable.nothingToDispose)
            
        use _ = disposeScope

        let projectedCount =
            cr.changeResult.OrderedChanges
            |> Seq.map (ChangeResult.result >> function
                | Simple(Create x) -> sprintf "Added item %A" x
                | Simple(Delete x) -> sprintf "Removed item %A" x
                | Replace (x, y) -> sprintf "Replaced item %A with %A" y x)
            |> Seq.fold (fun s x ->
                Logger.log1 "%s" x logger
                s + 1) 0

        if projectedCount = 0 then Logger.log0 "No changes to project" logger
        cr
