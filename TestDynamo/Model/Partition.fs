namespace TestDynamo.Model

open TestDynamo.Data
open TestDynamo.Data.BasicStructures
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open System.Runtime.CompilerServices

type private PartitionInfo =
    { keys: KeyConfig
      name: string
      projections: string array voption
      sortKeysUnique: bool }

[<Struct; IsReadOnly>]
type private PartitionResource<'a> =
    { info: PartitionInfo
      data: 'a }

/// <summary>
/// A block of items in a partition. The block cannot be empty
/// If the partition belongs to the table primary index, then this block will have a single item
/// If the partition belongs to an index, then all of the items in the block will have the same key values
/// </summary>
[<IsReadOnly; Struct>]
type PartitionBlock =
    private
    | Pb of struct (int * NonEmptyList<Item>)

    with
    override this.ToString() =
        match this with
        | Pb (_, l) -> (NonEmptyList.head l).ToString()

module PartitionBlock =

    let toList = function
        | Pb (_, l) -> NonEmptyList.unwrap l

    let toSeq = toList >> Seq.ofList

    let create item = struct (1, NonEmptyList.singleton item) |> Pb

    let pop (Pb (l, items)) =
        NonEmptyList.pop items
        |> mapSnd (ValueOption.map (tpl l >> Pb))

    let removeByInternalId =
        let remove (internalId: IncrementingId) =
            Collection.foldBack (fun struct (count, keep, remove) x ->
                match internalId.Value = (Item.internalId x).Value with
                | true -> struct (count, keep, x::remove)
                | false -> struct (count + 1, x::keep, remove)) struct (0, [], [])

        fun internalId (Pb (_, data)) ->
            match remove internalId (NonEmptyList.unwrap data) with
            | _, _, _::_::_ -> serverError "An unexpected error has occurred"
            | _, [], removed -> struct (removed |> Collection.tryHead, ValueNone)
            | count, vals, removed ->
                tpl count (NonEmptyList.ofList vals)
                |> Pb |> ValueSome |> tpl (Collection.tryHead removed)

    let peek (Pb (_, xs)) = NonEmptyList.head xs

    let length = function | Pb (l, _) -> l

    /// <summary>Will throw an error if 2 items with the same internal id are added to the same block</summary>
    /// <returns>Returns an object if it was evicted from the partition block</returns>
    let put sortKeysUnique x (Pb (l, xs)) =
        if sortKeysUnique
            then create x |> tpl (ValueSome (NonEmptyList.head xs))
        elif Seq.contains (Item.internalId x).Value (NonEmptyList.unwrap xs |> Seq.map (Item.internalId >> IncrementingId.value))
            then serverError "Duplicate internal item id found"
        else
            struct (l + 1, NonEmptyList.prepend x xs)
            |> Pb
            |> tpl ValueNone

module private PartitionData =
    let addData x d = {x with data = d}

    let sortKey item data =
        KeyConfig.sortKey item data.keys

    let i1Of3 struct (x, _, _) = x
    let i2Of3 struct (_, x, _) = x
    let i3Of3 struct (_, _, x) = x

type private PartitionWithSortKeyData = (struct (AttributeType * PartitionResource<AvlTree<AttributeValue, PartitionBlock>>))

/// <summary>
/// A partition of items belonging to an Index where all items have the same Partition key
/// </summary>
type Partition =
    private
    | WithSortKey of PartitionWithSortKeyData
    | WithoutSortKey of PartitionResource<PartitionBlock>

    with
    override this.ToString() =
        match this with
        | WithSortKey (_, { info = {name = t }})
        | WithoutSortKey { info = {name = t} } -> t

[<RequireQualifiedAccess>]
module Partition =
    let scanCount = function
        | WithoutSortKey x -> PartitionBlock.length x.data
        | WithSortKey struct (_, x) ->
            AvlTree.toSeq x.data
            |> Seq.map (sndT >> PartitionBlock.length)
            |> Seq.sum

    let private getRequiredSortKey =
        let get1 keyConfig attributes =
            KeyConfig.sortKey attributes keyConfig
            |> ValueOption.defaultWith (fun _ -> ClientError.clientError "Sort key is required")

        let get2 (partitionData: PartitionResource<_>) =
            get1 partitionData.info.keys

        let get3 keyConfig =
            PartitionBlock.peek
            >> Item.attributes
            >> get1 keyConfig

        struct (get1, get2, get3)

    let private asItem =

        let itemName itemData key partitionName =

            KeyConfig.optionalSortKey itemData key
            ?|> (
                AttributeValue.describe
                >> sprintf "%s/%s" partitionName)
            |> ValueOption.defaultValue partitionName

        fun partitionName projections key ->
            function
            | Either1 itemData ->
                let itemName = itemName itemData key partitionName
                Item.create itemName itemData
            | Either2 item ->
                let itemName = itemName (Item.attributes item) key partitionName
                Item.reIndex itemName projections item

    let first = function
        | WithoutSortKey data -> PartitionBlock.peek data.data
        | WithSortKey struct (_, data) ->
            Seq.head data.data |> sndT |> PartitionBlock.peek

    let private withSortKey (x: PartitionWithSortKeyData) =
        match sndT x |> _.data |> Collection.tryHead with
        | ValueSome _ -> WithSortKey x
        | ValueNone -> invalidOp "Empty partition not supported"

    let create =
        let name indexName config =
            flip KeyConfig.partitionKey config
            >> AttributeValue.describe
            >> sprintf "%s/%s" indexName

        let name indexName config = function
            | Either1 itemData -> name indexName config itemData
            | Either2 item -> Item.attributes item |> name indexName config

        fun sortKeysUnique indexName keys projections item ->
            let name = name indexName keys item
            let item' = asItem name projections keys item
            let items = PartitionBlock.create item'
            let keySelection = PartitionData.i3Of3 getRequiredSortKey keys

            match KeyConfig.sortKeyDefinition keys with
            | ValueSome (_, t) ->
                { info =
                   { keys = keys
                     name = name
                     projections = projections
                     sortKeysUnique = sortKeysUnique }
                  data = AvlTree.empty |> AvlTree.add (keySelection items) items }
                |> tpl t
                |> withSortKey
            | ValueNone ->
                { info =
                   { keys = keys
                     name = name
                     projections = projections
                     sortKeysUnique = sortKeysUnique }
                  data = items }
                |> WithoutSortKey
            |> tpl (ChangeResult.ofPut item' ValueNone)

    let get =
        function
        | struct (ValueNone, WithSortKey _) -> ClientError.clientError "Sort key required"
        | ValueSome _, WithoutSortKey _ -> ClientError.clientError "Sort key not required"
        | struct (ValueSome (sortKey: AttributeValue), WithSortKey struct (_, partition)) ->
            AvlTree.tryFind sortKey partition.data ?|> PartitionBlock.toSeq |> ValueOption.defaultValue Seq.empty
        | struct (ValueNone, WithoutSortKey partition) ->
            partition.data |> PartitionBlock.toSeq
        |> curry

    let put =
        let itemsKeySelection = PartitionData.i3Of3 getRequiredSortKey
        let itemKeySelection = PartitionData.i1Of3 getRequiredSortKey
        let addOrReplace data x = AvlTree.addOrReplace (itemsKeySelection data.info.keys x) x data.data

        let inline addId x = struct (IncrementingId.next(), x)

        let putWithSortKey data item =
            let item = asItem data.info.name data.info.projections data.info.keys item

            Item.attributes item
            |> itemKeySelection data.info.keys
            |> flip AvlTree.tryFind data.data
            ?|> (
                PartitionBlock.put data.info.sortKeysUnique item)
            |> ValueOption.defaultWith (fun _ ->
                PartitionBlock.create item
                |> tpl ValueNone)
            |> mapSnd (
                addOrReplace data
                >> sndT
                >> PartitionData.addData data)
            |> mapFst (ChangeResult.ofPut item)

        let putWithoutSortKey data item =
            let item = asItem data.info.name data.info.projections data.info.keys item

            PartitionBlock.put data.info.sortKeysUnique item data.data
            |> mapSnd (PartitionData.addData data)
            |> mapFst (ChangeResult.ofPut item)

        fun item ->
            function
            | WithSortKey (t, data) ->
                putWithSortKey data item
                |> mapSnd (tpl t >> withSortKey)

            | WithoutSortKey data ->
                putWithoutSortKey data item
                |> mapSnd WithoutSortKey

    /// <returns>
    /// <para>
    /// Item1: the change result if a change was applied
    /// </para>
    /// <para>
    /// Item2:
    /// <ul>
    /// <li>The unchanged partition if the item is not found</li>
    /// <li>The modified partition if the item was found</li>
    /// <li>ValueNone if the partition is now empty (partitions must have at least 1 value)</li>
    /// </ul>
    /// </para>
    /// </returns>
    let remove =
        let itemsKeySelection = PartitionData.i3Of3 getRequiredSortKey
        let itemKeySelection = PartitionData.i1Of3 getRequiredSortKey

        let removeFromPartitionItems struct (deleteId, item) =
            PartitionBlock.removeByInternalId (Item.internalId item)
            >> mapFst (
                ValueOption.map (
                    tpl (ValueOption.defaultWith IncrementingId.next deleteId)
                    >> uncurry ChangeResult.ofDelete))

        let inline nothingToRemove items _ = struct (ValueNone, items)
        let removeFromTree' items item key =

            flip AvlTree.tryFind items key
            ?|> (
                removeFromPartitionItems item
                >> mapSnd (function
                    | ValueSome x -> AvlTree.addOrReplace key x items |> sndT
                    | ValueNone -> AvlTree.remove key items))
            |> ValueOption.defaultWith (nothingToRemove items)

        let removeFromTree (k: KeyConfig) (struct (_, item) & iWithid) items =

            Item.attributes item
            |> flip KeyConfig.optionalSortKey k
            ?|> (
                removeFromTree' items iWithid)
            |> ValueOption.defaultWith (nothingToRemove items)
            |> mapSnd (function
                | xs when AvlTree.isEmpty xs -> ValueNone
                | xs -> ValueSome xs)

        fun item ->
            function
            | WithSortKey (t, {data = xs; info = {keys = k}} & pd) ->
                removeFromTree k item xs
                |> mapSnd (ValueOption.map (
                    PartitionData.addData pd
                    >> tpl t
                    >> withSortKey))

            | WithoutSortKey ({data = xs} & pd) ->
                removeFromPartitionItems item xs
                |> mapSnd (ValueOption.map (
                    PartitionData.addData pd
                    >> WithoutSortKey))

    type Forwards = bool
    let subset from ``to`` inclusive (forwards: Forwards) partition =
        match struct (partition, struct (from, ``to``)) with
        | WithSortKey (t, _), (ValueSome v, _)
        | WithSortKey (t, _), (ValueSome v, _)
        | WithSortKey (t, _), (_, ValueSome v)
        | WithSortKey (t, _), (_, ValueSome v) when AttributeValue.getType v <> t ->
            ClientError.clientError  $"Attempting to use {AttributeValue.getType v} data to query partition \"{partition}\"; expected {t}"

        | WithSortKey (_, {data = data; info = {keys = keys}}), (from, ``to``) ->

            AvlTree.seek from ``to`` inclusive forwards data
            |> Seq.map sndT

        | WithoutSortKey {data = data; info = {keys = keys}}, (ValueNone, ValueNone) ->
            Seq.singleton data
        | WithoutSortKey _, _ -> ClientError.clientError "Partition has no sort key"

    let scan lastEvaluatedSortKey forwards =
        subset lastEvaluatedSortKey ValueNone false forwards

    let partitionSortComparer =
        { new IComparer<Partition> with
            member this.Compare(x, y) =
                match struct (x, y) with
                | WithSortKey struct (_, {info = {keys = xk}}) & x, WithSortKey struct (_, {info = {keys = yk}}) & y
                | WithSortKey struct (_, {info = {keys = xk}}) & x, WithoutSortKey {info = {keys = yk}} & y
                | WithoutSortKey {info = {keys = xk}} & x, WithSortKey struct (_, {info = {keys = yk}}) & y
                | WithoutSortKey {info = {keys = xk}} & x, WithoutSortKey {info = {keys = yk}} & y ->
                    let xF = first x |> Item.attributes |> flip KeyConfig.partitionKey xk
                    let yF = first y |> Item.attributes |> flip KeyConfig.partitionKey yk

                    compare xF yF
                }
