namespace TestDynamo.Model

open System.Runtime.CompilerServices
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

/// <summary>
/// A partition and range key reference
/// Contains functions to get keys from an Item. Does not contain keys itself 
/// </summary>
[<Struct; IsReadOnly>]
type KeyConfig =
    private
    | Kc of struct (AttributeSelector * AttributeSelector voption)

module KeyConfig =
    
    let describe = function
        | Kc (pk, sk) ->
            let pk' = AttributeSelector.describe pk

            sk
            ?|> AttributeSelector.describe
            ?|> (fun x -> [pk'; x])
            |> ValueOption.defaultValue [pk']

    let sortKeyDefinition = function
        | Kc (_, sk) -> ValueOption.map (fun x -> struct (x.attributeName, x.attributeType)) sk

    let partitionKeyDefinition = function
        | Kc (pk, _) -> struct (pk.attributeName, pk.attributeType)

    let itemStatus item = function
        | Kc struct (pk, sk) ->
            let pkResult = AttributeSelector.test item pk
            let skResult = ValueOption.map (AttributeSelector.test item) sk

            struct (pkResult, skResult)

    let hasKeys =
        let asBool = AttributeLookupResult.asOption >> ValueOption.isSome

        fun item ->
            itemStatus item
            >> function
                | struct (p, s) -> struct (asBool p, ValueOption.map asBool s |> ValueOption.defaultValue false)

    let validateAttributeKeys item (Kc (pk, sk)) =
        item
        |> tplDouble
        |> mapFst (flip AttributeSelector.test pk)
        |> mapSnd (ValueSome >> (ValueOption.map (flip AttributeSelector.test) sk |> Maybe.apply))

    let validateKeys item =
        Item.attributes item
        |> validateAttributeKeys

    let create =

        let createKey name key =
            { attributeName = fstT key
              attributeDescription =  name
              attributeType = sndT key }

        fun partitionKey sortKey ->
            match struct (partitionKey, sortKey) with
            | _, ValueNone -> ()
            | struct (x, _), ValueSome struct (y, _) when x = y ->
                clientError $"Cannot use the a single attribute for partition and sort keys \"{x}\""
            | _ -> ()

            Kc struct (createKey "partition key" partitionKey, ValueOption.map (createKey "sort key") sortKey)

    let hasSortKey = function
        | Kc struct (_, sk) -> ValueOption.isSome sk

    let keyNames = function
        | Kc struct (pk, ValueSome sk) -> [pk.attributeName; sk.attributeName]
        | Kc struct (pk, ValueNone) -> [pk.attributeName;]

    let partitionKeyName = function
        | Kc struct (pk, _) -> pk.attributeName

    let sortKeyName = function
        | Kc struct (_, sk) -> sk ?|> _.attributeName

    let private noEmptyStrings name = function
        | String x when x = "" -> clientError $"Empty string for key {name} is not permitted"
        | x -> x

    let partitionKey item (Kc (pk, _)) =
        AttributeSelector.get item pk |> noEmptyStrings pk.attributeName
    let partitionKeyWithName item (Kc (pk, _) & k) =
        partitionKey item k |> tpl pk.attributeName
    let partitionType = function
        | Kc struct (pk, _) -> pk.attributeType

    let sortKey item (Kc struct (_, sk)) =
        ValueOption.map (fun sk -> sk |> AttributeSelector.get item |> noEmptyStrings sk.attributeName) sk
    let sortKeyWithName item = function
        | Kc (_, ValueSome sk) & k -> sortKey item k ?|> (tpl sk.attributeName)
        | Kc (_, ValueNone) -> ValueNone
    let sortKeyType = function
        | Kc struct (_, sk) -> sk ?|> _.attributeType

    let keyCols = function
        | Kc struct (pk, ValueNone) -> [AttributeSelector.asTpl pk]
        | Kc struct (pk, ValueSome sk) -> [AttributeSelector.asTpl pk; AttributeSelector.asTpl sk]

    let optionalSortKey item = function
        | (Kc struct (_, ValueSome sk)) & kc ->
            match AttributeSelector.test item sk with
            | HasValue _ -> sortKey item kc
            | ExpectedType _ -> ValueNone
            | NoValue -> ValueNone
        | Kc struct (_, ValueNone) -> ValueNone

    let optionalPartitionKey item = function
        | (Kc struct (pk, _)) & kc ->
            match AttributeSelector.test item pk with
            | HasValue _ -> partitionKey item kc |> ValueSome
            | ExpectedType _ -> ValueNone
            | NoValue -> ValueNone

    /// <summary>Remove all attributes from a map except for the key attributes</summary>
    let keyAttributesOnly item keyConfig =
        let first3Count = MapUtils.toSeq item |> Seq.truncate 3 |> Seq.length

        match struct (partitionKey item keyConfig, sortKey item keyConfig) with
        | _, ValueNone when first3Count = 1 -> item
        | _, ValueSome _ when first3Count = 2 -> item
        | pk, ValueNone -> Map.add (partitionKeyName keyConfig) pk Map.empty
        | pk, ValueSome sk ->
            Map.add (partitionKeyName keyConfig) pk Map.empty
            |> Map.add (sortKeyName keyConfig |> Maybe.expectSome) sk

    let asAttributeMap pk sk keyConfig =
        let pk = partitionKeyName keyConfig |> flip tpl pk
        let sk = sortKeyName keyConfig |> flip tpl sk |> uncurry Maybe.tpl

        let map =
            uncurry Map.add pk Map.empty
            |> (
                ValueOption.map (uncurry Map.add) sk
                |> ValueOption.defaultValue id)

        partitionKey map keyConfig |> ignoreTyped<AttributeValue>
        sortKey map keyConfig |> ignoreTyped<AttributeValue voption>
        map
