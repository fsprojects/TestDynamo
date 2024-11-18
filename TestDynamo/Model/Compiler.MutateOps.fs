
/// <summary>
/// Dotnet versions of the operations which can be executed in
/// a dynamodb update or projection expression.
///
/// Operations are liberal and do only the minimum validation required
/// to operate. Dynamodb rules (e.g. "A = A" is not valid) are implemented elsewhere 
/// </summary>
[<RequireQualifiedAccess>]
module TestDynamo.Model.Compiler.MutateOps

open TestDynamo.Data.Monads
open TestDynamo.Model
open TestDynamo.Model.Compiler
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

module Accessor =

    let private emptyMap = HashMapX Map.empty |> AttributeValue.create
    let private emptySparseList = SparseList Map.empty
    let private emptySparseListAttr = AttributeListX emptySparseList |> AttributeValue.create

    let private emptySetterArgs attr =
        match attr |> AttributeValue.value with
        | HashMapX _ & e -> struct (e |> AttributeValue.create, emptyMap) |> ValueSome
        | AttributeListX _ & x -> struct (x |> AttributeValue.create, emptySparseListAttr) |> ValueSome
        | _ -> ValueNone

    let toWriter = function
        | struct (ValueNone, x) -> Writer.retn x
        | struct (ValueSome emit, x) -> Writer.create emit x

    let rec private mutatePath (attrs: struct (struct (AttributeValue * AttributeValue) * ResolvedAccessorType list)): AttributeValue voption =

        let attrs' =
            mapFst (
                mapFst AttributeValue.value
                >> mapSnd AttributeValue.value) attrs
        
        match attrs' with

        // convert compressed lists to sparse lists (final)
        | struct (struct (AttributeListX _ & f, AttributeListX (CompressedList _ & t)), [ResolvedListIndex _] & i) ->
            mutatePath struct (struct (f |> AttributeValue.create, AttributeListType.asSparse t |> AttributeListX |> AttributeValue.create), i)

        // convert compressed lists to sparse lists (path part)
        | (AttributeListX _ & f, AttributeListX (CompressedList _ & t)), ResolvedListIndex _::_ & i ->
            mutatePath struct (struct (f |> AttributeValue.create, AttributeListType.asSparse t |> AttributeListX |> AttributeValue.create), i)

        // process final path part map
        | (HashMapX from, HashMapX ``to`` & t), [ResolvedAttribute attr] ->
            MapUtils.tryFind attr from
            ?|> (flip (Map.add attr) ``to`` >> HashMapX >> AttributeValue.create)

        // process final path part list
        | (AttributeListX from, AttributeListX ``to`` & t), [ResolvedListIndex i] ->
            AttributeListType.tryFind (int i) from
            ?>>= flip (AttributeListType.set (int i)) ``to``
            ?|> (AttributeListX >> AttributeValue.create)

        // process path part map
        | (HashMapX from, HashMapX ``to`` & t), ResolvedAttribute attr::tail ->

            MapUtils.tryFind attr from
            ?>>= (fun x ->
                MapUtils.tryFind attr ``to``
                ?|> (tpl x >> ValueSome)
                |> ValueOption.defaultWith (fun _ -> emptySetterArgs x))
            ?|> (flip tpl tail)
            ?>>= mutatePath
            ?|> (flip (Map.add attr) ``to`` >> HashMapX >> AttributeValue.create)

        // process path part list
        | (AttributeListX from, AttributeListX ``to`` & t), ResolvedListIndex i::tail ->
            AttributeListType.tryFind (int i) from
            ?>>= (fun x ->
                AttributeListType.tryFind (int i) ``to``
                ?|> (tpl x >> ValueSome)
                |> ValueOption.defaultWith (fun _ -> emptySetterArgs x))
            ?|> (flip tpl tail)
            ?>>= mutatePath
            ?>>= flip (AttributeListType.set (int i)) ``to``
            ?|> (AttributeListX >> AttributeValue.create)

        | _ -> ValueNone

    let rec private getValueFromPath = function
        | [] -> sndT >> ValueSome
        | ListIndex i::tail ->
            let next = getValueFromPath tail
            mapSnd AttributeValue.value >> function
                | t, AttributeListX from -> AttributeListType.tryFind (int i) from ?|> tpl t ?>>= next
                | _ -> ValueNone
        | Attribute attr::tail
        | RawAttribute attr::tail ->
            let next = getValueFromPath tail
            mapSnd AttributeValue.value >> function
                | t, HashMapX from -> MapUtils.tryFind attr from ?|> tpl t ?>>= next
                | _ -> ValueNone
        | ExpressionAttrName name::tail ->
            let next = getValueFromPath tail
            mapSnd AttributeValue.value >> function
                | t: ProjectionTools, HashMapX from ->
                    t.filterParams.expressionAttributeNameLookup name
                    ?>>= flip MapUtils.tryFind from
                    ?|> tpl t
                    ?>>= next
                | _ -> ValueNone

    let doNothing = asLazy ValueNone

    let rec private listIndexSetter i x =
        match x |> mapSnd (mapSnd (ValueOption.map AttributeValue.value)) with
        | struct (_, struct (ValueNone, ValueNone)) -> ValueNone
        | t, (value, ValueNone) -> listIndexSetter i (t, (value, ValueSome emptySparseListAttr))
        | _, (ValueSome value, ValueSome (AttributeListX state)) ->
            AttributeListType.asSparse state
            |> AttributeListType.set i value
            ?|> (AttributeListX >> AttributeValue.create)
        | _, (ValueNone, ValueSome (AttributeListX state)) ->
            AttributeListType.asSparse state
            |> AttributeListType.remove i
            |> AttributeListX
            |> AttributeValue.create
            |> ValueSome
        | _, (_, ValueSome x) -> clientError $"Cannot set list item [{i}] on type {x |> AttributeValue.create |> AttributeValue.getType}"

    let rec private attributeSetter attrName x =
        match x |> mapSnd (mapSnd (ValueOption.map AttributeValue.value)) with
        | struct (_, struct (ValueNone, ValueNone)) -> ValueNone
        | t, (value, ValueNone) -> attributeSetter attrName (t, (value, ValueSome emptyMap))
        | _, (ValueSome value, ValueSome (HashMapX state)) -> Map.add attrName value state |> HashMapX |> AttributeValue.create |> ValueSome
        | _, (ValueNone, ValueSome (HashMapX state)) -> Map.remove attrName state |> HashMapX |> AttributeValue.create |> ValueSome
        | _, (_, ValueSome x) -> clientError $"Cannot set attribute \"{attrName}\" on type {x |> AttributeValue.create |> AttributeValue.getType}"

    let private expressionAttributeNameSetter attrName (struct (t: ProjectionTools, _) & input) =
        t.filterParams.expressionAttributeNameLookup attrName
        ?>>= flip attributeSetter input

    let rec midListIndexSetter (struct (i, next) & config) x =
        match x |> mapSnd (mapSnd (ValueOption.map AttributeValue.value)) with
        | struct (t, struct (value, ValueNone)) -> midListIndexSetter config (t, (value, ValueSome emptySparseListAttr))
        | t, (value, ValueSome (AttributeListX state & s)) ->
            let state = AttributeListType.asSparse state
            AttributeListType.tryFind i state
            |> tpl value |> tpl t
            |> next
            ?>>= flip (AttributeListType.set i) state
            ?|> (AttributeListX >> AttributeValue.create)
        | _, (_, s) -> ValueNone

    let rec private midAttributeSetter (struct (attr, next) & config) x =
        match x |> mapSnd (mapSnd (ValueOption.map AttributeValue.value)) with
        | struct (t, struct (value, ValueNone)) -> midAttributeSetter config (t, (value, ValueSome emptyMap))
        | t, (value, ValueSome (HashMapX state & s)) ->
            MapUtils.tryFind attr state
            |> tpl value |> tpl t
            |> next
            ?|> flip (Map.add attr) state
            ?|> (HashMapX >> AttributeValue.create)
        | _, (_, s) -> ValueNone

    let private midExpressionAttributeNameSetter struct (attrName, next) (struct (t: ProjectionTools, _) & input) =
        t.filterParams.expressionAttributeNameLookup attrName
        ?|> flip tpl next
        ?>>= flip midAttributeSetter input

    let rec private setValueFromPath =

        function
        | [] -> doNothing

        | [RawAttribute attr]
        | [Attribute attr] -> attributeSetter attr
        | [ListIndex i] -> listIndexSetter (int i)
        | [ExpressionAttrName attrName] -> expressionAttributeNameSetter attrName

        | RawAttribute attr::tail
        | Attribute attr::tail -> setValueFromPath tail |> tpl attr |> midAttributeSetter
        | ListIndex i::tail -> setValueFromPath tail |> tpl (int i) |> midListIndexSetter
        | ExpressionAttrName attrName::tail -> setValueFromPath tail |> tpl attrName |> midExpressionAttributeNameSetter

    let private expectHashMap =
        AttributeValue.value >> function
        | HashMapX x -> x
        | _ -> serverError "Expected hash map"

    let private throw = Result.throw "Error compiling expression\n%s"

    let mutation settings acc =
        let getter' = getValueFromPath acc
        let setter' = setValueFromPath acc

        let getter (t: ProjectionTools) =
            t.item
            |> Item.attributes
            |> HashMapX
            |> AttributeValue.create
            |> tpl t
            |> getter'

        let validatePath =
            Validator.validatePath settings
            >> MaybeLazyResult.map List.singleton

        let setter struct (t, value) state =
            let path =
                validatePath acc
                |> MaybeLazyResult.execute t.filterParams
                |> throw

            setter' (t, (value, HashMapX state |> AttributeValue.create |> ValueSome))
            ?|> expectHashMap
            |> ValueOption.defaultValue state
            |> flip tpl path

        ItemMutation.create getter setter