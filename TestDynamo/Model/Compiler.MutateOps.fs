
/// <summary>
/// Dotnet versions of the operations which can be executed in
/// a dynamodb update or projection expression.
///
/// Operations are liberal and do only the minimum validation required
/// to operate. Dynamodb rules (e.g. "A = A" is not valid) are implemented elsewhere 
/// </summary>
[<RequireQualifiedAccess>]
module TestDynamo.Model.Compiler.MutateOps

open System.Diagnostics.CodeAnalysis
open TestDynamo.Data.Monads
open TestDynamo.Model
open TestDynamo.Model.Compiler
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

module Accessor =

    let private emptyMap = HashMap Map.empty
    let private emptySparseList = SparseList Map.empty
    let private emptySparseListAttr = AttributeList emptySparseList

    let private emptySetterArgs attr =
        match attr with
        | HashMap _ & e -> struct (e, emptyMap) |> ValueSome
        | AttributeList _ & x -> struct (x, emptySparseListAttr) |> ValueSome
        | _ -> ValueNone

    [<ExcludeFromCodeCoverage>] // A lot of this method is defensive for cases in the data model that cannot occur in the code 
    let rec private mutatePath:
        struct (struct (AttributeValue * AttributeValue) * ResolvedAccessorType list) -> AttributeValue voption =

        function

        // convert compressed lists to sparse lists (final)
        | struct (struct (AttributeList _ & f, AttributeList (CompressedList _ & t)), [ResolvedListIndex _] & i) ->
            mutatePath struct (struct (f, AttributeListType.asSparse t |> AttributeList), i)

        // convert compressed lists to sparse lists (path part)
        | (AttributeList _ & f, AttributeList (CompressedList _ & t)), ResolvedListIndex _::_ & i ->
            mutatePath struct (struct (f, AttributeListType.asSparse t |> AttributeList), i)

        // process final path part map
        | (HashMap from, HashMap ``to`` & t), [ResolvedAttribute attr] ->
            MapUtils.tryFind attr from
            ?|> (flip (Map.add attr) ``to`` >> HashMap)

        // process final path part list
        | (AttributeList from, AttributeList ``to`` & t), [ResolvedListIndex i] ->
            AttributeListType.tryFind (int i) from
            ?>>= flip (AttributeListType.set (int i)) ``to``
            ?|> AttributeList

        // process path part map
        | (HashMap from, HashMap ``to`` & t), ResolvedAttribute attr::tail ->

            MapUtils.tryFind attr from
            ?>>= (fun x ->
                MapUtils.tryFind attr ``to``
                ?|> (tpl x >> ValueSome)
                |> ValueOption.defaultWith (fun _ -> emptySetterArgs x))
            ?|> (flip tpl tail)
            ?>>= mutatePath
            ?|> (flip (Map.add attr) ``to`` >> HashMap)

        // process path part list
        | (AttributeList from, AttributeList ``to`` & t), ResolvedListIndex i::tail ->
            AttributeListType.tryFind (int i) from
            ?>>= (fun x ->
                AttributeListType.tryFind (int i) ``to``
                ?|> (tpl x >> ValueSome)
                |> ValueOption.defaultWith (fun _ -> emptySetterArgs x))
            ?|> flip tpl tail
            ?>>= mutatePath
            ?>>= flip (AttributeListType.set (int i)) ``to``
            ?|> AttributeList

        | _ -> ValueNone

    let rec private getValueFromPath = function
        | [] -> sndT >> ValueSome
        | ListIndex i::tail ->
            let next = getValueFromPath tail
            function
                | t, AttributeList from -> AttributeListType.tryFind (int i) from ?|> tpl t ?>>= next
                | _ -> ValueNone
        | Attribute attr::tail
        | RawAttribute attr::tail ->
            let next = getValueFromPath tail
            function
                | t, HashMap from -> MapUtils.tryFind attr from ?|> tpl t ?>>= next
                | _ -> ValueNone
        | ExpressionAttrName name::tail ->
            let next = getValueFromPath tail
            function
                | t: ProjectionTools, HashMap from ->
                    t.filterParams.expressionAttributeNameLookup name
                    ?>>= flip MapUtils.tryFind from
                    ?|> tpl t
                    ?>>= next
                | _ -> ValueNone

    let doNothing = asLazy ValueNone

    let rec private listIndexSetter i = function
        | struct (_, struct (ValueNone, ValueNone)) -> ValueNone
        | t, (value, ValueNone) -> listIndexSetter i (t, (value, ValueSome emptySparseListAttr))
        | _, (ValueSome value, ValueSome (AttributeList state)) ->
            AttributeListType.asSparse state
            |> AttributeListType.set i value
            ?|> AttributeList
        | _, (ValueNone, ValueSome (AttributeList state)) ->
            AttributeListType.asSparse state
            |> AttributeListType.remove i
            |> AttributeList
            |> ValueSome
        | _, (_, ValueSome x) -> ClientError.clientError $"Cannot set list item [{i}] on type {AttributeValue.getType x}"

    let rec private attributeSetter attrName = function
        | struct (_, struct (ValueNone, ValueNone)) -> ValueNone
        | t, (value, ValueNone) -> attributeSetter attrName (t, (value, ValueSome emptyMap))
        | _, (ValueSome value, ValueSome (HashMap state)) -> Map.add attrName value state |> HashMap |> ValueSome
        | _, (ValueNone, ValueSome (HashMap state)) -> Map.remove attrName state |> HashMap |> ValueSome
        | _, (_, ValueSome x) -> ClientError.clientError $"Cannot set attribute \"{attrName}\" on type {AttributeValue.getType x}"

    let private expressionAttributeNameSetter attrName (struct (t: ProjectionTools, _) & input) =
        t.filterParams.expressionAttributeNameLookup attrName
        ?>>= flip attributeSetter input

    let rec midListIndexSetter (struct (i, next) & config) = function
        | struct (t, struct (value, ValueNone)) -> midListIndexSetter config (t, (value, ValueSome emptySparseListAttr))
        | t, (value, ValueSome (AttributeList state & s)) ->
            let state = AttributeListType.asSparse state
            AttributeListType.tryFind i state
            |> tpl value |> tpl t
            |> next
            ?>>= flip (AttributeListType.set i) state
            ?|> AttributeList
        | _, (_, s) -> ValueNone

    let rec private midAttributeSetter (struct (attr, next) & config) = function
        | struct (t, struct (value, ValueNone)) -> midAttributeSetter config (t, (value, ValueSome emptyMap))
        | t, (value, ValueSome (HashMap state & s)) ->
            MapUtils.tryFind attr state
            |> tpl value |> tpl t
            |> next
            ?|> flip (Map.add attr) state
            ?|> HashMap
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

    let private expectHashMap = function
        | HashMap x -> x
        | _ -> serverError "Expected hash map"

    let private throw = Result.throw ClientError.clientError "Error compiling expression\n%s"

    let mutation settings acc =
        let getter' = getValueFromPath acc
        let setter' = setValueFromPath acc

        let getter (t: ProjectionTools) =
            t.item
            |> Item.attributes
            |> HashMap
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

            setter' (t, (value, HashMap state |> ValueSome))
            ?|> expectHashMap
            |> ValueOption.defaultValue state
            |> flip tpl path

        ItemMutation.create getter setter