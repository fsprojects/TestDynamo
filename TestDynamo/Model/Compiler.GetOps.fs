
/// <summary>
/// Dotnet versions of the operations which can be executed in
/// a dynamodb filter expression.
///
/// Operations are liberal and do only the minimum validation required
/// to operate. Dynamodb rules (e.g. "A = A" is not valid) are implemented elsewhere
/// </summary>
[<RequireQualifiedAccess>]
module TestDynamo.Model.Compiler.GetOps

open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

type ItemData = Compiler.ItemData

// aka $

let inline private boolResult x = x |> Boolean |> ValueSome
let inline private decimalResult x = x |> Number |> ValueSome
let private intResult x = x |> decimal |> decimalResult
let private falseResult = boolResult false
let private trueResult = boolResult true

let rootItem: ItemData -> AttributeValue voption = _.item >> Item.attributes >> HashMap >> ValueSome

let attribute struct (expression, name) (itemData: ItemData) =
    let expression = ValueOption.defaultWith (fun _ _ -> rootItem itemData) expression
    ValueSome tpl
    <|? expression itemData
    <|? ValueSome name
    ?>>= function
        | struct (HashMap x, name) -> MapUtils.tryFind name x
        | _ -> ValueNone

let expressionAttrName struct (item, name) (itemData: ItemData) =
    itemData.filterParams.expressionAttributeNameLookup name
    ?|> tpl item
    ?>>= flip attribute itemData

let listIndex struct (expression, index) (itemData: ItemData) =
    let expression = ValueOption.defaultWith (fun _ _ -> rootItem itemData) expression
    ValueSome tpl
    <|? expression itemData
    <|? (ValueSome index)
    ?>>= function
        | struct (AttributeList (CompressedList x), index) when index >= 0 && Array.length x > index -> Array.get x index |> ValueSome
        | struct (AttributeList (SparseList x & sl), index) when index >= 0 && Map.count x > index -> AttributeListType.asSeq sl |> Seq.skip index |> Collection.tryHead
        | _ -> ValueNone

let inline expressionAttrValue name (itemData: ItemData) =
    itemData.filterParams.expressionAttributeValueLookup name

let private compareUncurried = uncurry AttributeValue.compare
let private op' operator struct (left, right) itemData =
    ValueSome tpl
    <|? left itemData
    <|? right itemData
    ?>>= compareUncurried
    ?|> operator

let private op operator leftRight itemData =
    op' operator leftRight itemData ?|> Boolean

type private BinaryOp = (struct ((ItemData -> AttributeValue voption) * (ItemData -> AttributeValue voption))) -> ItemData -> AttributeValue voption
let eq: BinaryOp = op ((=)0)

let private neq': BinaryOp = op ((<>)0)
let lt: BinaryOp = op (flip (<)0)
let lte: BinaryOp = op (flip (<=)0)
let gt: BinaryOp = op (flip (>)0)
let gte: BinaryOp = op (flip (>=)0)

// Special case for not equals. A None compared to a non None returns false, instead of None
let neq: BinaryOp = fun struct (left, right) itemData ->
    let l = left itemData
    let r = right itemData

    match struct (l, r) with
    | ValueNone, ValueSome _ -> trueResult
    | ValueSome _, ValueNone -> trueResult
    | _ -> neq' struct (left, right) itemData

let private lte' = op' (flip (<=)0)
let between struct (item, low, high) itemData =
    lte' struct (low, item) itemData
    ?>>= function
        | false -> falseResult
        | true -> lte struct (item, high) itemData

let not' = function
    | Boolean x -> not x |> boolResult
    | _ -> ValueNone

let private sysNot = not
let inline not item = item ?>=> not'

let ``and`` struct (left, right) itemData =
    left itemData
    ?>>= function
        | Boolean true -> right itemData
        | Boolean false -> falseResult
        | _ -> ValueNone

let ``or`` struct (left, right) itemData =
    match left itemData with
    | ValueSome (Boolean true) -> trueResult
    | ValueNone
    | ValueSome (Boolean false) -> right itemData
    | ValueSome _ -> ValueNone

let toList values =
    apply
    >> flip Seq.map values
    >> Maybe.traverse
    >> Array.ofSeq
    >> CompressedList
    >> AttributeList
    >> ValueSome

let inline private (>>>) f g x y = f x y |> g
let private listContainsEq: AttributeValue seq -> AttributeValue -> bool voption =
    let rec test' (items: IEnumerator<AttributeValue -> bool voption>) defaultBuilder value =
        if items.MoveNext() |> sysNot
        then
            match defaultBuilder with
            | struct (true, _)
            | false, false -> ValueSome false
            | _ -> ValueNone
        else
            match items.Current value with
            | ValueSome true & t -> t
            | ValueSome false -> test' items struct (true, sndT defaultBuilder) value
            | ValueNone -> test' items struct (fstT defaultBuilder, true) value

    let test (items: (AttributeValue -> bool voption) seq) =
        use enm = items.GetEnumerator()
        test' enm struct (false, false)

    uncurry AttributeValue.compare
    ??|> ((=) 0)
    |> curry
    |> Seq.map
    >> test

let listContains struct (test, valueList) itemData =

    valueList itemData
    ?>>= function | AttributeList xs -> ValueSome xs | _ -> ValueNone
    ?|> AttributeListType.asSeq
    ?|> listContainsEq
    ?|> tpl
    <|? test itemData
    ?>>= applyTpl
    ?|> Boolean

module HashSets =
    let private arithmetic operation args itemData =
        match args |> mapFst (apply itemData) |> mapSnd (apply itemData) with
        | ValueSome (AttributeValue.Number x), ValueSome (AttributeValue.Number y) ->
            ValueSome (AttributeValue.Number (operation x y))
        | _ -> ValueNone

    let union args itemData =
        match args |> mapFst (apply itemData) |> mapSnd (apply itemData) with
        | ValueSome (AttributeValue.HashSet l), ValueSome (AttributeValue.HashSet r) ->
            AttributeSet.tryUnion struct (l, r)
            ?|> AttributeValue.HashSet
        | _ -> ValueNone

    let xOr args itemData =
        match args |> mapFst (apply itemData) |> mapSnd (apply itemData) with
        | ValueSome (AttributeValue.HashSet l), ValueSome (AttributeValue.HashSet r) ->
            AttributeSet.tryXOr struct (l, r)
            ?|> (ValueOption.map AttributeValue.HashSet)
        | _ -> ValueNone

module Arithmetic =
    let private arithmetic operation args itemData =
        match args |> mapFst (apply itemData) |> mapSnd (apply itemData) with
        | ValueSome (AttributeValue.Number x), ValueSome (AttributeValue.Number y) ->
            ValueSome (AttributeValue.Number (operation x y))
        | _ -> ValueNone

    let add args = arithmetic (+) args
    let subtract args = arithmetic (-) args

module Functions =

    let if_not_exists struct (l, r) itemData =
        l itemData
        ?|> ValueSome
        |> ValueOption.defaultWith (fun _ -> r itemData)

    let list_append lr itemData =
        match lr |> mapFst (apply itemData) |> mapSnd (apply itemData) with
        | ValueNone, ValueNone -> ValueNone
        | ValueSome (AttributeList _ & x), ValueNone
        | ValueNone, ValueSome (AttributeList _ & x) -> ValueSome x
        | ValueSome (AttributeList l), ValueSome (AttributeList r) ->
            AttributeListType.append struct(l, r) |> AttributeList |> ValueSome
        | _ -> ValueNone

    // requires validation: if r is expr attr value, the type must be a binary or string
    // requires validation: l and r cannot be the same attr
    let begins_with struct (l, r) itemData =
        ValueOption.Some tpl
        <|? l itemData
        <|? r itemData
        ?>>= function
            // not 100% sure if this is a valid operation or not
            | struct (AttributeValue.Binary x, AttributeValue.Binary y) ->
                (x.Length >= y.Length && Comparison.compareArrays 0 (Array.length y - 1) x y)
                |> boolResult
            | struct (AttributeValue.String x, AttributeValue.String y) ->
                x.StartsWith(y)
                |> boolResult
            | _ -> ValueNone

    let private attribute_exists' attr =
        attr >> ValueOption.isSome

    // requires validation: input must be an attribute or alias
    let attribute_exists attr =
        attribute_exists' attr
        >> boolResult

    let attribute_not_exists attr =
        attribute_exists' attr
        >> sysNot
        >> boolResult

    let attribute_type struct (attr, typ) itemData =

        let testType =
            typ itemData
            ?>>= function
                | String x -> AttributeType.tryParse x
                | _ -> ValueNone
            ?|> (=)
            |> Maybe.traverseFn

        attr itemData
        ?|> AttributeValue.getType
        ?>>= testType
        ?|> Boolean

    let contains struct (l, r) itemData =
        ValueOption.Some tpl
        <|? l itemData
        <|? r itemData
        ?>>= function
            | struct (AttributeValue.String path, AttributeValue.String operand) ->
                path.Contains operand |> boolResult
            | struct (AttributeValue.HashSet set, AttributeValue.String _ & operand)
               when AttributeSet.getSetType set = AttributeType.String ->
                   AttributeSet.contains operand set |> boolResult
            | struct (AttributeValue.HashSet set, AttributeValue.Number _ & operand)
               when AttributeSet.getSetType set = AttributeType.Number ->
                   AttributeSet.contains operand set |> boolResult
            | struct (AttributeValue.HashSet set, AttributeValue.Binary _ & operand)
               when AttributeSet.getSetType set = AttributeType.Binary ->
                   AttributeSet.contains operand set |> boolResult
            | struct (AttributeValue.AttributeList (CompressedList list), operand) ->
                   Array.contains operand list |> boolResult
            | struct (AttributeValue.AttributeList (SparseList list), operand) ->
                   Seq.contains operand list.Values |> boolResult
            | _ -> ValueNone

    let size item =
        item
        ?>=> function
            | AttributeValue.String str -> str.Length |> intResult
            | AttributeValue.Binary bin -> bin.Length |> intResult
            | AttributeValue.HashMap h -> Map.count h |> intResult
            | AttributeValue.AttributeList l -> AttributeListType.length l |> intResult
            | AttributeValue.HashSet s -> AttributeSet.asSet s |> Set.count |> intResult
            | _ -> ValueNone
