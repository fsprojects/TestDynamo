
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

let inline private boolResult x = x |> AttributeValue.createBoolean |> ValueSome
let inline private decimalResult x = x |> AttributeValue.createNumber |> ValueSome
let private intResult x = x |> decimal |> decimalResult
let private falseResult = boolResult false
let private trueResult = boolResult true

let rootItem: ItemData -> AttributeValue voption = _.item >> Item.attributes >> AttributeValue.createHashMap >> ValueSome

let attribute struct (expression, name) (itemData: ItemData) =
    let expression = ValueOption.defaultWith (fun _ _ -> rootItem itemData) expression
    ValueSome tpl
    <|? expression itemData
    <|? ValueSome name
    ?>>= fun attr ->
        match mapFst AttributeValue.value attr with
        | struct (HashMapX x, name) -> MapUtils.tryFind name x
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
    ?>>= fun attr ->
        match mapFst AttributeValue.value attr with
        | struct (AttributeListX (CompressedList x), index) when index >= 0 && Array.length x > index -> Array.get x index |> ValueSome
        | struct (AttributeListX (SparseList x & sl), index) when index >= 0 && Map.count x > index -> AttributeListType.asSeq sl |> Seq.skip index |> Collection.tryHead
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
    op' operator leftRight itemData ?|> (AttributeValue.createBoolean)

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

let not' attr = 
    match AttributeValue.value attr with
    | BooleanX x -> not x |> boolResult
    | _ -> ValueNone

let private sysNot = not
let inline not item = item ?>=> not'

let ``and`` struct (left, right) itemData =
    left itemData
    ?>>= fun attr ->
        match AttributeValue.value attr with
        | BooleanX true -> right itemData
        | BooleanX false -> falseResult
        | _ -> ValueNone

let ``or`` struct (left, right) itemData =
    match left itemData ?|> AttributeValue.value with
    | ValueSome (BooleanX true) -> trueResult
    | ValueNone
    | ValueSome (BooleanX false) -> right itemData
    | ValueSome _ -> ValueNone

let toList values =
    apply
    >> flip Seq.map values
    >> Maybe.traverse
    >> Array.ofSeq
    >> CompressedList
    >> AttributeValue.createAttributeList
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
    ?>>= fun attr -> match AttributeValue.value attr with | AttributeListX xs -> ValueSome xs | _ -> ValueNone
    ?|> AttributeListType.asSeq
    ?|> listContainsEq
    ?|> tpl
    <|? test itemData
    ?>>= applyTpl
    ?|> (AttributeValue.createBoolean)

module HashSets =
    // let private arithmetic operation args itemData =
    //     match args |> mapFst (apply itemData >> AttributeValue.value) |> mapSnd (apply itemData >> AttributeValue.value) with
    //     | ValueSome (WorkingAttributeValue.NumberX x), ValueSome (WorkingAttributeValue.NumberX y) ->
    //         ValueSome (AttributeValue.Number (operation x y))
    //     | _ -> ValueNone

    let union args itemData =
        match args |> mapFst (apply itemData ??|> AttributeValue.value) |> mapSnd (apply itemData ??|> AttributeValue.value) with
        | ValueSome (WorkingAttributeValue.HashSetX l), ValueSome (WorkingAttributeValue.HashSetX r) ->
            AttributeSet.tryUnion struct (l, r)
            ?|> (AttributeValue.createHashSet)
        | _ -> ValueNone

    let xOr args itemData =
        match args |> mapFst (apply itemData ??|> AttributeValue.value) |> mapSnd (apply itemData ??|> AttributeValue.value) with
        | ValueSome (WorkingAttributeValue.HashSetX l), ValueSome (WorkingAttributeValue.HashSetX r) ->
            AttributeSet.tryXOr struct (l, r)
            ?|> (ValueOption.map (AttributeValue.createHashSet))
        | _ -> ValueNone

module Arithmetic =
    let private arithmetic operation args itemData =
        match args |> mapFst (apply itemData ??|> AttributeValue.value) |> mapSnd (apply itemData ??|> AttributeValue.value) with
        | ValueSome (WorkingAttributeValue.NumberX x), ValueSome (WorkingAttributeValue.NumberX y) ->
            ValueSome (AttributeValue.createNumber (operation x y))
        | _ -> ValueNone

    let add args = arithmetic (+) args
    let subtract args = arithmetic (-) args

module Functions =

    let if_not_exists struct (l, r) itemData =
        l itemData
        ?|> ValueSome
        |> ValueOption.defaultWith (fun _ -> r itemData)

    let list_append lr itemData =
        match lr |> mapFst (apply itemData ??|> AttributeValue.value) |> mapSnd (apply itemData ??|> AttributeValue.value) with
        | ValueNone, ValueNone -> ValueNone
        | ValueSome (AttributeListX _ & x), ValueNone
        | ValueNone, ValueSome (AttributeListX _ & x) -> ValueSome (AttributeValue.create x)
        | ValueSome (AttributeListX l), ValueSome (AttributeListX r) ->
            AttributeListType.append struct(l, r) |> AttributeValue.createAttributeList |> ValueSome
        | _ -> ValueNone

    // requires validation: if r is expr attr value, the type must be a binary or string
    // requires validation: l and r cannot be the same attr
    let begins_with struct (l, r) itemData =
        ValueOption.Some tpl
        <|? l itemData
        <|? r itemData
        ?>>= fun attrs ->
            match attrs |> mapFst AttributeValue.value |> mapSnd AttributeValue.value with
            // not 100% sure if this is a valid operation or not
            | struct (WorkingAttributeValue.BinaryX x, WorkingAttributeValue.BinaryX y) ->
                (x.Length >= y.Length && Comparison.compareArrays 0 (Array.length y - 1) x y)
                |> boolResult
            | struct (WorkingAttributeValue.StringX x, WorkingAttributeValue.StringX y) ->
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
            ?>>= fun attr ->
                match AttributeValue.value attr with
                | StringX x -> AttributeType.tryParse x
                | _ -> ValueNone
            ?|> (=)
            |> Maybe.traverseFn

        attr itemData
        ?|> AttributeValue.getType
        ?>>= testType
        ?|> (AttributeValue.createBoolean) 

    let contains struct (l, r) itemData =
        ValueOption.Some tpl
        <|? l itemData
        <|? r itemData
        ?>>= fun attrs ->
            let operand' = sndT attrs
            match attrs |> mapFst AttributeValue.value |> mapSnd AttributeValue.value with
            | struct (WorkingAttributeValue.StringX path, WorkingAttributeValue.StringX operand) ->
                path.Contains operand |> boolResult
            | struct (WorkingAttributeValue.HashSetX set, WorkingAttributeValue.StringX _ & operand)
               when AttributeSet.getSetType set = AttributeType.String ->
                   AttributeSet.contains operand' set |> boolResult
            | struct (WorkingAttributeValue.HashSetX set, WorkingAttributeValue.NumberX _ & operand)
               when AttributeSet.getSetType set = AttributeType.Number ->
                   AttributeSet.contains operand' set |> boolResult
            | struct (WorkingAttributeValue.HashSetX set, WorkingAttributeValue.BinaryX _ & operand)
               when AttributeSet.getSetType set = AttributeType.Binary ->
                   AttributeSet.contains operand' set |> boolResult
            | struct (WorkingAttributeValue.AttributeListX (CompressedList list), operand) ->
                   Array.contains operand' list |> boolResult
            | struct (WorkingAttributeValue.AttributeListX (SparseList list), operand) ->
                   Seq.contains operand' list.Values |> boolResult
            | _ -> ValueNone

    let size item =
        item
        ?>=> (AttributeValue.value >> function
                | WorkingAttributeValue.StringX str -> str.Length |> intResult
                | WorkingAttributeValue.BinaryX bin -> bin.Length |> intResult
                | WorkingAttributeValue.HashMapX h -> Map.count h |> intResult
                | WorkingAttributeValue.AttributeListX l -> AttributeListType.length l |> intResult
                | WorkingAttributeValue.HashSetX s -> AttributeSet.asSet s |> Set.count |> intResult
                | _ -> ValueNone)
