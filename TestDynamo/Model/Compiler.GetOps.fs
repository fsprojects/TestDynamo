
/// <summary>
/// Dotnet versions of the operations which can be executed in
/// a dynamodb filter expression.
///
/// Operations are liberal and do only the minimum validation required
/// to operate. Dynamodb rules (e.g. "A = A" is not valid) are implemented elsewhere
/// </summary>
[<RequireQualifiedAccess>]
module TestDynamo.Model.Compiler.GetOps

open TestDynamo.Model.Compiler.ExpressionFnXOps
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

type ItemData = Compiler.ItemData

let private boolFn = Boolean >> ExpressionFnX.retn
let private boolResult = Boolean >> ExpressionFnX.result
let private decimalResult = Number >> ExpressionFnX.result
let private intFn = decimal >> decimalResult >> asLazy
let private intResult = decimal >> decimalResult
let private falseFn = false |> Boolean |> ExpressionFnX.retn
let private falseResult = false |> Boolean |> ExpressionFnX.result
let private trueFn = true |> Boolean |> ExpressionFnX.retn
let private trueResult = true |> Boolean |> ExpressionFnX.result

let rootItem: ExpressionFnX = ExpressionFnX.create (_.item >> Item.attributes >> HashMap)
let defaultRoot = ExpressionFnX.defaultFn rootItem

let private attributeTpl = ExpressionFnX.retn tpl 
let attribute struct (expression: ExpressionFnX voption, name): ExpressionFnX =
    attributeTpl
    <|?& ValueOption.defaultValue rootItem expression
    <|?& ExpressionFnX.retn name
    &?>>= function
        | struct (HashMap x, name) -> MapUtils.tryFind name x |> ExpressionFnX.retnOk
        | _ -> ExpressionFnX.noneFn

let expressionAttrName struct (item: ExpressionFnX voption, name): ExpressionFnX =
    ExpressionFnX.createOk (fun x -> x.filterParams.expressionAttributeNameLookup name)
    &?|> tpl item
    &?>>= attribute

let private listIndexTpl = ExpressionFnX.retn tpl
let listIndex struct (expression: ExpressionFnX voption, index): ExpressionFnX =
    
    listIndexTpl
    <|?& ValueOption.defaultValue rootItem expression
    <|?& ExpressionFnX.retn index
    &?>>= function
        | struct (AttributeList (CompressedList x), index) when index >= 0 && Array.length x > index ->
            Array.get x index |> ExpressionFnX.retn
        | struct (AttributeList (SparseList x & sl), index) when index >= 0 && Map.count x > index ->
            AttributeListType.asSeq sl |> Seq.skip index |> Collection.tryHead |> ExpressionFnX.retnOk
        | _ -> ExpressionFnX.noneFn

let inline expressionAttrValue name =
    ExpressionFnX.createOk (fun x -> x.filterParams.expressionAttributeValueLookup name)

let private opTpl = ExpressionFnX.retn tpl
let private compareUncurried = uncurry AttributeValue.compare >> ExpressionFnX.retnOk
let private op' operator struct (left: ExpressionFnX, right: ExpressionFnX): ExpressionFnX<bool> =
    opTpl
    <|?& left
    <|?& right
    &?>>= compareUncurried
    &?|> operator

let private op (operator: int -> bool) leftRight =
    op' operator leftRight &?|> Boolean

type private BinaryOp = (struct (ExpressionFnX * ExpressionFnX)) -> ExpressionFnX
let eq: BinaryOp = op ((=)0)

let private neq': BinaryOp = op ((<>)0)
let lt: BinaryOp = op (flip (<)0)
let lte: BinaryOp = op (flip (<=)0)
let gt: BinaryOp = op (flip (>)0)
let gte: BinaryOp = op (flip (>=)0)

// Special case for not equals. A None compared to a non None returns false, instead of None
let neq struct (left, right) itemData =

    match struct (left itemData, right itemData) with
    | Ok (ValueSome _), Ok ValueNone
    | Ok ValueNone, Ok (ValueSome _) -> trueResult
    | l', r' -> neq' struct (left, right) itemData

let private lte' = op' (flip (<=)0)
let between struct (item, low, high) =
    lte' struct (low, item)
    &?>>= function
        | false -> falseFn
        | true -> lte struct (item, high)

let private sysNot = not
let not: ExpressionFnX -> ExpressionFnX =
    ExpressionFnX.bind (function
        | Boolean true -> falseFn
        | Boolean false -> trueFn
        | _ -> ExpressionFnX.noneFn)

let ``and`` struct (left: ExpressionFnX, right: ExpressionFnX): ExpressionFnX =
    left
    &?>>= function
        | Boolean true -> right
        | Boolean false -> falseFn
        | _ -> ExpressionFnX.noneFn

let ``or`` struct (left: ExpressionFnX, right: ExpressionFnX): ExpressionFnX =
    ExpressionFnX.defaultResult falseResult left
    &?>>= function
        | Boolean true -> trueFn
        | Boolean false -> right
        | _ -> ExpressionFnX.noneFn

let toList: ExpressionFnX seq -> ExpressionFnX =
    ExpressionFnX.traverse
    &??|> (Array.ofSeq >> CompressedList >> AttributeList)

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

let private listOnly = function | AttributeList xs -> ExpressionFnX.retn xs | _ -> ExpressionFnX.noneFn
let private applyAttr = fun struct (f, x) -> f x |> ExpressionFnX.retnOk
let listContains struct (test: ExpressionFnX, valueList: ExpressionFnX): ExpressionFnX =

    valueList
    &?>>= listOnly
    &?|> AttributeListType.asSeq
    &?|> listContainsEq
    &?|> tpl
    <|?& test
    &?>>= applyAttr 
    &?|> Boolean

module HashSets =
    
    let private notSetErr = ExpressionFnX.errFn "Both operands of a this operation must be sets"    // TODO: verify if this is used for set ADD
    let private typeErr = ExpressionFnX.errFn "Both operands of a this operation must have the same set type"    // TODO: verify this message
        
    let private hashTpl = ExpressionFnX.retn tpl
    let private union': _ -> ExpressionFnX = AttributeSet.tryUnion ??|> AttributeValue.HashSet >> ExpressionFnX.retnOk
    let private xOr': _ -> ExpressionFnX<AttributeValue voption> =
        AttributeSet.tryXOr
        >> function
            | ValueNone -> typeErr
            | ValueSome x -> x ?|> AttributeValue.HashSet |> ExpressionFnX.retn
            
    let private ensureHashSet = ExpressionFnX.bind (function | AttributeValue.HashSet x -> ExpressionFnX.retn x | _ -> notSetErr)
    
    let union struct (l: ExpressionFnX, r: ExpressionFnX): ExpressionFnX =
        hashTpl
        <|?& ensureHashSet l
        <|?& ensureHashSet r
        &?>>= union'

    /// <summary>ValueNone signals that there are no elements in the set after xor</summary>
    let xOr struct (l: ExpressionFnX, r: ExpressionFnX): ExpressionFnX<AttributeValue voption> =
        hashTpl
        <|?& ensureHashSet l
        <|?& ensureHashSet r
        &?>>= xOr'

module Arithmetic =
        
    let private arithmeticTpl = ExpressionFnX.retn tpl
    let private err = ExpressionFnX.errFn "Both operands of an arithmetic operation must be numbers"    // TODO: verify if this is used for set ADD
    let private ensureDecimal =
        ExpressionFnX.bind (function | AttributeValue.Number x -> ExpressionFnX.retn x | _ -> err)
        >> ExpressionFnX.defaultFn err
    let private arithmetic operation struct (l: ExpressionFnX, r: ExpressionFnX): ExpressionFnX =
        arithmeticTpl
        <|?& ensureDecimal l
        <|?& ensureDecimal r
        &?|> uncurry operation
        &?|> AttributeValue.Number

    let add = arithmetic (+)
    let subtract = arithmetic (-)

module Functions =
        
    let if_not_exists: _ -> ExpressionFnX = ExpressionFnX.defaultFn |> flip |> uncurry
        
    let private interpretListResult =
        ExpressionFnX.bind (function
            | AttributeList xs -> Either1 xs |> ExpressionFnX.retn
            | _ -> ExpressionFnX.errFn "Arguments to list_append function must be lists")
        >> ExpressionFnX.defaultValue (Either2 ())
        
    let list_append =
        mapFst interpretListResult
        >> mapSnd interpretListResult
        >> ExpressionFnX.tpl
        &?>=> function
            | struct (Either1 x, Either1 y) ->
                AttributeListType.append struct(x, y)
                |> AttributeList
                |> ExpressionFnX.retn
            | struct (_, Either1 x)
            | struct (Either1 x, _) ->
                AttributeList x
                |> ExpressionFnX.retn
            | _ -> ExpressionFnX.noneFn

    // TODO
    // requires validation: if r is expr attr value, the type must be a binary or string
    // requires validation: l and r cannot be the same attr
    let begins_with =
        ExpressionFnX.tpl
        &?>=> function
            // not 100% sure if this is a valid operation or not
            | struct (AttributeValue.Binary x, AttributeValue.Binary y) ->
                (x.Length >= y.Length && Comparison.compareArrays 0 (Array.length y - 1) x y)
                |> boolFn
            | struct (AttributeValue.String x, AttributeValue.String y) ->
                x.StartsWith(y)
                |> boolFn
            | _ -> ExpressionFnX.noneFn
            
    let private attribute_exists': ExpressionFnX -> ExpressionFnX<bool> =
        ExpressionFnX.map (fun _ -> true)
        >> ExpressionFnX.defaultValue false

    // TODO
    // requires validation: input must be an attribute or alias
    let attribute_exists: ExpressionFnX -> ExpressionFnX =
        attribute_exists'
        &??|> Boolean

    // TODO
    // requires validation: input must be an attribute or alias
    let attribute_not_exists: ExpressionFnX -> ExpressionFnX =
        attribute_exists'
        &??|> (sysNot >> Boolean)

    // l is the attribute, r is the required type
    let attribute_type: struct (ExpressionFnX * ExpressionFnX) -> ExpressionFnX =
        
        let parseType =
            ExpressionFnX.bind (function
                | String x -> AttributeType.tryParse x |> ExpressionFnX.retnOk 
                | _ -> ExpressionFnX.noneFn)
        let attrType =
            ExpressionFnX.map AttributeValue.getType
        
        mapFst attrType
        >> mapSnd parseType
        >> ExpressionFnX.tpl
        &??|> (uncurry (=) >> Boolean)
            
    let contains: struct (ExpressionFnX * ExpressionFnX) -> ExpressionFnX =
        ExpressionFnX.tpl
        &?>=> function
            | struct (AttributeValue.String path, AttributeValue.String operand) ->
                path.Contains operand |> boolFn
            | struct (AttributeValue.HashSet set, AttributeValue.String _ & operand)
               when AttributeSet.getSetType set = AttributeType.String ->
                   AttributeSet.contains operand set |> boolFn
            | struct (AttributeValue.HashSet set, AttributeValue.Number _ & operand)
               when AttributeSet.getSetType set = AttributeType.Number ->
                   AttributeSet.contains operand set |> boolFn
            | struct (AttributeValue.HashSet set, AttributeValue.Binary _ & operand)
               when AttributeSet.getSetType set = AttributeType.Binary ->
                   AttributeSet.contains operand set |> boolFn
            | struct (AttributeValue.AttributeList (CompressedList list), operand) ->
                   Array.contains operand list |> boolFn
            | struct (AttributeValue.AttributeList (SparseList list), operand) ->
                   Seq.contains operand list.Values |> boolFn
            | _ -> ExpressionFnX.noneFn

    let size: ExpressionFnX -> ExpressionFnX =
        ExpressionFnX.bind (function
            | AttributeValue.String str -> str.Length |> intFn
            | AttributeValue.Binary bin -> bin.Length |> intFn
            | AttributeValue.HashMap h -> Map.count h |> intFn
            | AttributeValue.AttributeList l -> AttributeListType.length l |> intFn
            | AttributeValue.HashSet s -> AttributeSet.asSet s |> Set.count |> intFn
            | _ -> ExpressionFnX.noneFn)
