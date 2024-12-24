[<RequireQualifiedAccess>]
module TestDynamo.Client.Fetch

open System.Runtime.CompilerServices
open TestDynamo.Client.Shared
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.Model
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model.ExpressionExecutors.Fetch
open TestDynamo.Utils

[<Struct; IsReadOnly>]
type Mapping =
    { exprAttrNames: struct (string * string) list
      exprAttrValues: struct (string * AttributeValue) list }
    with
    static member empty = {exprAttrNames = []; exprAttrValues = []}

    static member create exprAttrNames exprAttrValues = {exprAttrNames = exprAttrNames; exprAttrValues = exprAttrValues}

    static member concat x y =
        { exprAttrNames = x.exprAttrNames@y.exprAttrNames
          exprAttrValues = x.exprAttrValues@y.exprAttrValues }

    static member asMapAdders x =
        let names = flip (Seq.fold (fun s struct (k, v) -> Map.add k v s)) x.exprAttrNames
        let values = flip (Seq.fold (fun s struct (k, v) -> Map.add k v s)) x.exprAttrValues

        struct (names, values)

let private exists = function
    | Either1 (x: Condition<_>) -> ValueNone
    | Either2 (x: ExpectedAttributeValue<_>) -> x.Exists
    // TODO: can Condition be mapped onto ExpectedAttributeValue automatically?? 

let private attributeValueList = function
    | Either1 (x: Condition<_>) -> x.AttributeValueList ?|? []
    | Either2 ({ AttributeValueList = ValueSome (_::_ & l) }: ExpectedAttributeValue<_>) -> l
    | Either2 ({ Value = ValueSome v }: ExpectedAttributeValue<_>) -> [v]
    | Either2 _ -> []

let private comparisonOperator =
    function
    | Either1 (x: Condition<_>) -> x.ComparisonOperator
    | Either2 (x: ExpectedAttributeValue<_>) -> x.ComparisonOperator

let private buildKeyCondition' struct (struct (attrAlias, valName), struct (attrName, value)) =
    let attrValues =
        attributeValueList value
        |> Seq.mapi (curry <| function
            | struct (0, x) -> struct (valName, x)
            | i, x -> struct ($"{valName}_{i}", x))
        |> List.ofSeq

    let exprString =
        match struct (comparisonOperator value, attrValues, exists value) with
        | _, _, ValueSome true -> sprintf "attribute_exists(%s)" attrAlias
        | _, _, ValueSome false -> sprintf "attribute_not_exists(%s)" attrAlias
        | ValueNone, _, _ -> ClientError.clientError "Condition operator is mandatory"  // TODO: test? 
        | ValueSome x, [struct (value, _)], ValueNone when x.Value = ComparisonOperator.EQ.Value -> sprintf "%s = %s" attrAlias value
        | ValueSome x, [(value, _)], ValueNone when x.Value = ComparisonOperator.GE.Value -> sprintf "%s >= %s" attrAlias value
        | ValueSome x, [(value, _)], ValueNone when x.Value = ComparisonOperator.GT.Value -> sprintf "%s > %s" attrAlias value
        | ValueSome x, values, ValueNone when x.Value = ComparisonOperator.IN.Value ->
            let vs = values |> Seq.map fstT |> Str.join ","
            sprintf "%s IN (%s)" attrAlias vs
        | ValueSome x, [(value, _)], ValueNone when x.Value = ComparisonOperator.LE.Value -> sprintf "%s <= %s" attrAlias value
        | ValueSome x, [(value, _)], ValueNone when x.Value = ComparisonOperator.LT.Value -> sprintf "%s < %s" attrAlias value
        | ValueSome x, [(value, _)], ValueNone when x.Value = ComparisonOperator.NE.Value -> sprintf "%s <> %s" attrAlias value
        | ValueSome x, [], ValueNone when x.Value = ComparisonOperator.NULL.Value -> sprintf "attribute_not_exists(%s)" attrAlias
        | ValueSome x, [(value1, _); (value2, _)], ValueNone when x.Value = ComparisonOperator.BETWEEN.Value -> sprintf "%s BETWEEN %s AND %s" attrAlias value1 value2
        | ValueSome x, [], ValueNone when x.Value = ComparisonOperator.NOT_NULL.Value -> sprintf "attribute_exists(%s)" attrAlias
        | ValueSome x, [(value, _)], ValueNone when x.Value = ComparisonOperator.CONTAINS.Value -> sprintf "contains(%s, %s)" attrAlias value
        | ValueSome x, [(value, _)], ValueNone when x.Value = ComparisonOperator.BEGINS_WITH.Value -> sprintf "begins_with(%s, %s)" attrAlias value
        | ValueSome x, [(value, _)], ValueNone when x.Value = ComparisonOperator.NOT_CONTAINS.Value -> sprintf "NOT contains(%s, %s)" attrAlias value
        | ValueSome x, args , ValueNone -> ClientError.clientError $"Unknown operation {x.Value} with {args.Length} args"

    struct (exprString, Mapping.create [struct (attrAlias, attrName)] attrValues)

let private buildKeyConditions' conditionType (op: ConditionalOperator) keyConditions =

    keyConditions
    |> Collection.zip (NameValueEnumerator.infiniteNames conditionType)
    |> Seq.map buildKeyCondition'
    |> Collection.unzip
    |> mapFst (Str.join $" %s{op.Value} ")
    |> mapSnd (
        Collection.foldBack (flip Mapping.concat) Mapping.empty)

let buildFetchExpression expressionName conditionsName conditionType op expression (conditions: Map<_, _> voption) =
    match struct (expression, conditions) with
    | ValueNone, ValueNone -> ValueNone
    | ValueSome _, ValueSome _ -> ClientError.clientError $"Cannot specify {conditionsName} and {expressionName} in the same query"
    | ValueSome x, ValueNone -> struct (x, struct (id, id)) |> ValueSome
    | ValueNone, ValueSome x ->
        MapUtils.toSeq x
        |> buildKeyConditions' conditionType op
        |> mapSnd Mapping.asMapAdders
        |> ValueSome

let buildSelectTypes select projectionExpression (attributesToGet: string list voption) indexName =

    match struct (select, projectionExpression, attributesToGet, indexName) with
    | _, ValueSome _, ValueSome (_::_), _ -> ClientError.clientError $"Cannot have a projection expression and AttributesToGet on the same request"
    | ValueNone, ValueNone, ValueNone, _
    | ValueNone, ValueNone, ValueSome [], _ -> ExpressionExecutors.Fetch.AllAttributes |> flip tpl id
    | ValueSome (select: Select), proj, attr, _ when select.Value = Select.SPECIFIC_ATTRIBUTES.Value ->
        GetUtils.buildProjection proj attr
        ?|> mapFst (ValueSome >> ProjectedAttributes)
        ?|? struct (ProjectedAttributes ValueNone, id)
    | ValueNone, _, _, _ -> ExpressionExecutors.Fetch.AllAttributes |> flip tpl id
    | ValueSome (x: Select), _, _, _ when x.Value = Select.ALL_ATTRIBUTES.Value -> ExpressionExecutors.Fetch.AllAttributes |> flip tpl id
    | ValueSome x, _, _, _ when x.Value = Select.COUNT.Value -> ExpressionExecutors.Fetch.Count |> flip tpl id
    | ValueSome x, _, _, ValueNone when x.Value = Select.ALL_PROJECTED_ATTRIBUTES.Value ->
        ClientError.clientError "ALL_PROJECTED_ATTRIBUTES is only valid on indexes"
    | ValueSome x, _, _, ValueSome _ when x.Value = Select.ALL_PROJECTED_ATTRIBUTES.Value ->
        ExpressionExecutors.Fetch.AllAttributes |> flip tpl id
    | ValueSome x, proj, _, _ -> ClientError.clientError $"Invalid Select and ProjectionExpression properties ({x.Value}, {proj})"
    