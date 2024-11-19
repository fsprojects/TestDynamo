module TestDynamo.Client.Query

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Model
open TestDynamo.Client
open TestDynamo.Client.ItemMapper
open TestDynamo.CSharp
open TestDynamo.Model.ExpressionExecutors.Fetch
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open Shared

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue
type MList<'a> = System.Collections.Generic.List<'a>

let lastEvaluatedKeyIn (req: Dictionary<string, DynamoAttributeValue>) =
    match req with
    | null -> ValueNone
    | x when x.Count = 0 -> ValueNone
    | xs -> itemFromDynamodb "$" xs |> ValueSome

let lastEvaluatedKeyOut (selectOutput: ExpressionExecutors.Fetch.FetchOutput) =
    selectOutput.evaluatedKeys
    ?|> _.lastEvaluatedKey
    |> ValueOption.defaultValue Map.empty
    |> CSharp.toDictionary id attributeToDynamoDb

let getScanIndexForward =
    getOptionalBool<QueryRequest, bool> "ScanIndexForward"
    >> ValueOption.defaultValue true

let buildSelectTypes select projectionExpression (attributesToGet: IReadOnlyList<string>) indexName =

    let inline attrC (attr: IReadOnlyList<string>) = if attr = null then 0 else attr.Count
    match struct (select, projectionExpression, attributesToGet) with
    | _, ValueSome _, attr when attrC attr > 0 -> clientError $"Cannot have a projection expression and AttributesToGet on the same request"
    | null, ValueNone, attr when attrC attr = 0 -> ExpressionExecutors.Fetch.AllAttributes |> flip tpl id
    | select: Select, proj, attr when select = null || select.Value = Select.SPECIFIC_ATTRIBUTES.Value ->
        GetUtils.buildProjection proj attr
        ?|> mapFst (ValueSome >> ExpressionExecutors.Fetch.ProjectedAttributes)
        ?|? struct (ProjectedAttributes ValueNone, id)
    | x: Select, _, _ when x.Value = Select.ALL_ATTRIBUTES.Value -> ExpressionExecutors.Fetch.AllAttributes |> flip tpl id
    | x, _, _ when x.Value = Select.COUNT.Value -> ExpressionExecutors.Fetch.Count |> flip tpl id
    | x, _, _ when x.Value = Select.ALL_PROJECTED_ATTRIBUTES.Value ->
        if String.IsNullOrEmpty indexName then clientError "ALL_PROJECTED_ATTRIBUTES is only valid on indexes" 
        ExpressionExecutors.Fetch.AllAttributes |> flip tpl id
    | x, proj, _ -> clientError $"Invalid Select and ProjectionExpression properties ({x.Value}, {proj})"

let private buildSelectTypes' (req: QueryRequest) =
    buildSelectTypes req.Select (req.ProjectionExpression |> CSharp.emptyStringToNull |> CSharp.toOption) req.AttributesToGet req.IndexName

let mapItem = Item.attributes >> itemToDynamoDb
let mapItems items =
    let output = List<_>(Array.length items)
    output.AddRange(Seq.map itemToDynamoDb items)
    output

let expressionAttrNames =
    toOption
    >> ValueOption.map (toMap id id)
    >> ValueOption.defaultValue Map.empty

let expressionAttrValues =
    toOption
    >> ValueOption.map (toMap id (attributeFromDynamodb "$"))
    >> ValueOption.defaultValue Map.empty

[<Struct; IsReadOnly>]
type Mapping =
    { exprAttrNames: struct (string * string) list
      exprAttrValues: struct (string * AttributeValue) list }
    with
    static member concat x y =
        { exprAttrNames = x.exprAttrNames@y.exprAttrNames
          exprAttrValues = x.exprAttrValues@y.exprAttrValues }

    static member asMapAdders x =
        let names = flip (Seq.fold (fun s struct (k, v) -> Map.add k v s)) x.exprAttrNames
        let values = flip (Seq.fold (fun s struct (k, v) -> Map.add k v s)) x.exprAttrValues

        struct (names, values)

let private exists' = getOptionalBool<ExpectedAttributeValue, bool> (nameof Unchecked.defaultof<ExpectedAttributeValue>.Exists)

let private attributeValueList = function
    | Either1 (x: Condition) -> x.AttributeValueList |> CSharp.sanitizeSeq
    | Either2 (x: ExpectedAttributeValue) when x.AttributeValueList <> null && x.AttributeValueList.Count > 0 -> x.AttributeValueList |> CSharp.sanitizeSeq
    | Either2 (x: ExpectedAttributeValue) when x.Value <> null -> MList<_>([x.Value])
    | Either2 _ -> MList<_>()

let private comparisonOperator =
    function
    | Either1 (x: Condition) -> x.ComparisonOperator
    | Either2 (x: ExpectedAttributeValue) -> x.ComparisonOperator
    >> CSharp.mandatory "ComparisonOperator is mandatory"

let private exists = function
    | Either1 (x: Condition) -> ValueNone
    | Either2 (x: ExpectedAttributeValue) -> exists' x

let private buildKeyCondition' struct (struct (attrAlias, valName), struct (attrName, value)) =
    let attrValues =
        attributeValueList value
        |> Seq.mapi (curry <| function
            | struct (0, x) -> struct (valName, x)
            | i, x -> struct ($"{valName}_{i}", x))
        |> List.ofSeq

    let exprString =
        match struct (comparisonOperator value, attrValues, exists value) with
        | struct (_, _, ValueSome true) -> sprintf "attribute_exists(%s)" attrAlias
        | _, _, ValueSome false -> sprintf "attribute_not_exists(%s)" attrAlias
        | x, [struct (value, _)], ValueNone when x.Value = ComparisonOperator.EQ.Value -> sprintf "%s = %s" attrAlias value
        | x, [(value, _)], ValueNone when x.Value = ComparisonOperator.GE.Value -> sprintf "%s >= %s" attrAlias value
        | x, [(value, _)], ValueNone when x.Value = ComparisonOperator.GT.Value -> sprintf "%s > %s" attrAlias value
        | x, values, ValueNone when x.Value = ComparisonOperator.IN.Value ->
            let vs = values |> Seq.map fstT |> Str.join ","
            sprintf "%s IN (%s)" attrAlias vs
        | x, [(value, _)], ValueNone when x.Value = ComparisonOperator.LE.Value -> sprintf "%s <= %s" attrAlias value
        | x, [(value, _)], ValueNone when x.Value = ComparisonOperator.LT.Value -> sprintf "%s < %s" attrAlias value
        | x, [(value, _)], ValueNone when x.Value = ComparisonOperator.NE.Value -> sprintf "%s <> %s" attrAlias value
        | x, [], ValueNone when x.Value = ComparisonOperator.NULL.Value -> sprintf "attribute_not_exists(%s)" attrAlias
        | x, [(value1, _); (value2, _)], ValueNone when x.Value = ComparisonOperator.BETWEEN.Value -> sprintf "%s BETWEEN %s AND %s" attrAlias value1 value2
        | x, [], ValueNone when x.Value = ComparisonOperator.NOT_NULL.Value -> sprintf "attribute_exists(%s)" attrAlias
        | x, [(value, _)], ValueNone when x.Value = ComparisonOperator.CONTAINS.Value -> sprintf "contains(%s, %s)" attrAlias value
        | x, [(value, _)], ValueNone when x.Value = ComparisonOperator.BEGINS_WITH.Value -> sprintf "begins_with(%s, %s)" attrAlias value
        | x, [(value, _)], ValueNone when x.Value = ComparisonOperator.NOT_CONTAINS.Value -> sprintf "NOT contains(%s, %s)" attrAlias value
        | x, args , ValueNone -> clientError $"Unknown operation {x.Value} with {args.Length} args"

    let values =
        Collection.mapSnd (ItemMapper.attributeFromDynamodb "$") attrValues
        |> List.ofSeq

    struct (exprString, { exprAttrValues = values; exprAttrNames = [struct (attrAlias, attrName)]})

let private buildKeyConditions' conditionType (op: ConditionalOperator) keyConditions =
    let op = match op with | null -> ConditionalOperator.AND | x -> x
    keyConditions
    |> Collection.zip (NameValueEnumerator.infiniteNames conditionType)
    |> Seq.map buildKeyCondition'
    |> Collection.unzip
    |> mapFst (Str.join $" %s{op.Value} ")
    |> mapSnd (
        Collection.foldBack (flip Mapping.concat) { exprAttrValues = []; exprAttrNames = []})

let buildFetchExpression expressionName conditionsName conditionType op expression (conditions: Dictionary<string, Condition>) =
    if ValueOption.isSome expression
        && conditions <> null
        && conditions.Count > 0 then clientError $"Cannot specify {conditionsName} and {expressionName} in the same query"

    if ValueOption.isNone expression
        && (conditions = null
            || conditions.Count = 0) then ValueNone
    else    
        match expression with
        | ValueSome x -> struct (x, struct (id, id))
        | ValueNone ->
            buildKeyConditions' conditionType op (conditions |> Seq.map (fun x -> struct (x.Key, Either1 x.Value)))
            |> mapSnd Mapping.asMapAdders
        |> ValueSome

let buildUpdateConditionExpression op expression (conditions: Dictionary<_, _>) =
    if ValueOption.isSome expression
        && conditions <> null
        && conditions.Count > 0 then clientError $"Cannot specify Expected and ConditionExpression in the same query"

    if ValueOption.isNone expression
        && (conditions = null
            || conditions.Count = 0) then ValueNone
    else    
        match expression with
        | ValueSome x -> struct (x, struct (id, id))
        | ValueNone ->
            buildKeyConditions' "ue" op (conditions |> Seq.map (fun x -> struct (x.Key, Either2 x.Value)))
            |> mapSnd Mapping.asMapAdders
        |> ValueSome

let private buildQueryExpression = buildFetchExpression "KeyConditionExpression" "KeyConditions" "q" ConditionalOperator.AND

let buildFilterExpression = buildFetchExpression "FilterExpression" "QueryFilter" "qf"

let inputs =
    // https://docs.aws.amazon.com/cli/latest/reference/dynamodb/query.html
    let mapAttribute (attr: KeyValuePair<string, DynamoAttributeValue>) =
        struct (attr.Key, attributeFromDynamodb "$" attr.Value)

    fun limits (req: QueryRequest) ->
        
        let struct (selectTypes, addNames1) = buildSelectTypes' req
        let struct (queryExpression, struct (addNames2, addValues2)) =
            buildQueryExpression (CSharp.emptyStringToNull req.KeyConditionExpression |> CSharp.toOption) req.KeyConditions
            ?|>? fun _ -> clientError $"KeyConditionExpression not found" 
        let struct (filterExpression, struct (addNames3, addValues3)) =
            buildFilterExpression req.ConditionalOperator (CSharp.emptyStringToNull req.FilterExpression |> CSharp.toOption) req.QueryFilter
            ?|> mapFst ValueSome
            ?|? struct (ValueNone, struct (id, id))

        { tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
          indexName = req.IndexName |> toOption
          queryExpression = ExpressionExecutors.Fetch.ExpressionString queryExpression
          pageSize = if req.IsLimitSet then req.Limit else Int32.MaxValue
          updateExpression = ValueNone 
          limits = limits
          filterExpression = filterExpression
          expressionAttrNames = req.ExpressionAttributeNames |> expressionAttrNames |> addNames1 |> addNames2 |> addNames3
          expressionAttrValues = req.ExpressionAttributeValues |> expressionAttrValues |> addValues2 |> addValues3
          forwards = getScanIndexForward req
          lastEvaluatedKey = lastEvaluatedKeyIn req.ExclusiveStartKey
          selectTypes = selectTypes } : ExpressionExecutors.Fetch.FetchInput

let output databaseId (selectOutput: ExpressionExecutors.Fetch.FetchOutput) =

    let output = Shared.amazonWebServiceResponse<QueryResponse>()
    output.Items <- mapItems selectOutput.items

    output.Count <- selectOutput.resultCount
    output.ScannedCount <- selectOutput.scannedCount
    output.LastEvaluatedKey <- lastEvaluatedKeyOut selectOutput

    output