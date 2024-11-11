module TestDynamo.Client.Query

open System
open System.Collections.Generic
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

let filterExpression = emptyStringToNull >> toOption
let expressionAttrNames =
    toOption
    >> ValueOption.map (toMap id id)
    >> ValueOption.defaultValue Map.empty

let expressionAttrValues =
    toOption
    >> ValueOption.map (toMap id (attributeFromDynamodb "$"))
    >> ValueOption.defaultValue Map.empty

let inputs1 =
    // https://docs.aws.amazon.com/cli/latest/reference/dynamodb/query.html
    let mapAttribute (attr: KeyValuePair<string, DynamoAttributeValue>) =
        struct (attr.Key, attributeFromDynamodb "$" attr.Value)

    fun limits (req: QueryRequest) ->
        if req.KeyConditions <> null && req.KeyConditions.Count <> 0 then notSupported "Legacy KeyConditions parameter is not supported"
        if req.QueryFilter <> null && req.QueryFilter.Count <> 0 then notSupported "Legacy QueryFilter parameter is not supported"
        if req.ConditionalOperator <> null then notSupported "Legacy ConditionalOperator parameter is not supported"
            
        let struct (selectTypes, addNames) = buildSelectTypes' req
    
        { tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
          indexName = req.IndexName |> toOption
          queryExpression =
              req.KeyConditionExpression
              |> emptyStringToNull
              |> mandatory "KeyConditionExpression not set"
              |> ExpressionExecutors.Fetch.ExpressionString
          pageSize = if req.IsLimitSet then req.Limit else Int32.MaxValue
          updateExpression = ValueNone 
          limits = limits
          filterExpression = req.FilterExpression |> filterExpression
          expressionAttrNames = req.ExpressionAttributeNames |> expressionAttrNames |> addNames
          expressionAttrValues = req.ExpressionAttributeValues |> expressionAttrValues 
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