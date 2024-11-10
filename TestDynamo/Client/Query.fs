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

let buildSelectTypes select projectionExpression indexName =
    match struct (select, projectionExpression) with
    | null, null
    | null, "" -> ExpressionExecutors.Fetch.AllAttributes
    | null, proj -> ExpressionExecutors.Fetch.ProjectedAttributes (ValueSome proj)
    | x: Select, _ when x.Value = Select.ALL_ATTRIBUTES.Value -> ExpressionExecutors.Fetch.AllAttributes
    | x, _ when x.Value = Select.COUNT.Value -> ExpressionExecutors.Fetch.Count
    | x, proj when x.Value = Select.SPECIFIC_ATTRIBUTES.Value -> ExpressionExecutors.Fetch.ProjectedAttributes (toOption proj)
    | x, _ when x.Value = Select.ALL_PROJECTED_ATTRIBUTES.Value ->
        if String.IsNullOrEmpty indexName then clientError "ALL_PROJECTED_ATTRIBUTES is only valid on indexes" 
        ExpressionExecutors.Fetch.AllAttributes
    | x, proj -> clientError $"Invalid Select and ProjectionExpression properties ({x.Value}, {proj})"

let private buildSelectTypes' (req: QueryRequest) =
    buildSelectTypes req.Select req.ProjectionExpression req.IndexName

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
        // TODO: AttributesToGet is supported in other fetches
        if req.AttributesToGet <> null && req.AttributesToGet.Count <> 0 then notSupported "Legacy AttributesToGet parameter is not supported"
        if req.KeyConditions <> null && req.KeyConditions.Count <> 0 then notSupported "Legacy KeyConditions parameter is not supported"
        if req.QueryFilter <> null && req.QueryFilter.Count <> 0 then notSupported "Legacy QueryFilter parameter is not supported"
        if req.ConditionalOperator <> null then notSupported "Legacy ConditionalOperator parameter is not supported"

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
          expressionAttrNames = req.ExpressionAttributeNames |> expressionAttrNames
          expressionAttrValues = req.ExpressionAttributeValues |> expressionAttrValues 
          forwards = getScanIndexForward req
          lastEvaluatedKey = lastEvaluatedKeyIn req.ExclusiveStartKey
          selectTypes = buildSelectTypes' req } : ExpressionExecutors.Fetch.FetchInput

let output databaseId (selectOutput: ExpressionExecutors.Fetch.FetchOutput) =

    let output = Shared.amazonWebServiceResponse<QueryResponse>()
    output.Items <- mapItems selectOutput.items

    output.Count <- selectOutput.resultCount
    output.ScannedCount <- selectOutput.scannedCount
    output.LastEvaluatedKey <- lastEvaluatedKeyOut selectOutput

    output