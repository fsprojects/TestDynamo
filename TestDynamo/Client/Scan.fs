module TestDynamo.Client.Scan

open System
open System.Reflection
open System.Linq.Expressions
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Utils
open TestDynamo.Model
open TestDynamo.Client
open TestDynamo.Client.ItemMapper
open TestDynamo.CSharp
open TestDynamo.Client.Query
open TestDynamo.Data.Monads.Operators

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue
type MList<'a> = System.Collections.Generic.List<'a>

let getScanIndexForward: QueryRequest -> bool =
    let isSet =
        let expr =
            let param = Expression.Parameter typedefof<QueryRequest>
            let method =
                typedefof<QueryRequest>.GetMethods(BindingFlags.Instance ||| BindingFlags.NonPublic)
                |> Seq.filter (fun x ->
                    x.Name = "IsSetScanIndexForward"
                        && x.ReturnType = typedefof<bool>
                        && (x.GetParameters() |> Array.length = 0))
                |> Seq.head

            let call = Expression.Call(param, method)
            Expression.Lambda<Func<QueryRequest, bool>>(call, [|param|])

        expr.Compile()

    fun x -> isSet.Invoke x |> not || x.ScanIndexForward

let private buildSelectTypes' (req: ScanRequest) =
    buildSelectTypes req.Select (req.ProjectionExpression |> CSharp.emptyStringToNull |> CSharp.toOption) req.AttributesToGet req.IndexName

let private buildScanExpression = buildFetchExpression "FilterExpression" "KeyConditions" "s"

let inputs limits (req: ScanRequest) =

    let struct (selectTypes, addNames1) = buildSelectTypes' req
    let struct (filterExpression, struct (addNames2, addValues)) =
        buildScanExpression req.ConditionalOperator (CSharp.emptyStringToNull req.FilterExpression |> CSharp.toOption) req.ScanFilter
        ?|> mapFst ValueSome
        ?|? struct (ValueNone, struct (id, id))

    { tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
      indexName = req.IndexName |> toOption
      queryExpression = ExpressionExecutors.Fetch.ScanFullIndex
      filterExpression = filterExpression
      pageSize = if req.IsLimitSet then req.Limit else Int32.MaxValue
      updateExpression = ValueNone 
      limits = limits
      expressionAttrNames =
          req.ExpressionAttributeNames
          |> toOption
          ?|> (toMap id id)
          |> ValueOption.defaultValue Map.empty
          |> addNames1
          |> addNames2
      expressionAttrValues =
          req.ExpressionAttributeValues
          |> toOption
          ?|> (toMap id (attributeFromDynamodb "$"))
          |> ValueOption.defaultValue Map.empty
          |> addValues
      forwards = true
      lastEvaluatedKey = lastEvaluatedKeyIn req.ExclusiveStartKey
      selectTypes = selectTypes } : ExpressionExecutors.Fetch.FetchInput

let output databaseId (selectOutput: ExpressionExecutors.Fetch.FetchOutput) =

    let output = Shared.amazonWebServiceResponse<ScanResponse>()
    output.Items <- mapItems selectOutput.items

    output.Count <- selectOutput.resultCount
    output.ScannedCount <- selectOutput.scannedCount
    output.LastEvaluatedKey <- lastEvaluatedKeyOut selectOutput

    output