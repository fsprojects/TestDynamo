module TestDynamo.Client.Scan

open System
open System.Collections.Generic
open System.Reflection
open System.Linq.Expressions
open Amazon.DynamoDBv2.Model
open TestDynamo
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

let inputs1 limits (req: ScanRequest) =
    
    let struct (selectTypes, addNames) = buildSelectTypes' req

    { tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
      indexName = req.IndexName |> toOption
      queryExpression = ExpressionExecutors.Fetch.ScanFullIndex
      filterExpression = req.FilterExpression |> emptyStringToNull |> toOption
      pageSize = if req.IsLimitSet then req.Limit else Int32.MaxValue 
      updateExpression = ValueNone 
      limits = limits
      expressionAttrNames =
          req.ExpressionAttributeNames
          |> toOption
          ?|> (toMap id id)
          |> ValueOption.defaultValue Map.empty
          |> addNames
      expressionAttrValues =
          req.ExpressionAttributeValues
          |> toOption
          ?|> (toMap id (attributeFromDynamodb "$"))
          |> ValueOption.defaultValue Map.empty
      forwards = true
      lastEvaluatedKey = lastEvaluatedKeyIn req.ExclusiveStartKey
      selectTypes = selectTypes } : ExpressionExecutors.Fetch.FetchInput

let inputs2 limits struct (tableName, attributesToGet: List<string>) =
    let req = ScanRequest()
    req.TableName <- tableName
    req.AttributesToGet <- attributesToGet

    inputs1 limits req

let inputs3 limits struct (tableName, scanFilter) =
    let req = ScanRequest()
    req.TableName <- tableName
    req.ScanFilter <- scanFilter

    inputs1 limits req

let inputs4 limits struct (tableName, attributesToGet, scanFilter) =
    let req = ScanRequest()
    req.TableName <- tableName
    req.AttributesToGet <- attributesToGet
    req.ScanFilter <- scanFilter

    inputs1 limits req

let output databaseId (selectOutput: ExpressionExecutors.Fetch.FetchOutput) =

    let output = Shared.amazonWebServiceResponse<ScanResponse>()
    output.Items <- mapItems selectOutput.items

    output.Count <- selectOutput.resultCount
    output.ScannedCount <- selectOutput.scannedCount
    output.LastEvaluatedKey <- lastEvaluatedKeyOut selectOutput

    output