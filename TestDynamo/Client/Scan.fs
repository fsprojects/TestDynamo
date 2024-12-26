[<RequireQualifiedAccess>]
module TestDynamo.Client.Scan

open System
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.Client.Shared
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

let private either1Snd _ x = Either1 x

let private buildSelectTypes' (req: ScanRequest<_>) =
    Fetch.buildSelectTypes req.Select (noneifyStrings req.ProjectionExpression) req.AttributesToGet req.IndexName

let buildScanExpression op expr =
    ValueOption.map (Map.map either1Snd)
    >> Fetch.buildFetchExpression "FilterExpression" "KeyConditions" "s" op expr

let input limits (req: ScanRequest<_>) =

    let struct (selectTypes, addNames1) = buildSelectTypes' req
    let struct (filterExpression, struct (addNames2, addValues)) =
        buildScanExpression (req.ConditionalOperator ?|? ConditionalOperator.AND) (noneifyStrings req.FilterExpression) req.ScanFilter
        ?|> mapFst ValueSome
        ?|? struct (ValueNone, struct (id, id))

    { tableName = req.TableName <!!> nameof req.TableName
      indexName = req.IndexName |> noneifyStrings
      queryExpression = ExpressionExecutors.Fetch.ScanFullIndex
      filterExpression = filterExpression
      pageSize = req.Limit ?|? Int32.MaxValue
      updateExpression = ValueNone 
      limits = limits
      expressionAttrNames =
          req.ExpressionAttributeNames
          |> ValueOption.defaultValue Map.empty
          |> addNames1
          |> addNames2
      expressionAttrValues =
          req.ExpressionAttributeValues
          |> ValueOption.defaultValue Map.empty
          |> addValues
      forwards = true
      lastEvaluatedKey = req.ExclusiveStartKey
      selectTypes = selectTypes } : ExpressionExecutors.Fetch.FetchInput

let output databaseId (selectOutput: ExpressionExecutors.Fetch.FetchOutput): ScanResponse<_> =

    { ConsumedCapacity = ValueNone
      Count = !!<selectOutput.resultCount
      Items = selectOutput.items |> ValueSome
      LastEvaluatedKey = selectOutput.evaluatedKeys ?|> _.lastEvaluatedKey
      ScannedCount = !!<selectOutput.scannedCount
      ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
      ContentLength = !!<Shared.ResponseHeaders.contentLength
      HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }
