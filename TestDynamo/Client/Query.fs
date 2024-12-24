[<RequireQualifiedAccess>]
module TestDynamo.Client.Query

open System
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.Client.Shared
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model
open TestDynamo.Utils

let private buildSelectTypes' (req: QueryRequest<_>) =
    Fetch.buildSelectTypes req.Select (noneifyStrings req.ProjectionExpression) req.AttributesToGet req.IndexName

let private either1Snd _ x = Either1 x

let private buildQueryExpression expr =
    ValueOption.map (Map.map either1Snd)
    >> Fetch.buildFetchExpression "KeyConditionExpression" "KeyConditions" "q" ConditionalOperator.AND expr

let buildFilterExpression op expr =
    ValueOption.map (Map.map either1Snd)
    >> Fetch.buildFetchExpression "FilterExpression" "QueryFilter" "qf" op expr

// https://docs.aws.amazon.com/cli/latest/reference/dynamodb/query.html
let input limits (req: QueryRequest<_>) =

    let struct (selectTypes, addNames1) = buildSelectTypes' req
    let struct (queryExpression, struct (addNames2, addValues2)) =
        buildQueryExpression (noneifyStrings req.KeyConditionExpression) req.KeyConditions
        |> ClientError.expectSomeClientErr "KeyConditionExpression not found%s" "" 
    let struct (filterExpression, struct (addNames3, addValues3)) =
        buildFilterExpression (req.ConditionalOperator ?|? ConditionalOperator.AND) (noneifyStrings req.FilterExpression) req.QueryFilter
        ?|> mapFst ValueSome
        ?|? struct (ValueNone, struct (id, id))

    { tableName = req.TableName <!!> nameof req.TableName
      indexName = req.IndexName |> noneifyStrings
      queryExpression = ExpressionExecutors.Fetch.ExpressionString queryExpression
      pageSize = req.Limit ?|? Int32.MaxValue
      updateExpression = ValueNone 
      limits = limits
      filterExpression = filterExpression
      expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty |> addNames1 |> addNames2 |> addNames3
      expressionAttrValues = req.ExpressionAttributeValues ?|? Map.empty |> addValues2 |> addValues3
      forwards = req.ScanIndexForward ?|? true
      lastEvaluatedKey = req.ExclusiveStartKey
      selectTypes = selectTypes } : ExpressionExecutors.Fetch.FetchInput

let output databaseId (selectOutput: ExpressionExecutors.Fetch.FetchOutput): QueryResponse<_> =

    { ConsumedCapacity = ValueNone
      Count = !!<selectOutput.resultCount
      // TODO: array to list
      Items = selectOutput.items |> List.ofArray |> ValueSome
      LastEvaluatedKey = selectOutput.evaluatedKeys ?|> _.lastEvaluatedKey
      ScannedCount = !!<selectOutput.scannedCount
      ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
      ContentLength = !!<Shared.ResponseHeaders.contentLength
      HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }