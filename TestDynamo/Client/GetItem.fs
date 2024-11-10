module TestDynamo.Client.GetItem

open System
open System.Linq
open System.Runtime.CompilerServices
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Model
open TestDynamo.Client
open TestDynamo.Client.ItemMapper
open TestDynamo.Client.Query
open TestDynamo.Model.ExpressionExecutors.Fetch
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue
type MList<'a> = System.Collections.Generic.List<'a>

let buildProjection projectionExpression (attributesToGet: IReadOnlyList<string>) =
    if attributesToGet <> null && attributesToGet.Count > 0 && ValueOption.isSome projectionExpression
    then clientError $"Cannot use {nameof Unchecked.defaultof<GetItemRequest>.ProjectionExpression} and {nameof Unchecked.defaultof<GetItemRequest>.AttributesToGet} in the same request"
    
    if attributesToGet <> null && attributesToGet.Count > 0
    then
        attributesToGet
        |> Seq.map (fun x ->
            // TODO: uniqueid factory
            let id = Guid.NewGuid().ToString().Replace("-", "") |> sprintf "#x%s"
            struct (id, struct (id, x)))
        |> Collection.unzip
        |> mapFst (
            Str.join ","
            >> ValueSome
            >> ProjectedAttributes)
    else
        projectionExpression
        |> ValueOption.map (ValueSome >> ProjectedAttributes)
        |> ValueOption.defaultValue AllAttributes
        |> flip tpl List.empty

let inputs1 (req: GetItemRequest) =
    let struct (returnValues, exprAttrNames) = buildProjection (CSharp.toOption req.ProjectionExpression) req.AttributesToGet
    
    { keys = [|itemFromDynamodb "$" req.Key|]
      maxPageSizeBytes = Int32.MaxValue
      conditionExpression =
          { tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
            conditionExpression = ValueNone
            expressionAttrValues = Map.empty
            expressionAttrNames =
                req.ExpressionAttributeNames
                |> expressionAttrNames
                |> flip (Seq.fold (fun s struct (k, v) -> Map.add k v s)) exprAttrNames
            returnValues = returnValues }} : GetItemArgs

let inputs2 struct (tableName, key) =
    let req = GetItemRequest()
    req.TableName <- tableName
    req.Key <- key

    inputs1 req

let inputs3 struct (tableName, key, consistentRead) =
    let req = GetItemRequest()
    req.TableName <- tableName
    req.Key <- key
    req.ConsistentRead <- consistentRead

    inputs1 req

let output databaseId (selectOutput: GetResponseData) =

    let output = Shared.amazonWebServiceResponse<Amazon.DynamoDBv2.Model.GetItemResponse>()
    output.Item <-  selectOutput.items |> Seq.map itemToDynamoDb |> Collection.tryHead |> CSharp.fromOption
    output.IsItemSet <- output.Item <> null
    output

module Transaction =

    let private inputs1' (req: Get) =

        { keys = [|itemFromDynamodb "$" req.Key|]
          maxPageSizeBytes = Int32.MaxValue
          conditionExpression =
              { tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
                conditionExpression = ValueNone
                expressionAttrValues = Map.empty
                expressionAttrNames = req.ExpressionAttributeNames |> expressionAttrNames
                returnValues =
                    req.ProjectionExpression
                    |> CSharp.toOption
                    |> ValueOption.map (ValueSome >> ProjectedAttributes)
                    |> ValueOption.defaultValue AllAttributes }} : GetItemArgs

    let inputs1 (req: TransactGetItemsRequest) =
        if req.TransactItems <> null && req.TransactItems.Count > Settings.TransactReadSettings.MaxItemsPerRequest
        then clientError $"The limit on TransactGetItems is {Settings.TransactReadSettings.MaxItemsPerRequest} records. You can change this value by modifying {nameof Settings}.{nameof Settings.TransactReadSettings}.{nameof Settings.TransactReadSettings.MaxItemsPerRequest}"

        req.TransactItems |> CSharp.sanitizeSeq |> Seq.map (_.Get >> inputs1')

    let private output' (selectOutput: GetResponseData) =

        let output = ItemResponse()
        let item = selectOutput.items |> Collection.tryHead
        let size =
            ValueOption.bind ItemSize.tryCreate item
            ?|> ItemSize.size
            |> ValueOption.defaultValue 0   // ItemSize.tryCreate shouldn't return None 

        output.Item <- item ?|> itemToDynamoDb |> CSharp.fromOption
        struct (output, size)

    let output _ (selectOutput: GetResponseData seq) =

        let struct (responses, size) =
            selectOutput
            |> Seq.map output'
            |> Collection.unzip
            |> mapFst Enumerable.ToList
            |> mapSnd Seq.sum

        if size > 4_000_000
        then clientError "The limit on data size from TransactGetItems is 4MB"

        let output = Shared.amazonWebServiceResponse<Amazon.DynamoDBv2.Model.TransactGetItemsResponse>()
        output.Responses <- responses
        output

module Batch =

    [<Struct; IsReadOnly>]
    type BatchItemKey =
        { databaseId: DatabaseId
          tableName: string
          userProvidedKey: string }

    type BatchGetValue =
        { keys: Map<string, AttributeValue> array
          consistentRead: bool
          projectionExpression: string voption
          expressionAttrNames: Map<string, string> }

    type BatchGetRequest =
        { requests: Map<BatchItemKey, BatchGetValue> }

    type BatchGetResponse =
        { notProcessed: Map<BatchItemKey, BatchGetValue>
          found: Map<BatchItemKey, Map<string, AttributeValue> array> }

    let private buildKey defaultDatabaseId awsAccountId key =
        AwsUtils.parseTableArn key
        ?|> (fun struct (x, y, z) -> struct (y, { databaseId = { regionId = x }; tableName = z; userProvidedKey = key}) |> ValueSome)
        |> ValueOption.defaultWith (fun _ ->
            AwsUtils.parseTableName key
            ?|> (fun x -> struct (awsAccountId, { databaseId = defaultDatabaseId; tableName = x; userProvidedKey = key})))
        |> ValueOption.defaultWith (fun _ -> $"Cannot parse table name or ARN {key}" |> clientError)

    let toBatchGetValue (x: KeysAndAttributes): BatchGetValue =
        { keys = x.Keys |> CSharp.orEmpty |> Seq.map (itemFromDynamodb "$") |> Array.ofSeq
          consistentRead = x.ConsistentRead
          projectionExpression = x.ProjectionExpression |> CSharp.toOption
          expressionAttrNames = x.ExpressionAttributeNames |> expressionAttrNames }

    let fromBatchGetValue (x: BatchGetValue): KeysAndAttributes =
        let out = KeysAndAttributes()
        out.ConsistentRead <- x.consistentRead
        out.ProjectionExpression <- CSharp.fromOption x.projectionExpression
        out.ExpressionAttributeNames <- CSharp.toDictionary id id x.expressionAttrNames
        out.Keys <-
            x.keys
            |> Seq.map itemToDynamoDb
            |> Enumerable.ToList

        out

    let private suppressMessage =
        $"TestDynamoClient.SetAwsAccountId or by "
        + $"setting {nameof Settings}.{nameof Settings.SupressErrorsAndWarnings}.{nameof Settings.SupressErrorsAndWarnings.AwsAccountIdErrors} = true"

    let reKey awsAccountId defaultDatabaseId (req: struct (string * _) seq) =

        let reKeyed =
            req
            |> Collection.mapFst (buildKey defaultDatabaseId awsAccountId)
            |> List.ofSeq

        match reKeyed |> Collection.groupByL (fstT >> fstT) |> List.map fstT with
        | [] -> reKeyed
        | [acc] when acc <> awsAccountId && not Settings.SupressErrorsAndWarnings.AwsAccountIdErrors ->
            [
                $"Invalid aws account id {acc} in ARN. The expected aws account "
                $"id is {awsAccountId}. You can fix this issue by calling "
                suppressMessage
            ] |> Str.join "" |> clientError
        | [_] -> reKeyed
        | xs ->
            [
                $"Multiple account ids found {xs}. The expected aws account "
                $"id is {awsAccountId}. You can fix this issue by calling "
                suppressMessage
            ] |> Str.join "" |> clientError

    let inputs1 awsAccountId defaultDatabaseId (req: BatchGetItemRequest) =
        let totalCount =
            req.RequestItems
            |> CSharp.orEmpty
            |> Seq.sumBy (
                _.Value
                >> CSharp.toOption
                >> ValueOption.map (
                    _.Keys >> CSharp.sanitizeSeq >> Seq.length)
                >> ValueOption.defaultValue 0)

        if totalCount > 100
        then clientError "Max allowed items is 100 for the BatchGetItem call."

        let requests =
            req.RequestItems
            |> CSharp.orEmpty
            |> Seq.map CSharp.kvpToTpl
            |> reKey awsAccountId defaultDatabaseId
            |> Seq.map (
                mapFst sndT
                >> mapSnd toBatchGetValue)
            |> MapUtils.fromTuple

        { requests = requests }

    let inputs2 awsAccountId defaultDatabaseId struct (requestItems, returnConsumedCapacity) =
        let req = BatchGetItemRequest()
        req.RequestItems <- requestItems
        req.ReturnConsumedCapacity <- returnConsumedCapacity

        inputs1 awsAccountId defaultDatabaseId req

    let inputs3 awsAccountId defaultDatabaseId requestItems =
        let req = BatchGetItemRequest()
        req.RequestItems <- requestItems

        inputs1 awsAccountId defaultDatabaseId req

    let output _ (selectOutput: BatchGetResponse) =

        let output = Shared.amazonWebServiceResponse<BatchGetItemResponse>()
        output.Responses <-
            selectOutput.found
            |> MapUtils.toSeq
            |> Seq.map (
                mapFst _.userProvidedKey
                >> mapSnd (
                    Seq.map itemToDynamoDb
                    >> Enumerable.ToList))
            |> CSharp.tplToDictionary

        output.UnprocessedKeys <- 
            selectOutput.notProcessed
            |> MapUtils.toSeq
            |> Seq.map (
                mapFst _.userProvidedKey
                >> mapSnd fromBatchGetValue)
            |> CSharp.tplToDictionary

        output