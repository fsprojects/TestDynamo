[<RequireQualifiedAccess>]
module TestDynamo.Client.Item

open Microsoft.Extensions.Logging
open TestDynamo
open TestDynamo.Client.Shared
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.Model
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils
open TestDynamo.Model.ExpressionExecutors.Fetch
open System.Runtime.CompilerServices

type Dictionary<'k, 'v> = System.Collections.Generic.Dictionary<'k, 'v>

// TODO: there are multiple similar (not same) mapReturnValues fns 
let mapReturnValues (returnValues: ReturnValue): BasicReturnValues =
    match returnValues with
    | x when x.Value = ReturnValue.NONE.Value -> None
    | x when x.Value = ReturnValue.ALL_OLD.Value -> AllOld
    | x -> ClientError.clientError "Only NONE and ALL_OLD are supported as ReturnValues"

let mapReturnValues2 (returnValues: ReturnValue): UpdateReturnValues =
    match returnValues with
    | x when x.Value = ReturnValue.NONE.Value -> Basic None
    | x when x.Value = ReturnValue.ALL_OLD.Value -> Basic AllOld
    | x when x.Value = ReturnValue.ALL_NEW.Value -> AllNew
    | x when x.Value = ReturnValue.UPDATED_OLD.Value -> UpdatedOld
    | x when x.Value = ReturnValue.UPDATED_NEW.Value -> UpdatedNew
    | x -> ClientError.clientError $"Invalid ReturnValues {x}"

let private either2Snd _ x = Either2 x

let buildUpdateConditionExpression
    (op: ConditionalOperator)
    (expression: string voption)
    (conditions: Map<_, _> voption): struct (string * struct ((Map<string,string> -> Map<string,string>) * (Map<string,AttributeValue> -> Map<string,AttributeValue>))) voption =

    conditions
    ?|> Map.map either2Snd
    |> Fetch.buildFetchExpression "Expected" "ConditionExpression" "ue" op expression

module Put =

    let input (req: PutItemRequest<_>): PutItemArgs<Map<string,_>> =
        // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

        let struct (filterExpression, struct (addNames, addValues)) =
            buildUpdateConditionExpression (req.ConditionalOperator ?|? ConditionalOperator.AND) (noneifyStrings req.ConditionExpression) req.Expected
            ?|> mapFst ValueSome
            ?|? struct (ValueNone, struct (id, id))

        { item = req.Item <!!> nameof req.Item
          conditionExpression =
              { conditionExpression = filterExpression
                tableName = req.TableName <!!> nameof req.TableName
                returnValues = req.ReturnValues ?|> mapReturnValues ?|? None
                expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty |> addNames
                expressionAttrValues = req.ExpressionAttributeValues ?|? Map.empty |> addValues } } : PutItemArgs<_>

        // 'a -> Map<string,AttributeValue> voption -> PutItemResponse
    let output databaseId (attributes: Map<string,_> voption): PutItemResponse<_> =

        { Attributes = attributes
          ConsumedCapacity = ValueNone 
          ItemCollectionMetrics =  ValueNone
          ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
          ContentLength = !!<Shared.ResponseHeaders.contentLength
          HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

module Get =

    let buildProjection =
        GetUtils.buildProjection
        >>> ValueOption.map (mapFst (ValueSome >> ProjectedAttributes))
        >>> ValueOption.defaultValue struct (AllAttributes, id)

    let input  (req: GetItemRequest<AttributeValue>) =
        let struct (returnValues, exprAttrNames) = buildProjection req.ProjectionExpression req.AttributesToGet

        { keys = req.Key <!!> nameof req.Key |> Array.singleton
          maxPageSizeBytes = System.Int32.MaxValue
          conditionExpression =
              { tableName = req.TableName <!!> nameof req.TableName
                conditionExpression = ValueNone
                expressionAttrValues = Map.empty
                expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty |> exprAttrNames
                returnValues = returnValues }} : GetItemArgs

    let output databaseId (selectOutput: GetResponseData): GetItemResponse<_> =

        { Item = selectOutput.items |> Collection.tryHead
          ConsumedCapacity = ValueNone 
          ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
          ContentLength = !!<Shared.ResponseHeaders.contentLength
          HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

module Delete =

    let input  (req: DeleteItemRequest<_>) =
    // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

        let struct (filterExpression, struct (addNames, addValues)) =
            buildUpdateConditionExpression (req.ConditionalOperator ?|? ConditionalOperator.AND) (noneifyStrings req.ConditionExpression) req.Expected
            ?|> mapFst ValueSome
            ?|? struct (ValueNone, struct (id, id))

        { key = req.Key <!!> nameof req.Key
          conditionExpression =
              { conditionExpression = filterExpression
                tableName = req.TableName <!!> nameof req.TableName
                returnValues = req.ReturnValues ?|> mapReturnValues ?|? None
                expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty |> addNames
                expressionAttrValues = req.ExpressionAttributeValues ?|? Map.empty |> addValues } } : DeleteItemArgs<_>

    let output databaseId item: DeleteItemResponse<_> =

        { Attributes = item
          ConsumedCapacity = ValueNone 
          ItemCollectionMetrics =  ValueNone
          ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
          ContentLength = !!<Shared.ResponseHeaders.contentLength
          HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

module Update =
    [<Struct; IsReadOnly>]
    type private AttributeUpdateClause =
        { expression: string
          actualAttributeName: string
          name: string
          value: struct (string * AttributeValue) voption }

    [<Struct; IsReadOnly>]
    type private AttributeUpdateExpression =
        { expression: string
          clauses: AttributeUpdateClause list }

        with
        static member concat =
            Seq.map (fun (x: AttributeUpdateExpression) -> struct (x.expression, x))
            >> Collection.unzip
            >> fun struct (expr, xs) ->
                { expression = expr |> Str.join " "; clauses = List.collect _.clauses xs }

        static member asOutput (logger: ILogger) (reqUpdateExpression: string voption) = function
            | x when x.clauses = [] -> ValueNone
            | x ->
                // TODO: nested match
                match noneifyStrings reqUpdateExpression with
                | ValueSome _ -> ClientError.clientError "Legacy AttributeUpdates parameter cannot be used with an UpdateExpression"
                | ValueNone -> 
                    x.clauses
                    |> Seq.fold (fun struct (names, values) x ->
                        struct (
                            Map.add x.name x.actualAttributeName names,
                            x.value
                            ?|> (uncurry (flip3To1 Map.add values))
                            ?|? values)) struct (Map.empty, Map.empty)
                    |> tpl x.expression
                    |> ValueSome

    let private attributeUpdate setValue expr struct (name, value) (update: KeyValuePair<string, AttributeValueUpdate<AttributeValue>>) =

        { expression = sprintf expr name value
          actualAttributeName = update.Key
          name = name
          value = if setValue then ValueSome struct (value, update.Value.Value <!!> "AttributeValueUpdate.Value") else ValueNone }

    let private putAttributeUpdate = attributeUpdate true "%s = %s"
    let private addAttributeUpdate = attributeUpdate true "%s %s"
    let private deleteAttributeUpdate = flip tpl "" >> attributeUpdate false "%s%s"

    let private combineUpdates name (updates: AttributeUpdateClause seq) =
        updates
        |> Seq.fold (fun struct (expressions, clauses) x ->
            struct (x.expression::expressions, x::clauses)) struct ([], [])
        |> mapFst (Str.join "," >> sprintf "%s %s" name)
        |> fun struct (expr, xs) -> { expression = expr; clauses = xs }

    let private buildFromAttributeUpdates': Map<string, AttributeValueUpdate<AttributeValue>> -> AttributeUpdateExpression =

        Collection.groupBy (fun x ->
            let v = x.Value.Action <!!> "AttributeValueUpdate.Value.Action"
            let v = noneifyStrings (ValueSome v.Value) <!!> "AttributeValueUpdate.Value.Action.Value"
            v.ToUpper())
        >> Seq.collect (function
            | "PUT", x -> Seq.map Choice1Of3 x
            | "ADD", x -> Seq.map Choice2Of3 x
            | "DELETE", x -> Seq.map Choice3Of3 x
            | x, _ -> ClientError.clientError $"Unknown attribute update value \"{x}\"")
        >> Collection.zip (NameValueEnumerator.infiniteNames "p")
        >> Seq.map (fun struct (nameValue, x) ->
            match x with
            | Choice1Of3 x -> putAttributeUpdate nameValue x |> tpl "SET"
            | Choice2Of3 x -> addAttributeUpdate nameValue x |> tpl "ADD"
            | Choice3Of3 x -> deleteAttributeUpdate (fstT nameValue) x |> tpl "REMOVE")
        >> Collection.groupBy fstT
        >> Collection.mapSnd (Seq.map sndT)
        >> Seq.map (fun struct (k, vs) -> combineUpdates k vs)
        >> AttributeUpdateExpression.concat

    let private buildFromAttributeUpdates (req: UpdateItemRequest<_>) =
        req.AttributeUpdates
        ?|? Map.empty
        |> buildFromAttributeUpdates'
        |> flip1To3 AttributeUpdateExpression.asOutput req.UpdateExpression

    let private basicNone = Basic None
    let input logger (req: UpdateItemRequest<_>) =

        let struct (filterExpression, struct (addNames, addValues)) =
            buildUpdateConditionExpression (req.ConditionalOperator ?|? ConditionalOperator.AND) (noneifyStrings req.ConditionExpression) req.Expected
            ?|> mapFst ValueSome
            ?|? struct (ValueNone, struct (id, id))

        let struct (updateExpression, struct (names, values)) =
            buildFromAttributeUpdates req logger
            ?|> mapFst ValueSome
            ?|? struct (req.UpdateExpression, struct (Map.empty, Map.empty))

        { key = req.Key <!!> nameof req.Key
          updateExpression = updateExpression
          conditionExpression =
              { conditionExpression = filterExpression
                tableName = req.TableName <!!> nameof req.TableName
                returnValues = req.ReturnValues ?|> mapReturnValues2 ?|? basicNone
                expressionAttrNames =
                    req.ExpressionAttributeNames
                    ?|? Map.empty
                    |> MapUtils.concat2 names
                    |> Maybe.expectSome
                    |> addNames
                expressionAttrValues =
                    req.ExpressionAttributeValues
                    ?|? Map.empty
                    |> MapUtils.concat2 values
                    |> Maybe.expectSome
                    |> addValues } } : UpdateItemArgs

    let output databaseId item: UpdateItemResponse<_> =

        { Attributes = item
          ConsumedCapacity = ValueNone 
          ItemCollectionMetrics =  ValueNone
          ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
          ContentLength = !!<Shared.ResponseHeaders.contentLength
          HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

module Transact =

    module Get =

        let private inputs' (req: Get<AttributeValue>) =

            { keys = req.Key <!!> nameof req.Key |> Array.singleton
              maxPageSizeBytes = System.Int32.MaxValue
              conditionExpression =
                  { tableName = req.TableName <!!> nameof req.TableName
                    conditionExpression = ValueNone
                    expressionAttrValues = Map.empty
                    expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty
                    returnValues = req.ProjectionExpression
                        |> ValueOption.map (ValueSome >> ProjectedAttributes)
                        |> ValueOption.defaultValue AllAttributes }} : GetItemArgs

        let input (req: TransactGetItemsRequest<AttributeValue>) =
            match req.TransactItems with
            | ValueSome x when x.Length > Settings.TransactReadSettings.MaxItemsPerRequest ->
                ClientError.clientError $"The limit on TransactGetItems is {Settings.TransactReadSettings.MaxItemsPerRequest} records. You can change this value by modifying {nameof Settings}.{nameof Settings.TransactReadSettings}.{nameof Settings.TransactReadSettings.MaxItemsPerRequest}"
            | ValueSome x -> x |> Seq.map _.Get |> Maybe.traverse |> Seq.map inputs'
            | ValueNone -> Seq.empty

        let private output' (selectOutput: GetResponseData) =

            let item = selectOutput.items |> Collection.tryHead
            let size =
                item
                ?>>= ItemSize.tryCreate
                ?|> ItemSize.size
                ?|? 0   // ItemSize.tryCreate shouldn't return None

            ({ Item = item }: ItemResponse<AttributeValue>)
            |> flip tpl size

        let output _ (selectOutput: GetResponseData seq): TransactGetItemsResponse<AttributeValue> =

            let struct (responses, size) =
                selectOutput
                |> Seq.map output'
                |> Collection.unzip
                |> mapSnd Seq.sum

            if size > 4_000_000
            then ClientError.clientError "The limit on data size from TransactGetItems is 4MB"

            { Responses = !!<responses
              ConsumedCapacity = ValueNone
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

    module Write =
        let private put (req: Put<AttributeValue>) =
            // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

            { item = req.Item <!!> nameof req.Item
              conditionExpression =
                  { conditionExpression = noneifyStrings req.ConditionExpression
                    tableName = req.TableName <!!> nameof req.TableName
                    returnValues = None
                    expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty
                    expressionAttrValues = req.ExpressionAttributeValues ?|? Map.empty } } : PutItemArgs<_>

        let private delete (req: Delete<AttributeValue>) =
            // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

            { key = req.Key <!!> "Delete.Key"
              conditionExpression =
                  { conditionExpression = noneifyStrings req.ConditionExpression
                    tableName = req.TableName <!!> nameof req.TableName
                    returnValues = None
                    expressionAttrNames = req.ExpressionAttributeNames  ?|? Map.empty
                    expressionAttrValues = req.ExpressionAttributeValues  ?|? Map.empty } } : DeleteItemArgs<_>

        let private basicNone = Basic None

        let private update (req: Update<AttributeValue>) =
            // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

            { key = req.Key <!!> "Update.Key"
              updateExpression = noneifyStrings req.UpdateExpression
              conditionExpression =
                  { conditionExpression = noneifyStrings req.ConditionExpression
                    tableName = req.TableName <!!> nameof req.TableName
                    returnValues = basicNone
                    expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty
                    expressionAttrValues = req.ExpressionAttributeValues ?|? Map.empty } } : UpdateItemArgs

        let private condition (req: ConditionCheck<AttributeValue>) =
            // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

            { keys = req.Key <!!>  "ConditionCheck.Key" |> Array.singleton
              maxPageSizeBytes = System.Int32.MaxValue
              conditionExpression =
                  { conditionExpression = noneifyStrings req.ConditionExpression
                    tableName = req.TableName <!!> nameof req.TableName
                    returnValues = ProjectedAttributes ValueNone
                    expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty
                    expressionAttrValues = req.ExpressionAttributeValues ?|? Map.empty } } : GetItemArgs

        let private thereCanOnlyBeOne (struct (struct (w, x), struct (y, z)) & args) =
            [
                w |> ValueOption.count
                x |> ValueOption.count
                y |> ValueOption.count
                z |> ValueOption.count
            ]
            |> List.sum
            |> function
                | 1 -> ()
                | x -> ClientError.clientError $"Exactly 1 of [Put; Delete; Update; ConditionCheck] must be set"

            args

        let private getCommand (x: TransactWriteItem<AttributeValue>) =

            struct (
                struct (
                    x.Put ?|> put,
                    x.Delete ?|> delete),
                struct (
                    x.Update ?|> update,
                    x.ConditionCheck ?|> condition)) |> thereCanOnlyBeOne

        let input (req: TransactWriteItemsRequest<AttributeValue>) =
            let struct (struct (put, delete), struct (update, condition)) =
                req.TransactItems
                ?|? []
                |> Seq.map getCommand
                |> Collection.unzip
                |> mapFst Collection.unzip
                |> mapSnd Collection.unzip

            { puts = put |> Maybe.traverse |> List.ofSeq
              deletes = delete |> Maybe.traverse |> List.ofSeq
              updates = update |> Maybe.traverse |> List.ofSeq
              idempotencyKey = req.ClientRequestToken |> noneifyStrings 
              conditionCheck = condition |> Maybe.traverse |> List.ofSeq } : Database.TransactWrite.TransactWrites

        let private sizeEstimateRangeGB =
            [ fstT Settings.TransactWriteSettings.SizeRangeEstimateResponse
              sndT Settings.TransactWriteSettings.SizeRangeEstimateResponse ] |> ValueSome

        let private itemCollectionMetrics v =
            { ItemCollectionKey = ValueSome v
              SizeEstimateRangeGB = sizeEstimateRangeGB }: ItemCollectionMetrics<AttributeValue>

        let output _ (modifiedTables: Map<string, Map<string, AttributeValue> list>): TransactWriteItemsResponse<AttributeValue> =

            let metrics = 
                modifiedTables
                |> Map.map (
                    Seq.map itemCollectionMetrics
                    >> List.ofSeq
                    |> asLazy)

            { ItemCollectionMetrics = ValueSome metrics
              ConsumedCapacity = ValueNone
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

module Batch =

    [<Struct; IsReadOnly>]
    type BatchItemKey =
        { databaseId: DatabaseId
          tableName: string
          userProvidedKey: string }

    let private suppressMessage =
        $"TestDynamoClient.SetAwsAccountId or by "
        + $"setting {nameof Settings}.{nameof Settings.SupressErrorsAndWarnings}.{nameof Settings.SupressErrorsAndWarnings.AwsAccountIdErrors} = true"

    let private buildKey defaultDatabaseId awsAccountId key =
        AwsUtils.parseTableArn key
        ?|> (fun struct (x, y, z) -> struct (y, { databaseId = { regionId = x }; tableName = z; userProvidedKey = key}) |> ValueSome)
        |> ValueOption.defaultWith (fun _ ->
            AwsUtils.parseTableName key
            ?|> (fun x -> struct (awsAccountId, { databaseId = defaultDatabaseId; tableName = x; userProvidedKey = key})))
        |> ValueOption.defaultWith (fun _ -> $"Cannot parse table name or ARN {key}" |> ClientError.clientError)

    let private reKey awsAccountId defaultDatabaseId (req: struct (string * _) seq) =

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
            ] |> Str.join "" |> ClientError.clientError
        | [_] -> reKeyed
        | xs ->
            [
                $"Multiple account ids found {xs}. The expected aws account "
                $"id is {awsAccountId}. You can fix this issue by calling "
                suppressMessage
            ] |> Str.join "" |> ClientError.clientError

    module Get =

        type BatchGetValue =
            { keys: Map<string, AttributeValue> array
              consistentRead: bool
              projectionExpression: string voption
              expressionAttrNames: Map<string, string>
              /// <summary>Not used by query engine. Only used to pass results back to client</summary>
              attributesToGetRedux: string list voption }

        type BatchGetRequest =
            { requests: Map<BatchItemKey, BatchGetValue> }

        type BatchGetResponse =
            { notProcessed: Map<BatchItemKey, BatchGetValue>
              found: Map<BatchItemKey, Map<string, AttributeValue> array> }

        let toBatchGetValue (req: KeysAndAttributes<AttributeValue>): BatchGetValue =

            let struct (returnValues, addExprAttrNames) = Get.buildProjection (noneifyStrings req.ProjectionExpression) req.AttributesToGet

            { keys = req.Keys ?|> Array.ofSeq ?|? [||]
              consistentRead = req.ConsistentRead ?|? false
              attributesToGetRedux = req.AttributesToGet
              projectionExpression =
                  match returnValues with
                  | Count -> ValueNone
                  | AllAttributes -> ValueNone
                  | ProjectedAttributes x -> x
              expressionAttrNames = req.ExpressionAttributeNames ?|? Map.empty |> addExprAttrNames }

        let fromBatchGetValue (x: BatchGetValue): KeysAndAttributes<AttributeValue> =

            //TODO: array to list
            { Keys = !!< (List.ofArray x.keys)
              ConsistentRead = !!<x.consistentRead
              ProjectionExpression = x.projectionExpression
              ExpressionAttributeNames = !!<x.expressionAttrNames
              AttributesToGet = x.attributesToGetRedux }: KeysAndAttributes<AttributeValue>

        let input awsAccountId defaultDatabaseId (req: BatchGetItemRequest<AttributeValue>) =
            let totalCount =
                req.RequestItems
                ?|> Seq.sumBy (
                    _.Value.Keys
                    ??|> Seq.length
                    ??|? 0)
                ?|? 0

            if totalCount > 100
            then ClientError.clientError "Max allowed items is 100 for the BatchGetItem call."

            let requests =
                req.RequestItems
                ?|? Map.empty 
                |> Seq.map kvpToTuple
                |> reKey awsAccountId defaultDatabaseId
                |> Seq.map (
                    mapFst sndT
                    >> mapSnd toBatchGetValue)
                |> MapUtils.fromTuple

            { requests = requests }

        let output _ (selectOutput: BatchGetResponse) =
            { Responses =
                selectOutput.found
                |> MapUtils.toSeq
                |> Seq.map (
                    mapFst _.userProvidedKey
                    // TODO: array to list
                    >> mapSnd List.ofArray)
                |> List.ofSeq
                |> MapUtils.ofSeq
                |> ValueSome
              UnprocessedKeys = 
                selectOutput.notProcessed
                |> MapUtils.toSeq
                |> Seq.map (
                    mapFst _.userProvidedKey
                    >> mapSnd fromBatchGetValue)
                |> MapUtils.ofSeq
                |> ValueSome
              ConsumedCapacity = ValueNone
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }: BatchGetItemResponse<AttributeValue>

    module Write =

        [<Struct; IsReadOnly>]
        type Write =
            | Put of a: PutItemArgs<Map<string, AttributeValue>>
            | Delete of DeleteItemArgs<Map<string, AttributeValue>>

        type BatchWriteRequest =
            { requests: Map<BatchItemKey, Write list> }

        type BatchWriteResponse =
            { notProcessed: Map<BatchItemKey, Write list> }
              // low priority feature
              // processedPartitions: Map<BatchItemKey, struct(string * AttributeValue) array>

        let private put tableName (req: PutRequest<AttributeValue>) =

            { item = req.Item <!!> nameof req.Item
              conditionExpression =
                  { conditionExpression = ValueNone
                    tableName = tableName
                    returnValues = BasicReturnValues.None
                    expressionAttrNames = Map.empty
                    expressionAttrValues = Map.empty } }
            |> Put

        let private delete tableName (req: DeleteRequest<AttributeValue>) =

            { key = req.Key <!!> nameof req.Key
              conditionExpression =
                  { conditionExpression = ValueNone
                    tableName = tableName
                    returnValues = BasicReturnValues.None
                    expressionAttrNames = Map.empty
                    expressionAttrValues = Map.empty } }
            |> Delete

        let private toBatchWriteValue tableName (req: WriteRequest<AttributeValue>) =
            match struct (req.DeleteRequest, req.PutRequest) with
            | ValueNone, ValueNone -> ValueNone
            | ValueNone, ValueSome p -> put tableName p |> ValueSome
            | ValueSome d, ValueNone -> delete tableName d |> ValueSome
            | _ -> ClientError.clientError "Cannot specify PutRequest and DeleteRequest on a single WriteRequest"

        let private fromBatchWriteValue (req: Write): WriteRequest<AttributeValue> =
            match req with
            | Put x ->
                { DeleteRequest = ValueNone
                  PutRequest = { Item = x.item |> ValueSome } |> ValueSome }
            | Delete x ->
                { PutRequest = ValueNone
                  DeleteRequest = { Key = x.key |> ValueSome } |> ValueSome }

        let private maps struct (tableName, req: WriteRequest<AttributeValue> seq) =
            req |> Seq.map (toBatchWriteValue tableName) |> Maybe.traverse |> List.ofSeq

        let input awsAccountId defaultDatabaseId (req: BatchWriteItemRequest<AttributeValue>) =
            let totalCount =
                req.RequestItems
                ?|? Map.empty
                |> Seq.sumBy _.Value.Length

            if totalCount > Settings.BatchItems.MaxBatchWriteItems
            then
                [
                    $"Max allowed items is {Settings.BatchItems.MaxBatchWriteItems} for the BatchWriteItem call. "
                    $"You can change this value by modifying {nameof Settings}.{nameof Settings.BatchItems}.{nameof Settings.BatchItems.MaxBatchWriteItems}."
                ] |> Str.join "" |> ClientError.clientError

            let requests =
                req.RequestItems
                ?|? Map.empty
                |> Seq.map kvpToTuple
                |> reKey awsAccountId defaultDatabaseId
                |> Seq.map (fun struct (struct (_, k), v) -> struct (k, struct (k.tableName, v |> Collection.ofSeq)))
                |> Collection.mapSnd (maps)
                |> MapUtils.fromTuple

            { requests = requests }: BatchWriteRequest

        let private sizeEstimateRangeGB: double list voption = ValueSome [0; 1000]
        let asItemCollectionMetrics struct (pkName, pkValue) =
            { ItemCollectionKey =
                Map.empty
                |> Map.add pkName pkValue
                |> ValueSome
              SizeEstimateRangeGB = sizeEstimateRangeGB }: ItemCollectionMetrics<AttributeValue>

        let output _ (selectOutput: BatchWriteResponse): BatchWriteItemResponse<AttributeValue> =

            // low priority feature
            // output.ItemCollectionMetrics <-
            //     selectOutput.processedPartitions
            //     |> MapUtils.toSeq
            //     |> Seq.map (
            //         mapFst _.userProvidedKey
            //         >> mapSnd (
            //             Seq.map asItemCollectionMetrics
            //             >> Enumerable.ToList))
            //     |> CSharp.tplToDictionary

            { UnprocessedItems =
                selectOutput.notProcessed
                |> MapUtils.toSeq
                |> Seq.map (
                    mapFst _.userProvidedKey
                    >> mapSnd (
                        Seq.map fromBatchWriteValue
                        >> List.ofSeq))
                |> MapUtils.fromTuple
                |> ValueSome
              ConsumedCapacity = ValueNone
              ItemCollectionMetrics = ValueNone
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }: BatchWriteItemResponse<AttributeValue>