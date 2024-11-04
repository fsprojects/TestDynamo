module DynamoDbInMemory.Client.PutItem

open System.Collections.Generic
open System.Linq
open System.Runtime.CompilerServices
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory
open DynamoDbInMemory.Client
open DynamoDbInMemory.Client.GetItem.Batch
open DynamoDbInMemory.Model
open DynamoDbInMemory.Utils
open DynamoDbInMemory.Data.Monads.Operators
open DynamoDbInMemory.Client.ItemMapper
open DynamoDbInMemory.Client.Query

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue
type MList<'a> = System.Collections.Generic.List<'a>

let mapReturnValues (returnValues: ReturnValue) =
    returnValues
    |> CSharp.toOption
    ?|> (function
        | x when x.Value = ReturnValue.NONE.Value -> None
        | x when x.Value = ReturnValue.ALL_OLD.Value -> AllOld
        | x -> clientError "Only NONE and ALL_OLD are supported as ReturnValues")
    |> ValueOption.defaultValue None

let inputs1  (req: PutItemRequest) =
    // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

    if req.Expected <> null && req.Expected.Count <> 0 then notSupported "Legacy Expected parameter is not supported"
    if req.ConditionalOperator <> null then notSupported "Legacy ConditionalOperator parameter is not supported"

    { item = ItemMapper.itemFromDynamodb "$" req.Item
      conditionExpression =
          { conditionExpression = req.ConditionExpression |> filterExpression
            tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
            returnValues = mapReturnValues req.ReturnValues
            expressionAttrNames = req.ExpressionAttributeNames |> expressionAttrNames
            expressionAttrValues = req.ExpressionAttributeValues |> expressionAttrValues } } : PutItemArgs<_>

let inputs2 struct (
    tableName: string,
    item: Dictionary<string, DynamoAttributeValue>) =

    PutItemRequest (tableName, item) |> inputs1

let inputs3 struct (
    tableName: string,
    item: Dictionary<string, DynamoAttributeValue>,
    returnValue: ReturnValue) =

    PutItemRequest (tableName, item, returnValue) |> inputs1

let private newDict () = Dictionary<_, _>()
let output databaseId items =

    let output = Shared.amazonWebServiceResponse<PutItemResponse>()
    output.Attributes <-
        items
        ?|> itemToDynamoDb
        |> ValueOption.defaultWith newDict
    output

module BatchWrite =

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

    let private put tableName (req: PutRequest) =

        { item = ItemMapper.itemFromDynamodb "$" req.Item
          conditionExpression =
              { conditionExpression = ValueNone
                tableName = tableName
                returnValues = BasicReturnValues.None
                expressionAttrNames = Map.empty
                expressionAttrValues = Map.empty } }
        |> Put

    let private delete tableName (req: DeleteRequest) =

        { key = ItemMapper.itemFromDynamodb "$" req.Key
          conditionExpression =
              { conditionExpression = ValueNone
                tableName = tableName
                returnValues = BasicReturnValues.None
                expressionAttrNames = Map.empty
                expressionAttrValues = Map.empty } }
        |> Delete

    let private toBatchWriteValue tableName (req: WriteRequest) =
        match struct (req.DeleteRequest, req.PutRequest) with
        | struct (null, null) -> ValueNone
        | null, p -> put tableName p |> ValueSome
        | d, null -> delete tableName d |> ValueSome
        | _ -> clientError "Cannot specify PutRequest and DeleteRequest on a single WriteRequest"

    let private fromBatchWriteValue (req: Write) =
        let wr = WriteRequest()
        match req with
        | Put x ->
            wr.PutRequest <-
                let pr = PutRequest()
                pr.Item <- itemToDynamoDb x.item
                pr
        | Delete x ->
            wr.DeleteRequest <-
                let pr = DeleteRequest()
                pr.Key <- itemToDynamoDb x.key
                pr

        wr

    let private maps struct (tableName, req: WriteRequest seq) =
        req |> Seq.map (toBatchWriteValue tableName) |> Maybe.traverse |> List.ofSeq

    let inputs1 awsAccountId defaultDatabaseId (req: BatchWriteItemRequest) =
        let totalCount =
            req.RequestItems
            |> CSharp.orEmpty
            |> Seq.sumBy _.Value.Count

        if totalCount > Settings.BatchItems.MaxBatchWriteItems
        then
            [
                $"Max allowed items is {Settings.BatchItems.MaxBatchWriteItems} for the BatchWriteItem call. "
                $"You can modify this with {nameof Settings}.{nameof Settings.BatchItems}.{nameof Settings.BatchItems.MaxBatchWriteItems}."
            ] |> Str.join "" |> clientError

        let requests =
            req.RequestItems
            |> CSharp.orEmpty
            |> Seq.map CSharp.kvpToTpl
            |> GetItem.Batch.reKey awsAccountId defaultDatabaseId
            |> Seq.map (fun struct (struct (_, k), v) -> struct (k, struct (k.tableName, v |> Collection.ofSeq)))
            |> Collection.mapSnd (maps)
            |> MapUtils.fromTuple

        { requests = requests }: BatchWriteRequest

    let inputs2 awsAccountId defaultDatabaseId (items: Dictionary<string, List<WriteRequest>>) =
        BatchWriteItemRequest items |> inputs1 awsAccountId defaultDatabaseId

    let asItemCollectionMetrics struct (pkName, pkValue) =
        let icm = ItemCollectionMetrics()

        icm.ItemCollectionKey <- Dictionary()
        icm.ItemCollectionKey.Add(pkName, attributeToDynamoDb pkValue)

        icm.SizeEstimateRangeGB <- List(2)
        icm.SizeEstimateRangeGB.Add(0)
        icm.SizeEstimateRangeGB.Add(1000)

        icm

    let output _ (selectOutput: BatchWriteResponse) =

        let output = Shared.amazonWebServiceResponse<BatchWriteItemResponse>()

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

        output.UnprocessedItems <- 
            selectOutput.notProcessed
            |> MapUtils.toSeq
            |> Seq.map (
                mapFst _.userProvidedKey
                >> mapSnd (
                    Seq.map fromBatchWriteValue
                    >> Enumerable.ToList))
            |> CSharp.tplToDictionary

        output