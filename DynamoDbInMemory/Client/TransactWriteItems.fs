module DynamoDbInMemory.Client.TransactWriteItems

open System.Linq
open Microsoft.FSharp.Core
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory
open DynamoDbInMemory.Utils
open DynamoDbInMemory.Data.Monads.Operators
open DynamoDbInMemory.Client
open DynamoDbInMemory.Model
open DynamoDbInMemory.Model.ExpressionExecutors.Fetch
open DynamoDbInMemory.Client.ItemMapper

//type MList<'a> = System.Collections.Generic.List<'a>

let private put (req: Put) =
    // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/
    
    { item = ItemMapper.itemFromDynamodb "$" req.Item
      conditionExpression =
          { conditionExpression = req.ConditionExpression |> Query.filterExpression
            tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
            returnValues = None
            expressionAttrNames = req.ExpressionAttributeNames |> Query.expressionAttrNames
            expressionAttrValues = req.ExpressionAttributeValues |> Query.expressionAttrValues } } : PutItemArgs<_>

let private delete (req: Delete) =
    // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/
    
    { key = ItemMapper.itemFromDynamodb "$" req.Key
      conditionExpression =
          { conditionExpression = req.ConditionExpression |> Query.filterExpression
            tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
            returnValues = None
            expressionAttrNames = req.ExpressionAttributeNames |> Query.expressionAttrNames
            expressionAttrValues = req.ExpressionAttributeValues |> Query.expressionAttrValues } } : DeleteItemArgs<_>

let private basicNone = Basic None

let private update (req: Update) =
    // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/
    
    { key = ItemMapper.itemFromDynamodb "$" req.Key
      updateExpression = req.UpdateExpression |> CSharp.mandatory "Either UpdateExpression must be set"
      conditionExpression =
          { conditionExpression = req.ConditionExpression |> Query.filterExpression
            tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
            returnValues = basicNone
            expressionAttrNames =
                req.ExpressionAttributeNames
                |> Query.expressionAttrNames
            expressionAttrValues =
                req.ExpressionAttributeValues
                |> Query.expressionAttrValues } } : UpdateItemArgs

let private condition (req: ConditionCheck) =
    // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/
    
    { keys = [|ItemMapper.itemFromDynamodb "$" req.Key|]
      maxPageSizeBytes = System.Int32.MaxValue
      conditionExpression =
          { conditionExpression = req.ConditionExpression |> Query.filterExpression
            tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
            returnValues = ProjectedAttributes ValueNone
            expressionAttrNames =
                req.ExpressionAttributeNames
                |> Query.expressionAttrNames
            expressionAttrValues =
                req.ExpressionAttributeValues
                |> Query.expressionAttrValues } } : GetItemArgs
    
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
        | x -> clientError $"Exactly 1 of [Put; Delete; Update; ConditionCheck] must be set"
    
    args
    
let private getCommand (x: TransactWriteItem) =
    
    struct (
        struct (
            x.Put |> CSharp.toOption ?|> put,
            x.Delete |> CSharp.toOption ?|> delete),
        struct (
            x.Update |> CSharp.toOption ?|> update,
            x.ConditionCheck |> CSharp.toOption ?|> condition)) |> thereCanOnlyBeOne

let inputs1 (req: TransactWriteItemsRequest) =
    let struct (struct (put, delete), struct (update, condition)) =
        CSharp.mandatory "TransactWriteItemsRequest cannot be null" req
        |> _.TransactItems
        |> CSharp.sanitizeSeq
        |> Seq.map getCommand
        |> Collection.unzip
        |> mapFst Collection.unzip
        |> mapSnd Collection.unzip
    
    { puts = put |> Maybe.traverse |> List.ofSeq
      deletes = delete |> Maybe.traverse |> List.ofSeq
      updates = update |> Maybe.traverse |> List.ofSeq
      idempotencyKey = req.ClientRequestToken |> CSharp.toNullOrEmptyOption 
      conditionCheck = condition |> Maybe.traverse |> List.ofSeq } : Database.TransactWrite.TransactWrites

let output _ (modifiedTables: Map<string, Map<string, AttributeValue> list>) =

    let metrics = 
        modifiedTables
        |> Map.map (fun k ->
            Seq.map (fun v ->        
                let metrics = ItemCollectionMetrics()
                metrics.SizeEstimateRangeGB <- MList<_>()
                metrics.SizeEstimateRangeGB.Add(fstT Settings.TransactWriteSettings.SizeRangeEstimateResponse)
                metrics.SizeEstimateRangeGB.Add(sndT Settings.TransactWriteSettings.SizeRangeEstimateResponse)
                metrics.ItemCollectionKey <- itemToDynamoDb v
                metrics)
            >> Enumerable.ToList)
        |> CSharp.toDictionary id id
        
    let output = Shared.amazonWebServiceResponse<TransactWriteItemsResponse>()
    output.ItemCollectionMetrics <- metrics
    
    output