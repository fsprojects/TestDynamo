namespace TestDynamo.Client

open TestDynamo.GeneratedCode.Dtos
open TestDynamo
open TestDynamo.Api.FSharp
open TestDynamo.Client
open TestDynamo.Model
open Microsoft.Extensions.Logging
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model.ExpressionExecutors.Fetch
open Utils
open System.Runtime.CompilerServices

// TODO: scan this file for commented out code

module MultiClientOperations =

    let private globalOnly = "Some update table requests only work on GlobalDatabases. Please ensure that this TestDynamoClient was initiated with the correct args"

    type ApiDb = TestDynamo.Api.FSharp.Database

    let private chooseDatabase databaseId =
        Either.map2Of2 (fun struct (db: GlobalDatabase, _) ->
            match db.TryGetDatabase databaseId with
            | ValueNone -> ClientError.clientError $"No resources have been created in DB region {databaseId.regionId}"
            | ValueSome db -> db)
        >> Either.reduce

    [<Struct; IsReadOnly>]    
    type private ResponseAggregator<'found, 'notProcessed> =
        { notProcessed: Map<Item.Batch.BatchItemKey, 'found list>
          found: Map<Item.Batch.BatchItemKey, 'notProcessed list> }

    module private ResponseAggregator =

        let private addNotProcessed k v agg =
            let change _ v_old v =
                match v_old with
                | ValueNone -> [v]
                | ValueSome ks -> v::ks
            { agg with notProcessed = MapUtils.change change k v agg.notProcessed }: ResponseAggregator<_, _>

        let private addFound k v agg =
            let change _ v_old v =
                match v_old with
                | ValueNone -> [v]
                | ValueSome x -> v::x
            { agg with found =  MapUtils.change change k v agg.found }: ResponseAggregator<_, _>

        let rec private addResults key result = function
            | [] -> result
            | Either1 head::tail -> result |> addFound key head |> flip (addResults key) tail
            | Either2 head::tail -> result |> addNotProcessed key head |> flip (addResults key) tail

        let private execute'
            operation
            (database: ApiDb)
            struct (state, result: ResponseAggregator<_, _>)
            (struct (k: Item.Batch.BatchItemKey, v) & kv) =

            if database.Id <> k.databaseId then notSupported globalOnly

            operation database state kv
            |> mapSnd (addResults k result)

        let private execute operation database acc (struct (batchGetKey: Item.Batch.BatchItemKey, _) & inputs) =

            chooseDatabase batchGetKey.databaseId database
            |> execute' operation
            <| acc
            <| inputs

        let executeBatch operation initialState (database: Either<ApiDb, struct (GlobalDatabase * DatabaseId)>) (batchRequests: struct (Item.Batch.BatchItemKey * _) seq) =
            batchRequests
            |> Seq.fold (execute operation database) struct (initialState, { notProcessed = Map.empty; found = Map.empty })
            |> sndT

    module BatchGetItem =

        let private execute
            logger
            (database: ApiDb)
            remaining
            struct (k: Item.Batch.BatchItemKey, v: Item.Batch.Get.BatchGetValue) =

            match struct (remaining, database) with
            | remaining, _ when remaining <= 0 -> struct (remaining, [Either2 v])
            | remaining, db ->
                let args =
                    { maxPageSizeBytes = remaining
                      keys = v.keys
                      conditionExpression =
                          { tableName = k.tableName
                            returnValues =
                                v.projectionExpression
                                |> ValueOption.map (ValueSome >> ProjectedAttributes)
                                |> ValueOption.defaultValue AllAttributes
                            expressionAttrNames = v.expressionAttrNames
                            expressionAttrValues = Map.empty
                            conditionExpression = ValueNone } }: GetItemArgs

                let response = db.Get logger args
                [
                    if response.unevaluatedKeys.Length = 0
                        then ValueNone
                        else { v with keys = response.unevaluatedKeys } |> Either2 |> ValueSome
                    if response.items.Length = 0
                        then ValueNone
                        else response.items |> Either1 |> ValueSome
                ]
                |> Maybe.traverse
                |> List.ofSeq
                |> tpl (remaining - response.itemSizeBytes)

        let private noBatchGetValue =
            { keys = Array.empty
              consistentRead = false
              projectionExpression = ValueNone
              attributesToGetRedux = ValueNone 
              expressionAttrNames = Map.empty } : Item.Batch.Get.BatchGetValue

        let batchGetItem (database: Either<ApiDb, struct (GlobalDatabase * DatabaseId)>) logger (req: Item.Batch.Get.BatchGetRequest) =

            let requests =
                req.requests
                |> MapUtils.toSeq
                |> Seq.collect (function
                    // simulate inconsistent read by splitting req into smaller parts  
                    | struct (_, {consistentRead = true}) & x
                    | (_, {keys = [||]}) & x
                    | (_, {keys = [|_|]}) & x -> [x] |> Seq.ofList
                    | k, v -> v.keys |> Seq.map (fun keys -> struct (k, { v with keys = [|keys|] })))

            ResponseAggregator.executeBatch
            <| (execute logger)
            <| Settings.BatchItems.BatchGetItemMaxSizeBytes
            <| database
            <| requests
            |> fun x ->
                { notProcessed =
                      x.notProcessed
                      |> Map.map (fun _ -> function
                          | [] -> noBatchGetValue
                          | [x] -> x
                          | head::_ & values -> { head with keys = values |> Seq.collect (fun x -> x.keys) |> Array.ofSeq })
                  found =
                      x.found
                      |> Map.map (fun _ xs ->
                          Seq.collect id xs
                          |> Array.ofSeq) }: Item.Batch.Get.BatchGetResponse

    module BatchWriteItem =

        open Item.Batch.Write

        let tryExecute (logger: ILogger voption) f v =
            try
                f v |> Either1
            with
            | e ->
                logger ?|> (_.LogError(e, "")) |> ValueOption.defaultValue ()
                v |> Either2

        let private execute
            logger
            (database: ApiDb)
            remaining
            struct (k: Item.Batch.BatchItemKey, v: Write list) =

            match struct (remaining, database) with
            | remaining, _ when remaining <= 0 -> struct (remaining, [Either2 v])
            | remaining, db ->
                List.fold (fun struct (remaining, acc) ->
                    function
                    | Delete x ->
                        tryExecute logger (db.Delete logger) x
                        |> Either.map1Of2 ignoreTyped<Map<string,AttributeValue> voption>
                        |> Either.map2Of2 (Delete >> List.singleton)
                        |> flip Collection.prependL acc
                        |> tpl (remaining - ItemSize.calculate x.key)
                    | Put x ->
                        tryExecute logger (db.Put logger) x
                        |> Either.map1Of2 ignoreTyped<Map<string,AttributeValue> voption>
                        |> Either.map2Of2 (Put >> List.singleton)
                        |> flip Collection.prependL acc
                        |> tpl (remaining - ItemSize.calculate x.item)) struct (remaining, []) v

        let batchPutItem (database: Either<ApiDb, struct (GlobalDatabase * DatabaseId)>) logger (req: Item.Batch.Write.BatchWriteRequest) =

            let requests =
                req.requests
                |> MapUtils.toSeq

            ResponseAggregator.executeBatch
            <| (execute logger)
            <| Settings.BatchItems.BatchPutItemMaxSizeBytes
            <| database
            <| requests
            |> _.notProcessed
            |> Map.map (fun _ -> List.collect id)
            |> fun x -> { notProcessed = x }: BatchWriteResponse

    module UpdateTable =

        let private updateTable' databaseOp globalOp request =

            let updateGlobal =
                match struct (request.globalTableData.replicaInstructions, globalOp) with
                | [], _ -> ValueNone
                | xs, ValueNone -> notSupported globalOnly
                | xs, ValueSome globalDb ->
                    fun () -> globalDb request.tableName request.globalTableData
                    |> ValueSome

            // do not run local table part if there is nothing to update
            // this allows op to recover from any synchronization errors if that is the intention of the request
            let eagerTableResult =
                if request.tableData = UpdateSingleTableData.empty
                then ValueNone
                else request.tableData |> ValueSome
                |> databaseOp request.tableName

            updateGlobal
            ?|> (apply ())
            |> ValueOption.defaultValue eagerTableResult

        let updateTable awsAccountId ddb databaseId databaseOp globalOp (req: UpdateTableRequest) =            
            Table.Local.Update.inputs req
            |> updateTable' databaseOp globalOp
            |> Table.Local.Update.output awsAccountId ddb databaseId

        let createGlobalTable awsAccountId ddb databaseId databaseOp globalOp (req: CreateGlobalTableRequest) =            
            Table.Global.Create.input req
            |> updateTable' databaseOp globalOp
            |> Table.Global.Create.output awsAccountId ddb databaseId

        let updateGlobalTable awsAccountId ddb databaseId databaseOp globalOp (req: UpdateGlobalTableRequest) =            
            Table.Global.Update.input req
            |> updateTable' databaseOp globalOp
            |> Table.Global.Update.output awsAccountId ddb databaseId