
/// <summary>
/// Tools for compiling and executing Expressions
/// Expressions can include some or all of
///     key condition expressions
///     filter expressions
///     projection expressions
///     update expressions
///
/// Any compiled expressions are cached 
/// </summary>
[<RequireQualifiedAccess>]
module TestDynamo.Model.ExpressionExecutors

open TestDynamo.Model.Compiler
open TestDynamo.Model.Compiler.Compilers
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators
open System.Runtime.CompilerServices

let private doubleLazyId x = (id |> asLazy |> asLazy) x
let inline private logOperation x = DatabaseLogger.logOperation "ItemSelector" x

module Fetch =

    [<Struct; IsReadOnly>]
    type ScanLimits =
        { /// <summary>
          /// <para>
          /// The max number of items to scan in a single operation
          /// If this number is reached before a single item is found, the operation will return nothing, but an
          /// exclusive start key is returned to resume the query.
          /// </para>
          ///
          /// <para>
          /// When scanning a GSI where multiple items have the same partition and sort key, all items will be included
          /// in the same page, regardless of this value
          /// </para>
          /// </summary>
          maxScanItems: int

          /// <summary>
          /// <para>
          /// The max number size of data dynamodb will return in a single page
          /// </para>
          /// <para>
          /// When scanning a GSI where multiple items have the same partition and sort key, all items will be included
          /// in the same page, regardless of this value
          /// </para>
          /// </summary>
          maxPageSizeBytes: int }

    type SelectTypes<'projection> =
        | AllAttributes
        | Count
        | ProjectedAttributes of 'projection

    module SelectTypes =
        let map f = function
            | AllAttributes -> AllAttributes
            | Count -> Count
            | ProjectedAttributes x -> f x |> ProjectedAttributes

    type FilterInput =
        { tableName: string
          indexName: string voption
          queryExpression: string voption
          filterExpression: string voption
          expressionAttrValues: Map<string, AttributeValue>
          expressionAttrNames: Map<string, string>
          limits: ScanLimits
          pageSize: int
          lastEvaluatedKey: Map<string, AttributeValue> voption
          forwards: bool
          selectTypes: SelectTypes<string voption> }

    [<Struct; IsReadOnly>]
    type QueryItems =
        | ScanFullIndex
        | ExpressionString of e: string
        | ScannedItems of is: Item seq

    [<Struct; IsReadOnly>]
    type QueryConditionAndProjectItems =
        | QueryItems of QueryItems
        | ExecuteConditionOnMissingItem

        with
        static member queryableOnly = function
            | ExecuteConditionOnMissingItem -> ValueNone
            | QueryItems x -> ValueSome x

    type FetchInput =
        { tableName: string
          indexName: string voption
          queryExpression: QueryItems
          filterExpression: string voption
          // update is not used by fetch algorithm, however it is required
          // to validate all expressionAttrNames and expressionAttrValues are used
          updateExpression: string voption
          expressionAttrValues: Map<string, AttributeValue>
          expressionAttrNames: Map<string, string>
          limits: ScanLimits
          pageSize: int
          lastEvaluatedKey: Map<string, AttributeValue> voption
          forwards: bool
          selectTypes: SelectTypes<string voption> }

    [<Struct; IsReadOnly>]
    type EvaluatedKeys =
        { lastEvaluatedKey: Map<string, AttributeValue>

          /// <summary>
          /// Will be populated if the query is not complete and the next page is not empty 
          /// </summary>
          nextUnevaluatedItem: Item voption }

    type FetchOutput =
        { items: Map<string, AttributeValue> array
          resultCount: int
          scannedCount: int
          scannedSize: int

          /// <summary>
          /// will be None if there are no more items to evaluate 
          /// </summary>
          evaluatedKeys: EvaluatedKeys voption }

        with
        member private this.complete = this.evaluatedKeys ?|> _.lastEvaluatedKey ?|> Map.isEmpty |> ValueOption.defaultValue true

        override this.ToString() = $"Scanned: {this.scannedCount}, found: {this.items |> Array.length}, complete: {this.complete}"

    module private LastEvaluatedKey =
        let fromInputToken (index: Index) (key: Map<string, AttributeValue>) =
            let k = Index.getKeyConfig index

            let pkName = KeyConfig.partitionKeyName k
            let pk = 
                pkName
                |> flip MapUtils.tryFind key
                |> ValueOption.defaultWith (fun _ -> clientError $"Partition key {pkName} not specified in ExclusiveStartKey")

            let sk =
                KeyConfig.sortKeyName k
                ?|> (fun skName ->
                    MapUtils.tryFind skName key
                    |> ValueOption.defaultWith (fun _ -> clientError $"Sort key {skName} not specified in ExclusiveStartKey"))

            match struct (Map.count key, sk) with
            | 2, ValueSome _ 
            | 1, ValueNone -> struct (pk, sk)
            | _ -> clientError $"Invalid ExclusiveStartKey. Expected {KeyConfig.keyNames k}"

        let toOutputToken index item =
            let sk =
                ValueSome tpl
                <|? Index.sortKeyName index
                <|? Index.sortKey item index

            Map.empty
            |> Map.add (Index.partitionKeyName index) (Index.partitionKey item index)
            |> flip (ValueOption.fold (flip <| fun struct (k, v) -> Map.add k v)) sk

    module private Validation =

        let findPlaceholdersAndAttributes =

            let find =
                AstNode.depthFirstSearch (fun _ (struct (accNames, accVals) & s) ->
                    function
                    | AstNode.ExpressionAttrValue x -> struct (accNames, x::accVals)
                    | AstNode.Accessor (AccessorType.ExpressionAttrName x) -> struct (x::accNames, accVals)
                    | AstNode.Synthetic (AccessorPath (AccessorType.ExpressionAttrName x::tail)) -> struct (x::accNames, accVals)
                    | _ -> s) struct ([], [])

            let postProcess = Seq.distinct >> Array.ofSeq

            Seq.map find
            >> Seq.fold (fun struct (accNames1, accVals1) struct (accNames2, accVals2) ->
                struct (accNames2 @ accNames1, accVals2 @ accVals1)) struct ([], [])
            >> mapFst postProcess
            >> mapSnd postProcess

        let validatePlaceholdersAndAttributes struct (expressionAttrNames, expressionAttrValues) (qParams: ExpressionParams) =
            let namesNotFound =
                qParams.expressionAttrNames.Keys
                |> Seq.filter (flip Array.contains expressionAttrNames >> not)

            let valuesNotFound =
                qParams.expressionAttrValues.Keys
                |> Seq.filter (flip Array.contains expressionAttrValues >> not)

            let err =
                Seq.concat [namesNotFound; valuesNotFound]
                |> Str.join ", "

            if err.Length > 0 then clientError $"Expression attribute names or values were not used: {err}"

    module private OpenIndex =

        let scanIndex (queryParams: ExpressionParams) index =
            let lastEvalSk = queryParams.lastEvaluatedKey ?>>= sndT
            let openPartition = Partition.scan lastEvalSk queryParams.forwards

            let lastEvalPk = queryParams.lastEvaluatedKey ?|> fstT
            Index.scan lastEvalPk queryParams.forwards true index
            |> Seq.collect openPartition

        let scanIndexQ: QueryParams -> _ =
            _.expressionParams >> scanIndex

    module private FilterItems =

         // mutability optimization. Prevents doing a lot of allocations on lists of lists
         let applyFilters logger struct (filter, mapResults, input: ExpressionParams, index, maxReturnItems, limits) (blocks: PartitionBlock seq) =

             let mutable maxScanItems =
                 if limits.maxScanItems < 1 then nameof limits.maxScanItems |> sprintf "%s must be greater than 0" |> clientError
                 else limits.maxScanItems
             // note: this includes items scanned but filtered out, so is different to max page size
             let mutable scannedItemSize = 0
             let mutable maxPageSizeBytes =
                 if limits.maxPageSizeBytes < 1 then nameof limits.maxPageSizeBytes |> sprintf "%s must be greater than 0" |> clientError
                 else limits.maxPageSizeBytes
             let mutable lastEvaluatedItem = ValueNone
             let mutable maxReturnItems = maxReturnItems
             let mutable scanned = 0
             let mutable manualKill = false

             let filterParams =
                 { logger = logger
                   expressionAttributeNameLookup = flip MapUtils.tryFind input.expressionAttrNames
                   expressionAttributeValueLookup = flip MapUtils.tryFind input.expressionAttrValues }: FilterTools

             let results =
                 seq {
                     let mutable lastEvaluated = ValueNone
                     use enm = blocks.GetEnumerator()
                     while not manualKill && enm.MoveNext() do

                         let mutable items = PartitionBlock.toList enm.Current
                         while items <> [] do
                             match items with
                             | [] -> ()
                             | head::tail ->
                                 let itemSize = Item.size head
                                 scannedItemSize <- scannedItemSize + itemSize
                                 items <- tail
                                 maxScanItems <- maxScanItems - 1
                                 scanned <- scanned + 1
                                 lastEvaluated <- ValueSome head

                                 let itemData = { item = head; filterParams = filterParams }: ItemData
                                 if filter itemData
                                 then
                                     Logger.debug1 "Item included %A" head logger
                                     maxReturnItems <- maxReturnItems - 1
                                     maxPageSizeBytes <- maxPageSizeBytes - itemSize
                                     yield itemData
                                 elif maxReturnItems > 0 then
                                     Logger.debug1 "Item excluded %A" head logger

                         // do not kill mid-partition block
                         // these items have the same PK and SK and cannot
                         // be split without changing paging
                         manualKill <- maxScanItems <= 0 || maxPageSizeBytes <= 0 || maxReturnItems <= 0

                     if manualKill |> not then
                         Logger.log0 "Search complete" logger
                         lastEvaluated <- ValueNone
                     elif ValueOption.isNone lastEvaluated then
                         Logger.log0 "No items found" logger 
                     else
                         Logger.log0 "Seach paged" logger

                     let next = if enm.MoveNext() then enm.Current |> PartitionBlock.peek |> ValueSome else ValueNone
                     lastEvaluatedItem <- lastEvaluated ?|> (flip tpl next)
                 }

             let struct (items, count) = mapResults results
             let evaluatedKeys =
                lastEvaluatedItem
                ?|> mapFst (LastEvaluatedKey.toOutputToken index)
                ?|> fun struct (x, y) -> { lastEvaluatedKey = x; nextUnevaluatedItem = y }

             { items = items
               resultCount = count
               scannedSize = scannedItemSize 
               scannedCount = scanned
               evaluatedKeys = evaluatedKeys }

    let private arrayWithLength = Array.ofSeq >> tplDouble >> mapSnd Array.length

    let private defaultMapper = ItemData.getItem >> Item.attributes |> Seq.map
    let private mapResults selectTypes results =
        match selectTypes with
        | AllAttributes -> defaultMapper results |> arrayWithLength
        | ProjectedAttributes f -> results |> Seq.map (_.item >> f) |> arrayWithLength
        | Count -> struct (Array.empty, Seq.length results)

    let private returnTrue = true |> asLazy |> asLazy

    let private projectNothing: struct (AstNode voption * (FilterTools -> Item -> Map<string,AttributeValue>)) =
        struct (ValueNone, Map.empty |> asLazy |> asLazy)

    let private buildFetch' buildMessage executeMessage (req: FetchInput) =

        fun logger struct (index, tableAttributeNames) ->

            let keyConfig = Index.getKeyConfig index

            let struct (queryAst, openIndex) =
                match req.queryExpression with
                | ExpressionString expr ->
                    expr
                    |> Logger.logFn1 "Compiling key condition expression %A" logger
                    |> flip (OpenIndex.compileQuery logger) keyConfig
                    |> mapFst ValueSome
                    |> mapSnd Either1
                | ScannedItems x -> struct (ValueNone, Either2 x)
                | ScanFullIndex -> struct (ValueNone, Either1 OpenIndex.scanIndexQ)

            let struct (filterAst, filterItem) =
                let keys =
                    match req.queryExpression with
                    | ScannedItems _
                    | ScanFullIndex -> ValueNone
                    | ExpressionString _ -> ValueSome keyConfig

                req.filterExpression
                ?|> (
                    Logger.logFn1 "Compiling filter expression %A" logger
                    >> Filters.compile logger
                           { invalidTableKeyUpdateAttributes = ValueNone
                             invalidIndexKeyAttributes = keys
                             tableScalarAttributes = tableAttributeNames }
                    >> mapFst ValueSome)
                |> ValueOption.defaultValue struct (ValueNone, returnTrue)

            let updateAst =
                req.updateExpression
                ?|> (
                    Logger.logFn1 "Compiling update expression %A" logger
                    >> Updates.compileAst logger)

            let struct (projectorAst, projectorSelect) =
                match req.selectTypes with
                | ProjectedAttributes x ->
                    ValueOption.map (
                        Logger.logFn1 "Compiling projection expression %A" logger
                        >> Projections.compile logger tableAttributeNames
                        >> mapFst ValueSome
                        >> mapSnd (flip (>>>) _.result)) x
                    |> ValueOption.defaultValue projectNothing
                    |> mapSnd ProjectedAttributes
                | Count -> struct (ValueNone, Count)
                | AllAttributes -> struct (ValueNone, AllAttributes)

            let inputValidator =
                let combineAst = ValueOption.fold (flip Collection.prependL) |> flip

                combineAst queryAst []
                |> combineAst filterAst
                |> combineAst projectorAst
                |> combineAst updateAst
                |> Validation.findPlaceholdersAndAttributes
                |> Validation.validatePlaceholdersAndAttributes

            let queryAndFilter logger (q: ExpressionParams) =

                inputValidator q 
                let lookups =
                    { logger = logger
                      expressionAttributeNameLookup = flip MapUtils.tryFind q.expressionAttrNames
                      expressionAttributeValueLookup = flip MapUtils.tryFind q.expressionAttrValues }

                let filter = filterItem lookups
                let project = SelectTypes.map (apply lookups) projectorSelect

                Either.map1Of2 (fun idx ->
                    Logger.logFn1 "Opening index %A" logger index
                    |> idx { expressionParams = q; logger = logger }) openIndex
                |> Either.map2Of2 (Seq.map PartitionBlock.create)
                |> Either.reduce
                |> Logger.logFn0 "Scanning index results" logger
                |> FilterItems.applyFilters logger struct (filter, (mapResults project), q, index, req.pageSize, req.limits)
                |> Logger.logFn1 "Fetch output - %O" logger

            queryAndFilter
            |> logOperation executeMessage
        |> logOperation buildMessage

    let private buildFetch = buildFetch' "BUILD FETCH" "EXECUTE FETCH"
    let private buildConditionAndProject = buildFetch' "BUILD CONDITION AND PROJECT" "EXECUTE CONDITION AND PROJECT"

    let executeFetch' build (req: FetchInput) logger index tableAttributeNames =

        let q = build req logger struct (index, tableAttributeNames)

        { expressionAttrValues = req.expressionAttrValues
          expressionAttrNames = req.expressionAttrNames
          forwards = req.forwards
          lastEvaluatedKey = req.lastEvaluatedKey ?|> LastEvaluatedKey.fromInputToken index }
        |> q logger

    let executeFetch = executeFetch' buildFetch
    let executeConditionAndProject = executeFetch' buildConditionAndProject

module Update =

    [<IsReadOnly; Struct>]
    type UpdateInput =
        { updateExpression: string
          expressionAttrValues: Map<string, AttributeValue>
          expressionAttrNames: Map<string, string> }

    let private expectHashMap = function
        | HashMap x -> x
        | _ -> serverError "Expected hash map"

    let compressSparseLists = HashMap >> AttributeValue.compressSparseLists >> expectHashMap

    let private returnInput = Map.empty |> asLazy |> asLazy
    let private buildUpdateProjector logger attributeNames =

        Seq.map ValidatedPath.unwrap
        >> Seq.map (List.map (function
            | ResolvedAttribute x -> Either1 x
            | ResolvedListIndex x -> Either2 x))
        >> Parser.generateAstFromPaths
        >> ValueOption.map (
            Projections.compileFromAst logger attributeNames
            >> sndT
            >>> (ExpressionCompiler.CompiledExpression.writer >> flip >> apply Map.empty >>> _.result))
        >> ValueOption.defaultValue returnInput

    let inline private writerResultAsTpl (x: ExpressionCompiler.WriterResult) = struct (x.result, x.mutatedPaths)
    let private buildUpdate (req: UpdateInput) =

        fun logger tableKeys tableAttributeNames ->
            let updaterSelect =
                Logger.logFn1 "Compiling update expression %A" logger req.updateExpression
                |> Updates.compile logger tableKeys
                |> sndT

            // Skip input validation (like in fetch)
            // A fetch will be executed when projecting outputs, and validation is done there

            let mutate logger () =

                let tools =
                    { logger = logger
                      expressionAttributeNameLookup = flip MapUtils.tryFind req.expressionAttrNames
                      expressionAttributeValueLookup = flip MapUtils.tryFind req.expressionAttrValues }

                let projector = flip (buildUpdateProjector logger tableAttributeNames) tools |> lazyF

                tools
                |> updaterSelect
                >> writerResultAsTpl
                >> mapFst compressSparseLists
                >> mapSnd projector
                >> Logger.logFn0 "Update applied" logger

            mutate
            |> logOperation "EXECUTE UPDATE"
            |> flip
            |> apply ()
        |> logOperation "BUILD UPDATE"

    let executeUpdate (req: UpdateInput) logger =

        buildUpdate req logger >>> apply logger