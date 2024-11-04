namespace TestDynamo.Model

open System
open System.Runtime.CompilerServices
open System.Text.RegularExpressions
open TestDynamo
open TestDynamo.Data
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model

type private DTOffset = System.DateTimeOffset

[<Struct; IsReadOnly>]
type private DeletedItem =
    { deletedDate: DTOffset
      deleteId: IncrementingId
      partitionKey: AttributeValue
      sortKey: AttributeValue voption }

    with static member deleteItemId {deleteId = x} = x

[<Struct; IsReadOnly>]
type private DeletedItems =
    private
    | Dis of DeletedItem list

    with
    static member empty = Dis []

    static member unwrap (Dis x) = x

    static member private pruneRecentlyDeleted' = function
        | struct (logger, d, {deleteId = id; deletedDate = del}::tail & x) when d > del ->
            Logger.debug1 "Pruning item with delete id %A" id logger
            DeletedItems.pruneRecentlyDeleted' struct (logger, d, tail)
        | _, _, x -> x

    static member pruneRecentlyDeleted logger (Dis deletedItems) =
        let cutoff = DTOffset.Now - Settings.MaxReplicationTime
        DeletedItems.pruneRecentlyDeleted' struct (logger, cutoff, deletedItems)
        |> Dis

    static member private add' = function
        | struct (_, x, []) -> [x]
        | recursive, x, head::tail when head.deleteId.Value > x.deleteId.Value ->
            // non tail call should be fine here. In the unlikely case that an item should not go at the head
            // of this list, it will go a small number of items back
            assert (recursive < 500)
            if recursive >= 500
            then
                // In extreme cases, accept defeat and take sort performance hit (memory + cpu)
                x::head::tail |> List.sortWith (fun x y -> y.deleteId.Value - x.deleteId.Value)
            else
                head::DeletedItems.add' struct (recursive, x, tail)
        | recursive, x, xs -> x::xs

    static member add item (Dis x) = DeletedItems.add' struct (0, item, x) |> Dis

    static member tryGetSuperceedingDelete struct (item, keys) (Dis xs) =
        let hasSk = KeyConfig.sortKeyDefinition keys
        let attrs = Item.attributes item

        let itemId = item |> Item.internalId |> IncrementingId.value
        let superceeding = Seq.takeWhile (fun x -> x.deleteId.Value >= itemId) xs
        match Collection.tryHeadAndReturnUnmodified superceeding with
        | ValueNone, _ -> ValueNone
        | ValueSome _, items ->
            let inline filter struct (pk, sk) x = x.partitionKey = pk && x.sortKey = sk

            let addSortKey =
                if KeyConfig.sortKeyDefinition keys |> ValueOption.isSome
                then ValueOption.bind (fun pk ->
                    KeyConfig.optionalSortKey attrs keys
                    ?|> (ValueSome >> tpl pk))
                else ValueOption.map (flip tpl ValueNone)

            let filterAndMap =
                filter
                >> flip Seq.filter items
                >> Seq.map _.deleteId
                |> flip (?|>)

            KeyConfig.optionalPartitionKey attrs keys
            |> addSortKey
            |> filterAndMap
            ?>>= Collection.tryHead

[<Struct; IsReadOnly>]
type private IndexItemsData =
    { partitions: AvlTree<AttributeValue, Partition>
      itemCount: int
      version: int

      /// <summary>
      /// A short list of items recently deleted from the primary index used to perform CDC conflict checks
      /// List is cleared regularly
      /// </summary>
      recentlyDeleted: DeletedItems }

[<Struct; IsReadOnly>]
type private IndexItems =
    private
    | Ii of IndexItemsData

module private IndexItems =
    let empty = { partitions = AvlTree.empty; itemCount = 0; version = 0; recentlyDeleted = DeletedItems.empty } |> Ii

    let indexChangeResult change (Ii x) =
        {changeResult = change; dataVersion =  x.version }

    let itemCount (Ii x) = x.itemCount

    let partitions (Ii x) = x.partitions

    let version (Ii x) = x.version

    let recentlyDeleted (Ii x) = x.recentlyDeleted

    let private recordAndlogDelete logger x =
        Logger.debug0 "Recording delete" logger
        flip DeletedItems.add x

    /// <summary>
    /// Applies mutations to index data, update any related properties and build an IndexChangeResult
    ///  * Replace a mutated partition
    ///  * Update item counts and version
    ///  * Prune old recently deleted items
    ///  * Add any recently new deleted items (attempted or succesful)
    /// </summary>
    let afterMutation logger changes modifiedPartition keyConfig = function
        | Ii data ->
            let putModifiedPartition =
                modifiedPartition
                ?|> (fun struct (k, v) p ->
                    ValueOption.map (flip (AvlTree.add k) p) v
                    |> ValueOption.defaultWith (fun () -> AvlTree.remove k p))
                |> ValueOption.defaultValue id

            let inline deleteId struct (id: IncrementingId, _) = id.Value
            let itemCount = data.itemCount + ChangeResults.modificationCount changes
            let version = data.version + 1
            let recentlyDeleted =
                let pruned = DeletedItems.pruneRecentlyDeleted logger data.recentlyDeleted

                let now = DateTimeOffset.Now
                let realDeletes =
                    changes.OrderedChanges
                    |> Seq.map (fun x -> x.Deleted ?|> (tpl x.Id))
                    |> Maybe.traverse
                    |> Collection.mapSnd (Item.attributes >> flip KeyConfig.keyAttributesOnly keyConfig)
                    |> Seq.map (fun struct (id, keys) ->
                        { deletedDate = now
                          deleteId = id
                          partitionKey = KeyConfig.partitionKey keys keyConfig
                          sortKey = KeyConfig.sortKey keys keyConfig })

                let attemptedDeletes =
                    Seq.map (fun (del: DeleteAttemptData) ->
                      { deletedDate = now
                        deleteId = del.deleteId
                        partitionKey = sndT del.partitionKey
                        sortKey = ValueOption.map sndT del.sortKey }) changes.OrderedDeleteAttempts

                Collection.concat2 realDeletes attemptedDeletes
                |> Seq.fold (recordAndlogDelete logger) pruned

            { data with
                partitions = putModifiedPartition data.partitions
                itemCount = itemCount
                version = version
                recentlyDeleted = recentlyDeleted }
            |> tpl { changeResult = changes; dataVersion = version; }
            |> mapSnd Ii

type IndexName = string

type IndexType =
    | Primary
    | GlobalSecondary of IndexName
    | LocalSecondary of IndexName

type ProjectionType =
    | All
    // array faster than set in this case
    | Keys of string array
    | Attributes of string array

module ProjectionType =
    /// <summary>Returns None if all</summary>
    let cols = function
        | All -> ValueNone
        | Keys x -> ValueSome x
        | Attributes x -> ValueSome x
    
type IndexInfo =
    { compositeName: string
      indexType: IndexType
      keyConfig: KeyConfig
      projection: ProjectionType
      
      /// <summary>Primary indexes do not have an arn</summary>
      arn: (struct (AwsAccountId * RegionId) -> string) voption }

    with
    member this.isPrimary =
        match this.indexType with
        | Primary -> true
        | GlobalSecondary _ -> false
        | LocalSecondary _ -> false
        
    member this.isLocal =
        match this.indexType with
        | Primary -> false
        | GlobalSecondary _ -> false
        | LocalSecondary _ -> true

    member this.name =
        match this.indexType with
        | Primary -> ValueNone
        | GlobalSecondary x -> ValueSome x
        | LocalSecondary x -> ValueSome x

[<IsReadOnly; Struct>]
type private IndexResource =
    { info: IndexInfo
      data: IndexItems }
    with
    static member afterMutation logger (changeResult: ChangeResults) (indexValues: IndexResource) partitions =
        IndexItems.afterMutation logger changeResult partitions indexValues.info.keyConfig indexValues.data
        |> mapSnd (fun x -> { indexValues with data = x })

/// <summary>
/// An Index which contains Items
/// </summary>
[<Struct; IsReadOnly>]
type Index =
    private
    | Idx of IndexResource
    with

    override this.ToString() = match this with Idx x -> x.info.compositeName
    
type IndexKeys = (struct (string * string voption))

[<Struct; IsReadOnly>]
type CreateIndexData =
    { keys: IndexKeys
      projectionsAreKeys: bool
      projection: string list voption }

type IndexConfig =
    { keyConfig: KeyConfig
      tableKeyConfig: KeyConfig
      tableName: string
      indexName: string voption
      local: bool
      config: CreateIndexData }

[<RequireQualifiedAccess>]
module Index =
    let primaryIndexName = "PRIMARY"

    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
    let private validSecondaryIndexProp = Regex(@"^.{1,255}$", RegexOptions.Compiled)

    let private validateName name =
        AwsUtils.parseIndexName name
        |> ValueOption.defaultWith (fun _ -> clientError $"Invalid index name {name}")

    let buildProjectionCols (keys: KeyConfig seq) projections =
        projections
        |> Collection.concat2 (keys |> Seq.collect KeyConfig.keyNames)
        |> Seq.distinct

    let empty (config: IndexConfig) =
        let name = $"{config.tableName}/{ValueOption.defaultValue primaryIndexName config.indexName}"
        let pjns = config.config.projection ?|> (
            buildProjectionCols [config.keyConfig; config.tableKeyConfig] >> Array.ofSeq)

        if ValueOption.isSome config.indexName then
            let invalidCols =
                KeyConfig.keyCols config.keyConfig
                |> Seq.map fstT
                |> Seq.filter (validSecondaryIndexProp.IsMatch >> not)

            let invalidProj =
                ValueOption.map (Array.filter (fun (x: string) -> validSecondaryIndexProp.IsMatch x |> not)) pjns
                |> ValueOption.defaultValue Array.empty

            let invalid =
                Collection.concat2 invalidCols invalidProj
                |> Seq.distinct
                |> Seq.map (sprintf "Invalid secondary index key attribute name or projection %A")
                |> Str.join "; "

            if invalid <> "" then clientError invalid

        let indexType =
            match struct (config.indexName ?|> validateName, config.local) with
            | struct (ValueNone, _) -> Primary
            | ValueSome x, false -> GlobalSecondary x
            | ValueSome x, true -> LocalSecondary x

        let proj =
            pjns
            ?|> (if config.config.projectionsAreKeys then Keys else Attributes)
            |> ValueOption.defaultValue All

        { data = IndexItems.empty
          info =
              { keyConfig = config.keyConfig
                compositeName = name
                indexType = indexType 
                projection = proj
                arn =
                    config.indexName
                    ?|> fun name struct (awsAccount: AwsAccountId, region: RegionId) ->
                        $"arn:aws:dynamodb:{region}:{awsAccount}:table/{config.tableName}/index/{name}" } }
        |> Idx

    let getVersion = function
        | Idx {data = data} -> IndexItems.version data

    let getArn location = function
        | Idx {info = {arn = arn}} -> arn <|? ValueSome location

    let getName (Idx {info = info}) = info.name

    let isGsi = function
        | Idx {info = {indexType = GlobalSecondary _}} -> true
        | _ -> false

    let isLsi = function
        | Idx {info = {indexType = LocalSecondary _}} -> true
        | _ -> false

    let partitionKeyName = function
        | Idx {info = {keyConfig = c}} -> KeyConfig.partitionKeyName c

    let partitionKey item = function
        | Idx {info = {keyConfig = c}} -> KeyConfig.partitionKey (Item.attributes item) c

    let hasSortKey = function
        | Idx {info = {keyConfig = k}} -> KeyConfig.hasSortKey k

    let sortKeyName = function
        | Idx {info = {keyConfig = c}} -> KeyConfig.sortKeyName c

    let sortKey item = function
        | Idx {info = {keyConfig = c}} -> KeyConfig.sortKey (Item.attributes item) c

    let keyConfig (Idx {info = {keyConfig = k}}) = k

    let itemCount = function
        | Idx {data = data} -> IndexItems.itemCount data

    let projections (Idx {info = {projection = p}}) = p

    let describe = function
        | Idx {info = {compositeName = n; keyConfig = k; projection = p}} ->
            let keyDescription = KeyConfig.describe k |> List.map (fun x -> $"  {x}")
            let attributes =
                ProjectionType.cols p
                ?|> (Seq.map (sprintf "    %s"))
                ?|> (Collection.concat2 ["  Projections:"])
                |> ValueOption.defaultValue ["  Projections: ALL"]

            n::(Collection.concat2 keyDescription attributes |> List.ofSeq)

    let inline private seqify xs: 'a seq = xs

    let get partitionKey sortKey = function
        | Idx {data = data; info = {keyConfig = keyConfig}} ->
            if KeyConfig.partitionType keyConfig <> AttributeValue.getType partitionKey
            then clientError "Invalid partition key type"

            if KeyConfig.sortKeyType keyConfig <> ValueOption.map AttributeValue.getType sortKey
            then clientError "Invalid sort key type"

            IndexItems.partitions data
            |> AvlTree.tryFind partitionKey
            ?|> (Partition.get sortKey)
            |> ValueOption.defaultValue Seq.empty

    let getPartition logger partitionKey = function
        | Idx {info = {keyConfig = keyConfig}} & idx when KeyConfig.partitionType keyConfig <> AttributeValue.getType partitionKey ->
            let actual = AttributeValue.getType partitionKey
            let expected = KeyConfig.partitionType keyConfig
            clientError $"Attempting to use {actual} data to query index \"{idx}\"; expected {expected}"

        | Idx {data = data} -> IndexItems.partitions data |> AvlTree.tryFind partitionKey

    let private getPartitions = function
        | Idx {data = data} -> IndexItems.partitions data

    let getKeyConfig = function
        | Idx index -> index.info.keyConfig

    let scan lastEvaluatedPartitionKey forward inclusive index =
        let struct (start, ``end``) =
            lastEvaluatedPartitionKey
            ?|> (fun x ->
                match forward with
                | true -> struct (ValueSome x, ValueNone)
                | false -> struct (ValueNone, ValueSome x))
            |> ValueOption.defaultValue struct (ValueNone, ValueNone)

        getPartitions index
        |> AvlTree.seek start ``end`` inclusive forward
        |> Seq.map sndT

    [<Struct; IsReadOnly>]
    type private PartitionChangeResult =
        | Changed of Partition
        | NotChanged
        | Deleted
        with
        static member ofMutation = function
            | ValueNone -> Deleted
            | ValueSome x -> Changed x

        static member asReplaceCommand partitionKey = function
            | Changed x -> struct (partitionKey, ValueSome x) |> ValueSome
            | Deleted -> struct (partitionKey, ValueNone) |> ValueSome
            | NotChanged -> ValueNone

    let private validateResultPrecedence struct (logger, index) struct (changeResult: ChangeResult, partition) =

        // mini cache. Can't find a way to use match statements without calling this fn twice
        let mutable superceedingDeleteCache = ValueNone 
        let superceedingDelete item =
            match superceedingDeleteCache with
            | ValueSome struct (input, output) when input = item -> output
            | _ ->
                superceedingDeleteCache <-
                    IndexItems.recentlyDeleted index.data
                    |> flip DeletedItems.tryGetSuperceedingDelete
                    |> apply struct (item, index.info.keyConfig)
                    |> tpl item
                    |> ValueSome

                ValueOption.bind sndT superceedingDeleteCache    

        match struct (index.info.isPrimary, changeResult.Put, changeResult.Deleted) with

        // the item replaced by the put was added after the put
        | true, ValueSome put, ValueSome del when Item.compareByCreationOrder put del < 0 ->
            Logger.debug1 "PUT item superceeded by existing data (put, existing) %A" (put, del) logger
            struct ([], NotChanged)

        // the put item has already been deleted
        | true, ValueSome put, _ when superceedingDelete put |> ValueOption.isSome ->
            let delId = superceedingDelete put |> Maybe.expectSome
            Logger.debug1 "PUT item superceeded by previous delete (put, delete) %A" (put, delId) logger
            struct ([changeResult; ChangeResult.ofDelete delId put], NotChanged)

        // the item removed by the delete was added after the delete
        | true, ValueNone, ValueSome delete when (Item.internalId delete).Value > changeResult.Id.Value ->
            Logger.debug1 "DELETE item superceeded by existing data (deleted, existing) %A" (delete, changeResult.Id) logger
            struct ([], NotChanged)

        | _, put, del ->
            Logger.debug1 "Change is most recent (put, delete) %A" struct (put, del) logger
            struct ([changeResult], partition)

    let inline private noResultToValidate struct (_, x) _ = struct ([], x)
    let private validateOptionalResultPrecedence loggerIndex result =
        mapSnd ValueSome result
        |> uncurry Maybe.tpl
        ?|> (validateResultPrecedence loggerIndex)
        |> ValueOption.defaultWith (noResultToValidate result)

    let put =
        let createPartition (indexInfo: IndexInfo) logger item =

            let p =
                // logger sortKeysUnique indexName keys projections item
                Partition.create
                    indexInfo.isPrimary
                    indexInfo.compositeName
                    indexInfo.keyConfig
                    (ProjectionType.cols indexInfo.projection)
                    item

            Logger.log1 "Adding partition %O" (sndT p) logger
            p

        let partitionKey logger keyConfig = function
            | Either1 itemData -> KeyConfig.partitionKey itemData keyConfig
            | Either2 item -> Item.attributes item |> flip KeyConfig.partitionKey keyConfig

        let complete logger (index: IndexResource) struct (result, partition) =
            let struct (x, y) = IndexResource.afterMutation logger result index partition
            struct (x, Idx y)

        fun logger item ->
            function
            | Idx index ->
                let inline newPartition _ = createPartition index.info logger item
                let k = partitionKey logger index.info.keyConfig item

                IndexItems.partitions index.data
                |> AvlTree.tryFind k
                ?|> (Partition.put item)
                |> ValueOption.defaultWith newPartition
                |> mapSnd Changed
                |> validateResultPrecedence struct (logger, index)
                |> mapFst (ChangeResults.ofPartitionChangeResults index.info.keyConfig)
                |> mapSnd (PartitionChangeResult.asReplaceCommand k)
                |> complete logger index

    let emptyIndexChangeResult (Idx {info = {keyConfig = keyConfig}; data = data}) =
        ChangeResults.empty keyConfig |> flip IndexItems.indexChangeResult data

    let private attrsAreValidOnSparseIndex attrs =
        function
        | Idx index ->
            let hasKeys = KeyConfig.hasKeys attrs index.info.keyConfig

            match struct (hasKeys, KeyConfig.hasSortKey index.info.keyConfig) with
            | (false, _), _
            | (_, false), true -> false
            | (true, true), true
            | (true, true), false
            | (true, false), false -> true

    let private itemIsValidOnSparseIndex = Item.attributes >> attrsAreValidOnSparseIndex

    let validateItem item =
        keyConfig
        >> KeyConfig.validateKeys item

    let tryPut logger item =
        function
        | Idx { info = {keyConfig = keyConfig}; data = data } & idx when itemIsValidOnSparseIndex item idx |> not ->
            Logger.log1 "Item %O not valid on index" item logger
            struct (emptyIndexChangeResult idx, idx)

        | idx ->
            put logger (Either2 item) idx

    module private SharedDeleteUtils =

        let complete logger (index: IndexResource) struct (result, partition) =
            let struct (x, y) = IndexResource.afterMutation logger result index partition
            struct (x, Idx y)

        let private buildDeleteAttempt struct (deleteId, item, Idx { info = { keyConfig = keys } }) =
            { deleteId = deleteId |> ValueOption.defaultWith IncrementingId.next
              partitionKey = KeyConfig.partitionKeyWithName item keys
              sortKey = KeyConfig.sortKeyWithName item keys }

        let asChangeResults (struct (id, _, index) & wrapUpData) (crs: ChangeResult list) =
            let deleteAttempts =
                match id with
                | ValueSome (id: IncrementingId) when crs |> Collection.tryFind (_.Id.Value >> ((=) id.Value)) |> ValueOption.isSome -> []
                | ValueNone when List.isEmpty crs |> not -> []
                | _ -> [buildDeleteAttempt wrapUpData]

            let keys = keyConfig index
            ChangeResults.create keys crs deleteAttempts

    let deleteItem logger (struct (deleteId, item) & iWithId) =
        function
        | Idx {info = {keyConfig=keys}} & idx when itemIsValidOnSparseIndex item idx |> not ->
            emptyIndexChangeResult idx |> flip tpl idx

        | Idx index & idx ->
            let attrs = Item.attributes item
            let k = KeyConfig.partitionKey attrs index.info.keyConfig
            let wrapUpData = struct (deleteId, attrs, idx)

            IndexItems.partitions index.data
            |> AvlTree.tryFind k
            ?|> (
                Partition.remove iWithId
                >> mapSnd PartitionChangeResult.ofMutation
                >> validateOptionalResultPrecedence struct (logger, index))
            |> ValueOption.defaultValue struct ([], NotChanged)
            |> mapFst (SharedDeleteUtils.asChangeResults wrapUpData)
            |> mapSnd (PartitionChangeResult.asReplaceCommand k)
            |> SharedDeleteUtils.complete logger index

    let delete logger struct (deleteId, struct (pk, sk)) (Idx index & idx) =        
        match get pk sk idx |> List.ofSeq with
        | [] ->
            Logger.log0 "Item not found, registering delete" logger

            keyConfig idx
            |> KeyConfig.asAttributeMap pk sk
            |> tpl3 deleteId
            |> apply idx
            |> flip SharedDeleteUtils.asChangeResults []
            |> flip tpl ValueNone
            |> SharedDeleteUtils.complete logger index
        | xs ->
            Logger.log0 "Item found, deleting" logger

            List.fold (fun struct (result1, index) x ->
                deleteItem logger struct (deleteId, x) index
                |> mapFst (CdcPacket.concat result1)) struct (emptyIndexChangeResult idx, idx) xs