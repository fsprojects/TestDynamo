namespace TestDynamo.Model

open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators
open TestDynamo.Data.BasicStructures
open System.Runtime.CompilerServices

type UpdateTableSchema =
    { // it is possible to create and delete an index in the same request
      // deletes are always processed first
      deleteGsi: Set<string>
      createGsi: Map<string, CreateIndexData>
      attributes: struct (string * AttributeType) list }

    with
    // this empty value is important. It tells the system to ignore certain errors around corruption due to synchronization
    static member empty =
        { deleteGsi = Set.empty
          createGsi = Map.empty
          attributes = List.empty }

type UpdateTableData =
    { schemaChange: UpdateTableSchema
      deletionProtection: bool voption }

    // this empty value is important. It tells the system to ignore certain errors around corruption due to synchronization
    static member empty =
        { schemaChange = UpdateTableSchema.empty
          deletionProtection = ValueNone }

[<Struct; IsReadOnly>]
type IndexConfigInput =
    { data: CreateIndexData
      isLocal: bool }

type TableConfig =
    { name: string
      primaryIndex: IndexKeys
      indexes: Map<string, IndexConfigInput>
      attributes: struct (string * AttributeType) list
      addDeletionProtection: bool }

type TableInfo =
    { name: string
      arn: struct (AwsAccountId * RegionId) -> string
      id: string
      nameAndIndexes: string
      hasDeletionProtection: bool
      createdDate: System.DateTimeOffset }

[<Struct; IsReadOnly>]
type private TableData =
    { primaryIndex: Index
      indexes: Map<string, Index> }

/// <summary>
/// A composite of info and data. These properties
/// are split into multiple types to reduce allocations when
/// Items are written to the table with copy on write, while keeping
/// struct size namagable
/// </summary>
[<Struct; IsReadOnly>]    
type private TableResource =
    { info: TableInfo
      data: TableData }

/// <summary>
/// A dynamodb Table containing Indexes 
/// </summary>
[<Struct; IsReadOnly>]
type Table =
    private
    | Tbl of TableResource

    override this.ToString() =
        match this with
        | Tbl { info = { name = name } }-> name

[<RequireQualifiedAccess>]
module Table =

    let private attributes' t =
        t.data.indexes.Values
        |> Collection.prepend t.data.primaryIndex
        |> Seq.collect (
            Index.getKeyConfig
            >> KeyConfig.keyCols)

    let private attributeNames' =
        attributes'
        >> TableKeyAttributeList.create

    let attributeNames (Tbl t) = attributeNames' t

    let internal arnBuilder (Tbl t) = t.info.arn

    let private validateAttributeNames inputAttributes table =

        let attrNotOnTable =
            attributeNames' table
            |> flip TableKeyAttributeList.contains
            >> not

        let errs =
            inputAttributes
            |> Seq.filter (fstT >> attrNotOnTable)
            |> Seq.map (sprintf " * %A")
            |> Str.join "\n"

        if errs.Length > 0
        then sprintf "Invalid table column configuration. Found unused definitions for cols\n%s" errs |> ClientError.clientError

        table

    let private tbl: struct (string * AttributeType) seq -> TableResource -> Table = validateAttributeNames >>> Tbl

    let name = function | Tbl t -> t.info.name

    let indexes = function
        | Tbl t -> t.data.indexes

    let primaryIndex = function
        | Tbl t -> t.data.primaryIndex

    let keyConfig = primaryIndex >> Index.keyConfig

    let itemCount = function
        | Tbl { data = { primaryIndex = items } } -> Index.itemCount items

    /// <summary>
    /// Generates a new id for a table
    /// Useful when cloning
    /// </summary>
    let internal reId (Tbl table) =
        { table with info.id = System.Guid.NewGuid().ToString() } |> Tbl

    let getId (Tbl {info = {id = id}}) = id

    let private nameAndIndexes name (indexes: Map<string, _>) =
        let idxString = indexes.Keys |> Str.join ", "
        if idxString = "" then $"Table: {name}" else $"Table: {name}; Indexes: {idxString}"

    let deleteIndex logger name = function
        | Tbl t ->
            match MapUtils.tryFind name t.data.indexes with
            | ValueNone -> ClientError.clientError $"Cannot find index {name}"
            | ValueSome idx ->
                Logger.log1 "Removing index %O" idx logger
                let gsi = Map.remove name t.data.indexes
                tbl [] {
                    t with
                        data.indexes = gsi
                        info.nameAndIndexes = nameAndIndexes t.info.name gsi 
                }

    let private applyChangesToSecondary =
        let tryPut logger item index =
            Index.tryPut logger item index
            |> mapFst (CdcPacket.log false logger)
            |> sndT

        let remove logger =
            Index.deleteItem logger
            >>> mapFst (CdcPacket.log false logger)
            >>> sndT

        fun (logger: Logger) (result: ChangeResults) index ->
            Logger.log1 "Projecting to index %O" index logger
            let struct (idxLogger, disp) = Logger.scope logger
            use _ = disp

            result.OrderedChanges
            // strip the delete id out before applying CDC
            // Because index => index replication is atomic, the primary index can manage this type of operation
            |> Seq.map _.Result
            |> Seq.fold (fun s -> function
                | Simple(Delete x) -> remove idxLogger (ValueNone, x) s
                | Replace (put, delete) ->
                    remove idxLogger (ValueNone, delete) s
                    |> tryPut idxLogger put
                | Simple(Create x) -> tryPut idxLogger x s) index

    let private applyChangesToSecondaries: Logger -> ChangeResults -> Map<string, Index> -> Map<string, Index> =
        asLazy3 applyChangesToSecondary >>> Map.map

    let private buildIndex =
        let buildKeyAttr pk =
            Seq.filter (fstT >> ((=)pk))
            >> Collection.tryHead
            >> Maybe.expectSomeErr "Cannot find key attribute %A" pk

        let buildKeyConfig allAttributes pk sk =
            KeyConfig.create
            <| buildKeyAttr pk allAttributes
            <| ValueOption.map (flip buildKeyAttr allAttributes) sk

        fun name local (config: CreateIndexData) tableKeyCols allAttributes ->
            let struct (pk, sk) = config.keys
            let buildKeyConfig = buildKeyConfig allAttributes

            { keyConfig = buildKeyConfig pk sk
              tableKeyConfig = uncurry buildKeyConfig tableKeyCols
              config = config
              tableName = fstT name
              indexName = sndT name
              local = local }
            |> Index.empty

    type private AddIndexArgs =
        { logger: Logger
          indexName: string
          local: bool
          data: CreateIndexData
          allAttributes: struct (string * AttributeType) list
          tableKeyConfig: struct (string * string voption)
          table: TableResource }

    let private addIndex args =
        match MapUtils.tryFind args.indexName args.table.data.indexes with
        | ValueSome idx -> ClientError.clientError $"Index {idx} already exists"
        | ValueNone ->

            let indexItems =
                Index.scan ValueNone true true args.table.data.primaryIndex
                |> Seq.collect (Partition.scan ValueNone true)
                |> Seq.collect PartitionBlock.toList
                |> ChangeResults.ofPutItems (Index.getKeyConfig args.table.data.primaryIndex)

            buildIndex
                struct (args.table.info.name, ValueSome args.indexName)
                args.local 
                args.data
                args.tableKeyConfig
                args.allAttributes
            |> Logger.logFn1 "Adding index %A" args.logger
            |> applyChangesToSecondary args.logger indexItems
            |> flip (Map.add args.indexName) args.table.data.indexes
            |> fun gsis ->
                {
                    args.table with
                        data.indexes = gsis
                        info.nameAndIndexes = nameAndIndexes args.table.info.name gsis 
                }

    let inline private unwrap (Tbl t) = t 

    let empty logger (data: TableConfig) =
        let name =
            AwsUtils.parseTableName data.name
            |> ValueOption.defaultWith (fun _ -> ClientError.clientError $"Invalid table name {data.name}")

        let indexData =
            { keys = data.primaryIndex
              projectionsAreKeys = false
              projectionCols = ValueNone }

        let primaryIndex =
            buildIndex
            <| struct (name, ValueNone)
            <| false
            <| indexData
            <| data.primaryIndex
            <| data.attributes

        let table =
            { info =
                  { name = name
                    id = System.Guid.NewGuid().ToString()
                    arn = fun struct(awsAccount: AwsAccountId, region: RegionId) -> $"arn:aws:dynamodb:{region}:{awsAccount}:table/{name}"
                    hasDeletionProtection = data.addDeletionProtection 
                    nameAndIndexes = nameAndIndexes name Map.empty
                    createdDate = System.DateTimeOffset.UtcNow }
              data =
                  { primaryIndex = primaryIndex
                    indexes = Map.empty } }

        data.indexes
        |> Seq.fold (fun s x ->
            { logger = logger
              indexName = x.Key
              local = x.Value.isLocal
              data = x.Value.data
              allAttributes = data.attributes
              tableKeyConfig = data.primaryIndex
              table = s } |> addIndex) table
        |> tbl data.attributes

    let describe = function
        | Tbl x ->
            let primary = "PrimaryIndex"::Index.describe x.data.primaryIndex
            let secondary =
                x.data.indexes
                |> MapUtils.toSeq
                |> Seq.collect (fun struct (k, v) -> ""::k::Index.describe v)

            Seq.concat [primary; secondary |> List.ofSeq]
            |> Seq.map (sprintf "  %s")
            |> Str.join "\n"
            |> sprintf "%s\n%s" x.info.name

    let private getItemAttributeTypeErrors =

        let describable = Logger.describable<Either<Map<string, AttributeValue>, Item>> (function
            | Either2 x -> $"from item {x} "
            | Either1 _ -> "")

        let invalidTypeErrors = function
            | struct (_, TableKeyAttributeList.MissingAttribute) -> ValueNone
            | x, TableKeyAttributeList.InvalidType y -> ValueSome struct (x, y)

        let individualMessage logger item struct (name, struct (expected, actual)) =
            sprintf " * Expected attribute %A %Oto have type: %A, got type %A (%i)" name item expected actual (Logger.id logger)

        let exceptEmpty = function
            | "" -> ValueNone
            | x -> ValueSome x

        let getErrs logger item =
            let attributes =
                Either.map2Of2 Item.attributes item
                |> Either.reduce

            TableKeyAttributeList.create
            >> TableKeyAttributeList.validate attributes
            >> Seq.map invalidTypeErrors
            >> Maybe.traverse
            >> Seq.map (individualMessage logger (describable item))
            >> Str.join "\n"
            >> exceptEmpty

        flip2To3 getErrs

    let private itemNotValidOnIndex = "TestDynamo:ItemNotValidOnIndex"
    let private itemNotValidOnIndexData: struct (obj * obj) seq = [struct (itemNotValidOnIndex, ())]

    let isItemNotValidOnIndexError (e: exn) =
        e.Data <> null && e.Data.Contains itemNotValidOnIndex

    let private validateNewItem =

        let throw (Tbl { info = {name = name} }) = function
            | ValueNone -> ()
            | ValueSome x ->
                sprintf "Item not valid for table \"%s\"\n%s" name x
                |> ClientError.clientErrorWithData itemNotValidOnIndexData

        fun logger (Tbl data & table) ->
            attributes' data
            |> getItemAttributeTypeErrors logger
            >> throw table

    let private validateItemsForNewIndex logger attrs =
        let validator = Either2 >> getItemAttributeTypeErrors logger attrs

        fun (table: TableResource) ->
            let struct (invalid1, invalid2) =
                table.data.primaryIndex
                |> Index.scan ValueNone true true
                |> Seq.collect (Partition.scan ValueNone true)
                |> Seq.collect PartitionBlock.toList
                |> Seq.map validator
                |> Maybe.traverse
                |> Seq.mapi tpl
                |> Collection.partition (fstT >> flip (<) 5)
                |> mapFst (
                    Seq.map sndT
                    >> Str.join "\n")
                |> mapSnd List.length

            match struct (invalid1, invalid2) with
            | "", _ -> ()
            | err, 0 -> sprintf "Error adding new index\n%s" err |> ClientError.clientErrorWithData itemNotValidOnIndexData
            | err, x -> sprintf "Error adding new index\n%s\n  + %i more items" err x |> ClientError.clientErrorWithData itemNotValidOnIndexData

    let internal updateTable (req: UpdateTableData) logger (Tbl data & t) =
        Logger.log1 "Updating table %O" t logger

        let afterDeleted =
            req.schemaChange.deleteGsi
            |> Seq.fold (flip (deleteIndex logger)) t
            |> unwrap

        let indexResults =
            let tableIndex =
                Index.keyConfig data.data.primaryIndex
                |> tplDouble
                |> mapFst KeyConfig.partitionKeyName
                |> mapSnd KeyConfig.sortKeyName

            // order of concat is important
            // We want the input attributes to come first, so that any index is assigned
            // an attribute value from the input, rather than the existing cols
            // This allows the creation of an invalid index if attr types differ, which will be caught later 
            let attrs = req.schemaChange.attributes |> flip Collection.concat2 (attributes' data) |> List.ofSeq

            validateItemsForNewIndex logger attrs data
            Map.fold (fun s k v ->
                { logger = logger
                  indexName = k
                  local = false
                  data = v
                  allAttributes = attrs
                  tableKeyConfig = tableIndex
                  table = s } |> addIndex) afterDeleted req.schemaChange.createGsi

        let deletionProtection = ValueOption.defaultValue data.info.hasDeletionProtection req.deletionProtection
        {indexResults with info.hasDeletionProtection = deletionProtection }
        |> tbl req.schemaChange.attributes

    let getIndex indexName = function
        | Tbl {data = {primaryIndex = primary; indexes = secondary} } ->
            match indexName with
            | ValueNone -> ValueSome primary
            | ValueSome i -> MapUtils.tryFind i secondary

    let getVersion = function
        | Tbl {data = {primaryIndex = primary }} -> Index.getVersion primary

    let getCreatedDate = function
        | Tbl {info = {createdDate = x }} -> x

    let getArn location = function
        | Tbl {info = {arn = x}} -> x location

    let hasDeletionProtection = function
        | Tbl {info = {hasDeletionProtection = x}} -> x

    let listGlobalSecondaryIndexes = function
        | Tbl {data = {indexes = secondary}} -> secondary.Keys

    let private put' (logger: Logger) item = function
        | Tbl ({data = {primaryIndex = primary; indexes = secondary }; info = { nameAndIndexes = nameAndIndexes;  } } & t) & tbl ->

            Logger.debug1 "PUT %s" nameAndIndexes logger

            validateNewItem logger tbl item
            let struct (result, primary') =
                Index.put logger item primary
                |> mapFst (CdcPacket.log true logger)

            Logger.debug0 "Primary complete, applying to secondaries" logger
            let secondaries = applyChangesToSecondaries logger result.changeResult secondary
            Logger.debug0 "Secondaries complete" logger
            struct (result, Tbl { t with data.primaryIndex = primary'; data.indexes = secondaries})

    let put logger = Either1 >> put' logger
    let putReplicated logger = Either2 >> put' logger

    /// <summary>Throws if some key attributes are missing</summary>
    let get item = function
        | Tbl ({ data = { primaryIndex = primary }} & t) ->
            let pkName = Index.partitionKeyName primary
            let skName = Index.sortKeyName primary

            let struct (item', pk) =
                MapUtils.tryPop pkName item
                |> mapSnd (function
                    | ValueNone -> ClientError.clientError $"Could not find partition key attribute {pkName}"
                    | ValueSome x -> x)

            let struct (item', sk) =
                skName
                ?|> (
                    flip MapUtils.tryPop item'
                    >> mapSnd (function
                        | ValueNone -> ClientError.clientError $"Could not find sort key attribute {skName}"
                        | x -> x))
                |> ValueOption.defaultValue struct (item', ValueNone)

            let keyErrors =
                match Map.keys item' |> Str.join ", " with
                | "" -> ()
                | err -> ClientError.clientError $"Found non key attributes in input {err}"

            Index.get pk sk primary |> Collection.tryHead

    let private delete' (logger: Logger) allowNonKeys struct (deleteId, item) = function
        | Tbl ({ data = {primaryIndex = primary; indexes = secondary }} & t) ->
            let pkName = Index.partitionKeyName primary
            let skName = Index.sortKeyName primary

            let struct (item', pk) =
                MapUtils.tryPop pkName item
                |> mapSnd (function
                    | ValueNone -> ClientError.clientError "Could not find partition key attribute"
                    | ValueSome x -> x)

            let struct (item', sk) =
                skName
                ?|> (
                    flip MapUtils.tryPop item'
                    >> mapSnd (function
                        | ValueNone -> ClientError.clientError "Could not find sort key attribute"
                        | x -> x))
                |> ValueOption.defaultValue struct (item', ValueNone)

            let keyErrors =
                match struct (allowNonKeys, Map.keys item' |> Str.join ", ") with
                | true, _
                | false, "" -> ()
                | _, err -> ClientError.clientError $"Found non key attributes in input {err}"

            let struct (result, primary') =
                Index.delete logger struct (deleteId, struct (pk, sk)) primary

            Logger.debug0 "Primary complete, applying to secondaries" logger
            let secondaries = applyChangesToSecondaries logger result.changeResult secondary
            Logger.debug0 "Secondaries complete" logger
            struct (result, Tbl { t with data.primaryIndex = primary'; data.indexes = secondaries})

    let delete logger allowNonKeys = tpl ValueNone >> delete' logger allowNonKeys

    let deleteReplicated logger = mapFst ValueSome >> delete' logger true

    let private unwrapPartitions = Partition.scan ValueNone true
    let private combineCdcPackets =
        List.fold (fun s x ->
            match s with
            | ValueNone -> ValueSome x
            | ValueSome x1 -> CdcPacket.concat x1 x |> ValueSome) ValueNone

    /// <summary>Returns null if the table was empty to begin with</summary>
    let clear (logger: Logger) = function
        | Tbl {data = {primaryIndex = primary}} & t ->
            Index.scan ValueNone true true primary
            |> Seq.collect unwrapPartitions
            |> Seq.collect PartitionBlock.toSeq
            |> Seq.map Item.attributes
            |> Seq.fold (fun struct (cdc, s) x ->
                delete logger true x s
                |> mapFst (flip Collection.prependL cdc)) struct ([], t)
            |> mapFst combineCdcPackets
            |> function
                | struct (ValueNone, _) -> ValueNone
                | struct (ValueSome x, y) -> ValueSome struct (x, y)
