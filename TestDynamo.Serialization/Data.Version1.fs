[<RequireQualifiedAccess>]
module TestDynamo.Serialization.Data.Version1

open System.Threading
open TestDynamo.Api.FSharp
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators

type SerializableIndexType =
    | Primary = 1
    | GlobalSecondary = 2
    | LocalSecondary = 3

type SerializableProjectionType =
    | All = 1
    | Keys = 2
    | Attributes = 3

type AttributeDescription = (struct (string * AttributeType))
type SerializableKeyConfig = (struct (AttributeDescription * AttributeDescription voption))

type SerializableIndexInfo(
      indexType: SerializableIndexType,
      indexName: string voption,
      keyConfig: SerializableKeyConfig,
      projection: SerializableProjectionType,
      projectionAttributes: string array) =

      member _.indexType = indexType
      member _.indexName = indexName
      member _.keyConfig = keyConfig
      member _.projection = projection
      member _.projectionAttributes = projectionAttributes

type SerializableIndex(
    info: SerializableIndexInfo,
    items: Map<string, AttributeValue> array) =

    member _.info = info
    member _.items = items

type SerializableTableInfo(
      name: string,
      hasDeletionProtection: bool,
      streamsEnabled: bool) =
    member _.name = name
    member _.hasDeletionProtection = hasDeletionProtection
    member _.streamsEnabled = streamsEnabled

type SerializableTable(
    info: SerializableTableInfo,
    primaryIndex: SerializableIndex,
    secondaryIndexes: SerializableIndexInfo list) =

    member _.info = info
    member _.primaryIndex = primaryIndex
    member _.secondaryIndexes = secondaryIndexes

type SerializableDatabase(
    databaseId: DatabaseId,
    tables: SerializableTable list) =

    member _.databaseId = databaseId
    member _.tables = tables

type SerializableReplication(
    from: DatabaseId,
    ``to``: DatabaseId,
    tableName: string) =
    member _.from = from
    member _.``to`` = ``to``
    member _.tableName = tableName

type SerializableGlobalDatabase(
    databases: SerializableDatabase list,
    replications: SerializableReplication list) =

    member _.databases = databases
    member _.replications = replications

module ToSerializable =
    module Database =

        let serializableIndexMetadata (idx: Index) =

            let sk =
                Index.keyConfig idx
                |> KeyConfig.sortKeyDefinition

            let pk =
                Index.keyConfig idx
                |> KeyConfig.partitionKeyDefinition

            SerializableIndexInfo(
                    if Index.isGsi idx then SerializableIndexType.GlobalSecondary
                    elif Index.isLsi idx then SerializableIndexType.LocalSecondary
                    else SerializableIndexType.Primary
                ,
                Index.getName idx,
                struct (pk, sk),
                    match Index.projections idx with
                    | All -> SerializableProjectionType.All
                    | Attributes _ -> SerializableProjectionType.Attributes
                    | Keys _ -> SerializableProjectionType.Keys
                ,
                    match Index.projections idx with
                    | All -> Array.empty
                    | Attributes x
                    | Keys x -> x)

        let serializeableIndex idx =

            let vals =
                Index.scan ValueNone true true idx
                |> Seq.collect (Partition.scan ValueNone true)
                |> Seq.collect PartitionBlock.toSeq
                |> Seq.map Item.attributes
                |> Array.ofSeq

            SerializableIndex(serializableIndexMetadata idx, vals)

        let emptyIndex idx =
            SerializableIndex(serializableIndexMetadata idx, [||])

        let serializableTable schemaOnly streamsEnabled (table: Table) =

            let primaryIndex =
                if schemaOnly
                then emptyIndex
                else serializeableIndex

            let name = Table.name table
            SerializableTable(
                SerializableTableInfo(
                    name,
                    Table.hasDeletionProtection table,
                    streamsEnabled name),

                Table.primaryIndex table |> primaryIndex,

                Table.indexes table
                |> MapUtils.toSeq
                |> Seq.map (sndT >> serializableIndexMetadata)
                |> List.ofSeq)

        let toSerializable replicaTables schemaOnly (db: Api.FSharp.Database) =
            let streamsEnabled = db.StreamsEnabled
            SerializableDatabase(
                db.Id,
                db.ListTables ValueNone (ValueNone, System.Int32.MaxValue)
                |> Seq.map (fun struct (name, data) ->
                    let schemaOnly = schemaOnly || replicaTables |> Array.contains name
                    data.table |> serializableTable schemaOnly streamsEnabled)
                |> List.ofSeq)

    module GlobalDatabase =

        let toSerializable schemaOnly (db: GlobalDatabase) =

            let replications =
                db.ListReplications ValueNone true
                |> Seq.map (fun r ->
                    let toDb = db.GetDatabase ValueNone r.toDb
                    let toTable = toDb.DescribeTable ValueNone r.tableName

                    SerializableReplication(r.fromDb, r.toDb, r.tableName))
                |> List.ofSeq

            let replicaTables databaseId =
                replications
                |> Seq.filter (_.``to`` >> (=) databaseId)
                |> Seq.map _.tableName
                |> Array.ofSeq

            let databases =
                db.GetDatabases()
                |> Map.values
                |> Seq.map (fun db -> Database.toSerializable (replicaTables db.Id) schemaOnly db)
                |> List.ofSeq

            SerializableGlobalDatabase(databases, replications)

module FromSerializable =

    module Database =

        let private indexIsLocal (i: SerializableIndexInfo) =
            i.indexType = SerializableIndexType.LocalSecondary

        let private asPutItemArgs (t: SerializableTable) =
            t.primaryIndex.items
            |> Seq.map (fun item ->
                { item = item
                  conditionExpression =
                      { tableName = t.info.name
                        conditionExpression = ValueNone
                        expressionAttrValues = Map.empty
                        expressionAttrNames = Map.empty
                        returnValues = BasicReturnValues.None } })

        let addData globalLogger (fromJson: SerializableDatabase) (db: Api.FSharp.Database) =

            fromJson.tables
            |> Seq.collect asPutItemArgs
            |> Seq.fold (fun _ ->
                db.Put globalLogger
                >> ignoreTyped<Map<string,AttributeValue> voption>) ()

        let private buildSerializableTable (table: SerializableTable) =
            let attrs =
                table.primaryIndex.info::table.secondaryIndexes
                |> Seq.collect (_.keyConfig >> mapFst ValueSome >> tplToList)
                |> Maybe.traverse
                |> Seq.distinctBy fstT
                |> List.ofSeq

            let indexKeys (i: SerializableIndexInfo) =
                i.keyConfig
                |> mapFst fstT
                |> mapSnd (ValueOption.map fstT)

            let index (i: SerializableIndexInfo) =
                { data =
                    { keys = indexKeys i
                      projectionsAreKeys = i.projection = SerializableProjectionType.Keys
                      projectionCols =
                        match struct (i.projection, i.projectionAttributes) with
                        | SerializableProjectionType.All, _ -> ValueNone
                        | SerializableProjectionType.Attributes, attr
                        | SerializableProjectionType.Keys, attr -> List.ofArray attr |> ValueSome
                        | _ -> serverError "Invalid projection" }
                  isLocal = indexIsLocal i } |> tpl (i.indexName |> Maybe.expectSomeErr "Expected index to have a name%s" "")

            { name = table.info.name
              primaryIndex = indexKeys table.primaryIndex.info
              indexes = table.secondaryIndexes |> Seq.map index |> MapUtils.ofSeq
              attributes = attrs
              addDeletionProtection = table.info.hasDeletionProtection }

        let buildCloneSchema (fromJson: SerializableDatabase) =
            { databaseId = fromJson.databaseId
              data =
                  { initialState =
                        fromJson.tables
                        |> Seq.fold (fun s ->
                            buildSerializableTable
                            >> flip1To3 DatabaseTables.addTable Logger.empty s) DatabaseTables.empty
                    streamsEnabled =
                        fromJson.tables
                        |> Seq.filter _.info.streamsEnabled
                        |> Seq.map _.info.name
                        |> List.ofSeq }}

        let fromSerializable globalLogger (fromJson: SerializableDatabase) =

            let cloneSchema = buildCloneSchema fromJson
            let db =
                globalLogger
                ?|> fun l -> new Api.FSharp.Database(l, cloneSchema)
                ?|>? fun _ -> new Api.FSharp.Database(cloneSchema)

            addData globalLogger fromJson db
            db

    module GlobalDatabase =

        let fromSerializable globalLogger (fromJson: SerializableGlobalDatabase) =

            let cloneSchema =
                { databases = 
                    fromJson.databases
                    |> List.map  Database.buildCloneSchema
                  replicationKeys =
                    fromJson.replications
                    |> List.map (fun x -> {fromDb = x.from; toDb = x.``to``; tableName = x.tableName }) }
            let db =
                globalLogger
                ?|> fun logger -> new Api.FSharp.GlobalDatabase(cloneSchema, logger = logger)
                ?|>? fun _ -> new Api.FSharp.GlobalDatabase(cloneSchema)

            fromJson.databases
            |> Seq.fold (fun _ dbData -> Database.addData globalLogger dbData (db.GetDatabase ValueNone dbData.databaseId)) ()

            // Sacrificing a thread here to keep the interface simple           
            (db.AwaitAllSubscribers globalLogger CancellationToken.None).AsTask().ConfigureAwait(false).GetAwaiter().GetResult()
            db