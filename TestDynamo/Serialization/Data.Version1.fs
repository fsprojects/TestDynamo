[<RequireQualifiedAccess>]
module TestDynamo.Serialization.Data.Version1

open TestDynamo.Api
open TestDynamo.Data.BasicStructures
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
    
type SerializableDistributedDatabase(
    databases: SerializableDatabase list,
    replications: SerializableReplication list) =
    
    member _.databases = databases
    member _.replications = replications
    
module ToSerializable =
    module Database =
    
        let serializeableIndexMetadata (idx: Index) =
            
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
            
            SerializableIndex(serializeableIndexMetadata idx, vals)
        
        let emptyIndex idx =
            SerializableIndex(serializeableIndexMetadata idx, [||])
        
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
                |> Seq.map (sndT >> serializeableIndexMetadata)
                |> List.ofSeq)
        
        let toSerializable omitTables schemaOnly (db: Api.Database) =
            let streamsEnabled = db.StreamsEnabled
            SerializableDatabase(
                db.Id,
                db.ListTables ValueNone (ValueNone, System.Int32.MaxValue)
                |> Seq.filter (fstT >> flip Array.contains omitTables >> not)
                |> Seq.map (
                    sndT >> _.table >> serializableTable schemaOnly streamsEnabled)
                |> List.ofSeq)
        
    module DistributedDatabase =
        
        let toSerializable schemaOnly (db: DistributedDatabase) =
            
            let replications =
                db.ListReplications ValueNone true
                |> Seq.map (fun r -> SerializableReplication(r.fromDb, r.toDb, r.tableName))
                |> List.ofSeq
                    
            let tableOmissions databaseId =
                replications
                |> Seq.filter (_.``to`` >> (=) databaseId)
                |> Seq.map _.tableName
                |> Array.ofSeq
                
            let databases =
                db.GetDatabases()
                |> Map.values
                |> Seq.map (fun db -> Database.toSerializable (tableOmissions db.Id) schemaOnly db)
                |> List.ofSeq
                
            SerializableDistributedDatabase(databases, replications)

module FromSerializable =
    
    module Database =
            
        let private indexCols (i: SerializableIndexInfo) =
            struct (i.keyConfig |> fstT |> fstT, i.keyConfig |> sndT ?|> fstT)
        
        let private asTableAttributes (t: SerializableTable) =
            t.primaryIndex.info.keyConfig::(t.secondaryIndexes |> List.map _.keyConfig) 
            |> List.collect (fun struct (partitionKey, sortKey) -> [ ValueSome partitionKey; sortKey])
            |> Maybe.traverse
            |> Seq.distinctBy fstT
            |> List.ofSeq
        
        let private indexIsLocal (i: SerializableIndexInfo) =
            i.indexType = SerializableIndexType.LocalSecondary
                                
        let private asTableConfig (t: SerializableTable) =
            let secondaryIndexes =
                t.secondaryIndexes
                |> Seq.map (fun i -> struct (
                    i.indexName |> Maybe.expectSomeErr "Expected index to have a name%s" "",
                    struct (
                        indexIsLocal i,
                        { keys = indexCols i
                          projectionsAreKeys = i.projection = SerializableProjectionType.Keys
                          projection =
                            match struct (i.projection, i.projectionAttributes) with
                            | SerializableProjectionType.All, _ -> ValueNone
                            | SerializableProjectionType.Attributes, attr
                            | SerializableProjectionType.Keys, attr -> List.ofArray attr |> ValueSome
                            | _ -> serverError "Invalid projection" })))
                |> MapUtils.ofSeq
            
            { createStream = t.info.streamsEnabled
              tableConfig =
                  { name = t.info.name
                    primaryIndex = indexCols t.primaryIndex.info
                    indexes = secondaryIndexes
                    attributes = asTableAttributes t
                    addDeletionProtection = t.info.hasDeletionProtection } }
        
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
        
        let copyFromromSerializble globalLogger (fromJson: SerializableDatabase) (db: Api.Database) =
                        
            fromJson.tables
            |> Seq.map asTableConfig
            |> Seq.fold (fun _ ->
                db.AddTable globalLogger
                >> ignoreTyped<TableDetails>) ()
            
            fromJson.tables
            |> Seq.collect asPutItemArgs
            |> Seq.fold (fun _ ->
                db.Put globalLogger
                >> ignoreTyped<Map<string,AttributeValue> voption>) ()
        
        let fromSerializable globalLogger (fromJson: SerializableDatabase) =
            
            let db = new Api.Database(fromJson.databaseId)
            copyFromromSerializble globalLogger fromJson db
            db

    module DistributedDatabase =
        
        let dbHasTable (db: DistributedDatabase) dbId tableName =
            db.TryGetDatabase dbId
            ?>>= fun db -> db.TryDescribeTable ValueNone tableName
            |> ValueOption.isSome
        
        let rec private tryAddReplications = function
            | struct ([]: SerializableReplication list, struct (db: DistributedDatabase, logger)) -> struct (db, [])
            | r::tail, (db, logger) when dbHasTable db r.from r.tableName ->
                { createStreamsForReplication = true
                  replicaInstructions = [Create r.``to``] }
                |> db.UpdateTable r.from logger r.tableName
                |> ignoreTyped<TableDetails>
                
                tryAddReplications (tail, (db, logger))
            | r::tail, dbl ->
                tryAddReplications (tail, dbl)
                |> mapSnd (Collection.prependL r)
        
        let rec addReplications args =
            let repCount = args |> fstT |> List.length
            match tryAddReplications args with
            | db, [] -> ()
            | db, notProcesed when List.length notProcesed <= repCount -> addReplications (notProcesed, sndT args)
            | _, xs ->
                xs
                |> Seq.map (fun x -> sprintf " * Cannot find table %s in database %A when replicating to %A" x.tableName x.from x.``to``)
                |> Str.join "\n"
                |> sprintf "Error adding replications\n%s"
                |> invalidOp
        
        let fromSerializable globalLogger (fromJson: SerializableDistributedDatabase) =
            
            let db =
                globalLogger
                ?|> fun logger -> new Api.DistributedDatabase(logger = logger)
                ?|>? fun _ -> new Api.DistributedDatabase()
            
            fromJson.databases
            |> List.fold (fun _ x ->
                db.GetDatabase globalLogger x.databaseId
                |> Database.copyFromromSerializble globalLogger x) ()
            
            addReplications (fromJson.replications, (db, globalLogger))
            db