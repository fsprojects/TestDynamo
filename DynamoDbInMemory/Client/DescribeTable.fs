module DynamoDbInMemory.Client.DescribeTable

open System
open System.Linq
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory
open DynamoDbInMemory.Data.BasicStructures
open DynamoDbInMemory.Data.Monads.Operators
open DynamoDbInMemory.Utils
open DynamoDbInMemory.Client
open DynamoDbInMemory.Model

module Local =

    let fromKeySchema (xs: KeySchemaElement list) =
        let pk = xs |> Seq.filter (fun x -> x.KeyType.Value = KeyType.HASH.Value) |> Collection.tryHead
        let sk = xs |> Seq.filter (fun x -> x.KeyType.Value = KeyType.RANGE.Value) |> Collection.tryHead

        match struct (pk, sk, List.length xs) with
        | ValueSome pk', ValueNone, 1 -> struct (pk'.AttributeName, ValueNone)
        | ValueSome pk', ValueSome sk', 2 -> struct (pk'.AttributeName, ValueSome sk'.AttributeName)
        | ValueNone, _, _
        | ValueSome _, _, _ -> clientError "Invalid key"

    let private toKeySchema x =
        Index.getKeyConfig x
        |> KeyConfig.keyCols
        |> Seq.mapi (fun i struct (k, _) ->
            let e = KeySchemaElement()
            e.AttributeName <- k
            e.KeyType <-
                // hack. But not overly important + having PK as first in the list
                // is a nice convention to have
                match i with
                | 0 -> KeyType.HASH
                | _ -> KeyType.RANGE

            e)
        |> Enumerable.ToList

    let buildGsiSchema keySchema (projection: Projection) =
        let key = fromKeySchema (keySchema |> List.ofSeq)
        let struct (projection, projectionsAreKeys) =
            match projection.ProjectionType.Value with
            | t when t = ProjectionType.ALL.Value -> struct (ValueNone, false)
            | t when t = ProjectionType.KEYS_ONLY.Value -> struct (List.empty |> ValueSome, true)
            | t when t = ProjectionType.INCLUDE.Value -> struct (projection.NonKeyAttributes |> List.ofSeq |> ValueSome, false)
            | t -> clientError $"Invalid projection type {t}"

        { keys = key; projection = projection; projectionsAreKeys = projectionsAreKeys  }

    let private buildProjection (p: ProjectionType) keys =
        let prj = Projection()
        let k =
            Seq.collect KeyConfig.keyNames keys
            |> Seq.distinct
            |> Array.ofSeq
            |> flip Array.contains

        match p with
        | All ->
            prj.ProjectionType <- ProjectionType.ALL
            prj.NonKeyAttributes <- MList<_>()
        | Keys k ->
            prj.ProjectionType <- ProjectionType.KEYS_ONLY
            prj.NonKeyAttributes <- MList<_>()
        | Attributes attr ->
            prj.ProjectionType <- ProjectionType.INCLUDE
            prj.NonKeyAttributes <- MList<_>(System.Math.Max(0, attr.Length - 1))
            attr
            |> Seq.filter (k >> not)
            |> prj.NonKeyAttributes.AddRange

        prj

    let buildGsi awsAccountId databaseId t (x: Index) =
        let desc = GlobalSecondaryIndexDescription()
        desc.Backfilling <- false

        desc.IndexName <- Index.getName x |> ValueOption.defaultValue ""
        desc.IndexArn <- Index.getArn struct (awsAccountId, databaseId.regionId) x |> CSharp.fromOption
        desc.ItemCount <- Index.itemCount x
        desc.IndexStatus <- IndexStatus.ACTIVE
        desc.KeySchema <- toKeySchema x
        desc.Projection <- buildProjection (Index.projections x) [Index.keyConfig x; Table.keyConfig t]

        desc

    let buildLsi awsAccountId databaseId t (x: Index) =
        let desc = LocalSecondaryIndexDescription()

        desc.IndexName <- Index.getName x |> ValueOption.defaultValue ""
        desc.IndexArn <- Index.getArn struct (awsAccountId, databaseId.regionId) x |> CSharp.fromOption
        desc.ItemCount <- Index.itemCount x
        desc.KeySchema <- toKeySchema x
        desc.Projection <- buildProjection (Index.projections x) [Index.keyConfig x; Table.keyConfig t]

        desc

    let buildReplicaGsi (x: Index) =
        let desc = ReplicaGlobalSecondaryIndexDescription()
        desc.IndexName <- Index.getName x |> ValueOption.defaultValue ""
        desc

    let buildGsis awsAccountId databaseId t = Seq.filter Index.isGsi >> Seq.map (buildGsi awsAccountId databaseId t)

    let buildLsis awsAccountId databaseId t = Seq.filter Index.isLsi >> Seq.map (buildLsi awsAccountId databaseId t)

    let buildReplicaGsis = Seq.filter Index.isGsi >> Seq.map buildReplicaGsi

    let private rankConfig = function
        | KeysOnly -> 1
        | NewImage -> 2
        | OldImage -> 3
        | NewAndOldImages -> 4

    let buildStreamSpec (table: TableDetails) =
        let spec = StreamSpecification()

        spec.StreamEnabled <- table.streamEnabled

        // inaccurate. DyamodbinMemory tracks stream config at a subscriber level
        // Get the one with the least access, as it will have been defined by client
        // rather than by replication
        spec.StreamViewType <-
            table.streamConfigs
            |> Seq.sortWith (fun x y -> rankConfig x - rankConfig y)
            |> Seq.map (function
                | KeysOnly -> StreamViewType.KEYS_ONLY
                | NewImage -> StreamViewType.NEW_IMAGE
                | OldImage -> StreamViewType.OLD_IMAGE
                | NewAndOldImages -> StreamViewType.NEW_AND_OLD_IMAGES)
            |> Collection.tryHead
            |> CSharp.fromOption

        spec

    let buildReplica databaseId (replicaIndexes: Index seq) =
        let op = ReplicaDescription()
        op.GlobalSecondaryIndexes <- buildReplicaGsis replicaIndexes |> Enumerable.ToList
        op.RegionName <- databaseId.regionId

        op

    let toAttributeDefinitions attr =
        attr
        |> TableKeyAttributeList.attributes
        |> Seq.map (fun struct (name, t) ->
            match t with
            | AttributeType.String -> ScalarAttributeType.S
            | AttributeType.Number -> ScalarAttributeType.N
            | AttributeType.Binary -> ScalarAttributeType.B
            | _ -> null
            |> fun x ->
                let def = AttributeDefinition()
                def.AttributeName <- name
                def.AttributeType <- x
                def)

    let getReplicas databaseId (cluster: Api.DistributedDatabase voption) (table: TableDetails) includeCurrent =
        let hasIndex =
            flip Map.containsKey table.indexes
            |> flip (?|>)
            >> ValueOption.defaultValue false

        cluster
        ?>>= fun dbs -> dbs.TryDescribeGlobalTable ValueNone ValueNone table.name
        ?|> NonEmptyList.unwrap
        ?>>= fun xs ->
                if Collection.tryFind (fstT >> ((=) databaseId)) xs |> ValueOption.isSome
                then ValueSome xs
                else ValueNone
        ?|> if includeCurrent then id else List.filter (fstT >> (<>) databaseId)
        ?|? []
        |> List.sortBy (
            fstT
            >> fun x -> if x = databaseId then "" else x.regionId)
        |> Seq.map (
            mapSnd (
                Table.indexes
                >> _.Values
                >> Seq.filter (Index.getName >> hasIndex)))
        |> Seq.map (uncurry buildReplica)
        |> Enumerable.ToList

    let tableDescription awsAccountId databaseId (cluster: Api.DistributedDatabase voption) (table: TableDetails) status =

        let description = TableDescription()
        description.CreationDateTime <- table.createdDate.UtcDateTime
        description.ItemCount <- table.itemCount
        description.TableStatus <- status
        description.TableArn <- table.arn struct (awsAccountId, databaseId.regionId)
        description.TableId <- Table.getId table.table
        description.StreamSpecification <- buildStreamSpec table
        description.LatestStreamArn <- table.streamArn struct (awsAccountId, databaseId.regionId) |> CSharp.fromOption 
        description.LatestStreamLabel <- table.streamLabel |> CSharp.fromOption
        description.DeletionProtectionEnabled <- table.hasDeletionProtection
        description.TableName <- table.name
        description.GlobalSecondaryIndexes <- buildGsis awsAccountId databaseId table.table table.indexes.Values |> Enumerable.ToList
        description.LocalSecondaryIndexes <- buildLsis awsAccountId databaseId table.table table.indexes.Values |> Enumerable.ToList
        description.AttributeDefinitions <- toAttributeDefinitions table.attributes |> Enumerable.ToList
        description.KeySchema <- toKeySchema table.primaryIndex
        description.Replicas <- getReplicas databaseId cluster table false
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html
        if description.Replicas.Count > 0
        then description.GlobalTableVersion <- "2019.11.21"

        description

    let output awsAccountId databaseId (table: TableDetails) =

        let output = Shared.amazonWebServiceResponse<DescribeTableResponse>()
        output.Table <- tableDescription awsAccountId databaseId ValueNone table TableStatus.ACTIVE
        output
        
module List =
    
    let inputs1 (req: ListTablesRequest) =
        // hack here. Set the limit to + 1 and then truncate in the output layer to get exclusive start key
        struct (req.ExclusiveStartTableName |> CSharp.toNullOrEmptyOption, req.Limit)
        
    let inputs2 () =
        struct (ValueNone, Int32.MaxValue)
        
    let inputs3 name =
        struct (CSharp.mandatory "exclusiveStartTableName is mandatory" name |> ValueSome, Int32.MaxValue)
        
    let inputs4 (struct (name, limit) & x) =
        // hack here. Set the limit to + 1 and then truncate in the output layer to get exclusive start key
        struct (CSharp.mandatory "exclusiveStartTableName is mandatory" name |> ValueSome, limit)
        
    let inputs5 limit =
        struct (ValueNone, limit)

    let output expectedLimit _ (names: _ seq) =

        // hack here. Set the limit to + 1 in the input and then truncate in the output layer to get exclusive start key
        let output = Shared.amazonWebServiceResponse<ListTablesResponse>()
        
        output.TableNames <- new MList<_>(names |> Seq.map fstT)
        if output.TableNames.Count >= expectedLimit
        then output.LastEvaluatedTableName <- output.TableNames[output.TableNames.Count - 1]
            
        output
        
module Global =

    let globalTableDescription awsAccountId databaseId (cluster: Api.DistributedDatabase voption) (table: TableDetails) status =

        let hasIndex =
            flip Map.containsKey table.indexes
            |> flip (?|>)
            >> ValueOption.defaultValue false

        let description = GlobalTableDescription()
        description.CreationDateTime <- table.createdDate.UtcDateTime
        description.GlobalTableName <- table.name
        description.GlobalTableArn <- table.arn struct (awsAccountId, databaseId.regionId)
        description.GlobalTableStatus <- status
        description.ReplicationGroup <- Local.getReplicas databaseId cluster table true

        description

    let output awsAccountId (cluster: Api.DistributedDatabase voption) status databaseId (table: TableDetails) =

        let output = Shared.amazonWebServiceResponse<DescribeGlobalTableResponse>()
        output.GlobalTableDescription <- globalTableDescription awsAccountId databaseId cluster table status
        output
        
    module List =
        
        let inputs (req: ListGlobalTablesRequest) =
            struct (
                (req.RegionName |> CSharp.toNullOrEmptyOption ?|> fun x -> {regionId = x }),
                req.ExclusiveStartGlobalTableName |> CSharp.toNullOrEmptyOption,
                if req.Limit < 1 then Int32.MaxValue else req.Limit)
    
        let output expectedLimit _ (tables: NonEmptyList<struct (DatabaseId * Table)> seq) =
    
            let output = Shared.amazonWebServiceResponse<ListGlobalTablesResponse>()
            let globalTables =
                tables
                |> Seq.map NonEmptyList.unwrap
                |> Seq.map (fun group ->
                    let t = GlobalTable()
                    t.GlobalTableName <- List.head group |> sndT |> Table.name
                    t.ReplicationGroup <- MList<_>(
                        group
                        |> Seq.map fstT
                        |> Seq.map (fun item ->
                            let r = Replica()
                            r.RegionName <- item.regionId 
                            r))
                    t)
                |> Array.ofSeq
            
            if globalTables.Length >= expectedLimit
            then output.LastEvaluatedGlobalTableName <- globalTables[globalTables.Length - 1].GlobalTableName
            output.GlobalTables <- MList<_>(globalTables)
            output