[<RequireQualifiedAccess>]
module TestDynamo.Client.Table

open System
open TestDynamo
open TestDynamo.Api.FSharp
open TestDynamo.Data.BasicStructures
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils
open TestDynamo.Model
open TestDynamo.Client.Shared
open TestDynamo.GeneratedCode.Dtos

module Local =

    let private toKeySchema x =
        Index.getKeyConfig x
        |> KeyConfig.keyCols
        |> Seq.mapi (fun i struct (k, _) ->
            { AttributeName = !!<k
              KeyType =
                // hack. But not overly important + having PK as first in the list
                // is a nice convention to have
                match i with
                | 0 -> !!<KeyType.HASH
                | _ -> !!<KeyType.RANGE }: KeySchemaElement)
        |> Array.ofSeq

    let private buildProjection (p: TestDynamo.Model.ProjectionType) keys =

        match p with
        | All ->
            { ProjectionType = !!<ProjectionType.ALL
              NonKeyAttributes = !!<[||] }: Projection
        | Keys k ->
            { ProjectionType = !!<ProjectionType.KEYS_ONLY
              NonKeyAttributes = !!<[||] }
        | Attributes attr ->
            let k =
                Seq.collect KeyConfig.keyNames keys
                |> Seq.distinct
                |> Array.ofSeq
                |> flip Array.contains

            { ProjectionType = !!<ProjectionType.INCLUDE
              NonKeyAttributes = 
                attr
                |> Seq.filter (k >> not)
                |> Array.ofSeq
                |> ValueSome }

    let private buildGsi awsAccountId databaseId t (x: Index) =
        { Backfilling = !!<false
          IndexName = Index.getName x
          IndexArn = Index.getArn struct (awsAccountId, databaseId.regionId) x
          ItemCount = Index.itemCount x |> int64 |> ValueSome
          IndexStatus = !!<IndexStatus.ACTIVE
          KeySchema = toKeySchema x |> ValueSome
          Projection = buildProjection (Index.projections x) [Index.keyConfig x; Table.keyConfig t] |> ValueSome

          IndexSizeBytes = ValueNone
          OnDemandThroughput = ValueNone
          ProvisionedThroughput = ValueNone }: GlobalSecondaryIndexDescription

    let private buildLsi awsAccountId databaseId t (x: Index) =

        { IndexName = Index.getName x
          IndexArn = Index.getArn struct (awsAccountId, databaseId.regionId) x
          ItemCount = Index.itemCount x |> int64 |> ValueSome
          KeySchema = toKeySchema x |> ValueSome
          Projection = buildProjection (Index.projections x) [Index.keyConfig x; Table.keyConfig t] |> ValueSome

          IndexSizeBytes = ValueNone }: LocalSecondaryIndexDescription

    let private buildReplicaGsi (x: Index) =
        { IndexName = Index.getName x
          OnDemandThroughputOverride = ValueNone
          ProvisionedThroughputOverride =  ValueNone }: ReplicaGlobalSecondaryIndexDescription

    let private buildGsis awsAccountId databaseId t = Seq.filter Index.isGsi >> Seq.map (buildGsi awsAccountId databaseId t)

    let private buildLsis awsAccountId databaseId t = Seq.filter Index.isLsi >> Seq.map (buildLsi awsAccountId databaseId t)

    let buildReplicaGsis = Seq.filter Index.isGsi >> Seq.map buildReplicaGsi

    let private rankConfig = function
        | KeysOnly -> 1
        | NewImage -> 2
        | OldImage -> 3
        | NewAndOldImages -> 4

    let private buildStreamSpec (table: TableDetails) =
        { StreamEnabled = !!<table.streamEnabled
          StreamViewType =
            // inaccurate. TestDynamo tracks stream config at a subscriber level
            // Get the one with the least access, as it will have been defined by client
            // rather than by replication
            table.streamConfigs
            |> Seq.sortWith (fun x y -> rankConfig x - rankConfig y)
            |> Seq.map (function
                | KeysOnly -> StreamViewType.KEYS_ONLY
                | NewImage -> StreamViewType.NEW_IMAGE
                | OldImage -> StreamViewType.OLD_IMAGE
                | NewAndOldImages -> StreamViewType.NEW_AND_OLD_IMAGES)
            |> Collection.tryHead }: StreamSpecification

    let private buildReplica databaseId (replicaIndexes: Index seq) =
        { GlobalSecondaryIndexes = buildReplicaGsis replicaIndexes |> Array.ofSeq |> ValueSome
          RegionName = !!<databaseId.regionId

          KMSMasterKeyId = ValueNone
          OnDemandThroughputOverride =  ValueNone
          ProvisionedThroughputOverride = ValueNone
          ReplicaStatusPercentProgress = ValueNone
          ReplicaInaccessibleDateTime = ValueNone
          ReplicaStatus = ValueNone
          ReplicaStatusDescription = ValueNone
          ReplicaTableClassSummary = ValueNone }: ReplicaDescription

    let private toAttributeDefinitions attr =
        attr
        |> TableKeyAttributeList.attributes
        |> Seq.map (fun struct (name, t) ->
            match t with
            | AttributeType.String -> ScalarAttributeType.S
            | AttributeType.Number -> ScalarAttributeType.N
            | AttributeType.Binary -> ScalarAttributeType.B
            | x -> serverError $"Unexpected scalar attribute type {x}"
            |> fun x ->
                { AttributeName = !!<name
                  AttributeType = !!<x }: AttributeDefinition)

    let replicas databaseId (cluster: Api.FSharp.GlobalDatabase voption) (table: TableDetails) includeCurrent =

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
        |> Collection.mapSnd (
            Table.indexes
            >> _.Values
            >> Seq.filter (Index.getName >> hasIndex))
        |> Seq.map (fun struct (dbId, indexes) -> buildReplica dbId indexes)
        |> Array.ofSeq

    let tableDescription awsAccountId databaseId (cluster: Api.FSharp.GlobalDatabase voption) (table: TableDetails) status =

        let replicas = replicas databaseId cluster table false
        { CreationDateTime = table.createdDate.UtcDateTime |> ValueSome
          ItemCount = table.itemCount |> int64 |> ValueSome
          TableStatus = !!<status
          TableArn = table.arn struct (awsAccountId, databaseId.regionId) |> ValueSome
          TableId = Table.getId table.table |> ValueSome
          StreamSpecification = buildStreamSpec table |> ValueSome
          LatestStreamArn = table.streamArn struct (awsAccountId, databaseId.regionId)
          LatestStreamLabel = table.streamLabel
          DeletionProtectionEnabled = !!<table.hasDeletionProtection
          TableName = !!<table.name
          GlobalSecondaryIndexes = buildGsis awsAccountId databaseId table.table table.indexes.Values |> Array.ofSeq |> ValueSome
          LocalSecondaryIndexes = buildLsis awsAccountId databaseId table.table table.indexes.Values |> Array.ofSeq |> ValueSome
          AttributeDefinitions = toAttributeDefinitions table.attributes |> Array.ofSeq |> ValueSome
          KeySchema = toKeySchema table.primaryIndex |> ValueSome
          Replicas = !!<replicas
          GlobalTableVersion =
            // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html
            if replicas.Length > 0 then "2019.11.21"  |> ValueSome else ValueNone

          ArchivalSummary = ValueNone
          BillingModeSummary = ValueNone
          OnDemandThroughput = ValueNone
          ProvisionedThroughput = ValueNone
          RestoreSummary = ValueNone
          SSEDescription = ValueNone
          TableClassSummary = ValueNone
          TableSizeBytes = ValueNone }: TableDescription

    let private fromKeySchema (xs: KeySchemaElement list) =
        let pk = xs |> Seq.filter (_.KeyType ??|> (_.Value >> (=) KeyType.HASH.Value) ??|? false) |> Collection.tryHead
        let sk = xs |> Seq.filter (_.KeyType ??|> (_.Value >> (=) KeyType.RANGE.Value) ??|? false) |> Collection.tryHead

        match struct (pk, sk, List.length xs) with
        | ValueSome { AttributeName = ValueSome pka }, ValueNone, 1 -> struct (pka, ValueNone)
        | ValueSome { AttributeName = ValueSome pka }, ValueSome { AttributeName = ValueSome ska }, 2 -> struct (pka, ValueSome ska)
        | ValueNone, _, _
        | ValueSome _, _, _ -> ClientError.clientError "Invalid key"

    let private buildGsiSchema keySchema (projection: Projection) =
        let key = fromKeySchema (keySchema |> List.ofSeq)
        let struct (projection, projectionsAreKeys) =
            match projection.ProjectionType with
            | ValueSome t when t.Value = ProjectionType.ALL.Value -> struct (ValueNone, false)
            | ValueSome t when t.Value = ProjectionType.KEYS_ONLY.Value -> struct (List.empty |> ValueSome, true)
            | ValueSome t when t.Value = ProjectionType.INCLUDE.Value -> struct (projection.NonKeyAttributes ?|? [||] |> List.ofSeq |> ValueSome, false)
            | t -> ClientError.clientError $"Invalid projection type {t}"

        { keys = key; projectionCols = projection; projectionsAreKeys = projectionsAreKeys  }

    let private fromAttributeDefinitions attr =
        Seq.map (fun (x: AttributeDefinition) ->
            match x.AttributeType with
            | ValueSome ``type`` when ``type``.Value = ScalarAttributeType.S.Value -> AttributeType.String
            | ValueSome ``type`` when ``type``.Value = ScalarAttributeType.N.Value -> AttributeType.Number
            | ValueSome ``type`` when ``type``.Value = ScalarAttributeType.B.Value -> AttributeType.Binary
            | ValueSome ``type`` -> ClientError.clientError $"Invalid attribute type {``type``} for key schema element {x.AttributeName}"
            | ValueNone -> ClientError.clientError $"Attribute type not set for key schema element {x.AttributeName}"
            |> tpl (x.AttributeName <!!> nameof x.AttributeName)) attr

    let private buildStreamConfig (streamSpecification: StreamSpecification) =
        match streamSpecification with
        | x when x.StreamEnabled ?|? false |> not -> DeleteStream |> ValueSome
        | { StreamViewType = ValueNone } -> CreateStream |> ValueSome
        | { StreamViewType = ValueSome x } ->
            match StreamDataType.tryParse x.Value with
            | ValueNone -> ClientError.clientError $"Invalid StreamViewType \"{x.Value}\""
            | ValueSome _ & x -> CreateStream |> ValueSome

    module Describe =

        let output awsAccountId databaseId (table: TableDetails) =
            { Table = tableDescription awsAccountId databaseId ValueNone table TableStatus.ACTIVE |> ValueSome
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }: DescribeTableResponse

    module List =

        let input (req: ListTablesRequest) =
            struct (noneifyStrings req.ExclusiveStartTableName, req.Limit ?|? Int32.MaxValue)

        let output expectedLimit _ (names: _ seq): ListTablesResponse =
            let tableNames = names |> Seq.map fstT |> Array.ofSeq
            let tableNamesL = tableNames.Length
            { TableNames = tableNames |> ValueSome
              LastEvaluatedTableName =
                  if tableNamesL >= expectedLimit
                  then tableNames[tableNamesL - 1] |> ValueSome
                  else ValueNone
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

    module Create =

        let gsiSchema keySchema (projection: Projection) =
            let key = fromKeySchema (keySchema |> List.ofSeq)
            let struct (projection, projectionsAreKeys) =
                match projection.ProjectionType ?|> _.Value with
                | ValueSome t when t = ProjectionType.ALL.Value -> struct (ValueNone, false)
                | ValueSome t when t = ProjectionType.KEYS_ONLY.Value -> struct (List.empty |> ValueSome, true)
                | ValueSome t when t = ProjectionType.INCLUDE.Value -> struct (projection.NonKeyAttributes ?|? [||] |> List.ofSeq |> ValueSome, false)
                | ValueNone -> ClientError.clientError $"Projection type required"
                | t -> ClientError.clientError $"Invalid projection type {t}"

            { keys = key; projectionCols = projection; projectionsAreKeys = projectionsAreKeys  }

        let private noProjection: Projection = { ProjectionType = ValueNone; NonKeyAttributes = ValueNone }
        let private buildGsiSchema' (x: GlobalSecondaryIndex) = gsiSchema (x.KeySchema ?|? [||]) (x.Projection ?|? noProjection)

        let buildLsiSchema tablePk keySchema =
            match fromKeySchema (keySchema |> List.ofSeq) with
            | _, ValueNone -> ClientError.clientError "Sort key is mandatory for local secondary index"
            | indexPk, ValueSome _ when indexPk <> tablePk -> ClientError.clientError $"Partition key ({indexPk}) must be the same as table partition key ({tablePk}) for local secondary index"
            | _, ValueSome _ -> gsiSchema keySchema

        let addLsi key oldV newV =
            match oldV with
            | ValueNone -> newV
            | ValueSome _ -> ClientError.clientError $"Duplicate key definition {key}"

        let private buildLsiSchema' tablePk (x: LocalSecondaryIndex) = buildLsiSchema tablePk (x.KeySchema ?|? [||]) (x.Projection ?|? noProjection)

        let input (req: CreateTableRequest): CreateTableData =

            let indexes =
                req.GlobalSecondaryIndexes
                ?|? [||]
                |> Seq.map (fun x -> { isLocal = false; data = buildGsiSchema' x } |> tpl (x.IndexName <!!> nameof x.IndexName))
                |> MapUtils.fromTuple

            let (struct (pk, _) & primaryIndex) = fromKeySchema (req.KeySchema ?|> List.ofSeq ?|? [])
            let indexes =
                Seq.fold (fun s x ->
                    { isLocal = true; data = buildLsiSchema' pk x }
                    |> flip (MapUtils.change addLsi (x.IndexName <!!> nameof x.IndexName)) s) indexes (req.LocalSecondaryIndexes ?|? [||])

            let tableConfig =
                { name = req.TableName <!!> nameof req.TableName
                  primaryIndex = primaryIndex
                  addDeletionProtection = req.DeletionProtectionEnabled ?|? false
                  attributes = fromAttributeDefinitions (req.AttributeDefinitions ?|? [||]) |> List.ofSeq
                  indexes = indexes }

            { tableConfig = tableConfig
              createStream = req.StreamSpecification ?>>= buildStreamConfig |> ValueOption.isSome }

        let output awsAccountId dbId (table: TableDetails): CreateTableResponse =

            { TableDescription = tableDescription awsAccountId dbId ValueNone table TableStatus.ACTIVE |> ValueSome
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

    module Delete =

        let output awsAccountId databaseId (table: TableDetails): DeleteTableResponse =
            { TableDescription = tableDescription awsAccountId databaseId ValueNone table TableStatus.DELETING |> ValueSome
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

    module Update =

        let inputs (req: UpdateTableRequest) =

            let replicaInstructions =
                req.ReplicaUpdates
                ?|? [||]
                |> Seq.collect (fun x ->
                    [
                        x.Create
                        ?|> fun x ->
                            { databaseId =
                               { regionId = x.RegionName <!!> nameof x.RegionName }
                              copyIndexes =
                                  x.GlobalSecondaryIndexes
                                  ?|? [||]
                                  |> Seq.map (fun x -> x.IndexName <!!> nameof x.IndexName)
                                  |> List.ofSeq
                                  |> ReplicateFromSource } |> Either1

                        x.Delete
                        ?|> fun x -> x.RegionName <!!> nameof x.RegionName
                        ?|> fun x -> Either2 { regionId = x }

                        if x.Update.IsNone then ValueNone else notSupported "Update ReplicaUpdates are not supported"
                    ] |> Maybe.traverse)
                |> List.ofSeq

            let struct (gsiCreate, gsiDelete) =
                req.GlobalSecondaryIndexUpdates
                ?|? [||]
                |> Seq.collect (fun x ->
                    [
                        x.Create
                        ?|> (fun x ->
                            buildGsiSchema (x.KeySchema <!!> nameof x.KeySchema) (x.Projection <!!> nameof x.Projection)
                            |> tpl (x.IndexName <!!> nameof x.IndexName))
                        ?|> Either1

                        x.Delete
                        ?|> (fun x -> x.IndexName <!!> nameof x.IndexName)
                        ?|> Either2

                        if x.Update.IsNone then ValueNone else notSupported "Update indexes are not supported"
                    ] |> Maybe.traverse)
                |> List.ofSeq
                |> Either.partition

            if List.length gsiCreate + List.length gsiDelete > 1 then ClientError.clientError "You can only create or delete one global secondary index per UpdateTable operation."

            { tableName = req.TableName  <!!> nameof req.TableName
              globalTableData =
                  { replicaInstructions = replicaInstructions
                    createStreamsForReplication = false }
              tableData =
                  { updateTableData =
                        { schemaChange =
                            { createGsi = gsiCreate |> MapUtils.fromTuple
                              deleteGsi = gsiDelete |> Set.ofSeq
                              attributes =
                                  req.AttributeDefinitions
                                  ?|? [||]
                                  |> fromAttributeDefinitions
                                  |> List.ofSeq }
                          deletionProtection = req.DeletionProtectionEnabled }
                    streamConfig = req.StreamSpecification ?>>= buildStreamConfig } }: TestDynamo.Api.FSharp.UpdateTableData

        let output awsAccountId ddb databaseId (table: TableDetails): UpdateTableResponse =
            { TableDescription = tableDescription awsAccountId databaseId ddb table TableStatus.ACTIVE |> ValueSome
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

module Global =

    let private buildReplica databaseId (replicaIndexes: Index seq): ReplicaDescription =

        { GlobalSecondaryIndexes = Local.buildReplicaGsis replicaIndexes |> Array.ofSeq |> ValueSome
          RegionName = !!< databaseId.regionId
          KMSMasterKeyId = ValueNone
          OnDemandThroughputOverride =  ValueNone
          ProvisionedThroughputOverride =  ValueNone
          ReplicaInaccessibleDateTime =   ValueNone
          ReplicaStatus =  ValueNone
          ReplicaStatusDescription = ValueNone
          ReplicaTableClassSummary =   ValueNone
          ReplicaStatusPercentProgress =  ValueNone }

    let getReplicas databaseId (cluster: Api.FSharp.GlobalDatabase voption) (table: TableDetails) includeCurrent =
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
        |> Array.ofSeq

    let globalTableDescription awsAccountId databaseId (cluster: Api.FSharp.GlobalDatabase voption) (table: TableDetails) status: GlobalTableDescription =

        let hasIndex =
            flip Map.containsKey table.indexes
            |> flip (?|>)
            >> ValueOption.defaultValue false

        { CreationDateTime = !!<table.createdDate.UtcDateTime
          GlobalTableName = !!<table.name
          GlobalTableArn = !!< (table.arn struct (awsAccountId, databaseId.regionId))
          GlobalTableStatus = !!<status
          ReplicationGroup = !!< (getReplicas databaseId cluster table true) }

    module Describe =

        let output awsAccountId (cluster: Api.FSharp.GlobalDatabase voption) status databaseId (table: TableDetails): DescribeGlobalTableResponse =

            { GlobalTableDescription = globalTableDescription awsAccountId databaseId cluster table status |> ValueSome
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

    module List =

        let input (req: ListGlobalTablesRequest) =
            struct (
                (req.RegionName |> noneifyStrings ?|> fun x -> {regionId = x }),
                req.ExclusiveStartGlobalTableName |> noneifyStrings,
                req.Limit ?|> (fun x -> if x < 1 then Int32.MaxValue else x) ?|? Int32.MaxValue)

        let output expectedLimit _ (tables: NonEmptyList<struct (DatabaseId * Table)> seq): ListGlobalTablesResponse =

            let globalTables =
                tables
                |> Seq.map NonEmptyList.unwrap
                |> Seq.map (fun group ->
                    { GlobalTableName = List.head group |> sndT |> Table.name |> ValueSome
                      ReplicationGroup =
                        group
                        |> Seq.map fstT
                        |> Seq.map (fun item -> { RegionName = ValueSome item.regionId }: Replica)
                        |> Array.ofSeq
                        |> ValueSome }: GlobalTable)
                |> Array.ofSeq

            let globalTablesL = globalTables.Length

            { GlobalTables = !!<globalTables
              LastEvaluatedGlobalTableName =
                  if globalTablesL >= expectedLimit && expectedLimit <> 0
                  then globalTables[globalTablesL - 1].GlobalTableName
                  else ValueNone
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

    module Create =

        let input (req: CreateGlobalTableRequest) =

            let replicaInstructions =
                req.ReplicationGroup
                ?|? [||]
                |> Seq.collect (fun x ->
                     [
                         { copyIndexes = ReplicateAll
                           databaseId = { regionId = x.RegionName <!!> nameof x.RegionName } } |> Either1
                     ])
                |> List.ofSeq

            { tableName = req.GlobalTableName <!!> nameof req.GlobalTableName
              globalTableData =
                  { replicaInstructions = replicaInstructions
                    createStreamsForReplication = false }
              tableData =
                  { updateTableData = UpdateTableData.empty
                    streamConfig = ValueNone } }

        let output awsAccountId ddb databaseId (table: TableDetails): CreateGlobalTableResponse =

            { GlobalTableDescription = globalTableDescription awsAccountId databaseId ddb table GlobalTableStatus.ACTIVE |> ValueSome
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }

    module Update =

        let input (req: UpdateGlobalTableRequest) =

            let replicaInstructions =
                req.ReplicaUpdates
                ?|? [||]
                |> Seq.collect (fun x ->
                    [
                        x.Create
                        ?|> (fun x -> x.RegionName <!!> nameof x.RegionName)
                        ?|> (fun x -> { copyIndexes = ReplicateAll; databaseId = { regionId = x } } |> Either1)

                        x.Delete
                        ?|> (fun x -> x.RegionName <!!> nameof x.RegionName)
                        ?|> (fun x -> Either2 { regionId = x })
                    ] |> Maybe.traverse)
                |> List.ofSeq

            { tableName = req.GlobalTableName <!!> nameof req.GlobalTableName
              globalTableData =
                  { replicaInstructions = replicaInstructions
                    createStreamsForReplication = false }
              tableData =
                  { updateTableData = UpdateTableData.empty
                    streamConfig = ValueNone } }

        let output awsAccountId ddb databaseId (table: TableDetails): UpdateGlobalTableResponse =

            { GlobalTableDescription = globalTableDescription awsAccountId databaseId ddb table GlobalTableStatus.UPDATING |> ValueSome
              ResponseMetadata = !!<Shared.ResponseHeaders.responseMetadata
              ContentLength = !!<Shared.ResponseHeaders.contentLength
              HttpStatusCode = !!<Shared.ResponseHeaders.httpStatusCode }