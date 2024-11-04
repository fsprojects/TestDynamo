module Tests.Table

open System.Linq
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory
open DynamoDbInMemory.Data.BasicStructures
open Amazon.DynamoDBv2
open DynamoDbInMemory.Utils
open DynamoDbInMemory.Data.Monads.Operators

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue

type TableBuilder =
    { tName: string voption
      attrs: Map<string, string>
      kSchema: struct (string * string voption) voption
      deleteGsis: string list
      gsis: Map<string, struct (string * string voption * Projection)>
      lsis: Map<string, struct (string * string voption * Projection)>
      deletionProtection: bool voption
      streamType: CreateOrDelete<unit> voption
      createRegionReplications: string list
      deleteRegionReplications: string list }
    with

    static member empty =
        { tName = ValueNone
          attrs = Map.empty
          kSchema = ValueNone
          deleteGsis = []
          lsis = Map.empty 
          gsis = Map.empty
          streamType = ValueNone
          deletionProtection = ValueNone
          createRegionReplications = []
          deleteRegionReplications = [] }

    static member withTableName name x = { x with tName = ValueSome name }
    static member tableName x = x.tName |> ValueOption.defaultWith (fun _ -> invalidOp "missing table name")

    static member withStreamAction streamType x = { x with streamType = ValueSome streamType }

    static member deleteStream x = { x with streamType = ValueSome (Delete ()) }

    static member withDeleteGsi name x = { x with deleteGsis = name::x.deleteGsis }

    static member withDeletionProtection v x = { x with deletionProtection = ValueSome v }: TableBuilder

    static member withAttribute name ``type`` x = { x with attrs = Map.add name ``type`` x.attrs }
    static member attributes (x: TableBuilder) =
        x.attrs
        |> MapUtils.toSeq
        |> Seq.map (fun struct (k, v) ->
            let attr = AttributeDefinition()
            attr.AttributeName <- k
            attr.AttributeType <-
                match v with
                | "S" -> ScalarAttributeType.S
                | "N" -> ScalarAttributeType.N
                | "B" -> ScalarAttributeType.B
                | x -> invalidOp $"unknown {x}"

            attr)
        |> Enumerable.ToList

    static member withReplication regionName x =
        { x with createRegionReplications = regionName::x.createRegionReplications }

    static member withReplicationDelete regionName x =
        { x with deleteRegionReplications = regionName::x.deleteRegionReplications }

    static member provisionedThroughput () = ProvisionedThroughput()

    static member private keySchema x =
        let struct (p, s) = x

        [
            KeySchemaElement(p, KeyType.HASH) |> ValueSome
            s ?|> (fun s' -> KeySchemaElement(s', KeyType.RANGE))
        ]
        |> Maybe.traverse
        |> Enumerable.ToList

    static member withKeySchema partitionKey sortKey x =
        { x with kSchema = struct (partitionKey, sortKey) |> ValueSome }
    static member keySchema x =
        TableBuilder.keySchema (x.kSchema |> ValueOption.defaultWith (fun _ -> invalidOp "Key schema not defined"))

    static member withSimpleGsi name partitionKey sortKey projectAttrs x =
        let projection = Projection()
        projection.ProjectionType <- if projectAttrs then ProjectionType.ALL else ProjectionType.KEYS_ONLY

        { x with gsis = Map.add name struct (partitionKey, sortKey, projection) x.gsis }

    static member withSimpleLsi name partitionKey sortKey projectAttrs x =
        let projection = Projection()
        projection.ProjectionType <- if projectAttrs then ProjectionType.ALL else ProjectionType.KEYS_ONLY

        { x with lsis = Map.add name struct (partitionKey, sortKey, projection) x.lsis }

    static member withComplexGsi name partitionKey sortKey projectAttrs x =
        let projection = Projection()
        projection.ProjectionType <- ProjectionType.INCLUDE
        projection.NonKeyAttributes <- Enumerable.ToList projectAttrs

        { x with gsis = Map.add name struct (partitionKey, sortKey, projection) x.gsis }

    static member withComplexLsi name partitionKey sortKey projectAttrs x =
        let projection = Projection()
        projection.ProjectionType <- ProjectionType.INCLUDE
        projection.NonKeyAttributes <- Enumerable.ToList projectAttrs

        { x with lsis = Map.add name struct (partitionKey, sortKey, projection) x.lsis }

    static member gsi x =
        x.gsis
        |> Seq.map (fun x ->
            let struct (p, s, proj) = x.Value
            let index = GlobalSecondaryIndex()
            index.IndexName <- x.Key
            index.Projection <- proj
            index.KeySchema <- TableBuilder.keySchema struct (p, s)

            index)
        |> Enumerable.ToList

    static member lsi x =
        x.lsis
        |> Seq.map (fun x ->
            let struct (p, s, proj) = x.Value
            let index = LocalSecondaryIndex()
            index.IndexName <- x.Key
            index.Projection <- proj
            index.KeySchema <- TableBuilder.keySchema struct (p, s)

            index)
        |> Enumerable.ToList

    static member streamSpecification x =
        let view =
            x.streamType
            ?|> (function
                | CreateOrDelete.Create _ -> true
                | CreateOrDelete.Delete _ -> false)

        match view with
        | ValueNone -> null
        | ValueSome enabled ->
            let spec = StreamSpecification()
            spec.StreamEnabled <- enabled
            spec

    static member req x =

        if List.length x.createRegionReplications > 0 then invalidOp "Not supported"
        if List.length x.deleteRegionReplications > 0 then invalidOp "Not supported"
        if List.length x.deleteGsis > 0 then invalidOp "Not supported"

        let output = CreateTableRequest()

        output.TableName <- TableBuilder.tableName x
        output.AttributeDefinitions <- TableBuilder.attributes x
        output.ProvisionedThroughput <- TableBuilder.provisionedThroughput ()
        output.KeySchema <- TableBuilder.keySchema x
        output.GlobalSecondaryIndexes <- TableBuilder.gsi x
        output.LocalSecondaryIndexes <- TableBuilder.lsi x
        output.StreamSpecification <- TableBuilder.streamSpecification x
        x.deletionProtection ?|> (fun x -> output.DeletionProtectionEnabled <- x) |> ValueOption.defaultValue ()

        output

    static member updateReq x =
        if x.kSchema |> ValueOption.isSome then invalidOp "Not supported"
        if x.lsis <> Map.empty then invalidOp "Not supported"

        let output = UpdateTableRequest()

        output.TableName <- TableBuilder.tableName x
        output.AttributeDefinitions <- TableBuilder.attributes x
        output.ProvisionedThroughput <- TableBuilder.provisionedThroughput ()

        x.createRegionReplications
        |> List.fold (fun s x ->
            if output.ReplicaUpdates = null 
            then output.ReplicaUpdates <- System.Collections.Generic.List()

            output.ReplicaUpdates.Add(ReplicationGroupUpdate())
            output.ReplicaUpdates[output.ReplicaUpdates.Count - 1].Create <- CreateReplicationGroupMemberAction()
            output.ReplicaUpdates[output.ReplicaUpdates.Count - 1].Create.RegionName <- x
            s) ()

        x.deleteRegionReplications
        |> List.fold (fun s x ->
            if output.ReplicaUpdates = null 
            then output.ReplicaUpdates <- System.Collections.Generic.List()

            output.ReplicaUpdates.Add(ReplicationGroupUpdate())
            output.ReplicaUpdates[output.ReplicaUpdates.Count - 1].Delete <- DeleteReplicationGroupMemberAction()
            output.ReplicaUpdates[output.ReplicaUpdates.Count - 1].Delete.RegionName <- x
            s) ()

        output.GlobalSecondaryIndexUpdates <-
            TableBuilder.gsi x
            |> Seq.map (fun x ->
                let gsi = GlobalSecondaryIndexUpdate()
                gsi.Create <- CreateGlobalSecondaryIndexAction()
                gsi.Create.IndexName <- x.IndexName
                gsi.Create.Projection <- x.Projection
                gsi.Create.KeySchema <- x.KeySchema
                gsi.Create.ProvisionedThroughput <- x.ProvisionedThroughput
                gsi.Create.OnDemandThroughput <- x.OnDemandThroughput
                gsi)
            |> Collection.concat2 (
                x.deleteGsis
                |> Seq.map (fun x ->
                    let gsi = GlobalSecondaryIndexUpdate()
                    gsi.Delete <- DeleteGlobalSecondaryIndexAction()
                    gsi.Delete.IndexName <- x
                    gsi))
            |> CSharp.MList

        output.StreamSpecification <- TableBuilder.streamSpecification x
        x.deletionProtection ?|> (fun x -> output.DeletionProtectionEnabled <- x) |> ValueOption.defaultValue ()

        output
