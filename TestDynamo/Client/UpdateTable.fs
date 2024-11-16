module TestDynamo.Client.UpdateTable

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Client
open TestDynamo.Model
open TestDynamo.Data.BasicStructures
open TestDynamo.Api.FSharp
open TestDynamo.Client.CreateTable
open TestDynamo.Client.DescribeTable.Local
open Shared

type MList<'a> = System.Collections.Generic.List<'a>

let getDeletionProtectionEnabled =
    getOptionalBool<UpdateTableRequest, bool> "DeletionProtectionEnabled"

let inputs (req: UpdateTableRequest) =

    let replicaInstructions =
        req.ReplicaUpdates
        |> CSharp.sanitizeSeq
        |> Seq.collect (fun x ->
            [
                x.Create
                |> CSharp.toOption
                ?|> fun x ->
                    { databaseId =
                       { regionId = CSharp.mandatory "Region name is mandatory for replica updates" x.RegionName }
                      copyIndexes =
                          x.GlobalSecondaryIndexes
                          |> CSharp.sanitizeSeq
                          |> Seq.map (fun x -> x.IndexName |> CSharp.mandatory "IndexName is mandatory for ReplicaGlobalSecondaryIndex")
                          |> List.ofSeq
                          |> ReplicateFromSource } |> Either1

                x.Delete
                |> CSharp.toOption
                ?|> fun x -> CSharp.mandatory "Region name is mandatory for replica updates" x.RegionName
                ?|> fun x -> Either2 { regionId = x }

                if x.Update = null then ValueNone else notSupported "Update ReplicaUpdates are not supported"
            ] |> Maybe.traverse)
        |> List.ofSeq

    let struct (gsiCreate, gsiDelete) =
        req.GlobalSecondaryIndexUpdates
        |> CSharp.sanitizeSeq
        |> Seq.collect (fun x ->
            [
                x.Create
                |> CSharp.toOption
                ?|> (fun x -> buildGsiSchema
                                                 (CSharp.mandatory "KeySchema is mandatory for GSI updates" x.KeySchema)
                                                 (CSharp.mandatory "Projection is mandatory for GSI updates" x.Projection)
                                                 |> tpl (CSharp.mandatory "IndexName is mandatory for GSI updates" x.IndexName))
                ?|> Either1

                x.Delete
                |> CSharp.toOption
                ?|> (fun x -> CSharp.mandatory "IndexName is mandatory for GSI updates" x.IndexName)
                ?|> Either2

                if x.Update = null then ValueNone else notSupported "Update indexes are not supported"
            ] |> Maybe.traverse)
        |> List.ofSeq
        |> Either.partition

    if List.length gsiCreate + List.length gsiDelete > 1 then clientError "You can only create or delete one global secondary index per UpdateTable operation."

    { tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
      globalTableData =
          { replicaInstructions = replicaInstructions
            createStreamsForReplication = false }
      tableData =
          { updateTableData =
                { createGsi = gsiCreate |> MapUtils.fromTuple
                  deleteGsi = gsiDelete |> Set.ofSeq
                  deletionProtection = getDeletionProtectionEnabled req
                  attributes =
                      req.AttributeDefinitions
                      |> CSharp.sanitizeSeq
                      |> fromAttributeDefinitions
                      |> List.ofSeq }
            streamConfig = buildStreamConfig req.StreamSpecification } }

let output awsAccountId ddb databaseId (table: TableDetails) =

    let output = Shared.amazonWebServiceResponse<UpdateTableResponse>()
    output.TableDescription <- tableDescription awsAccountId databaseId ddb table TableStatus.ACTIVE
    output