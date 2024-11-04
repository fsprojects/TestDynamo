module TestDynamo.Client.CreateGlobalTable

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.Client
open TestDynamo.Model
open TestDynamo.Api.FSharp

type MList<'a> = System.Collections.Generic.List<'a>

let inputs1 (req: CreateGlobalTableRequest) =

    let replicaInstructions =
        req.ReplicationGroup
        |> CSharp.sanitizeSeq
        |> Seq.collect (fun x ->
             [
                 CreateOrDelete.Create { regionId = CSharp.mandatory "Region name is mandatory for replica updates" x.RegionName }
             ])
        |> List.ofSeq

    { tableName = req.GlobalTableName
      globalTableData =
          { replicaInstructions = replicaInstructions
            createStreamsForReplication = false }
      tableData =
          { updateTableData =
                { createGsi = Map.empty
                  deleteGsi = Set.empty
                  deletionProtection = ValueNone
                  attributes = [] }
            streamConfig = ValueNone } }

let output awsAccountId ddb databaseId (table: TableDetails) =

    let output = Shared.amazonWebServiceResponse<CreateGlobalTableResponse>()
    output.GlobalTableDescription <- DescribeTable.Global.globalTableDescription awsAccountId databaseId ddb table GlobalTableStatus.ACTIVE
    output