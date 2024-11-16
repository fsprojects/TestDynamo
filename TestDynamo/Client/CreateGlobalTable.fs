module TestDynamo.Client.CreateGlobalTable

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Client
open TestDynamo.Model
open TestDynamo.Api.FSharp
open TestDynamo.Utils

type MList<'a> = System.Collections.Generic.List<'a>

let inputs (req: CreateGlobalTableRequest) =

    let replicaInstructions =
        req.ReplicationGroup
        |> CSharp.sanitizeSeq
        |> Seq.collect (fun x ->
             [
                 { copyIndexes = ReplicateAll
                   databaseId = { regionId = CSharp.mandatory "Region name is mandatory for replica updates" x.RegionName } } |> Either1
             ])
        |> List.ofSeq

    { tableName = req.GlobalTableName
      globalTableData =
          { replicaInstructions = replicaInstructions
            createStreamsForReplication = false }
      tableData =
          { updateTableData = UpdateTableData.empty
            streamConfig = ValueNone } }

let output awsAccountId ddb databaseId (table: TableDetails) =

    let output = Shared.amazonWebServiceResponse<CreateGlobalTableResponse>()
    output.GlobalTableDescription <- DescribeTable.Global.globalTableDescription awsAccountId databaseId ddb table GlobalTableStatus.ACTIVE
    output