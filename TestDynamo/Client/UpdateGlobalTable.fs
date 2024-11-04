module TestDynamo.Client.UpdateGlobalTable

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Client
open TestDynamo.Model
open TestDynamo.Api
open TestDynamo.Client.DescribeTable.Global

type MList<'a> = System.Collections.Generic.List<'a>

let inputs1 (req: UpdateGlobalTableRequest) =

    let replicaInstructions =
        req.ReplicaUpdates
        |> CSharp.sanitizeSeq
        |> Seq.collect (fun x ->
            [
                x.Create
                |> CSharp.toOption
                ?|> (fun x -> CSharp.mandatory "Region name is mandatory for replica updates" x.RegionName)
                ?|> (fun x -> CreateOrDelete.Create { regionId = x })

                x.Delete
                |> CSharp.toOption
                ?|> (fun x -> CSharp.mandatory "Region name is mandatory for replica updates" x.RegionName)
                ?|> (fun x -> CreateOrDelete.Delete { regionId = x })
            ] |> Maybe.traverse)
        |> List.ofSeq

    { tableName = req.GlobalTableName
      distributedTableData =
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

    let output = Shared.amazonWebServiceResponse<UpdateGlobalTableResponse>()
    output.GlobalTableDescription <- globalTableDescription awsAccountId databaseId ddb table GlobalTableStatus.UPDATING
    output