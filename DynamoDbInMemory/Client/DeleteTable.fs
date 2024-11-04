module DynamoDbInMemory.Client.DeleteTable

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory.Client
open DynamoDbInMemory.Model

type MList<'a> = System.Collections.Generic.List<'a>

let output awsAccountId databaseId (table: TableDetails) =
    let output = Shared.amazonWebServiceResponse<DeleteTableResponse>()
    output.TableDescription <- DescribeTable.Local.tableDescription awsAccountId databaseId ValueNone table TableStatus.DELETING
    output