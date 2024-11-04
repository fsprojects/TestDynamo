module Tests.ClientLoggerContainer

open System
open System.Collections.Generic
open System.Threading.Tasks
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory.Api
open DynamoDbInMemory.Client
open Microsoft.Extensions.Logging

/// <summary>A wrapper around an IInMemoryDynamoDbClient and TestLogger which disposes of both</summary>
type ClientContainer(client: IInMemoryDynamoDbClient, logger: ILogger) =

    let disposableLogger =
        match box logger with
        | :? IDisposable as l -> l
        | _ -> invalidOp "Logger must be disposable"
        
    new(host: Database, logger: ILogger) = new ClientContainer(InMemoryDynamoDbClient.Create(host, logger), logger)

    member this.Client = client

    interface IDisposable with
        member this.Dispose() =
            client.Dispose()
            disposableLogger.Dispose()

    interface IInMemoryDynamoDbClient with

        member this.Database = client.Database
        member this.DistributedDatabase = client.DistributedDatabase
        member this.AwaitAllSubscribers(var0) = client.AwaitAllSubscribers(var0)
        member this.AwsAccountId = client.AwsAccountId
        member this.AwsAccountId with set value = client.AwsAccountId <- value
        member this.BatchExecuteStatementAsync(request, cancellationToken) = client.BatchExecuteStatementAsync(request, cancellationToken)
        member this.BatchGetItemAsync(requestItems, returnConsumedCapacity, cancellationToken) = client.BatchGetItemAsync(requestItems, returnConsumedCapacity, cancellationToken)
        member this.BatchGetItemAsync(requestItems: Dictionary<string,KeysAndAttributes>, cancellationToken: Threading.CancellationToken): Task<BatchGetItemResponse> = client.BatchGetItemAsync(requestItems, cancellationToken)
        member this.BatchGetItemAsync(request: BatchGetItemRequest, cancellationToken: Threading.CancellationToken): Task<BatchGetItemResponse> = client.BatchGetItemAsync(request, cancellationToken)
        member this.BatchWriteItemAsync(requestItems: Dictionary<string,List<WriteRequest>>, cancellationToken: Threading.CancellationToken): Task<BatchWriteItemResponse> = client.BatchWriteItemAsync(requestItems, cancellationToken)
        member this.BatchWriteItemAsync(request: BatchWriteItemRequest, cancellationToken: Threading.CancellationToken): Task<BatchWriteItemResponse> = client.BatchWriteItemAsync(request, cancellationToken)
        member this.CreateBackupAsync(request, cancellationToken) = client.CreateBackupAsync(request, cancellationToken)
        member this.CreateGlobalTableAsync(request, cancellationToken) = client.CreateGlobalTableAsync(request, cancellationToken)
        member this.CreateTableAsync(tableName, keySchema, attributeDefinitions, provisionedThroughput, cancellationToken) = client.CreateTableAsync(tableName, keySchema, attributeDefinitions, provisionedThroughput, cancellationToken)
        member this.CreateTableAsync(request, cancellationToken) = client.CreateTableAsync(request, cancellationToken)
        member this.DebugView = client.DebugView
        member this.DeleteBackupAsync(request, cancellationToken) = client.DeleteBackupAsync(request, cancellationToken)
        member this.DeleteItemAsync(tableName, key, cancellationToken) = client.DeleteItemAsync(tableName, key, cancellationToken)
        member this.DeleteItemAsync(tableName, key, returnValues, cancellationToken) = client.DeleteItemAsync(tableName, key, returnValues, cancellationToken)
        member this.DeleteItemAsync(request, cancellationToken) = client.DeleteItemAsync(request, cancellationToken)
        member this.DeleteResourcePolicyAsync(request, cancellationToken) = client.DeleteResourcePolicyAsync(request, cancellationToken)
        member this.DeleteTableAsync(tableName: string, cancellationToken: Threading.CancellationToken): Task<DeleteTableResponse> = client.DeleteTableAsync(tableName, cancellationToken)
        member this.DeleteTableAsync(request: DeleteTableRequest, cancellationToken: Threading.CancellationToken): Task<DeleteTableResponse> = client.DeleteTableAsync(request, cancellationToken)
        member this.DescribeBackupAsync(request, cancellationToken) = client.DescribeBackupAsync(request, cancellationToken)
        member this.DescribeContinuousBackupsAsync(request, cancellationToken) = client.DescribeContinuousBackupsAsync(request, cancellationToken)
        member this.DescribeContributorInsightsAsync(request, cancellationToken) = client.DescribeContributorInsightsAsync(request, cancellationToken)
        member this.DescribeEndpointsAsync(request, cancellationToken) = client.DescribeEndpointsAsync(request, cancellationToken)
        member this.DescribeExportAsync(request, cancellationToken) = client.DescribeExportAsync(request, cancellationToken)
        member this.DescribeGlobalTableAsync(request, cancellationToken) = client.DescribeGlobalTableAsync(request, cancellationToken)
        member this.DescribeGlobalTableSettingsAsync(request, cancellationToken) = client.DescribeGlobalTableSettingsAsync(request, cancellationToken)
        member this.DescribeImportAsync(request, cancellationToken) = client.DescribeImportAsync(request, cancellationToken)
        member this.DescribeKinesisStreamingDestinationAsync(request, cancellationToken) = client.DescribeKinesisStreamingDestinationAsync(request, cancellationToken)
        member this.DescribeLimitsAsync(request, cancellationToken) = client.DescribeLimitsAsync(request, cancellationToken)
        member this.DescribeTableAsync(tableName: string, cancellationToken: Threading.CancellationToken): Task<DescribeTableResponse> = client.DescribeTableAsync(tableName, cancellationToken)
        member this.DescribeTableAsync(request: DescribeTableRequest, cancellationToken: Threading.CancellationToken): Task<DescribeTableResponse> = client.DescribeTableAsync(request, cancellationToken)
        member this.DescribeTableReplicaAutoScalingAsync(request, cancellationToken) = client.DescribeTableReplicaAutoScalingAsync(request, cancellationToken)
        member this.DescribeTimeToLiveAsync(tableName: string, cancellationToken: Threading.CancellationToken): Task<DescribeTimeToLiveResponse> = client.DescribeTimeToLiveAsync(tableName, cancellationToken)
        member this.DescribeTimeToLiveAsync(request: DescribeTimeToLiveRequest, cancellationToken: Threading.CancellationToken): Task<DescribeTimeToLiveResponse> = client.DescribeTimeToLiveAsync(request, cancellationToken)
        member this.DetermineServiceOperationEndpoint(request) = client.DetermineServiceOperationEndpoint(request)
        member this.DisableKinesisStreamingDestinationAsync(request, cancellationToken) = client.DisableKinesisStreamingDestinationAsync(request, cancellationToken)
        member this.DistributedDebugView = client.DistributedDebugView
        member this.EnableKinesisStreamingDestinationAsync(request, cancellationToken) = client.EnableKinesisStreamingDestinationAsync(request, cancellationToken)
        member this.ExecuteStatementAsync(request, cancellationToken) = client.ExecuteStatementAsync(request, cancellationToken)
        member this.ExecuteTransactionAsync(request, cancellationToken) = client.ExecuteTransactionAsync(request, cancellationToken)
        member this.ExportTableToPointInTimeAsync(request, cancellationToken) = client.ExportTableToPointInTimeAsync(request, cancellationToken)
        member this.GetDistributedTable databaseId tableName = client.GetDistributedTable databaseId tableName
        member this.GetItemAsync(tableName, key, cancellationToken) = client.GetItemAsync(tableName, key, cancellationToken)
        member this.GetItemAsync(tableName, key, consistentRead, cancellationToken) = client.GetItemAsync(tableName, key, consistentRead, cancellationToken)
        member this.GetItemAsync(request, cancellationToken) = client.GetItemAsync(request, cancellationToken)
        member this.GetResourcePolicyAsync(request, cancellationToken) = client.GetResourcePolicyAsync(request, cancellationToken)
        member this.GetTable(tableName) = client.GetTable(tableName)
        member this.ImportTableAsync(request, cancellationToken) = client.ImportTableAsync(request, cancellationToken)
        member this.ListBackupsAsync(request, cancellationToken) = client.ListBackupsAsync(request, cancellationToken)
        member this.ListContributorInsightsAsync(request, cancellationToken) = client.ListContributorInsightsAsync(request, cancellationToken)
        member this.ListExportsAsync(request, cancellationToken) = client.ListExportsAsync(request, cancellationToken)
        member this.ListGlobalTablesAsync(request, cancellationToken) = client.ListGlobalTablesAsync(request, cancellationToken)
        member this.ListImportsAsync(request, cancellationToken) = client.ListImportsAsync(request, cancellationToken)
        member this.ListTablesAsync(cancellationToken) = client.ListTablesAsync(cancellationToken)
        member this.ListTablesAsync(exclusiveStartTableName: string, cancellationToken: Threading.CancellationToken): Task<ListTablesResponse> = client.ListTablesAsync(exclusiveStartTableName, cancellationToken)
        member this.ListTablesAsync(exclusiveStartTableName, limit, cancellationToken) = client.ListTablesAsync(exclusiveStartTableName, limit, cancellationToken)
        member this.ListTablesAsync(limit: int, cancellationToken: Threading.CancellationToken): Task<ListTablesResponse> = client.ListTablesAsync(limit, cancellationToken)
        member this.ListTablesAsync(request: ListTablesRequest, cancellationToken: Threading.CancellationToken): Task<ListTablesResponse> = client.ListTablesAsync(request, cancellationToken)
        member this.ListTagsOfResourceAsync(request, cancellationToken) = client.ListTagsOfResourceAsync(request, cancellationToken)
        member this.ProcessingDelay = client.ProcessingDelay
        member this.ProcessingDelay with set value = client.ProcessingDelay <- value
        member this.PutItemAsync(tableName, item, cancellationToken) = client.PutItemAsync(tableName, item, cancellationToken)
        member this.PutItemAsync(tableName, item, returnValues, cancellationToken) = client.PutItemAsync(tableName, item, returnValues, cancellationToken)
        member this.PutItemAsync(request, cancellationToken) = client.PutItemAsync(request, cancellationToken)
        member this.PutResourcePolicyAsync(request, cancellationToken) = client.PutResourcePolicyAsync(request, cancellationToken)
        member this.QueryAsync(request, cancellationToken) = client.QueryAsync(request, cancellationToken)
        member this.RestoreTableFromBackupAsync(request, cancellationToken) = client.RestoreTableFromBackupAsync(request, cancellationToken)
        member this.RestoreTableToPointInTimeAsync(request, cancellationToken) = client.RestoreTableToPointInTimeAsync(request, cancellationToken)
        member this.ScanAsync(tableName: string, attributesToGet: List<string>, cancellationToken: Threading.CancellationToken): Task<ScanResponse> = client.ScanAsync(tableName, attributesToGet, cancellationToken)
        member this.ScanAsync(tableName: string, scanFilter: Dictionary<string,Condition>, cancellationToken: Threading.CancellationToken): Task<ScanResponse> = client.ScanAsync(tableName, scanFilter, cancellationToken)
        member this.ScanAsync(tableName, attributesToGet, scanFilter, cancellationToken) = client.ScanAsync(tableName, attributesToGet, scanFilter, cancellationToken)
        member this.ScanAsync(request, cancellationToken) = client.ScanAsync(request, cancellationToken)
        member this.SetScanLimits(limits) = client.SetScanLimits(limits)
        member this.SubscribeToStream tableName subscriber = client.SubscribeToStream tableName subscriber
        member this.TagResourceAsync(request, cancellationToken) = client.TagResourceAsync(request, cancellationToken)
        member this.TransactGetItemsAsync(request, cancellationToken) = client.TransactGetItemsAsync(request, cancellationToken)
        member this.TransactWriteItemsAsync(request, cancellationToken) = client.TransactWriteItemsAsync(request, cancellationToken)
        member this.UntagResourceAsync(request, cancellationToken) = client.UntagResourceAsync(request, cancellationToken)
        member this.UpdateContinuousBackupsAsync(request, cancellationToken) = client.UpdateContinuousBackupsAsync(request, cancellationToken)
        member this.UpdateContributorInsightsAsync(request, cancellationToken) = client.UpdateContributorInsightsAsync(request, cancellationToken)
        member this.UpdateGlobalTableAsync(request, cancellationToken) = client.UpdateGlobalTableAsync(request, cancellationToken)
        member this.UpdateGlobalTableSettingsAsync(request, cancellationToken) = client.UpdateGlobalTableSettingsAsync(request, cancellationToken)
        member this.UpdateItemAsync(tableName, key, attributeUpdates, cancellationToken) = client.UpdateItemAsync(tableName, key, attributeUpdates, cancellationToken)
        member this.UpdateItemAsync(tableName, key, attributeUpdates, returnValues, cancellationToken) = client.UpdateItemAsync(tableName, key, attributeUpdates, returnValues, cancellationToken)
        member this.UpdateItemAsync(request, cancellationToken) = client.UpdateItemAsync(request, cancellationToken)
        member this.UpdateKinesisStreamingDestinationAsync(request, cancellationToken) = client.UpdateKinesisStreamingDestinationAsync(request, cancellationToken)
        member this.UpdateTableAsync(tableName, provisionedThroughput, cancellationToken) = client.UpdateTableAsync(tableName, provisionedThroughput, cancellationToken)
        member this.UpdateTableAsync(request, cancellationToken) = client.UpdateTableAsync(request, cancellationToken)
        member this.UpdateTableReplicaAutoScalingAsync(request, cancellationToken) = client.UpdateTableReplicaAutoScalingAsync(request, cancellationToken)
        member this.UpdateTimeToLiveAsync(request, cancellationToken) = client.UpdateTimeToLiveAsync(request, cancellationToken)
        member this.Config = client.Config
        member this.Paginators = client.Paginators