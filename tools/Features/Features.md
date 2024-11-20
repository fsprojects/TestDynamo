# Supported methods

 * [BatchGetItemAsync](#BatchGetItemAsync)
 * [BatchWriteItemAsync](#BatchWriteItemAsync)
 * [CreateBackupAsync](#CreateBackupAsync)
 * [CreateGlobalTableAsync](#CreateGlobalTableAsync)
 * [CreateTableAsync](#CreateTableAsync)
 * [DeleteBackupAsync](#DeleteBackupAsync)
 * [DeleteItemAsync](#DeleteItemAsync)
 * [DeleteTableAsync](#DeleteTableAsync)
 * [DescribeBackupAsync](#DescribeBackupAsync)
 * [DescribeGlobalTableAsync](#DescribeGlobalTableAsync)
 * [DescribeTableAsync](#DescribeTableAsync)
 * [GetItemAsync](#GetItemAsync)
 * [ListBackupsAsync](#ListBackupsAsync)
 * [ListGlobalTablesAsync](#ListGlobalTablesAsync)
 * [ListTablesAsync](#ListTablesAsync)
 * [PutItemAsync](#PutItemAsync)
 * [QueryAsync](#QueryAsync)
 * [RestoreTableFromBackupAsync](#RestoreTableFromBackupAsync)
 * [ScanAsync](#ScanAsync)
 * [TransactGetItemsAsync](#TransactGetItemsAsync)
 * [TransactWriteItemsAsync](#TransactWriteItemsAsync)
 * [UpdateGlobalTableAsync](#UpdateGlobalTableAsync)
 * [UpdateItemAsync](#UpdateItemAsync)
 * [UpdateTableAsync](#UpdateTableAsync)

## BatchGetItemAsync

### Inputs

 * ✅ RequestItems
 * ReturnConsumedCapacity

### Outputs

 * ✅ Responses
 * ✅ UnprocessedKeys
 * ConsumedCapacity

## BatchWriteItemAsync

### Inputs

 * ✅ RequestItems
 * ReturnConsumedCapacity
 * ReturnItemCollectionMetrics

### Outputs

 * ✅ UnprocessedItems
 * ConsumedCapacity
 * ItemCollectionMetrics

## CreateBackupAsync

### Inputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




### Outputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




## CreateGlobalTableAsync

### Inputs

 * ✅ GlobalTableName
 * ✅ ReplicationGroup

### Outputs

 * GlobalTableDescription
     * ✅ CreationDateTime
     * ✅ GlobalTableArn
     * ✅ GlobalTableName
     * ✅ GlobalTableStatus
     * ReplicationGroup - "[Partial support](#replica-description-support)"
         * GlobalSecondaryIndexes
             * ✅ IndexName
             * OnDemandThroughputOverride
             * ProvisionedThroughputOverride
         * ✅ RegionName
         * ✅ ReplicaStatus
         * KMSMasterKeyId
         * OnDemandThroughputOverride
         * ProvisionedThroughputOverride
         * ReplicaInaccessibleDateTime
         * ReplicaStatusDescription
         * ReplicaStatusPercentProgress
         * ReplicaTableClassSummary

## CreateTableAsync

### Inputs

 * ✅ AttributeDefinitions
 * ✅ DeletionProtectionEnabled
 * GlobalSecondaryIndexes
     * ✅ IndexName
     * ✅ KeySchema
     * ✅ Projection
     * OnDemandThroughput
     * ProvisionedThroughput
 * ✅ KeySchema
 * ✅ LocalSecondaryIndexes
 * ✅ StreamSpecification
 * ✅ TableName
 * BillingMode
 * OnDemandThroughput
 * ProvisionedThroughput
 * ResourcePolicy
 * SSESpecification
 * TableClass
 * Tags

### Outputs

 * TableDescription
     * ✅ AttributeDefinitions
     * ✅ CreationDateTime
     * ✅ DeletionProtectionEnabled
     * GlobalSecondaryIndexes
         * ✅ Backfilling
         * ✅ IndexArn
         * ✅ IndexName
         * ✅ IndexStatus
         * ✅ ItemCount
         * ✅ KeySchema
         * ✅ Projection
         * IndexSizeBytes
         * OnDemandThroughput
         * ProvisionedThroughput
     * ✅ GlobalTableVersion
     * ✅ ItemCount
     * ✅ KeySchema
     * ✅ LatestStreamArn
     * ✅ LatestStreamLabel
     * LocalSecondaryIndexes
         * ✅ IndexArn
         * ✅ IndexName
         * ✅ ItemCount
         * ✅ KeySchema
         * ✅ Projection
         * IndexSizeBytes
     * Replicas - "[Partial support](#replica-description-support)"
         * GlobalSecondaryIndexes
             * ✅ IndexName
             * OnDemandThroughputOverride
             * ProvisionedThroughputOverride
         * ✅ RegionName
         * ✅ ReplicaStatus
         * KMSMasterKeyId
         * OnDemandThroughputOverride
         * ProvisionedThroughputOverride
         * ReplicaInaccessibleDateTime
         * ReplicaStatusDescription
         * ReplicaStatusPercentProgress
         * ReplicaTableClassSummary
     * ✅ StreamSpecification
     * ✅ TableArn
     * ✅ TableId
     * ✅ TableName
     * ✅ TableStatus
     * ArchivalSummary
     * BillingModeSummary
     * OnDemandThroughput
     * ProvisionedThroughput
     * RestoreSummary
     * SSEDescription
     * TableClassSummary
     * TableSizeBytes

## DeleteBackupAsync

### Inputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




### Outputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




## DeleteItemAsync

### Inputs

 * ✅ ConditionalOperator
 * ✅ ConditionExpression
 * ✅ Expected
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ Key
 * ✅ ReturnValues
 * ✅ TableName
 * ReturnConsumedCapacity
 * ReturnItemCollectionMetrics
 * ReturnValuesOnConditionCheckFailure

### Outputs

 * ✅ Attributes
 * ConsumedCapacity
 * ItemCollectionMetrics

## DeleteTableAsync

### Inputs

 * ✅ TableName

### Outputs

 * TableDescription
     * ✅ AttributeDefinitions
     * ✅ CreationDateTime
     * ✅ DeletionProtectionEnabled
     * GlobalSecondaryIndexes
         * ✅ Backfilling
         * ✅ IndexArn
         * ✅ IndexName
         * ✅ IndexStatus
         * ✅ ItemCount
         * ✅ KeySchema
         * ✅ Projection
         * IndexSizeBytes
         * OnDemandThroughput
         * ProvisionedThroughput
     * ✅ GlobalTableVersion
     * ✅ ItemCount
     * ✅ KeySchema
     * ✅ LatestStreamArn
     * ✅ LatestStreamLabel
     * LocalSecondaryIndexes
         * ✅ IndexArn
         * ✅ IndexName
         * ✅ ItemCount
         * ✅ KeySchema
         * ✅ Projection
         * IndexSizeBytes
     * Replicas - "[Partial support](#replica-description-support)"
         * GlobalSecondaryIndexes
             * ✅ IndexName
             * OnDemandThroughputOverride
             * ProvisionedThroughputOverride
         * ✅ RegionName
         * ✅ ReplicaStatus
         * KMSMasterKeyId
         * OnDemandThroughputOverride
         * ProvisionedThroughputOverride
         * ReplicaInaccessibleDateTime
         * ReplicaStatusDescription
         * ReplicaStatusPercentProgress
         * ReplicaTableClassSummary
     * ✅ StreamSpecification
     * ✅ TableArn
     * ✅ TableId
     * ✅ TableName
     * ✅ TableStatus
     * ArchivalSummary
     * BillingModeSummary
     * OnDemandThroughput
     * ProvisionedThroughput
     * RestoreSummary
     * SSEDescription
     * TableClassSummary
     * TableSizeBytes

## DescribeBackupAsync

### Inputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




### Outputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




## DescribeGlobalTableAsync

### Inputs

 * ✅ GlobalTableName

### Outputs

 * GlobalTableDescription
     * ✅ CreationDateTime
     * ✅ GlobalTableArn
     * ✅ GlobalTableName
     * ✅ GlobalTableStatus
     * ReplicationGroup - "[Partial support](#replica-description-support)"
         * GlobalSecondaryIndexes
             * ✅ IndexName
             * OnDemandThroughputOverride
             * ProvisionedThroughputOverride
         * ✅ RegionName
         * ✅ ReplicaStatus
         * KMSMasterKeyId
         * OnDemandThroughputOverride
         * ProvisionedThroughputOverride
         * ReplicaInaccessibleDateTime
         * ReplicaStatusDescription
         * ReplicaStatusPercentProgress
         * ReplicaTableClassSummary

## DescribeTableAsync

### Inputs

 * ✅ TableName

### Outputs

 * Table
     * ✅ AttributeDefinitions
     * ✅ CreationDateTime
     * ✅ DeletionProtectionEnabled
     * GlobalSecondaryIndexes
         * ✅ Backfilling
         * ✅ IndexArn
         * ✅ IndexName
         * ✅ IndexStatus
         * ✅ ItemCount
         * ✅ KeySchema
         * ✅ Projection
         * IndexSizeBytes
         * OnDemandThroughput
         * ProvisionedThroughput
     * ✅ GlobalTableVersion
     * ✅ ItemCount
     * ✅ KeySchema
     * ✅ LatestStreamArn
     * ✅ LatestStreamLabel
     * LocalSecondaryIndexes
         * ✅ IndexArn
         * ✅ IndexName
         * ✅ ItemCount
         * ✅ KeySchema
         * ✅ Projection
         * IndexSizeBytes
     * Replicas - "[Partial support](#replica-description-support)"
         * GlobalSecondaryIndexes
             * ✅ IndexName
             * OnDemandThroughputOverride
             * ProvisionedThroughputOverride
         * ✅ RegionName
         * ✅ ReplicaStatus
         * KMSMasterKeyId
         * OnDemandThroughputOverride
         * ProvisionedThroughputOverride
         * ReplicaInaccessibleDateTime
         * ReplicaStatusDescription
         * ReplicaStatusPercentProgress
         * ReplicaTableClassSummary
     * ✅ StreamSpecification
     * ✅ TableArn
     * ✅ TableId
     * ✅ TableName
     * ✅ TableStatus
     * ArchivalSummary
     * BillingModeSummary
     * OnDemandThroughput
     * ProvisionedThroughput
     * RestoreSummary
     * SSEDescription
     * TableClassSummary
     * TableSizeBytes

## GetItemAsync

### Inputs

 * ✅ AttributesToGet
 * ✅ ExpressionAttributeNames
 * ✅ Key
 * ✅ ProjectionExpression
 * ✅ TableName
 * ConsistentRead
 * ReturnConsumedCapacity

### Outputs

 * ✅ IsItemSet
 * ✅ Item
 * ConsumedCapacity

## ListBackupsAsync

### Inputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




### Outputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




## ListGlobalTablesAsync

### Inputs

 * ✅ ExclusiveStartGlobalTableName
 * ✅ Limit
 * ✅ RegionName

### Outputs

 * ✅ GlobalTables
 * ✅ LastEvaluatedGlobalTableName

## ListTablesAsync

### Inputs

 * ✅ ExclusiveStartTableName
 * ✅ Limit

### Outputs

 * ✅ LastEvaluatedTableName
 * ✅ TableNames

## PutItemAsync

### Inputs

 * ✅ ConditionalOperator
 * ✅ ConditionExpression
 * ✅ Expected
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ Item
 * ✅ ReturnValues
 * ✅ TableName
 * ReturnConsumedCapacity
 * ReturnItemCollectionMetrics
 * ReturnValuesOnConditionCheckFailure

### Outputs

 * ✅ Attributes
 * ConsumedCapacity
 * ItemCollectionMetrics

## QueryAsync

### Inputs

 * ✅ AttributesToGet
 * ✅ ConditionalOperator
 * ConsistentRead - "Partial support. All reads are consistent"
 * ✅ ExclusiveStartKey
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ FilterExpression
 * ✅ IndexName
 * ✅ KeyConditionExpression
 * ✅ KeyConditions
 * ✅ Limit
 * ✅ ProjectionExpression
 * ✅ QueryFilter
 * ✅ ScanIndexForward
 * ✅ Select
 * ✅ TableName
 * ReturnConsumedCapacity

### Outputs

 * ✅ Count
 * ✅ Items
 * ✅ LastEvaluatedKey
 * ✅ ScannedCount
 * ConsumedCapacity

## RestoreTableFromBackupAsync

### Inputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




### Outputs

 * "There is an example of how to implement backups [in the README](https://github.com/ShaneGH/TestDynamo#implement-backups-functionality)"




## ScanAsync

### Inputs

 * ✅ AttributesToGet
 * ✅ ConditionalOperator
 * ConsistentRead - "Partial support. All reads are consistent"
 * ✅ ExclusiveStartKey
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ FilterExpression
 * ✅ IndexName
 * ✅ Limit
 * ✅ ProjectionExpression
 * ✅ ScanFilter
 * ✅ Select
 * ✅ TableName
 * ReturnConsumedCapacity
 * Segment
 * TotalSegments

### Outputs

 * ✅ Count
 * ✅ Items
 * ✅ LastEvaluatedKey
 * ✅ ScannedCount
 * ConsumedCapacity

## TransactGetItemsAsync

### Inputs

 * ✅ TransactItems
 * ReturnConsumedCapacity

### Outputs

 * ✅ Responses
 * ConsumedCapacity

## TransactWriteItemsAsync

### Inputs

 * ✅ ClientRequestToken
 * TransactItems
     * ConditionCheck
         * ✅ ConditionExpression
         * ✅ ExpressionAttributeNames
         * ✅ ExpressionAttributeValues
         * ✅ Key
         * ✅ TableName
         * ReturnValuesOnConditionCheckFailure
     * Delete
         * ✅ ConditionExpression
         * ✅ ExpressionAttributeNames
         * ✅ ExpressionAttributeValues
         * ✅ Key
         * ✅ TableName
         * ReturnValuesOnConditionCheckFailure
     * Put
         * ✅ ConditionExpression
         * ✅ ExpressionAttributeNames
         * ✅ ExpressionAttributeValues
         * ✅ Item
         * ✅ TableName
         * ReturnValuesOnConditionCheckFailure
     * Update
         * ✅ ConditionExpression
         * ✅ ExpressionAttributeNames
         * ✅ ExpressionAttributeValues
         * ✅ Key
         * ✅ TableName
         * ✅ UpdateExpression
         * ReturnValuesOnConditionCheckFailure
 * ReturnConsumedCapacity
 * ReturnItemCollectionMetrics

### Outputs

 * ItemCollectionMetrics
     * ✅ ItemCollectionKey
     * SizeEstimateRangeGB - "Partial support. A constant value can be configured"
 * ConsumedCapacity

## UpdateGlobalTableAsync

### Inputs

 * ✅ GlobalTableName
 * ✅ ReplicaUpdates

### Outputs

 * GlobalTableDescription
     * ✅ CreationDateTime
     * ✅ GlobalTableArn
     * ✅ GlobalTableName
     * ✅ GlobalTableStatus
     * ReplicationGroup - "[Partial support](#replica-description-support)"
         * GlobalSecondaryIndexes
             * ✅ IndexName
             * OnDemandThroughputOverride
             * ProvisionedThroughputOverride
         * ✅ RegionName
         * ✅ ReplicaStatus
         * KMSMasterKeyId
         * OnDemandThroughputOverride
         * ProvisionedThroughputOverride
         * ReplicaInaccessibleDateTime
         * ReplicaStatusDescription
         * ReplicaStatusPercentProgress
         * ReplicaTableClassSummary

## UpdateItemAsync

### Inputs

 * ✅ AttributeUpdates
 * ✅ ConditionalOperator
 * ✅ ConditionExpression
 * ✅ Expected
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ Key
 * ✅ ReturnValues
 * ✅ TableName
 * ✅ UpdateExpression
 * ReturnConsumedCapacity
 * ReturnItemCollectionMetrics
 * ReturnValuesOnConditionCheckFailure

### Outputs

 * ✅ Attributes
 * ConsumedCapacity
 * ItemCollectionMetrics

## UpdateTableAsync

### Inputs

 * ✅ AttributeDefinitions
 * ✅ DeletionProtectionEnabled
 * GlobalSecondaryIndexUpdates
     * Create
         * ✅ IndexName
         * ✅ KeySchema
         * ✅ Projection
         * OnDemandThroughput
         * ProvisionedThroughput
     * ✅ Delete
     * Update
 * ReplicaUpdates
     * Create
         * GlobalSecondaryIndexes
             * ✅ IndexName
             * OnDemandThroughputOverride
             * ProvisionedThroughputOverride
         * ✅ RegionName
         * KMSMasterKeyId
         * OnDemandThroughputOverride
         * ProvisionedThroughputOverride
         * TableClassOverride
     * ✅ Delete
     * Update
 * ✅ StreamSpecification
 * ✅ TableName
 * BillingMode
 * OnDemandThroughput
 * ProvisionedThroughput
 * SSESpecification
 * TableClass

### Outputs

 * TableDescription
     * ✅ AttributeDefinitions
     * ✅ CreationDateTime
     * ✅ DeletionProtectionEnabled
     * GlobalSecondaryIndexes
         * ✅ Backfilling
         * ✅ IndexArn
         * ✅ IndexName
         * ✅ IndexStatus
         * ✅ ItemCount
         * ✅ KeySchema
         * ✅ Projection
         * IndexSizeBytes
         * OnDemandThroughput
         * ProvisionedThroughput
     * ✅ GlobalTableVersion
     * ✅ ItemCount
     * ✅ KeySchema
     * ✅ LatestStreamArn
     * ✅ LatestStreamLabel
     * LocalSecondaryIndexes
         * ✅ IndexArn
         * ✅ IndexName
         * ✅ ItemCount
         * ✅ KeySchema
         * ✅ Projection
         * IndexSizeBytes
     * Replicas - "[Partial support](#replica-description-support)"
         * GlobalSecondaryIndexes
             * ✅ IndexName
             * OnDemandThroughputOverride
             * ProvisionedThroughputOverride
         * ✅ RegionName
         * ✅ ReplicaStatus
         * KMSMasterKeyId
         * OnDemandThroughputOverride
         * ProvisionedThroughputOverride
         * ReplicaInaccessibleDateTime
         * ReplicaStatusDescription
         * ReplicaStatusPercentProgress
         * ReplicaTableClassSummary
     * ✅ StreamSpecification
     * ✅ TableArn
     * ✅ TableId
     * ✅ TableName
     * ✅ TableStatus
     * ArchivalSummary
     * BillingModeSummary
     * OnDemandThroughput
     * ProvisionedThroughput
     * RestoreSummary
     * SSEDescription
     * TableClassSummary
     * TableSizeBytes

### ❌ BatchExecuteStatementAsync

### ❌ DeleteResourcePolicyAsync

### ❌ DescribeContinuousBackupsAsync

### ❌ DescribeContributorInsightsAsync

### ❌ DescribeEndpointsAsync

### ❌ DescribeExportAsync

### ❌ DescribeGlobalTableSettingsAsync

### ❌ DescribeImportAsync

### ❌ DescribeKinesisStreamingDestinationAsync

### ❌ DescribeLimitsAsync

### ❌ DescribeTableReplicaAutoScalingAsync

### ❌ DescribeTimeToLiveAsync

### ❌ DetermineServiceOperationEndpoint

### ❌ DisableKinesisStreamingDestinationAsync

### ❌ EnableKinesisStreamingDestinationAsync

### ❌ ExecuteStatementAsync

### ❌ ExecuteTransactionAsync

### ❌ ExportTableToPointInTimeAsync

### ❌ GetResourcePolicyAsync

### ❌ ImportTableAsync

### ❌ ListContributorInsightsAsync

### ❌ ListExportsAsync

### ❌ ListImportsAsync

### ❌ ListTagsOfResourceAsync

### ❌ PutResourcePolicyAsync

### ❌ RestoreTableToPointInTimeAsync

### ❌ TagResourceAsync

### ❌ UntagResourceAsync

### ❌ UpdateContinuousBackupsAsync

### ❌ UpdateContributorInsightsAsync

### ❌ UpdateGlobalTableSettingsAsync

### ❌ UpdateKinesisStreamingDestinationAsync

### ❌ UpdateTableReplicaAutoScalingAsync

### ❌ UpdateTimeToLiveAsync

#### Replica Description Support

Partial support. If an `AmazonDynamoDBClient` is built from a global database, then Replicas will be accurate. Otherwise this value will be empty