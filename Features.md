# Supported methods

 * [BatchGetItemAsync](#BatchGetItemAsync)
 * [BatchWriteItemAsync](#BatchWriteItemAsync)
 * [CreateGlobalTableAsync](#CreateGlobalTableAsync)
 * [CreateTableAsync](#CreateTableAsync)
 * [DeleteItemAsync](#DeleteItemAsync)
 * [DeleteTableAsync](#DeleteTableAsync)
 * [DescribeGlobalTableAsync](#DescribeGlobalTableAsync)
 * [DescribeTableAsync](#DescribeTableAsync)
 * [GetItemAsync](#GetItemAsync)
 * [ListGlobalTablesAsync](#ListGlobalTablesAsync)
 * [ListTablesAsync](#ListTablesAsync)
 * [PutItemAsync](#PutItemAsync)
 * [QueryAsync](#QueryAsync)
 * [ScanAsync](#ScanAsync)
 * [TransactGetItemsAsync](#TransactGetItemsAsync)
 * [TransactWriteItemsAsync](#TransactWriteItemsAsync)
 * [UpdateGlobalTableAsync](#UpdateGlobalTableAsync)
 * [UpdateItemAsync](#UpdateItemAsync)
 * [UpdateTableAsync](#UpdateTableAsync)

## BatchGetItemAsync

### Inputs

 * RequestItems
     * ✅ ConsistentRead
     * ✅ ExpressionAttributeNames
     * ✅ Keys
     * ✅ ProjectionExpression
     * AttributesToGet
 * ReturnConsumedCapacity

### Outputs

 * ✅ Responses
 * UnprocessedKeys
     * ✅ ConsistentRead
     * ✅ ExpressionAttributeNames
     * ✅ Keys
     * ✅ ProjectionExpression
     * AttributesToGet
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
     * ReplicationGroup - Partial support. If a `TestDynamoClient` is aware that it's database is part of a distributed db, then Replicas will be accurate. Otherwise this value will be empty
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
     * Replicas - Partial support. If a `TestDynamoClient` is aware that it's database is part of a distributed db, then Replicas will be accurate. Otherwise this value will be empty
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

## DeleteItemAsync

### Inputs

 * ✅ ConditionExpression
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ Key
 * ✅ ReturnValues
 * ✅ TableName
 * ConditionalOperator
 * Expected
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
     * Replicas - Partial support. If a `TestDynamoClient` is aware that it's database is part of a distributed db, then Replicas will be accurate. Otherwise this value will be empty
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

## DescribeGlobalTableAsync

### Inputs

 * ✅ GlobalTableName

### Outputs

 * GlobalTableDescription
     * ✅ CreationDateTime
     * ✅ GlobalTableArn
     * ✅ GlobalTableName
     * ✅ GlobalTableStatus
     * ReplicationGroup - Partial support. If a `TestDynamoClient` is aware that it's database is part of a distributed db, then Replicas will be accurate. Otherwise this value will be empty
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
     * Replicas - Partial support. If a `TestDynamoClient` is aware that it's database is part of a distributed db, then Replicas will be accurate. Otherwise this value will be empty
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

 * ✅ ExpressionAttributeNames
 * ✅ Key
 * ✅ ProjectionExpression
 * ✅ TableName
 * AttributesToGet
 * ConsistentRead
 * ReturnConsumedCapacity

### Outputs

 * ✅ IsItemSet
 * ✅ Item
 * ConsumedCapacity

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

 * ✅ ConditionExpression
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ Item
 * ✅ ReturnValues
 * ✅ TableName
 * ConditionalOperator
 * Expected
 * ReturnConsumedCapacity
 * ReturnItemCollectionMetrics
 * ReturnValuesOnConditionCheckFailure

### Outputs

 * ✅ Attributes
 * ConsumedCapacity
 * ItemCollectionMetrics

## QueryAsync

### Inputs

 * ✅ ExclusiveStartKey
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ FilterExpression
 * ✅ IndexName
 * ✅ KeyConditionExpression
 * ✅ Limit
 * ✅ ProjectionExpression
 * ✅ ScanIndexForward
 * ✅ Select
 * ✅ TableName
 * AttributesToGet
 * ConditionalOperator
 * ConsistentRead
 * IsLimitSet
 * KeyConditions
 * QueryFilter
 * ReturnConsumedCapacity

### Outputs

 * ✅ Count
 * ✅ Items
 * ✅ LastEvaluatedKey
 * ✅ ScannedCount
 * ConsumedCapacity

## ScanAsync

### Inputs

 * ✅ ExclusiveStartKey
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ FilterExpression
 * ✅ IndexName
 * ✅ Limit
 * ✅ ProjectionExpression
 * ✅ Select
 * ✅ TableName
 * AttributesToGet
 * ConditionalOperator
 * ConsistentRead
 * IsLimitSet
 * IsSegmentSet
 * IsTotalSegmentsSet
 * ReturnConsumedCapacity
 * ScanFilter
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
     * SizeEstimateRangeGB - Partial support. A constant value can be configured
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
     * ReplicationGroup - Partial support. If a `TestDynamoClient` is aware that it's database is part of a distributed db, then Replicas will be accurate. Otherwise this value will be empty
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
 * ✅ ConditionExpression
 * ✅ ExpressionAttributeNames
 * ✅ ExpressionAttributeValues
 * ✅ Key
 * ✅ ReturnValues
 * ✅ TableName
 * ✅ UpdateExpression
 * ConditionalOperator
 * Expected
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
         * OnDemandThroughput
         * Projection
         * ProvisionedThroughput
     * ✅ Delete
     * Update
 * ReplicaUpdates
     * Create
         * ✅ RegionName
         * GlobalSecondaryIndexes
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
     * Replicas - Partial support. If a `TestDynamoClient` is aware that it's database is part of a distributed db, then Replicas will be accurate. Otherwise this value will be empty
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

### ❌ CreateBackupAsync

### ❌ DeleteBackupAsync

### ❌ DeleteResourcePolicyAsync

### ❌ DescribeBackupAsync

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

### ❌ ListBackupsAsync

### ❌ ListContributorInsightsAsync

### ❌ ListExportsAsync

### ❌ ListImportsAsync

### ❌ ListTagsOfResourceAsync

### ❌ PutResourcePolicyAsync

### ❌ RestoreTableFromBackupAsync

### ❌ RestoreTableToPointInTimeAsync

### ❌ TagResourceAsync

### ❌ UntagResourceAsync

### ❌ UpdateContinuousBackupsAsync

### ❌ UpdateContributorInsightsAsync

### ❌ UpdateGlobalTableSettingsAsync

### ❌ UpdateKinesisStreamingDestinationAsync

### ❌ UpdateTableReplicaAutoScalingAsync

### ❌ UpdateTimeToLiveAsync