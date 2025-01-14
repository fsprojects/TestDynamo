
using System.Text.Json;
using System.Text.Json.Nodes;

public static class FeatureDescriptions
{
    public static JsonObject DescriptionsJson()
    {
        var replicaDescription = JsonSerializer.Serialize(new
        {
            __description = "[Partial support](#replica-description-support)",
            GlobalSecondaryIndexes = new
            {
                IndexName = true
            },
            RegionName = true,
            ReplicaStatus = true
        });

        var tableDescription = JsonSerializer.Serialize(new
        {
            CreationDateTime = true,
            ItemCount = true,
            TableSizeBytes = false,
            TableStatus = true,
            TableArn = true,
            TableId = true,
            StreamSpecification = true,
            LatestStreamArn = true,
            LatestStreamLabel = true,
            DeletionProtectionEnabled = true,
            TableName = true,
            LocalSecondaryIndexes = new
            {
                IndexName = true,
                IndexArn = true,
                ItemCount = true,
                KeySchema = true,
                Projection = true
            },
            GlobalSecondaryIndexes = new
            {
                Backfilling = true,
                IndexName = true,
                IndexArn = true,
                ItemCount = true,
                IndexStatus = true,
                KeySchema = true,
                Projection = true
            },
            GlobalTableVersion = true,
            AttributeDefinitions = true,
            KeySchema = true,
            Replicas = "{{replicaDescription}}"
        });

        var json = JsonSerializer.Serialize(Descriptions());
        return JsonSerializer.Deserialize<JsonObject>(
            json
                .Replace(@"""{{tableDescription}}""", tableDescription)
                .Replace(@"""{{replicaDescription}}""", replicaDescription))!;
    }

    private static object Descriptions()
    {
        var backupExample = new 
        {
            __description = "There is an example of how to implement backups [in the README](https://github.com/fsprojects/TestDynamo#implement-backups-functionality)",
            __omitChildren = true
        };

        return new
        {
            DeleteItemRequest = new
            {
                Key = true,
                TableName = true,
                ConditionExpression = true,
                ExpressionAttributeNames = true,
                ExpressionAttributeValues = true,
                ReturnValues = true,
                ConditionalOperator = true,
                Expected = true
            },
            DeleteItemResponse = new
            {
                Attributes = true
            },
            CreateTableRequest = new
            {
                TableName = true,
                DeletionProtectionEnabled = true,
                KeySchema = true,
                AttributeDefinitions = true,
                StreamSpecification = true,
                GlobalSecondaryIndexes = new
                {
                    IndexName = true,
                    KeySchema = true,
                    Projection = true
                },
                LocalSecondaryIndexes = new
                {
                    IndexName = true,
                    KeySchema = true,
                    Projection = true
                }
            },
            CreateTableResponse = new
            {
                TableDescription = "{{tableDescription}}"
            },
            DeleteTableRequest = new
            {
                TableName = true
            },
            DeleteTableResponse = new
            {
                TableDescription = "{{tableDescription}}"
            },
            UpdateTableRequest = new
            {
                TableName = true,
                DeletionProtectionEnabled = true,
                AttributeDefinitions = true,
                StreamSpecification = true,
                GlobalSecondaryIndexUpdates = new
                {
                    Create = new
                    {
                        IndexName = true,
                        KeySchema = true,
                        Projection = true
                    },
                    Delete = new
                    {
                        IndexName = true
                    }
                },
                ReplicaUpdates = new
                {
                    Create = new
                    {
                        RegionName = true,
                        GlobalSecondaryIndexes = new
                        {
                            IndexName = true
                        }
                    },
                    Delete = new
                    {
                        RegionName = true
                    }
                }
            },
            UpdateTableResponse = new
            {
                TableDescription = "{{tableDescription}}"
            },
            PutItemRequest = new
            {
                TableName = true,
                Item = true,
                ReturnValues = true,
                ConditionExpression = true,
                ExpressionAttributeNames = true,
                ExpressionAttributeValues = true,
                Expected = true,
                ConditionalOperator = true
            },
            PutItemResponse = new
            {
                Attributes = true
            },
            ListGlobalTablesRequest = new
            {
                RegionName = true,
                Limit = true,
                ExclusiveStartGlobalTableName = true
            },
            DescribeGlobalTableRequest = new
            {
                GlobalTableName = true
            },
            DescribeGlobalTableResponse = new
            {
                GlobalTableDescription = new
                {
                    CreationDateTime = true,
                    GlobalTableName = true,
                    GlobalTableArn = true,
                    GlobalTableStatus = true,
                    ReplicationGroup = "{{replicaDescription}}"
                }
            },
            ListGlobalTablesResponse = new
            {
                LastEvaluatedGlobalTableName = true,
                GlobalTables = new
                {
                    GlobalTableName = true,
                    ReplicationGroup = new
                    {
                        RegionName = true
                    }
                }
            },
            TransactGetItemsRequest = new
            {
                TransactItems = new
                {
                    Get = new
                    {
                        Key = true,
                        ExpressionAttributeNames = true,
                        TableName = true,
                        ProjectionExpression = true
                    }
                }
            },
            TransactGetItemsResponse = new
            {
                Responses = new
                {
                    Item = true
                }
            },
            ListTablesResponse = new 
            {
                TableNames = true,
                LastEvaluatedTableName = true
            },
            ListTablesRequest = new 
            {
                ExclusiveStartTableName = true,
                Limit = true
            },
            DescribeTableRequest = new
            {
                TableName = true
            },
            DescribeTableResponse = new 
            {
                Table = "{{tableDescription}}"
            },
            TransactWriteItemsRequest = new
            {
                ClientRequestToken = true,
                TransactItems = new
                {
                    Put = new
                    {
                        ConditionExpression = true,
                        ExpressionAttributeNames = true,
                        ExpressionAttributeValues = true,
                        Item = true,
                        TableName = true
                    },
                    Delete = new
                    {
                        ConditionExpression = true,
                        ExpressionAttributeNames = true,
                        ExpressionAttributeValues = true,
                        Key = true,
                        TableName = true
                    },
                    Update = new
                    {
                        ConditionExpression = true,
                        ExpressionAttributeNames = true,
                        ExpressionAttributeValues = true,
                        Key = true,
                        UpdateExpression = true,
                        TableName = true
                    },
                    ConditionCheck = new
                    {
                        ConditionExpression = true,
                        ExpressionAttributeNames = true,
                        ExpressionAttributeValues = true,
                        Key = true,
                        TableName = true
                    }
                }
            },
            TransactWriteItemsResponse = new
            {
                ItemCollectionMetrics = new
                {
                    ItemCollectionKey = true,
                    SizeEstimateRangeGB = new
                    {
                        __description = "Partial support. A constant value can be configured",
                    }
                }
            },
            QueryRequest = new
            {
                TableName = true,
                IndexName = true,
                KeyConditionExpression = true,
                FilterExpression = true,
                Limit = true,
                ExpressionAttributeNames = true,
                ExpressionAttributeValues = true,
                ScanIndexForward = true,
                ExclusiveStartKey = true,
                ProjectionExpression = true,
                Select = true,
                AttributesToGet = true,
                KeyConditions = true,
                QueryFilter = true,
                ConditionalOperator = true,
                ConsistentRead = new 
                {
                    __description = "Partial support. All reads are consistent",
                }
            },
            QueryResponse = new
            {
                Items = true,
                Count = true,
                ScannedCount = true,
                LastEvaluatedKey = true
            },
            CreateGlobalTableRequest = new
            {
                GlobalTableName = true,
                ReplicationGroup = new
                {
                    RegionName = true
                }
            },
            CreateGlobalTableResponse = new
            {
                GlobalTableDescription = new
                {
                    CreationDateTime = true,
                    GlobalTableName = true,
                    GlobalTableArn = true,
                    GlobalTableStatus = true,
                    ReplicationGroup = "{{replicaDescription}}"
                }
            },
            UpdateGlobalTableRequest = new
            {
                GlobalTableName = true,
                ReplicaUpdates = new
                {
                    Create = new
                    {
                        RegionName = true
                    },
                    Delete = new
                    {
                        RegionName = true
                    }
                }
            },
            UpdateGlobalTableResponse = new
            {
                GlobalTableDescription = new
                {
                    CreationDateTime = true,
                    GlobalTableName = true,
                    GlobalTableArn = true,
                    GlobalTableStatus = true,
                    ReplicationGroup = "{{replicaDescription}}"
                }
            },
            ScanRequest = new
            {
                TableName = true,
                IndexName = true,
                FilterExpression = true,
                Limit = true,
                ExpressionAttributeNames = true,
                ExpressionAttributeValues = true,
                ExclusiveStartKey = true,
                ProjectionExpression = true,
                Select = true,
                AttributesToGet = true,
                ConditionalOperator = true,
                ScanFilter = true,
                ConsistentRead = new 
                {
                    __description = "Partial support. All reads are consistent",
                }
            },
            ScanResponse = new
            {
                Items = true,
                Count = true,
                ScannedCount = true,
                LastEvaluatedKey = true
            },
            GetItemRequest = new
            {
                TableName = true,
                ProjectionExpression = true,
                Key = true,
                ExpressionAttributeNames = true,
                AttributesToGet = true
            },
            GetItemResponse = new
            {
                Item = true,
                IsItemSet = true
            },
            BatchGetItemRequest = new
            {
                RequestItems = new
                {
                    ConsistentRead = true,
                    ExpressionAttributeNames = true,
                    Keys = true,
                    ProjectionExpression = true,
                    AttributesToGet = true
                }
            },
            BatchGetItemResponse = new
            {
                Responses = true,
                UnprocessedKeys = new
                {
                    ConsistentRead = true,
                    ExpressionAttributeNames = true,
                    Keys = true,
                    ProjectionExpression = true,
                    AttributesToGet = true
                }
            },
            BatchWriteItemRequest = new
            {
                RequestItems = new
                {
                    PutRequest = new
                    {
                        Item = true
                    },
                    DeleteRequest = new
                    {
                        Key = true
                    }
                }
            },
            BatchWriteItemResponse = new
            {
                UnprocessedItems = new
                {
                    PutRequest = new
                    {
                        Item = true
                    },
                    DeleteRequest = new
                    {
                        Key = true
                    }
                }
            },
            CreateBackupRequest = backupExample,
            CreateBackupResponse = backupExample,
            RestoreTableFromBackupRequest = backupExample,
            RestoreTableFromBackupResponse = backupExample,
            DeleteBackupRequest = backupExample,
            DeleteBackupResponse = backupExample,
            ListBackupsRequest = backupExample,
            ListBackupsResponse = backupExample,
            DescribeBackupRequest = backupExample,
            DescribeBackupResponse = backupExample,
            UpdateItemRequest = new
            {
                UpdateExpression = true,
                Key = true,
                AttributeUpdates = true,
                ConditionExpression = true,
                TableName = true,
                ReturnValues = true,
                ExpressionAttributeNames = true,
                ExpressionAttributeValues = true,
                Expected = true,
                ConditionalOperator = true
            },
            UpdateItemResponse = new
            {
                Attributes = true
            }
        };
    }
}