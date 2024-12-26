module rec TestDynamo.GeneratedCode.Dtos

// ############################################################################
// #################### Auto generated code, do not modify ####################
// ############################################################################

open System
open System.Runtime.CompilerServices
open System.Net

#if NETSTANDARD2_0
type internal IsReadOnlyAttribute() = inherit System.Attribute()
#endif

[<AllowNullLiteral>]
type DynamodbTypeAttribute(name: string, empty: bool, ``const``: bool) =
    inherit Attribute()

    member _.Name = name
    member _.Empty = empty
    member _.Const = ``const``

    override _.ToString() = $"{name} - empty: {empty}, const: {``const``}"

let private emptyStringAsNone = function | ValueSome "" -> ValueNone | x -> x
let private emptyStringAsNull = function | "" -> null | x -> x

[<DynamodbType("Amazon.DynamoDBv2.AmazonDynamoDBRequest", true, false)>]
type AmazonDynamoDBRequest private () =
    static member value = AmazonDynamoDBRequest()

[<DynamodbType("Amazon.Runtime.AmazonWebServiceRequest", true, false)>]
type AmazonWebServiceRequest private () =
    static member value = AmazonWebServiceRequest()

[<DynamodbType("Amazon.Runtime.AmazonWebServiceResponse", false, false)>]
type AmazonWebServiceResponse =
    { ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.ApproximateCreationDateTimePrecision", false, true)>]
type ApproximateCreationDateTimePrecision private (value: string) =
    member _.Value = value
    static member MICROSECOND = ApproximateCreationDateTimePrecision("MICROSECOND")
    static member MILLISECOND = ApproximateCreationDateTimePrecision("MILLISECOND")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.ArchivalSummary", false, false)>]
type ArchivalSummary =
    { ArchivalBackupArn: String voption
      ArchivalDateTime: DateTime voption
      ArchivalReason: String voption }

[<DynamodbType("Amazon.DynamoDBv2.AttributeAction", false, true)>]
type AttributeAction private (value: string) =
    member _.Value = value
    static member ADD = AttributeAction("ADD")
    static member DELETE = AttributeAction("DELETE")
    static member PUT = AttributeAction("PUT")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.AttributeDefinition", false, false); Struct; IsReadOnly>]
type AttributeDefinition =
    { AttributeName: String voption
      AttributeType: ScalarAttributeType voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.AttributeValueUpdate", false, false); Struct; IsReadOnly>]
type AttributeValueUpdate<'attr> =
    { Action: AttributeAction voption
      Value: 'attr voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.AutoScalingPolicyDescription", false, false); Struct; IsReadOnly>]
type AutoScalingPolicyDescription =
    { PolicyName: String voption
      TargetTrackingScalingPolicyConfiguration: AutoScalingTargetTrackingScalingPolicyConfigurationDescription voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.AutoScalingPolicyUpdate", false, false); Struct; IsReadOnly>]
type AutoScalingPolicyUpdate =
    { PolicyName: String voption
      TargetTrackingScalingPolicyConfiguration: AutoScalingTargetTrackingScalingPolicyConfigurationUpdate voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.AutoScalingSettingsDescription", false, false)>]
type AutoScalingSettingsDescription =
    { AutoScalingDisabled: Boolean voption
      AutoScalingRoleArn: String voption
      MaximumUnits: Int64 voption
      MinimumUnits: Int64 voption
      ScalingPolicies: AutoScalingPolicyDescription array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.AutoScalingSettingsUpdate", false, false)>]
type AutoScalingSettingsUpdate =
    { AutoScalingDisabled: Boolean voption
      AutoScalingRoleArn: String voption
      MaximumUnits: Int64 voption
      MinimumUnits: Int64 voption
      ScalingPolicyUpdate: AutoScalingPolicyUpdate voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.AutoScalingTargetTrackingScalingPolicyConfigurationDescription", false, false)>]
type AutoScalingTargetTrackingScalingPolicyConfigurationDescription =
    { DisableScaleIn: Boolean voption
      ScaleInCooldown: Int32 voption
      ScaleOutCooldown: Int32 voption
      TargetValue: Double voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.AutoScalingTargetTrackingScalingPolicyConfigurationUpdate", false, false)>]
type AutoScalingTargetTrackingScalingPolicyConfigurationUpdate =
    { DisableScaleIn: Boolean voption
      ScaleInCooldown: Int32 voption
      ScaleOutCooldown: Int32 voption
      TargetValue: Double voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BackupDescription", false, false)>]
type BackupDescription =
    { BackupDetails: BackupDetails voption
      SourceTableDetails: SourceTableDetails voption
      SourceTableFeatureDetails: SourceTableFeatureDetails voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BackupDetails", false, false)>]
type BackupDetails =
    { BackupArn: String voption
      BackupCreationDateTime: DateTime voption
      BackupExpiryDateTime: DateTime voption
      BackupName: String voption
      BackupSizeBytes: Int64 voption
      BackupStatus: BackupStatus voption
      BackupType: BackupType voption }

[<DynamodbType("Amazon.DynamoDBv2.BackupStatus", false, true)>]
type BackupStatus private (value: string) =
    member _.Value = value
    static member AVAILABLE = BackupStatus("AVAILABLE")
    static member CREATING = BackupStatus("CREATING")
    static member DELETED = BackupStatus("DELETED")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.BackupSummary", false, false)>]
type BackupSummary =
    { BackupArn: String voption
      BackupCreationDateTime: DateTime voption
      BackupExpiryDateTime: DateTime voption
      BackupName: String voption
      BackupSizeBytes: Int64 voption
      BackupStatus: BackupStatus voption
      BackupType: BackupType voption
      TableArn: String voption
      TableId: String voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.BackupType", false, true)>]
type BackupType private (value: string) =
    member _.Value = value
    static member AWS_BACKUP = BackupType("AWS_BACKUP")
    static member SYSTEM = BackupType("SYSTEM")
    static member USER = BackupType("USER")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.BackupTypeFilter", false, true)>]
type BackupTypeFilter private (value: string) =
    member _.Value = value
    static member ALL = BackupTypeFilter("ALL")
    static member AWS_BACKUP = BackupTypeFilter("AWS_BACKUP")
    static member SYSTEM = BackupTypeFilter("SYSTEM")
    static member USER = BackupTypeFilter("USER")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchExecuteStatementRequest", false, false)>]
type BatchExecuteStatementRequest<'attr> =
    { ReturnConsumedCapacity: ReturnConsumedCapacity voption
      Statements: BatchStatementRequest<'attr> array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchExecuteStatementResponse", false, false)>]
type BatchExecuteStatementResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity array voption
      Responses: BatchStatementResponse<'attr> array voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchGetItemRequest", false, false)>]
type BatchGetItemRequest<'attr> =
    { RequestItems: Map<String, KeysAndAttributes<'attr>> voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchGetItemResponse", false, false)>]
type BatchGetItemResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity array voption
      Responses: Map<String, Map<String, 'attr> array> voption
      UnprocessedKeys: Map<String, KeysAndAttributes<'attr>> voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchStatementError", false, false)>]
type BatchStatementError<'attr> =
    { Code: BatchStatementErrorCodeEnum voption
      Item: Map<String, 'attr> voption
      Message: String voption }

[<DynamodbType("Amazon.DynamoDBv2.BatchStatementErrorCodeEnum", false, true)>]
type BatchStatementErrorCodeEnum private (value: string) =
    member _.Value = value
    static member AccessDenied = BatchStatementErrorCodeEnum("AccessDenied")
    static member ConditionalCheckFailed = BatchStatementErrorCodeEnum("ConditionalCheckFailed")
    static member DuplicateItem = BatchStatementErrorCodeEnum("DuplicateItem")
    static member InternalServerError = BatchStatementErrorCodeEnum("InternalServerError")
    static member ItemCollectionSizeLimitExceeded = BatchStatementErrorCodeEnum("ItemCollectionSizeLimitExceeded")
    static member ProvisionedThroughputExceeded = BatchStatementErrorCodeEnum("ProvisionedThroughputExceeded")
    static member RequestLimitExceeded = BatchStatementErrorCodeEnum("RequestLimitExceeded")
    static member ResourceNotFound = BatchStatementErrorCodeEnum("ResourceNotFound")
    static member ThrottlingError = BatchStatementErrorCodeEnum("ThrottlingError")
    static member TransactionConflict = BatchStatementErrorCodeEnum("TransactionConflict")
    static member ValidationError = BatchStatementErrorCodeEnum("ValidationError")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchStatementRequest", false, false)>]
type BatchStatementRequest<'attr> =
    { ConsistentRead: Boolean voption
      Parameters: 'attr array voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      Statement: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchStatementResponse", false, false)>]
type BatchStatementResponse<'attr> =
    { Error: BatchStatementError<'attr> voption
      Item: Map<String, 'attr> voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchWriteItemRequest", false, false)>]
type BatchWriteItemRequest<'attr> =
    { RequestItems: Map<String, WriteRequest<'attr> array> voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      ReturnItemCollectionMetrics: ReturnItemCollectionMetrics voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.BatchWriteItemResponse", false, false)>]
type BatchWriteItemResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity array voption
      ItemCollectionMetrics: Map<String, ItemCollectionMetrics<'attr> array> voption
      UnprocessedItems: Map<String, WriteRequest<'attr> array> voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.BillingMode", false, true)>]
type BillingMode private (value: string) =
    member _.Value = value
    static member PAY_PER_REQUEST = BillingMode("PAY_PER_REQUEST")
    static member PROVISIONED = BillingMode("PROVISIONED")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.BillingModeSummary", false, false); Struct; IsReadOnly>]
type BillingModeSummary =
    { BillingMode: BillingMode voption
      LastUpdateToPayPerRequestDateTime: DateTime voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.Capacity", false, false)>]
type Capacity =
    { CapacityUnits: Double voption
      ReadCapacityUnits: Double voption
      WriteCapacityUnits: Double voption }

[<DynamodbType("Amazon.Runtime.ChecksumValidationStatus", false, false)>]
type ChecksumValidationStatus =
    | NOT_VALIDATED = 0
    | PENDING_RESPONSE_READ = 1
    | SUCCESSFUL = 2
    | INVALID = 3

[<DynamodbType("Amazon.DynamoDBv2.ComparisonOperator", false, true)>]
type ComparisonOperator private (value: string) =
    member _.Value = value
    static member BEGINS_WITH = ComparisonOperator("BEGINS_WITH")
    static member BETWEEN = ComparisonOperator("BETWEEN")
    static member CONTAINS = ComparisonOperator("CONTAINS")
    static member EQ = ComparisonOperator("EQ")
    static member GE = ComparisonOperator("GE")
    static member GT = ComparisonOperator("GT")
    static member IN = ComparisonOperator("IN")
    static member LE = ComparisonOperator("LE")
    static member LT = ComparisonOperator("LT")
    static member NE = ComparisonOperator("NE")
    static member NOT_CONTAINS = ComparisonOperator("NOT_CONTAINS")
    static member NOT_NULL = ComparisonOperator("NOT_NULL")
    static member NULL = ComparisonOperator("NULL")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.Condition", false, false); Struct; IsReadOnly>]
type Condition<'attr> =
    { AttributeValueList: 'attr array voption
      ComparisonOperator: ComparisonOperator voption }

[<DynamodbType("Amazon.DynamoDBv2.ConditionalOperator", false, true)>]
type ConditionalOperator private (value: string) =
    member _.Value = value
    static member AND = ConditionalOperator("AND")
    static member OR = ConditionalOperator("OR")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.ConditionCheck", false, false)>]
type ConditionCheck<'attr> =
    { ConditionExpression: String voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      Key: Map<String, 'attr> voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      TableName: String voption }

[<DynamodbType("Amazon.Runtime.ConstantClass", false, false); Struct; IsReadOnly>]
type ConstantClass =
    { Value: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ConsumedCapacity", false, false)>]
type ConsumedCapacity =
    { CapacityUnits: Double voption
      GlobalSecondaryIndexes: Map<String, Capacity> voption
      LocalSecondaryIndexes: Map<String, Capacity> voption
      ReadCapacityUnits: Double voption
      Table: Capacity voption
      TableName: String voption
      WriteCapacityUnits: Double voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ContinuousBackupsDescription", false, false); Struct; IsReadOnly>]
type ContinuousBackupsDescription =
    { ContinuousBackupsStatus: ContinuousBackupsStatus voption
      PointInTimeRecoveryDescription: PointInTimeRecoveryDescription voption }

[<DynamodbType("Amazon.DynamoDBv2.ContinuousBackupsStatus", false, true)>]
type ContinuousBackupsStatus private (value: string) =
    member _.Value = value
    static member DISABLED = ContinuousBackupsStatus("DISABLED")
    static member ENABLED = ContinuousBackupsStatus("ENABLED")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.ContributorInsightsAction", false, true)>]
type ContributorInsightsAction private (value: string) =
    member _.Value = value
    static member DISABLE = ContributorInsightsAction("DISABLE")
    static member ENABLE = ContributorInsightsAction("ENABLE")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.ContributorInsightsStatus", false, true)>]
type ContributorInsightsStatus private (value: string) =
    member _.Value = value
    static member DISABLED = ContributorInsightsStatus("DISABLED")
    static member DISABLING = ContributorInsightsStatus("DISABLING")
    static member ENABLED = ContributorInsightsStatus("ENABLED")
    static member ENABLING = ContributorInsightsStatus("ENABLING")
    static member FAILED = ContributorInsightsStatus("FAILED")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.ContributorInsightsSummary", false, false)>]
type ContributorInsightsSummary =
    { ContributorInsightsStatus: ContributorInsightsStatus voption
      IndexName: String voption
      TableName: String voption }

[<DynamodbType("Amazon.Runtime.CoreChecksumAlgorithm", false, false)>]
type CoreChecksumAlgorithm =
    | NONE = 0
    | CRC32C = 1
    | CRC32 = 2
    | SHA256 = 3
    | SHA1 = 4

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateBackupRequest", false, false)>]
type CreateBackupRequest =
    { BackupName: String voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateBackupResponse", false, false)>]
type CreateBackupResponse =
    { BackupDetails: BackupDetails voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateGlobalSecondaryIndexAction", false, false)>]
type CreateGlobalSecondaryIndexAction =
    { IndexName: String voption
      KeySchema: KeySchemaElement array voption
      OnDemandThroughput: OnDemandThroughput voption
      Projection: Projection voption
      ProvisionedThroughput: ProvisionedThroughput voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateGlobalTableRequest", false, false)>]
type CreateGlobalTableRequest =
    { GlobalTableName: String voption
      ReplicationGroup: Replica array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateGlobalTableResponse", false, false)>]
type CreateGlobalTableResponse =
    { GlobalTableDescription: GlobalTableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateReplicaAction", false, false); Struct; IsReadOnly>]
type CreateReplicaAction =
    { RegionName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateReplicationGroupMemberAction", false, false)>]
type CreateReplicationGroupMemberAction =
    { GlobalSecondaryIndexes: ReplicaGlobalSecondaryIndex array voption
      KMSMasterKeyId: String voption
      OnDemandThroughputOverride: OnDemandThroughputOverride voption
      ProvisionedThroughputOverride: ProvisionedThroughputOverride voption
      RegionName: String voption
      TableClassOverride: TableClass voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateTableRequest", false, false)>]
type CreateTableRequest =
    { AttributeDefinitions: AttributeDefinition array voption
      BillingMode: BillingMode voption
      DeletionProtectionEnabled: Boolean voption
      GlobalSecondaryIndexes: GlobalSecondaryIndex array voption
      KeySchema: KeySchemaElement array voption
      LocalSecondaryIndexes: LocalSecondaryIndex array voption
      OnDemandThroughput: OnDemandThroughput voption
      ProvisionedThroughput: ProvisionedThroughput voption
      ResourcePolicy: String voption
      SSESpecification: SSESpecification voption
      StreamSpecification: StreamSpecification voption
      TableClass: TableClass voption
      TableName: String voption
      Tags: Tag array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CreateTableResponse", false, false)>]
type CreateTableResponse =
    { TableDescription: TableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.CsvOptions", false, false); Struct; IsReadOnly>]
type CsvOptions =
    { Delimiter: String voption
      HeaderList: String array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.Delete", false, false)>]
type Delete<'attr> =
    { ConditionExpression: String voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      Key: Map<String, 'attr> voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteBackupRequest", false, false)>]
type DeleteBackupRequest =
    { BackupArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteBackupResponse", false, false)>]
type DeleteBackupResponse =
    { BackupDescription: BackupDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteGlobalSecondaryIndexAction", false, false); Struct; IsReadOnly>]
type DeleteGlobalSecondaryIndexAction =
    { IndexName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteItemRequest", false, false)>]
type DeleteItemRequest<'attr> =
    { ConditionalOperator: ConditionalOperator voption
      ConditionExpression: String voption
      Expected: Map<String, ExpectedAttributeValue<'attr>> voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      Key: Map<String, 'attr> voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      ReturnItemCollectionMetrics: ReturnItemCollectionMetrics voption
      ReturnValues: ReturnValue voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteItemResponse", false, false)>]
type DeleteItemResponse<'attr> =
    { Attributes: Map<String, 'attr> voption
      ConsumedCapacity: ConsumedCapacity voption
      ItemCollectionMetrics: ItemCollectionMetrics<'attr> voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteReplicaAction", false, false); Struct; IsReadOnly>]
type DeleteReplicaAction =
    { RegionName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteReplicationGroupMemberAction", false, false); Struct; IsReadOnly>]
type DeleteReplicationGroupMemberAction =
    { RegionName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteRequest", false, false); Struct; IsReadOnly>]
type DeleteRequest<'attr> =
    { Key: Map<String, 'attr> voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteResourcePolicyRequest", false, false)>]
type DeleteResourcePolicyRequest =
    { ExpectedRevisionId: String voption
      ResourceArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteResourcePolicyResponse", false, false)>]
type DeleteResourcePolicyResponse =
    { RevisionId: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteTableRequest", false, false)>]
type DeleteTableRequest =
    { TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DeleteTableResponse", false, false)>]
type DeleteTableResponse =
    { TableDescription: TableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeBackupRequest", false, false)>]
type DescribeBackupRequest =
    { BackupArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeBackupResponse", false, false)>]
type DescribeBackupResponse =
    { BackupDescription: BackupDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeContinuousBackupsRequest", false, false)>]
type DescribeContinuousBackupsRequest =
    { TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeContinuousBackupsResponse", false, false)>]
type DescribeContinuousBackupsResponse =
    { ContinuousBackupsDescription: ContinuousBackupsDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeContributorInsightsRequest", false, false)>]
type DescribeContributorInsightsRequest =
    { IndexName: String voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeContributorInsightsResponse", false, false)>]
type DescribeContributorInsightsResponse =
    { ContributorInsightsRuleList: String array voption
      ContributorInsightsStatus: ContributorInsightsStatus voption
      FailureException: FailureException voption
      IndexName: String voption
      LastUpdateDateTime: DateTime voption
      TableName: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeEndpointsRequest", true, false)>]
type DescribeEndpointsRequest private () =
    static member value = DescribeEndpointsRequest()

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeEndpointsResponse", false, false)>]
type DescribeEndpointsResponse =
    { Endpoints: Endpoint array voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeExportRequest", false, false)>]
type DescribeExportRequest =
    { ExportArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeExportResponse", false, false)>]
type DescribeExportResponse =
    { ExportDescription: ExportDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeGlobalTableRequest", false, false)>]
type DescribeGlobalTableRequest =
    { GlobalTableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeGlobalTableResponse", false, false)>]
type DescribeGlobalTableResponse =
    { GlobalTableDescription: GlobalTableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeGlobalTableSettingsRequest", false, false)>]
type DescribeGlobalTableSettingsRequest =
    { GlobalTableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeGlobalTableSettingsResponse", false, false)>]
type DescribeGlobalTableSettingsResponse =
    { GlobalTableName: String voption
      ReplicaSettings: ReplicaSettingsDescription array voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeImportRequest", false, false)>]
type DescribeImportRequest =
    { ImportArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeImportResponse", false, false)>]
type DescribeImportResponse =
    { ImportTableDescription: ImportTableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeKinesisStreamingDestinationRequest", false, false)>]
type DescribeKinesisStreamingDestinationRequest =
    { TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeKinesisStreamingDestinationResponse", false, false)>]
type DescribeKinesisStreamingDestinationResponse =
    { KinesisDataStreamDestinations: KinesisDataStreamDestination array voption
      TableName: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeLimitsRequest", true, false)>]
type DescribeLimitsRequest private () =
    static member value = DescribeLimitsRequest()

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeLimitsResponse", false, false)>]
type DescribeLimitsResponse =
    { AccountMaxReadCapacityUnits: Int64 voption
      AccountMaxWriteCapacityUnits: Int64 voption
      TableMaxReadCapacityUnits: Int64 voption
      TableMaxWriteCapacityUnits: Int64 voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeTableReplicaAutoScalingRequest", false, false)>]
type DescribeTableReplicaAutoScalingRequest =
    { TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeTableReplicaAutoScalingResponse", false, false)>]
type DescribeTableReplicaAutoScalingResponse =
    { TableAutoScalingDescription: TableAutoScalingDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeTableRequest", false, false)>]
type DescribeTableRequest =
    { TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeTableResponse", false, false)>]
type DescribeTableResponse =
    { Table: TableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeTimeToLiveRequest", false, false)>]
type DescribeTimeToLiveRequest =
    { TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DescribeTimeToLiveResponse", false, false)>]
type DescribeTimeToLiveResponse =
    { TimeToLiveDescription: TimeToLiveDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.DestinationStatus", false, true)>]
type DestinationStatus private (value: string) =
    member _.Value = value
    static member ACTIVE = DestinationStatus("ACTIVE")
    static member DISABLED = DestinationStatus("DISABLED")
    static member DISABLING = DestinationStatus("DISABLING")
    static member ENABLE_FAILED = DestinationStatus("ENABLE_FAILED")
    static member ENABLING = DestinationStatus("ENABLING")
    static member UPDATING = DestinationStatus("UPDATING")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.DisableKinesisStreamingDestinationRequest", false, false)>]
type DisableKinesisStreamingDestinationRequest =
    { EnableKinesisStreamingConfiguration: EnableKinesisStreamingConfiguration voption
      StreamArn: String voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.DisableKinesisStreamingDestinationResponse", false, false)>]
type DisableKinesisStreamingDestinationResponse =
    { DestinationStatus: DestinationStatus voption
      EnableKinesisStreamingConfiguration: EnableKinesisStreamingConfiguration voption
      StreamArn: String voption
      TableName: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.EnableKinesisStreamingConfiguration", false, false); Struct; IsReadOnly>]
type EnableKinesisStreamingConfiguration =
    { ApproximateCreationDateTimePrecision: ApproximateCreationDateTimePrecision voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.EnableKinesisStreamingDestinationRequest", false, false)>]
type EnableKinesisStreamingDestinationRequest =
    { EnableKinesisStreamingConfiguration: EnableKinesisStreamingConfiguration voption
      StreamArn: String voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.EnableKinesisStreamingDestinationResponse", false, false)>]
type EnableKinesisStreamingDestinationResponse =
    { DestinationStatus: DestinationStatus voption
      EnableKinesisStreamingConfiguration: EnableKinesisStreamingConfiguration voption
      StreamArn: String voption
      TableName: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.Endpoint", false, false); Struct; IsReadOnly>]
type Endpoint =
    { Address: String voption
      CachePeriodInMinutes: Int64 voption }

[<DynamodbType("Amazon.Runtime.Endpoints.Endpoint", false, false)>]
type Endpoint2 =
    { URL: String voption
      Attributes: IPropertyBag voption
      Headers: Map<String, String array> voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ExecuteStatementRequest", false, false)>]
type ExecuteStatementRequest<'attr> =
    { ConsistentRead: Boolean voption
      Limit: Int32 voption
      NextToken: String voption
      Parameters: 'attr array voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      Statement: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ExecuteStatementResponse", false, false)>]
type ExecuteStatementResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity voption
      Items: Map<String, 'attr> array voption
      LastEvaluatedKey: Map<String, 'attr> voption
      NextToken: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ExecuteTransactionRequest", false, false)>]
type ExecuteTransactionRequest<'attr> =
    { ClientRequestToken: String voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      TransactStatements: ParameterizedStatement<'attr> array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ExecuteTransactionResponse", false, false)>]
type ExecuteTransactionResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity array voption
      Responses: ItemResponse<'attr> array voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ExpectedAttributeValue", false, false)>]
type ExpectedAttributeValue<'attr> =
    { AttributeValueList: 'attr array voption
      ComparisonOperator: ComparisonOperator voption
      Exists: Boolean voption
      Value: 'attr voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ExportDescription", false, false)>]
type ExportDescription =
    { BilledSizeBytes: Int64 voption
      ClientToken: String voption
      EndTime: DateTime voption
      ExportArn: String voption
      ExportFormat: ExportFormat voption
      ExportManifest: String voption
      ExportStatus: ExportStatus voption
      ExportTime: DateTime voption
      ExportType: ExportType voption
      FailureCode: String voption
      FailureMessage: String voption
      IncrementalExportSpecification: IncrementalExportSpecification voption
      ItemCount: Int64 voption
      S3Bucket: String voption
      S3BucketOwner: String voption
      S3Prefix: String voption
      S3SseAlgorithm: S3SseAlgorithm voption
      S3SseKmsKeyId: String voption
      StartTime: DateTime voption
      TableArn: String voption
      TableId: String voption }

[<DynamodbType("Amazon.DynamoDBv2.ExportFormat", false, true)>]
type ExportFormat private (value: string) =
    member _.Value = value
    static member DYNAMODB_JSON = ExportFormat("DYNAMODB_JSON")
    static member ION = ExportFormat("ION")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.ExportStatus", false, true)>]
type ExportStatus private (value: string) =
    member _.Value = value
    static member COMPLETED = ExportStatus("COMPLETED")
    static member FAILED = ExportStatus("FAILED")
    static member IN_PROGRESS = ExportStatus("IN_PROGRESS")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.ExportSummary", false, false)>]
type ExportSummary =
    { ExportArn: String voption
      ExportStatus: ExportStatus voption
      ExportType: ExportType voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ExportTableToPointInTimeRequest", false, false)>]
type ExportTableToPointInTimeRequest =
    { ClientToken: String voption
      ExportFormat: ExportFormat voption
      ExportTime: DateTime voption
      ExportType: ExportType voption
      IncrementalExportSpecification: IncrementalExportSpecification voption
      S3Bucket: String voption
      S3BucketOwner: String voption
      S3Prefix: String voption
      S3SseAlgorithm: S3SseAlgorithm voption
      S3SseKmsKeyId: String voption
      TableArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ExportTableToPointInTimeResponse", false, false)>]
type ExportTableToPointInTimeResponse =
    { ExportDescription: ExportDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.ExportType", false, true)>]
type ExportType private (value: string) =
    member _.Value = value
    static member FULL_EXPORT = ExportType("FULL_EXPORT")
    static member INCREMENTAL_EXPORT = ExportType("INCREMENTAL_EXPORT")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.ExportViewType", false, true)>]
type ExportViewType private (value: string) =
    member _.Value = value
    static member NEW_AND_OLD_IMAGES = ExportViewType("NEW_AND_OLD_IMAGES")
    static member NEW_IMAGE = ExportViewType("NEW_IMAGE")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.FailureException", false, false); Struct; IsReadOnly>]
type FailureException =
    { ExceptionDescription: String voption
      ExceptionName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.Get", false, false)>]
type Get<'attr> =
    { ExpressionAttributeNames: Map<String, String> voption
      Key: Map<String, 'attr> voption
      ProjectionExpression: String voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GetItemRequest", false, false)>]
type GetItemRequest<'attr> =
    { AttributesToGet: String array voption
      ConsistentRead: Boolean voption
      ExpressionAttributeNames: Map<String, String> voption
      Key: Map<String, 'attr> voption
      ProjectionExpression: String voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GetItemResponse", false, false)>]
type GetItemResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity voption
      Item: Map<String, 'attr> voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GetResourcePolicyRequest", false, false)>]
type GetResourcePolicyRequest =
    { ResourceArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GetResourcePolicyResponse", false, false)>]
type GetResourcePolicyResponse =
    { Policy: String voption
      RevisionId: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GlobalSecondaryIndex", false, false)>]
type GlobalSecondaryIndex =
    { IndexName: String voption
      KeySchema: KeySchemaElement array voption
      OnDemandThroughput: OnDemandThroughput voption
      Projection: Projection voption
      ProvisionedThroughput: ProvisionedThroughput voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GlobalSecondaryIndexAutoScalingUpdate", false, false); Struct; IsReadOnly>]
type GlobalSecondaryIndexAutoScalingUpdate =
    { IndexName: String voption
      ProvisionedWriteCapacityAutoScalingUpdate: AutoScalingSettingsUpdate voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GlobalSecondaryIndexDescription", false, false)>]
type GlobalSecondaryIndexDescription =
    { Backfilling: Boolean voption
      IndexArn: String voption
      IndexName: String voption
      IndexSizeBytes: Int64 voption
      IndexStatus: IndexStatus voption
      ItemCount: Int64 voption
      KeySchema: KeySchemaElement array voption
      OnDemandThroughput: OnDemandThroughput voption
      Projection: Projection voption
      ProvisionedThroughput: ProvisionedThroughputDescription voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GlobalSecondaryIndexInfo", false, false)>]
type GlobalSecondaryIndexInfo =
    { IndexName: String voption
      KeySchema: KeySchemaElement array voption
      OnDemandThroughput: OnDemandThroughput voption
      Projection: Projection voption
      ProvisionedThroughput: ProvisionedThroughput voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GlobalSecondaryIndexUpdate", false, false)>]
type GlobalSecondaryIndexUpdate =
    { Create: CreateGlobalSecondaryIndexAction voption
      Delete: DeleteGlobalSecondaryIndexAction voption
      Update: UpdateGlobalSecondaryIndexAction voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GlobalTable", false, false); Struct; IsReadOnly>]
type GlobalTable =
    { GlobalTableName: String voption
      ReplicationGroup: Replica array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GlobalTableDescription", false, false)>]
type GlobalTableDescription =
    { CreationDateTime: DateTime voption
      GlobalTableArn: String voption
      GlobalTableName: String voption
      GlobalTableStatus: GlobalTableStatus voption
      ReplicationGroup: ReplicaDescription array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.GlobalTableGlobalSecondaryIndexSettingsUpdate", false, false)>]
type GlobalTableGlobalSecondaryIndexSettingsUpdate =
    { IndexName: String voption
      ProvisionedWriteCapacityAutoScalingSettingsUpdate: AutoScalingSettingsUpdate voption
      ProvisionedWriteCapacityUnits: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.GlobalTableStatus", false, true)>]
type GlobalTableStatus private (value: string) =
    member _.Value = value
    static member ACTIVE = GlobalTableStatus("ACTIVE")
    static member CREATING = GlobalTableStatus("CREATING")
    static member DELETING = GlobalTableStatus("DELETING")
    static member UPDATING = GlobalTableStatus("UPDATING")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.IDynamoDBv2PaginatorFactory", true, false)>]
type IDynamoDBv2PaginatorFactory private () =
    static member value = IDynamoDBv2PaginatorFactory()

[<DynamodbType("Amazon.DynamoDBv2.ImportStatus", false, true)>]
type ImportStatus private (value: string) =
    member _.Value = value
    static member CANCELLED = ImportStatus("CANCELLED")
    static member CANCELLING = ImportStatus("CANCELLING")
    static member COMPLETED = ImportStatus("COMPLETED")
    static member FAILED = ImportStatus("FAILED")
    static member IN_PROGRESS = ImportStatus("IN_PROGRESS")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.ImportSummary", false, false)>]
type ImportSummary =
    { CloudWatchLogGroupArn: String voption
      EndTime: DateTime voption
      ImportArn: String voption
      ImportStatus: ImportStatus voption
      InputFormat: InputFormat voption
      S3BucketSource: S3BucketSource voption
      StartTime: DateTime voption
      TableArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ImportTableDescription", false, false)>]
type ImportTableDescription =
    { ClientToken: String voption
      CloudWatchLogGroupArn: String voption
      EndTime: DateTime voption
      ErrorCount: Int64 voption
      FailureCode: String voption
      FailureMessage: String voption
      ImportArn: String voption
      ImportedItemCount: Int64 voption
      ImportStatus: ImportStatus voption
      InputCompressionType: InputCompressionType voption
      InputFormat: InputFormat voption
      InputFormatOptions: InputFormatOptions voption
      ProcessedItemCount: Int64 voption
      ProcessedSizeBytes: Int64 voption
      S3BucketSource: S3BucketSource voption
      StartTime: DateTime voption
      TableArn: String voption
      TableCreationParameters: TableCreationParameters voption
      TableId: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ImportTableRequest", false, false)>]
type ImportTableRequest =
    { ClientToken: String voption
      InputCompressionType: InputCompressionType voption
      InputFormat: InputFormat voption
      InputFormatOptions: InputFormatOptions voption
      S3BucketSource: S3BucketSource voption
      TableCreationParameters: TableCreationParameters voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ImportTableResponse", false, false)>]
type ImportTableResponse =
    { ImportTableDescription: ImportTableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.IncrementalExportSpecification", false, false)>]
type IncrementalExportSpecification =
    { ExportFromTime: DateTime voption
      ExportToTime: DateTime voption
      ExportViewType: ExportViewType voption }

[<DynamodbType("Amazon.DynamoDBv2.IndexStatus", false, true)>]
type IndexStatus private (value: string) =
    member _.Value = value
    static member ACTIVE = IndexStatus("ACTIVE")
    static member CREATING = IndexStatus("CREATING")
    static member DELETING = IndexStatus("DELETING")
    static member UPDATING = IndexStatus("UPDATING")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.InputCompressionType", false, true)>]
type InputCompressionType private (value: string) =
    member _.Value = value
    static member GZIP = InputCompressionType("GZIP")
    static member NONE = InputCompressionType("NONE")
    static member ZSTD = InputCompressionType("ZSTD")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.InputFormat", false, true)>]
type InputFormat private (value: string) =
    member _.Value = value
    static member CSV = InputFormat("CSV")
    static member DYNAMODB_JSON = InputFormat("DYNAMODB_JSON")
    static member ION = InputFormat("ION")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.InputFormatOptions", false, false); Struct; IsReadOnly>]
type InputFormatOptions =
    { Csv: CsvOptions voption }

[<DynamodbType("Amazon.Runtime.Endpoints.IPropertyBag", false, false); Struct; IsReadOnly>]
type IPropertyBag =
    { Item: Object voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ItemCollectionMetrics", false, false); Struct; IsReadOnly>]
type ItemCollectionMetrics<'attr> =
    { ItemCollectionKey: Map<String, 'attr> voption
      SizeEstimateRangeGB: Double array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ItemResponse", false, false); Struct; IsReadOnly>]
type ItemResponse<'attr> =
    { Item: Map<String, 'attr> voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.KeysAndAttributes", false, false)>]
type KeysAndAttributes<'attr> =
    { AttributesToGet: String array voption
      ConsistentRead: Boolean voption
      ExpressionAttributeNames: Map<String, String> voption
      Keys: Map<String, 'attr> array voption
      ProjectionExpression: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.KeySchemaElement", false, false); Struct; IsReadOnly>]
type KeySchemaElement =
    { AttributeName: String voption
      KeyType: KeyType voption }

[<DynamodbType("Amazon.DynamoDBv2.KeyType", false, true)>]
type KeyType private (value: string) =
    member _.Value = value
    static member HASH = KeyType("HASH")
    static member RANGE = KeyType("RANGE")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.KinesisDataStreamDestination", false, false)>]
type KinesisDataStreamDestination =
    { ApproximateCreationDateTimePrecision: ApproximateCreationDateTimePrecision voption
      DestinationStatus: DestinationStatus voption
      DestinationStatusDescription: String voption
      StreamArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListBackupsRequest", false, false)>]
type ListBackupsRequest =
    { BackupType: BackupTypeFilter voption
      ExclusiveStartBackupArn: String voption
      Limit: Int32 voption
      TableName: String voption
      TimeRangeLowerBound: DateTime voption
      TimeRangeUpperBound: DateTime voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListBackupsResponse", false, false)>]
type ListBackupsResponse =
    { BackupSummaries: BackupSummary array voption
      LastEvaluatedBackupArn: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListContributorInsightsRequest", false, false)>]
type ListContributorInsightsRequest =
    { MaxResults: Int32 voption
      NextToken: String voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListContributorInsightsResponse", false, false)>]
type ListContributorInsightsResponse =
    { ContributorInsightsSummaries: ContributorInsightsSummary array voption
      NextToken: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListExportsRequest", false, false)>]
type ListExportsRequest =
    { MaxResults: Int32 voption
      NextToken: String voption
      TableArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListExportsResponse", false, false)>]
type ListExportsResponse =
    { ExportSummaries: ExportSummary array voption
      NextToken: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListGlobalTablesRequest", false, false)>]
type ListGlobalTablesRequest =
    { ExclusiveStartGlobalTableName: String voption
      Limit: Int32 voption
      RegionName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListGlobalTablesResponse", false, false)>]
type ListGlobalTablesResponse =
    { GlobalTables: GlobalTable array voption
      LastEvaluatedGlobalTableName: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListImportsRequest", false, false)>]
type ListImportsRequest =
    { NextToken: String voption
      PageSize: Int32 voption
      TableArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListImportsResponse", false, false)>]
type ListImportsResponse =
    { ImportSummaryList: ImportSummary array voption
      NextToken: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListTablesRequest", false, false)>]
type ListTablesRequest =
    { ExclusiveStartTableName: String voption
      Limit: Int32 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListTablesResponse", false, false)>]
type ListTablesResponse =
    { LastEvaluatedTableName: String voption
      TableNames: String array voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListTagsOfResourceRequest", false, false)>]
type ListTagsOfResourceRequest =
    { NextToken: String voption
      ResourceArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ListTagsOfResourceResponse", false, false)>]
type ListTagsOfResourceResponse =
    { NextToken: String voption
      Tags: Tag array voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.LocalSecondaryIndex", false, false)>]
type LocalSecondaryIndex =
    { IndexName: String voption
      KeySchema: KeySchemaElement array voption
      Projection: Projection voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.LocalSecondaryIndexDescription", false, false)>]
type LocalSecondaryIndexDescription =
    { IndexArn: String voption
      IndexName: String voption
      IndexSizeBytes: Int64 voption
      ItemCount: Int64 voption
      KeySchema: KeySchemaElement array voption
      Projection: Projection voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.LocalSecondaryIndexInfo", false, false)>]
type LocalSecondaryIndexInfo =
    { IndexName: String voption
      KeySchema: KeySchemaElement array voption
      Projection: Projection voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.OnDemandThroughput", false, false); Struct; IsReadOnly>]
type OnDemandThroughput =
    { MaxReadRequestUnits: Int64 voption
      MaxWriteRequestUnits: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.OnDemandThroughputOverride", false, false); Struct; IsReadOnly>]
type OnDemandThroughputOverride =
    { MaxReadRequestUnits: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ParameterizedStatement", false, false)>]
type ParameterizedStatement<'attr> =
    { Parameters: 'attr array voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      Statement: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.PointInTimeRecoveryDescription", false, false)>]
type PointInTimeRecoveryDescription =
    { EarliestRestorableDateTime: DateTime voption
      LatestRestorableDateTime: DateTime voption
      PointInTimeRecoveryStatus: PointInTimeRecoveryStatus voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.PointInTimeRecoverySpecification", false, false); Struct; IsReadOnly>]
type PointInTimeRecoverySpecification =
    { PointInTimeRecoveryEnabled: Boolean voption }

[<DynamodbType("Amazon.DynamoDBv2.PointInTimeRecoveryStatus", false, true)>]
type PointInTimeRecoveryStatus private (value: string) =
    member _.Value = value
    static member DISABLED = PointInTimeRecoveryStatus("DISABLED")
    static member ENABLED = PointInTimeRecoveryStatus("ENABLED")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.Projection", false, false); Struct; IsReadOnly>]
type Projection =
    { NonKeyAttributes: String array voption
      ProjectionType: ProjectionType voption }

[<DynamodbType("Amazon.DynamoDBv2.ProjectionType", false, true)>]
type ProjectionType private (value: string) =
    member _.Value = value
    static member ALL = ProjectionType("ALL")
    static member INCLUDE = ProjectionType("INCLUDE")
    static member KEYS_ONLY = ProjectionType("KEYS_ONLY")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.ProvisionedThroughput", false, false); Struct; IsReadOnly>]
type ProvisionedThroughput =
    { ReadCapacityUnits: Int64 voption
      WriteCapacityUnits: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ProvisionedThroughputDescription", false, false)>]
type ProvisionedThroughputDescription =
    { LastDecreaseDateTime: DateTime voption
      LastIncreaseDateTime: DateTime voption
      NumberOfDecreasesToday: Int64 voption
      ReadCapacityUnits: Int64 voption
      WriteCapacityUnits: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ProvisionedThroughputOverride", false, false); Struct; IsReadOnly>]
type ProvisionedThroughputOverride =
    { ReadCapacityUnits: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.Put", false, false)>]
type Put<'attr> =
    { ConditionExpression: String voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      Item: Map<String, 'attr> voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.PutItemRequest", false, false)>]
type PutItemRequest<'attr> =
    { ConditionalOperator: ConditionalOperator voption
      ConditionExpression: String voption
      Expected: Map<String, ExpectedAttributeValue<'attr>> voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      Item: Map<String, 'attr> voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      ReturnItemCollectionMetrics: ReturnItemCollectionMetrics voption
      ReturnValues: ReturnValue voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.PutItemResponse", false, false)>]
type PutItemResponse<'attr> =
    { Attributes: Map<String, 'attr> voption
      ConsumedCapacity: ConsumedCapacity voption
      ItemCollectionMetrics: ItemCollectionMetrics<'attr> voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.PutRequest", false, false); Struct; IsReadOnly>]
type PutRequest<'attr> =
    { Item: Map<String, 'attr> voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.PutResourcePolicyRequest", false, false)>]
type PutResourcePolicyRequest =
    { ConfirmRemoveSelfResourceAccess: Boolean voption
      ExpectedRevisionId: String voption
      Policy: String voption
      ResourceArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.PutResourcePolicyResponse", false, false)>]
type PutResourcePolicyResponse =
    { RevisionId: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.QueryRequest", false, false)>]
type QueryRequest<'attr> =
    { AttributesToGet: String array voption
      ConditionalOperator: ConditionalOperator voption
      ConsistentRead: Boolean voption
      ExclusiveStartKey: Map<String, 'attr> voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      FilterExpression: String voption
      IndexName: String voption
      KeyConditionExpression: String voption
      KeyConditions: Map<String, Condition<'attr>> voption
      Limit: Int32 voption
      ProjectionExpression: String voption
      QueryFilter: Map<String, Condition<'attr>> voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      ScanIndexForward: Boolean voption
      Select: Select voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.QueryResponse", false, false)>]
type QueryResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity voption
      Count: Int32 voption
      Items: Map<String, 'attr> array voption
      LastEvaluatedKey: Map<String, 'attr> voption
      ScannedCount: Int32 voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.Replica", false, false); Struct; IsReadOnly>]
type Replica =
    { RegionName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaAutoScalingDescription", false, false)>]
type ReplicaAutoScalingDescription =
    { GlobalSecondaryIndexes: ReplicaGlobalSecondaryIndexAutoScalingDescription array voption
      RegionName: String voption
      ReplicaProvisionedReadCapacityAutoScalingSettings: AutoScalingSettingsDescription voption
      ReplicaProvisionedWriteCapacityAutoScalingSettings: AutoScalingSettingsDescription voption
      ReplicaStatus: ReplicaStatus voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaAutoScalingUpdate", false, false)>]
type ReplicaAutoScalingUpdate =
    { RegionName: String voption
      ReplicaGlobalSecondaryIndexUpdates: ReplicaGlobalSecondaryIndexAutoScalingUpdate array voption
      ReplicaProvisionedReadCapacityAutoScalingUpdate: AutoScalingSettingsUpdate voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaDescription", false, false)>]
type ReplicaDescription =
    { GlobalSecondaryIndexes: ReplicaGlobalSecondaryIndexDescription array voption
      KMSMasterKeyId: String voption
      OnDemandThroughputOverride: OnDemandThroughputOverride voption
      ProvisionedThroughputOverride: ProvisionedThroughputOverride voption
      RegionName: String voption
      ReplicaInaccessibleDateTime: DateTime voption
      ReplicaStatus: ReplicaStatus voption
      ReplicaStatusDescription: String voption
      ReplicaStatusPercentProgress: String voption
      ReplicaTableClassSummary: TableClassSummary voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaGlobalSecondaryIndex", false, false)>]
type ReplicaGlobalSecondaryIndex =
    { IndexName: String voption
      OnDemandThroughputOverride: OnDemandThroughputOverride voption
      ProvisionedThroughputOverride: ProvisionedThroughputOverride voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaGlobalSecondaryIndexAutoScalingDescription", false, false)>]
type ReplicaGlobalSecondaryIndexAutoScalingDescription =
    { IndexName: String voption
      IndexStatus: IndexStatus voption
      ProvisionedReadCapacityAutoScalingSettings: AutoScalingSettingsDescription voption
      ProvisionedWriteCapacityAutoScalingSettings: AutoScalingSettingsDescription voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaGlobalSecondaryIndexAutoScalingUpdate", false, false); Struct; IsReadOnly>]
type ReplicaGlobalSecondaryIndexAutoScalingUpdate =
    { IndexName: String voption
      ProvisionedReadCapacityAutoScalingUpdate: AutoScalingSettingsUpdate voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaGlobalSecondaryIndexDescription", false, false)>]
type ReplicaGlobalSecondaryIndexDescription =
    { IndexName: String voption
      OnDemandThroughputOverride: OnDemandThroughputOverride voption
      ProvisionedThroughputOverride: ProvisionedThroughputOverride voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaGlobalSecondaryIndexSettingsDescription", false, false)>]
type ReplicaGlobalSecondaryIndexSettingsDescription =
    { IndexName: String voption
      IndexStatus: IndexStatus voption
      ProvisionedReadCapacityAutoScalingSettings: AutoScalingSettingsDescription voption
      ProvisionedReadCapacityUnits: Int64 voption
      ProvisionedWriteCapacityAutoScalingSettings: AutoScalingSettingsDescription voption
      ProvisionedWriteCapacityUnits: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaGlobalSecondaryIndexSettingsUpdate", false, false)>]
type ReplicaGlobalSecondaryIndexSettingsUpdate =
    { IndexName: String voption
      ProvisionedReadCapacityAutoScalingSettingsUpdate: AutoScalingSettingsUpdate voption
      ProvisionedReadCapacityUnits: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaSettingsDescription", false, false)>]
type ReplicaSettingsDescription =
    { RegionName: String voption
      ReplicaBillingModeSummary: BillingModeSummary voption
      ReplicaGlobalSecondaryIndexSettings: ReplicaGlobalSecondaryIndexSettingsDescription array voption
      ReplicaProvisionedReadCapacityAutoScalingSettings: AutoScalingSettingsDescription voption
      ReplicaProvisionedReadCapacityUnits: Int64 voption
      ReplicaProvisionedWriteCapacityAutoScalingSettings: AutoScalingSettingsDescription voption
      ReplicaProvisionedWriteCapacityUnits: Int64 voption
      ReplicaStatus: ReplicaStatus voption
      ReplicaTableClassSummary: TableClassSummary voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaSettingsUpdate", false, false)>]
type ReplicaSettingsUpdate =
    { RegionName: String voption
      ReplicaGlobalSecondaryIndexSettingsUpdate: ReplicaGlobalSecondaryIndexSettingsUpdate array voption
      ReplicaProvisionedReadCapacityAutoScalingSettingsUpdate: AutoScalingSettingsUpdate voption
      ReplicaProvisionedReadCapacityUnits: Int64 voption
      ReplicaTableClass: TableClass voption }

[<DynamodbType("Amazon.DynamoDBv2.ReplicaStatus", false, true)>]
type ReplicaStatus private (value: string) =
    member _.Value = value
    static member ACTIVE = ReplicaStatus("ACTIVE")
    static member CREATING = ReplicaStatus("CREATING")
    static member CREATION_FAILED = ReplicaStatus("CREATION_FAILED")
    static member DELETING = ReplicaStatus("DELETING")
    static member INACCESSIBLE_ENCRYPTION_CREDENTIALS = ReplicaStatus("INACCESSIBLE_ENCRYPTION_CREDENTIALS")
    static member REGION_DISABLED = ReplicaStatus("REGION_DISABLED")
    static member UPDATING = ReplicaStatus("UPDATING")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicationGroupUpdate", false, false)>]
type ReplicationGroupUpdate =
    { Create: CreateReplicationGroupMemberAction voption
      Delete: DeleteReplicationGroupMemberAction voption
      Update: UpdateReplicationGroupMemberAction voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ReplicaUpdate", false, false); Struct; IsReadOnly>]
type ReplicaUpdate =
    { Create: CreateReplicaAction voption
      Delete: DeleteReplicaAction voption }

[<DynamodbType("Amazon.Runtime.ResponseMetadata", false, false)>]
type ResponseMetadata =
    { RequestId: String voption
      Metadata: Map<String, String> voption
      ChecksumAlgorithm: CoreChecksumAlgorithm voption
      ChecksumValidationStatus: ChecksumValidationStatus voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.RestoreSummary", false, false)>]
type RestoreSummary =
    { RestoreDateTime: DateTime voption
      RestoreInProgress: Boolean voption
      SourceBackupArn: String voption
      SourceTableArn: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.RestoreTableFromBackupRequest", false, false)>]
type RestoreTableFromBackupRequest =
    { BackupArn: String voption
      BillingModeOverride: BillingMode voption
      GlobalSecondaryIndexOverride: GlobalSecondaryIndex array voption
      LocalSecondaryIndexOverride: LocalSecondaryIndex array voption
      OnDemandThroughputOverride: OnDemandThroughput voption
      ProvisionedThroughputOverride: ProvisionedThroughput voption
      SSESpecificationOverride: SSESpecification voption
      TargetTableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.RestoreTableFromBackupResponse", false, false)>]
type RestoreTableFromBackupResponse =
    { TableDescription: TableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.RestoreTableToPointInTimeRequest", false, false)>]
type RestoreTableToPointInTimeRequest =
    { BillingModeOverride: BillingMode voption
      GlobalSecondaryIndexOverride: GlobalSecondaryIndex array voption
      LocalSecondaryIndexOverride: LocalSecondaryIndex array voption
      OnDemandThroughputOverride: OnDemandThroughput voption
      ProvisionedThroughputOverride: ProvisionedThroughput voption
      RestoreDateTime: DateTime voption
      SourceTableArn: String voption
      SourceTableName: String voption
      SSESpecificationOverride: SSESpecification voption
      TargetTableName: String voption
      UseLatestRestorableTime: Boolean voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.RestoreTableToPointInTimeResponse", false, false)>]
type RestoreTableToPointInTimeResponse =
    { TableDescription: TableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.ReturnConsumedCapacity", false, true)>]
type ReturnConsumedCapacity private (value: string) =
    member _.Value = value
    static member INDEXES = ReturnConsumedCapacity("INDEXES")
    static member NONE = ReturnConsumedCapacity("NONE")
    static member TOTAL = ReturnConsumedCapacity("TOTAL")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.ReturnItemCollectionMetrics", false, true)>]
type ReturnItemCollectionMetrics private (value: string) =
    member _.Value = value
    static member NONE = ReturnItemCollectionMetrics("NONE")
    static member SIZE = ReturnItemCollectionMetrics("SIZE")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.ReturnValue", false, true)>]
type ReturnValue private (value: string) =
    member _.Value = value
    static member ALL_NEW = ReturnValue("ALL_NEW")
    static member ALL_OLD = ReturnValue("ALL_OLD")
    static member NONE = ReturnValue("NONE")
    static member UPDATED_NEW = ReturnValue("UPDATED_NEW")
    static member UPDATED_OLD = ReturnValue("UPDATED_OLD")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.ReturnValuesOnConditionCheckFailure", false, true)>]
type ReturnValuesOnConditionCheckFailure private (value: string) =
    member _.Value = value
    static member ALL_OLD = ReturnValuesOnConditionCheckFailure("ALL_OLD")
    static member NONE = ReturnValuesOnConditionCheckFailure("NONE")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.S3BucketSource", false, false)>]
type S3BucketSource =
    { S3Bucket: String voption
      S3BucketOwner: String voption
      S3KeyPrefix: String voption }

[<DynamodbType("Amazon.DynamoDBv2.S3SseAlgorithm", false, true)>]
type S3SseAlgorithm private (value: string) =
    member _.Value = value
    static member AES256 = S3SseAlgorithm("AES256")
    static member KMS = S3SseAlgorithm("KMS")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.ScalarAttributeType", false, true)>]
type ScalarAttributeType private (value: string) =
    member _.Value = value
    static member B = ScalarAttributeType("B")
    static member N = ScalarAttributeType("N")
    static member S = ScalarAttributeType("S")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.ScanRequest", false, false)>]
type ScanRequest<'attr> =
    { AttributesToGet: String array voption
      ConditionalOperator: ConditionalOperator voption
      ConsistentRead: Boolean voption
      ExclusiveStartKey: Map<String, 'attr> voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      FilterExpression: String voption
      IndexName: String voption
      Limit: Int32 voption
      ProjectionExpression: String voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      ScanFilter: Map<String, Condition<'attr>> voption
      Segment: Int32 voption
      Select: Select voption
      TableName: String voption
      TotalSegments: Int32 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.ScanResponse", false, false)>]
type ScanResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity voption
      Count: Int32 voption
      Items: Map<String, 'attr> array voption
      LastEvaluatedKey: Map<String, 'attr> voption
      ScannedCount: Int32 voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Select", false, true)>]
type Select private (value: string) =
    member _.Value = value
    static member ALL_ATTRIBUTES = Select("ALL_ATTRIBUTES")
    static member ALL_PROJECTED_ATTRIBUTES = Select("ALL_PROJECTED_ATTRIBUTES")
    static member COUNT = Select("COUNT")
    static member SPECIFIC_ATTRIBUTES = Select("SPECIFIC_ATTRIBUTES")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.SourceTableDetails", false, false)>]
type SourceTableDetails =
    { BillingMode: BillingMode voption
      ItemCount: Int64 voption
      KeySchema: KeySchemaElement array voption
      OnDemandThroughput: OnDemandThroughput voption
      ProvisionedThroughput: ProvisionedThroughput voption
      TableArn: String voption
      TableCreationDateTime: DateTime voption
      TableId: String voption
      TableName: String voption
      TableSizeBytes: Int64 voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.SourceTableFeatureDetails", false, false)>]
type SourceTableFeatureDetails =
    { GlobalSecondaryIndexes: GlobalSecondaryIndexInfo array voption
      LocalSecondaryIndexes: LocalSecondaryIndexInfo array voption
      SSEDescription: SSEDescription voption
      StreamDescription: StreamSpecification voption
      TimeToLiveDescription: TimeToLiveDescription voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.SSEDescription", false, false)>]
type SSEDescription =
    { InaccessibleEncryptionDateTime: DateTime voption
      KMSMasterKeyArn: String voption
      SSEType: SSEType voption
      Status: SSEStatus voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.SSESpecification", false, false)>]
type SSESpecification =
    { Enabled: Boolean voption
      KMSMasterKeyId: String voption
      SSEType: SSEType voption }

[<DynamodbType("Amazon.DynamoDBv2.SSEStatus", false, true)>]
type SSEStatus private (value: string) =
    member _.Value = value
    static member DISABLED = SSEStatus("DISABLED")
    static member DISABLING = SSEStatus("DISABLING")
    static member ENABLED = SSEStatus("ENABLED")
    static member ENABLING = SSEStatus("ENABLING")
    static member UPDATING = SSEStatus("UPDATING")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.SSEType", false, true)>]
type SSEType private (value: string) =
    member _.Value = value
    static member AES256 = SSEType("AES256")
    static member KMS = SSEType("KMS")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.StreamSpecification", false, false); Struct; IsReadOnly>]
type StreamSpecification =
    { StreamEnabled: Boolean voption
      StreamViewType: StreamViewType voption }

[<DynamodbType("Amazon.DynamoDBv2.StreamViewType", false, true)>]
type StreamViewType private (value: string) =
    member _.Value = value
    static member KEYS_ONLY = StreamViewType("KEYS_ONLY")
    static member NEW_AND_OLD_IMAGES = StreamViewType("NEW_AND_OLD_IMAGES")
    static member NEW_IMAGE = StreamViewType("NEW_IMAGE")
    static member OLD_IMAGE = StreamViewType("OLD_IMAGE")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.TableAutoScalingDescription", false, false)>]
type TableAutoScalingDescription =
    { Replicas: ReplicaAutoScalingDescription array voption
      TableName: String voption
      TableStatus: TableStatus voption }

[<DynamodbType("Amazon.DynamoDBv2.TableClass", false, true)>]
type TableClass private (value: string) =
    member _.Value = value
    static member STANDARD = TableClass("STANDARD")
    static member STANDARD_INFREQUENT_ACCESS = TableClass("STANDARD_INFREQUENT_ACCESS")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.TableClassSummary", false, false); Struct; IsReadOnly>]
type TableClassSummary =
    { LastUpdateDateTime: DateTime voption
      TableClass: TableClass voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TableCreationParameters", false, false)>]
type TableCreationParameters =
    { AttributeDefinitions: AttributeDefinition array voption
      BillingMode: BillingMode voption
      GlobalSecondaryIndexes: GlobalSecondaryIndex array voption
      KeySchema: KeySchemaElement array voption
      OnDemandThroughput: OnDemandThroughput voption
      ProvisionedThroughput: ProvisionedThroughput voption
      SSESpecification: SSESpecification voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TableDescription", false, false)>]
type TableDescription =
    { ArchivalSummary: ArchivalSummary voption
      AttributeDefinitions: AttributeDefinition array voption
      BillingModeSummary: BillingModeSummary voption
      CreationDateTime: DateTime voption
      DeletionProtectionEnabled: Boolean voption
      GlobalSecondaryIndexes: GlobalSecondaryIndexDescription array voption
      GlobalTableVersion: String voption
      ItemCount: Int64 voption
      KeySchema: KeySchemaElement array voption
      LatestStreamArn: String voption
      LatestStreamLabel: String voption
      LocalSecondaryIndexes: LocalSecondaryIndexDescription array voption
      OnDemandThroughput: OnDemandThroughput voption
      ProvisionedThroughput: ProvisionedThroughputDescription voption
      Replicas: ReplicaDescription array voption
      RestoreSummary: RestoreSummary voption
      SSEDescription: SSEDescription voption
      StreamSpecification: StreamSpecification voption
      TableArn: String voption
      TableClassSummary: TableClassSummary voption
      TableId: String voption
      TableName: String voption
      TableSizeBytes: Int64 voption
      TableStatus: TableStatus voption }

[<DynamodbType("Amazon.DynamoDBv2.TableStatus", false, true)>]
type TableStatus private (value: string) =
    member _.Value = value
    static member ACTIVE = TableStatus("ACTIVE")
    static member ARCHIVED = TableStatus("ARCHIVED")
    static member ARCHIVING = TableStatus("ARCHIVING")
    static member CREATING = TableStatus("CREATING")
    static member DELETING = TableStatus("DELETING")
    static member INACCESSIBLE_ENCRYPTION_CREDENTIALS = TableStatus("INACCESSIBLE_ENCRYPTION_CREDENTIALS")
    static member UPDATING = TableStatus("UPDATING")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.Tag", false, false); Struct; IsReadOnly>]
type Tag =
    { Key: String voption
      Value: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TagResourceRequest", false, false)>]
type TagResourceRequest =
    { ResourceArn: String voption
      Tags: Tag array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TagResourceResponse", false, false)>]
type TagResourceResponse =
    { ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TimeToLiveDescription", false, false); Struct; IsReadOnly>]
type TimeToLiveDescription =
    { AttributeName: String voption
      TimeToLiveStatus: TimeToLiveStatus voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TimeToLiveSpecification", false, false); Struct; IsReadOnly>]
type TimeToLiveSpecification =
    { AttributeName: String voption
      Enabled: Boolean voption }

[<DynamodbType("Amazon.DynamoDBv2.TimeToLiveStatus", false, true)>]
type TimeToLiveStatus private (value: string) =
    member _.Value = value
    static member DISABLED = TimeToLiveStatus("DISABLED")
    static member DISABLING = TimeToLiveStatus("DISABLING")
    static member ENABLED = TimeToLiveStatus("ENABLED")
    static member ENABLING = TimeToLiveStatus("ENABLING")
    override this.ToString() = this.Value

[<DynamodbType("Amazon.DynamoDBv2.Model.TransactGetItem", false, false); Struct; IsReadOnly>]
type TransactGetItem<'attr> =
    { Get: Get<'attr> voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TransactGetItemsRequest", false, false)>]
type TransactGetItemsRequest<'attr> =
    { ReturnConsumedCapacity: ReturnConsumedCapacity voption
      TransactItems: TransactGetItem<'attr> array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TransactGetItemsResponse", false, false)>]
type TransactGetItemsResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity array voption
      Responses: ItemResponse<'attr> array voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TransactWriteItem", false, false)>]
type TransactWriteItem<'attr> =
    { ConditionCheck: ConditionCheck<'attr> voption
      Delete: Delete<'attr> voption
      Put: Put<'attr> voption
      Update: Update<'attr> voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TransactWriteItemsRequest", false, false)>]
type TransactWriteItemsRequest<'attr> =
    { ClientRequestToken: String voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      ReturnItemCollectionMetrics: ReturnItemCollectionMetrics voption
      TransactItems: TransactWriteItem<'attr> array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.TransactWriteItemsResponse", false, false)>]
type TransactWriteItemsResponse<'attr> =
    { ConsumedCapacity: ConsumedCapacity array voption
      ItemCollectionMetrics: Map<String, ItemCollectionMetrics<'attr> array> voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UntagResourceRequest", false, false)>]
type UntagResourceRequest =
    { ResourceArn: String voption
      TagKeys: String array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UntagResourceResponse", false, false)>]
type UntagResourceResponse =
    { ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.Update", false, false)>]
type Update<'attr> =
    { ConditionExpression: String voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      Key: Map<String, 'attr> voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      TableName: String voption
      UpdateExpression: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateContinuousBackupsRequest", false, false)>]
type UpdateContinuousBackupsRequest =
    { PointInTimeRecoverySpecification: PointInTimeRecoverySpecification voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateContinuousBackupsResponse", false, false)>]
type UpdateContinuousBackupsResponse =
    { ContinuousBackupsDescription: ContinuousBackupsDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateContributorInsightsRequest", false, false)>]
type UpdateContributorInsightsRequest =
    { ContributorInsightsAction: ContributorInsightsAction voption
      IndexName: String voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateContributorInsightsResponse", false, false)>]
type UpdateContributorInsightsResponse =
    { ContributorInsightsStatus: ContributorInsightsStatus voption
      IndexName: String voption
      TableName: String voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateGlobalSecondaryIndexAction", false, false)>]
type UpdateGlobalSecondaryIndexAction =
    { IndexName: String voption
      OnDemandThroughput: OnDemandThroughput voption
      ProvisionedThroughput: ProvisionedThroughput voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateGlobalTableRequest", false, false)>]
type UpdateGlobalTableRequest =
    { GlobalTableName: String voption
      ReplicaUpdates: ReplicaUpdate array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateGlobalTableResponse", false, false)>]
type UpdateGlobalTableResponse =
    { GlobalTableDescription: GlobalTableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateGlobalTableSettingsRequest", false, false)>]
type UpdateGlobalTableSettingsRequest =
    { GlobalTableBillingMode: BillingMode voption
      GlobalTableGlobalSecondaryIndexSettingsUpdate: GlobalTableGlobalSecondaryIndexSettingsUpdate array voption
      GlobalTableName: String voption
      GlobalTableProvisionedWriteCapacityAutoScalingSettingsUpdate: AutoScalingSettingsUpdate voption
      GlobalTableProvisionedWriteCapacityUnits: Int64 voption
      ReplicaSettingsUpdate: ReplicaSettingsUpdate array voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateGlobalTableSettingsResponse", false, false)>]
type UpdateGlobalTableSettingsResponse =
    { GlobalTableName: String voption
      ReplicaSettings: ReplicaSettingsDescription array voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateItemRequest", false, false)>]
type UpdateItemRequest<'attr> =
    { AttributeUpdates: Map<String, AttributeValueUpdate<'attr>> voption
      ConditionalOperator: ConditionalOperator voption
      ConditionExpression: String voption
      Expected: Map<String, ExpectedAttributeValue<'attr>> voption
      ExpressionAttributeNames: Map<String, String> voption
      ExpressionAttributeValues: Map<String, 'attr> voption
      Key: Map<String, 'attr> voption
      ReturnConsumedCapacity: ReturnConsumedCapacity voption
      ReturnItemCollectionMetrics: ReturnItemCollectionMetrics voption
      ReturnValues: ReturnValue voption
      ReturnValuesOnConditionCheckFailure: ReturnValuesOnConditionCheckFailure voption
      TableName: String voption
      UpdateExpression: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateItemResponse", false, false)>]
type UpdateItemResponse<'attr> =
    { Attributes: Map<String, 'attr> voption
      ConsumedCapacity: ConsumedCapacity voption
      ItemCollectionMetrics: ItemCollectionMetrics<'attr> voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateKinesisStreamingConfiguration", false, false); Struct; IsReadOnly>]
type UpdateKinesisStreamingConfiguration =
    { ApproximateCreationDateTimePrecision: ApproximateCreationDateTimePrecision voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateKinesisStreamingDestinationRequest", false, false)>]
type UpdateKinesisStreamingDestinationRequest =
    { StreamArn: String voption
      TableName: String voption
      UpdateKinesisStreamingConfiguration: UpdateKinesisStreamingConfiguration voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateKinesisStreamingDestinationResponse", false, false)>]
type UpdateKinesisStreamingDestinationResponse =
    { DestinationStatus: DestinationStatus voption
      StreamArn: String voption
      TableName: String voption
      UpdateKinesisStreamingConfiguration: UpdateKinesisStreamingConfiguration voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateReplicationGroupMemberAction", false, false)>]
type UpdateReplicationGroupMemberAction =
    { GlobalSecondaryIndexes: ReplicaGlobalSecondaryIndex array voption
      KMSMasterKeyId: String voption
      OnDemandThroughputOverride: OnDemandThroughputOverride voption
      ProvisionedThroughputOverride: ProvisionedThroughputOverride voption
      RegionName: String voption
      TableClassOverride: TableClass voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateTableReplicaAutoScalingRequest", false, false)>]
type UpdateTableReplicaAutoScalingRequest =
    { GlobalSecondaryIndexUpdates: GlobalSecondaryIndexAutoScalingUpdate array voption
      ProvisionedWriteCapacityAutoScalingUpdate: AutoScalingSettingsUpdate voption
      ReplicaUpdates: ReplicaAutoScalingUpdate array voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateTableReplicaAutoScalingResponse", false, false)>]
type UpdateTableReplicaAutoScalingResponse =
    { TableAutoScalingDescription: TableAutoScalingDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateTableRequest", false, false)>]
type UpdateTableRequest =
    { AttributeDefinitions: AttributeDefinition array voption
      BillingMode: BillingMode voption
      DeletionProtectionEnabled: Boolean voption
      GlobalSecondaryIndexUpdates: GlobalSecondaryIndexUpdate array voption
      OnDemandThroughput: OnDemandThroughput voption
      ProvisionedThroughput: ProvisionedThroughput voption
      ReplicaUpdates: ReplicationGroupUpdate array voption
      SSESpecification: SSESpecification voption
      StreamSpecification: StreamSpecification voption
      TableClass: TableClass voption
      TableName: String voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateTableResponse", false, false)>]
type UpdateTableResponse =
    { TableDescription: TableDescription voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateTimeToLiveRequest", false, false)>]
type UpdateTimeToLiveRequest =
    { TableName: String voption
      TimeToLiveSpecification: TimeToLiveSpecification voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.UpdateTimeToLiveResponse", false, false)>]
type UpdateTimeToLiveResponse =
    { TimeToLiveSpecification: TimeToLiveSpecification voption
      ResponseMetadata: ResponseMetadata voption
      ContentLength: Int64 voption
      HttpStatusCode: HttpStatusCode voption }

[<DynamodbType("Amazon.DynamoDBv2.Model.WriteRequest", false, false); Struct; IsReadOnly>]
type WriteRequest<'attr> =
    { DeleteRequest: DeleteRequest<'attr> voption
      PutRequest: PutRequest<'attr> voption }