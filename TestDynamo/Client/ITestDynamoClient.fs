namespace TestDynamo.Client

open System
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open TestDynamo.Api
open TestDynamo.Model

/// <summary>
/// A client which can execute operations on an in memory Database or an in memory DistributedDatabase
/// </summary>
type ITestDynamoClient =
    inherit IAmazonDynamoDB

    /// <summary>
    /// The database which this client will query
    /// </summary>
    abstract member Database: TestDynamo.Api.Database

    /// <summary>
    /// The distributed database which this client will query
    /// This property may be None if the client only works with a local database  
    /// </summary>
    abstract member DistributedDatabase: DistributedDatabase voption

    /// <summary>
    /// Return a task that will be completed when all subscribers have been completed and the system is at rest
    /// The returned task will throw any errors encountered in subscribers
    /// </summary>
    abstract member AwaitAllSubscribers: CancellationToken -> ValueTask

    /// <summary>
    /// <para>
    /// A lazily calculated dump of all of the data in the system
    /// This data is optimised for viewing with the debugger
    /// </para>
    ///
    /// <para>
    /// For programmatic access, use the Tables property
    /// </para>
    /// </summary>
    abstract member DebugView: DebugTable seq

    /// <summary>
    /// <para>
    /// A lazily calculated dump of all of the data in the system
    /// This data is optimised for viewing with the debugger
    /// </para>
    ///
    /// <para>
    /// If this client works on a non distributed database, there will only be one item in the result
    /// </para>
    ///
    /// <para>
    /// For programmatic access, use the DistributedTables property
    /// </para>
    /// </summary>
    abstract member DistributedDebugView: Map<DatabaseId, DebugTable list>

    /// <summary>
    /// <para>
    /// A table lookup where tables can be used to lookup individual items
    /// This data is based on IEnumerable. It has no key lookups, but it works
    /// well with foreach loops and System.Linq
    /// </para>
    ///
    /// <para>
    /// For query type semantics, use the QueryAsync method on IAmazonDynamoDB
    /// For a debugger display, see the DebugView property
    /// </para>
    /// </summary>
    abstract member GetTable: tableName: string -> LazyDebugTable

    /// <summary>
    /// <para>
    /// A table lookup where tables can be used to lookup individual items
    /// This data is based on IEnumerable. It has no key lookups, but it works
    /// well with foreach loops and System.Linq
    /// </para>
    ///
    /// <para>
    /// For query type semantics, use the QueryAsync method on IAmazonDynamoDB
    /// For a debugger display, see the DebugView property
    /// </para>
    /// </summary>
    abstract member GetDistributedTable: databaseId: DatabaseId -> tableName: string -> LazyDebugTable

    /// <summary>
    /// An artificial delay to add to processing to simulate IO
    /// If greater than zero, will also defer execution to another thread using Tasks 
    /// </summary>
    abstract member ProcessingDelay: TimeSpan with get, set

    /// <summary>
    /// Add a callback to any changes to a specific table. This functionality mocks DynamoDbStreams
    /// Returns an item that will remove the subscription 
    /// </summary>
    abstract member SubscribeToStream: tableName: string -> subscriber: StreamSubscriber -> IStreamSubscriberDisposal

    /// <summary>
    /// <para>
    /// Set custom scan limits for any queries or scans on with this client
    /// The default scan limits are
    /// <ul>
    /// <li>maxScanItems: 2,000</li>
    /// <li>maxPageSizeBytes: 1MB</li>
    /// </ul>
    /// </para>
    ///
    /// <para>
    /// Defaults can be modified in Settings.ScanSizeLimits
    /// </para>
    /// </summary>
    abstract member SetScanLimits: limits: ExpressionExecutors.Fetch.ScanLimits -> unit

    /// <summary>
    /// Set a superficial aws account id for this client
    /// Default: "123456789012" 
    /// </summary>
    abstract member AwsAccountId: string with get, set
