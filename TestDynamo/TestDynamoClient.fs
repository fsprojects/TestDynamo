namespace TestDynamo

open System
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open Amazon
open Amazon.DynamoDBv2
open Amazon.Runtime
open Microsoft.Extensions.Logging
open TestDynamo.Api.FSharp
open TestDynamo.Client
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils

type ApiDb = TestDynamo.Api.FSharp.Database
type GlobalApiDb = TestDynamo.Api.FSharp.GlobalDatabase
type CsApiDb = TestDynamo.Api.Database
type GlobalCsApiDb = TestDynamo.Api.GlobalDatabase

/// <summary>An AmazonDynamoDBClient which can also dispose of an arbitrary disposable input</summary>
type private DisposingAmazonDynamoDBClient private (region: RegionEndpoint, dispose: IDisposable) =
    inherit AmazonDynamoDBClient(region)
    
    new (dispose: IDisposable) = new DisposingAmazonDynamoDBClient(RegionEndpoint.GetBySystemName(Settings.DefaultRegion), dispose)
    new (databaseId: TestDynamo.Model.DatabaseId, dispose: IDisposable) = new DisposingAmazonDynamoDBClient(RegionEndpoint.GetBySystemName(databaseId.regionId), dispose)
    
    override _.Dispose(disposing) =
        dispose.Dispose()
        base.Dispose(disposing)

/// <summary>
/// Extensions to create a dynamodb db client from a Database or to attach a Database to
/// an existing dynamodb client.
/// Methods targeting C# have upper case names, methods targeting F# have lower case names
/// </summary>
[<Extension>]
type TestDynamoClient =
    
    static let getRuntimePipeline: Amazon.DynamoDBv2.AmazonDynamoDBClient -> Amazon.Runtime.Internal.RuntimePipeline =
        let param = System.Linq.Expressions.Expression.Parameter(typeof<Amazon.DynamoDBv2.AmazonDynamoDBClient>)
        let p = System.Linq.Expressions.Expression.PropertyOrField(param, "RuntimePipeline")
         
        System.Linq.Expressions.Expression
            .Lambda<System.Func<Amazon.DynamoDBv2.AmazonDynamoDBClient, Amazon.Runtime.Internal.RuntimePipeline>>(p, param)
            .Compile()
            .Invoke
        
    static let getOptionalInterceptor client =
        let inline cast (x: IPipelineHandler) = x :?> ObjPipelineInterceptor
        
        getRuntimePipeline client
        |> _.Handlers
        |> Seq.filter (fun h -> h.GetType() = typeof<ObjPipelineInterceptor>)
        |> Collection.tryHead
        ?|> cast
        
    static let getRequiredInterceptor client =
        getOptionalInterceptor client
        ?|>? fun _ -> invalidOp "Client does not have a database attached"
    
    static let defaultLogger: Either<Database,struct (GlobalDatabase * _)> -> _ =
        Either.map1Of2 _.DefaultLogger
        >> Either.map2Of2 (fstT >> _.DefaultLogger)
        >> Either.reduce
        
    static let attach' (logger: ILogger voption) db (client: AmazonDynamoDBClient) =
        
        let db = Either.map2Of2 (flip tpl ({ regionId = client.Config.RegionEndpoint.SystemName }: Model.DatabaseId)) db
        let id =
            db
            |> Either.map1Of2 (fun (x: ApiDb) -> x.Id)
            |> Either.map2Of2 sndT
            |> Either.reduce
        
        let attachedAlready =
            getOptionalInterceptor client
            ?|> (fun db' ->
                match db with
                | Either1 db when db'.Database = db -> true
                | Either1 db when db'.GlobalDatabase ?|> (fun x -> x.GetDatabase ValueNone id) = ValueSome db -> true
                | Either2 (db, _) when db'.GlobalDatabase = ValueSome db -> true
                | _ -> invalidOp "Client already has a TestDynamo database attached")
            ?|? false
        
        if not attachedAlready
        then
            let runtimePipeline = getRuntimePipeline client
            runtimePipeline.AddHandler(ObjPipelineInterceptor(db, Settings.DefaultClientResponseDelay, logger ?|> ValueSome ?|? defaultLogger db))

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given Database
    /// </summary>
    [<Extension>]
    static member CreateClient(
        database: CsApiDb,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let client = new AmazonDynamoDBClient(region = RegionEndpoint.GetBySystemName(Settings.DefaultRegion))
        TestDynamoClient.Attach(client, database, logger)
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase
    /// </summary>
    [<Extension>]
    static member CreateClient(
        database: GlobalCsApiDb,
        databaseId: TestDynamo.Model.DatabaseId,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let client = new AmazonDynamoDBClient(region = RegionEndpoint.GetBySystemName(databaseId.regionId))
        TestDynamoClient.Attach(client, database, logger)
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase
    /// </summary>
    [<Extension>]
    static member CreateClient(
        database: GlobalCsApiDb,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        TestDynamoClient.CreateClient(database, {regionId = Settings.DefaultRegion}, logger)

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new Database
    /// </summary>
    static member CreateClient(
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        
        let database = new ApiDb()
        let client = new DisposingAmazonDynamoDBClient(database.Id, database :> IDisposable)
        TestDynamoClient.attach (CSharp.toOption logger) database client
        client :> AmazonDynamoDBClient

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new GlobalDatabase
    /// </summary>
    static member CreateGlobalClient(
        databaseId: TestDynamo.Model.DatabaseId,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        
        let database = new GlobalApiDb()
        let client = new DisposingAmazonDynamoDBClient(databaseId, database :> IDisposable)
        TestDynamoClient.attachGlobal (logger |> CSharp.toOption) database client
        client :> AmazonDynamoDBClient

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new GlobalDatabase
    /// </summary>
    static member CreateGlobalClient(
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        TestDynamoClient.CreateGlobalClient({regionId = Settings.DefaultRegion}, logger)

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given Database or a new Database
    /// </summary>
    static member createClient logger (database: ApiDb voption) =
        
        match database with
        | ValueSome database ->
            let client = new AmazonDynamoDBClient(region = RegionEndpoint.GetBySystemName(database.Id.regionId))
            TestDynamoClient.attach logger database client
            client
        | ValueNone ->
            let database = new ApiDb()
            let client = new DisposingAmazonDynamoDBClient(database.Id, database :> IDisposable)
            TestDynamoClient.attach logger database client
            client :> AmazonDynamoDBClient

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase or a new GlobalDatabase
    /// </summary>
    static member createGlobalClient logger (dbId: TestDynamo.Model.DatabaseId voption) (database: GlobalApiDb voption) =
        
        let regionId = dbId ?|> _.regionId ?|? Settings.DefaultRegion
        match database with
        | ValueSome database ->
            let client = new AmazonDynamoDBClient(region = RegionEndpoint.GetBySystemName(regionId))
            TestDynamoClient.attachGlobal logger database client
            client
        | ValueNone ->
            let database = new GlobalApiDb()
            let client = new DisposingAmazonDynamoDBClient({regionId = regionId}, database :> IDisposable)
            TestDynamoClient.attachGlobal logger database client
            client :> AmazonDynamoDBClient
    
    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given Database  
    /// </summary>
    static member attach (logger: ILogger voption) db (client: AmazonDynamoDBClient) =
        attach' logger (Either1 db) client
    
    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given Database
    /// </summary>
    [<Extension>]
    static member Attach (
        client: AmazonDynamoDBClient,
        db: TestDynamo.Api.Database,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        
        TestDynamoClient.attach (CSharp.toOption logger) db.CoreDb client
    
    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given GlobalDatabase
    /// </summary>
    static member attachGlobal (logger: ILogger voption) db (client: AmazonDynamoDBClient) =
        Either2 db |> flip (attach' logger) client
    
    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given GlobalDatabase
    /// </summary>
    [<Extension>]
    static member Attach (
        client: AmazonDynamoDBClient,
        db: TestDynamo.Api.GlobalDatabase,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        
        TestDynamoClient.attachGlobal (CSharp.toOption logger) db.CoreDb client
        
    /// <summary>
    /// Set an artificial delay on all requests.  
    /// </summary>
    static member setProcessingDelay delay client =
        let interceptor = getRequiredInterceptor client
        interceptor.ProcessingDelay <- delay
        
    /// <summary>
    /// Set an artificial delay on all requests.  
    /// </summary>
    [<Extension>]
    static member SetProcessingDelay(client, delay) = TestDynamoClient.setProcessingDelay delay client
        
    /// <summary>
    /// Set limits on how much data can be scanned in a single page
    /// </summary>
    static member setScanLimits scanLimits client =
        let interceptor = getRequiredInterceptor client
        interceptor.SetScanLimits scanLimits
        
    /// <summary>
    /// Set limits on how much data can be scanned in a single page
    /// </summary>
    [<Extension>]
    static member SetScanLimits(client, scanLimits) = TestDynamoClient.setScanLimits scanLimits client
        
    /// <summary>
    /// Set the aws account id for an AmazonDynamoDBClient
    /// </summary>
    static member setAwsAccountId awsAccountId client =
        let interceptor = getRequiredInterceptor client
        interceptor.AwsAccountId <- awsAccountId
        
    /// <summary>
    /// Set limits on how much data can be scanned in a single page
    /// </summary>
    [<Extension>]
    static member SetAwsAccountId(client, awsAccountId) = TestDynamoClient.setAwsAccountId awsAccountId client
        
    /// <summary>
    /// Set the aws account id for an AmazonDynamoDBClient
    /// </summary>
    static member getAwsAccountId client =
        let interceptor = getRequiredInterceptor client
        interceptor.AwsAccountId
        
    /// <summary>
    /// Set limits on how much data can be scanned in a single page
    /// </summary>
    [<Extension>]
    static member GetAwsAccountId(client) = TestDynamoClient.getAwsAccountId client
        
    /// <summary>
    /// Get the underlying database from an AmazonDynamoDBClient
    /// </summary>
    static member getDatabase (client: AmazonDynamoDBClient) =
        let interceptor = getRequiredInterceptor client
        interceptor.Database
        
    /// <summary>
    /// Get the underlying database from an AmazonDynamoDBClient
    /// </summary>
    [<Extension>]
    static member GetDatabase(client: AmazonDynamoDBClient) =
        let db = TestDynamoClient.getDatabase client
        new Api.Database(db)
        
    /// <summary>
    /// Get the underlying global database from an AmazonDynamoDBClient
    /// Returns None if this client is attached to a non global database
    /// </summary>
    static member getGlobalDatabase (client: AmazonDynamoDBClient) =
        let interceptor = getRequiredInterceptor client
        interceptor.GlobalDatabase
        
    /// <summary>
    /// Get the underlying global database from an AmazonDynamoDBClient
    /// Returns null if this client is attached to a non global database
    /// </summary>
    [<Extension>]
    static member GetGlobalDatabase(client: AmazonDynamoDBClient) =
        
        TestDynamoClient.getGlobalDatabase client
        ?|> (fun x -> new Api.GlobalDatabase(x) |> box)
        |> CSharp.fromOption
        :?> Api.GlobalDatabase
        
    /// <summary>
    /// Get a table from the underlying database
    /// </summary>
    static member getTable tableName (client: AmazonDynamoDBClient) =
        let db = TestDynamoClient.getDatabase client
        db.GetTable ValueNone tableName
        
    /// <summary>
    /// Get a table from the underlying database
    /// </summary>
    [<Extension>]
    static member GetTable(client: AmazonDynamoDBClient, tableName) =
        TestDynamoClient.getTable tableName client
        
    /// <summary>
    /// Wait for all subscribers to complete from the underlying Database or GlobalDatabase
    /// </summary>
    static member awaitAllSubscribers logger cancellationToken (client: AmazonDynamoDBClient) =
        TestDynamoClient.getGlobalDatabase client
        ?|> fun db -> db.AwaitAllSubscribers logger cancellationToken
        ?|>? fun db -> (TestDynamoClient.getDatabase client).AwaitAllSubscribers logger cancellationToken
        
    /// <summary>
    /// Wait for all subscribers to complete from the underlying Database or GlobalDatabase
    /// </summary>
    [<Extension>]
    static member AwaitAllSubscribers (
        client: AmazonDynamoDBClient,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger,
        [<Optional; DefaultParameterValue(CancellationToken())>] cancellationToken: CancellationToken) =
        
        TestDynamoClient.awaitAllSubscribers (CSharp.toOption logger) cancellationToken client
        
        