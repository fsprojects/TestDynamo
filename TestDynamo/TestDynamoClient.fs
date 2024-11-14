namespace TestDynamo

open System
open System.Linq.Expressions
open System.Reflection
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

type private AmazonDynamoDBClientBuilder<'a when 'a :> AmazonDynamoDBClient>() =
    static member private builder' =
        
        let constructor =
            [
                typeof<'a>.GetConstructors()
                typeof<'a>.GetConstructors(BindingFlags.NonPublic ||| BindingFlags.Instance)
            ]
            |> Seq.concat
            |> Seq.filter (fun c ->
                let param = c.GetParameters()
                let isRegion = param |> Array.filter (_.ParameterType >> ((=)typeof<RegionEndpoint>))
                let mandatory = param |> Array.filter (_.IsOptional >> not)
                
                isRegion.Length = 1 && (mandatory.Length = 0 || mandatory.Length = 1 && mandatory[0] = isRegion[0]))
            |> Collection.tryHead
            ?|>? fun _ -> notSupported $"Type {typeof<'a>} must have a constructor which accepts a single RegionEndpoint argument"
        
        let param = System.Linq.Expressions.Expression.Parameter(typeof<RegionEndpoint>)
        let args =
            constructor.GetParameters()
            |> Seq.map (function
                | x when x.ParameterType = typeof<RegionEndpoint> -> param :> Expression
                | x -> Expression.Constant(x.DefaultValue))
        
        Expression.Lambda<Func<RegionEndpoint, 'a>>(
            Expression.New(constructor, args), param).Compile()
        
    static member builder = AmazonDynamoDBClientBuilder<'a>.builder'.Invoke

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

    static let validateRegion clientRegion (db: Api.FSharp.Database) =
        let clRegion = clientRegion
        if clRegion <> db.Id.regionId
        then invalidOp $"Cannot attach client from region {clRegion} to database from region {db.Id.regionId}. The regions must match"
        else db

    static let attach' (logger: ILogger voption) struct (db, disposeDb) interceptor (client: AmazonDynamoDBClient) =
    
        let db =
            db
            |> Either.map1Of2 (validateRegion client.Config.RegionEndpoint.SystemName) 
            |> Either.map2Of2 (flip tpl ({ regionId = client.Config.RegionEndpoint.SystemName }: Model.DatabaseId))

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
            runtimePipeline.AddHandler(
                new ObjPipelineInterceptor(
                    db,
                    Settings.DefaultClientResponseDelay,
                    interceptor,
                    logger ?|> ValueSome ?|? defaultLogger db,
                    disposeDb))

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given Database
    /// </summary>
    [<Extension>]
    static member CreateClient<'a when 'a :> AmazonDynamoDBClient>(
        database: CsApiDb,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let client = AmazonDynamoDBClientBuilder<'a>.builder (RegionEndpoint.GetBySystemName(database.Id.regionId))
        TestDynamoClient.Attach(client, database, interceptor, logger)
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase
    /// </summary>
    [<Extension>]
    static member CreateClient<'a when 'a :> AmazonDynamoDBClient>(
        database: GlobalCsApiDb,
        databaseId: TestDynamo.Model.DatabaseId,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let client = AmazonDynamoDBClientBuilder<'a>.builder (RegionEndpoint.GetBySystemName(databaseId.regionId))
        TestDynamoClient.Attach(client, database, interceptor, logger)
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase
    /// </summary>
    [<Extension>]
    static member CreateClient<'a when 'a :> AmazonDynamoDBClient>(
        database: GlobalCsApiDb,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        TestDynamoClient.CreateClient<'a>(database, {regionId = Settings.DefaultRegion}, interceptor, logger)

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new Database
    /// </summary>
    static member CreateClient<'a when 'a :> AmazonDynamoDBClient>(
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let database = new ApiDb()
        let client = AmazonDynamoDBClientBuilder<'a>.builder (RegionEndpoint.GetBySystemName(database.Id.regionId))
        attach' (CSharp.toOption logger) struct (Either1 database, true) (interceptor |> CSharp.toOption) client
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new GlobalDatabase
    /// </summary>
    static member CreateGlobalClient<'a when 'a :> AmazonDynamoDBClient>(
        databaseId: TestDynamo.Model.DatabaseId,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let database = new GlobalApiDb()
        let client = AmazonDynamoDBClientBuilder<'a>.builder (RegionEndpoint.GetBySystemName(databaseId.regionId))
        TestDynamoClient.attachGlobal' (logger |> CSharp.toOption) struct (database, true) (interceptor |> CSharp.toOption) client
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new GlobalDatabase
    /// </summary>
    static member CreateGlobalClient<'a when 'a :> AmazonDynamoDBClient>(
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        TestDynamoClient.CreateGlobalClient<'a>({regionId = Settings.DefaultRegion}, interceptor, logger)

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given Database or a new Database
    /// </summary>
    static member createClient<'a when 'a :> AmazonDynamoDBClient> logger (database: ApiDb voption) (interceptor: IRequestInterceptor voption) =

        match database with
        | ValueSome database ->
            let client = AmazonDynamoDBClientBuilder<'a>.builder (RegionEndpoint.GetBySystemName(database.Id.regionId))
            TestDynamoClient.attach logger database interceptor client
            client
        | ValueNone ->
            let database = new ApiDb()
            let client = AmazonDynamoDBClientBuilder<'a>.builder (RegionEndpoint.GetBySystemName(database.Id.regionId))
            attach' logger (Either1 database, true) interceptor client
            TestDynamoClient.attach logger database interceptor client
            client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase or a new GlobalDatabase
    /// </summary>
    static member createGlobalClient<'a when 'a :> AmazonDynamoDBClient> logger (dbId: TestDynamo.Model.DatabaseId voption) (interceptor: IRequestInterceptor voption) (database: GlobalApiDb voption) =

        let regionId = dbId ?|> _.regionId ?|? Settings.DefaultRegion |> RegionEndpoint.GetBySystemName
        match database with
        | ValueSome database ->
            let client = AmazonDynamoDBClientBuilder<'a>.builder regionId
            TestDynamoClient.attachGlobal logger database interceptor client
            client
        | ValueNone ->
            let database = new GlobalApiDb()
            let client = AmazonDynamoDBClientBuilder<'a>.builder regionId
            TestDynamoClient.attachGlobal' logger struct (database, true) interceptor client
            client

    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given Database  
    /// </summary>
    static member attach (logger: ILogger voption) db (interceptor: IRequestInterceptor voption) (client: AmazonDynamoDBClient) =
        attach' logger (Either1 db, false) interceptor client

    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given Database
    /// </summary>
    [<Extension>]
    static member Attach (
        client: AmazonDynamoDBClient,
        db: TestDynamo.Api.Database,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        TestDynamoClient.attach (CSharp.toOption logger) db.CoreDb (CSharp.toOption interceptor) client

    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given GlobalDatabase
    /// </summary>
    static member attachGlobal (logger: ILogger voption) db (interceptor: IRequestInterceptor voption) (client: AmazonDynamoDBClient) =
        struct (Either2 db, false) |> flip1To3 (attach' logger) interceptor client
        
    static member private attachGlobal' (logger: ILogger voption) db (interceptor: IRequestInterceptor voption) (client: AmazonDynamoDBClient) =
        db |> mapFst Either2 |> flip1To3 (attach' logger) interceptor client

    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given GlobalDatabase
    /// </summary>
    [<Extension>]
    static member Attach (
        client: AmazonDynamoDBClient,
        db: TestDynamo.Api.GlobalDatabase,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        TestDynamoClient.attachGlobal (CSharp.toOption logger) db.CoreDb (CSharp.toOption interceptor) client

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
