namespace TestDynamo

open System
open System.Linq.Expressions
open System.Reflection
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open Amazon
open Amazon.Runtime
open Microsoft.Extensions.Logging
open TestDynamo.Api.FSharp
open TestDynamo.Client
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils

#nowarn "3390"

type ApiDb = TestDynamo.Api.FSharp.Database
type GlobalApiDb = TestDynamo.Api.FSharp.GlobalDatabase
type CsApiDb = TestDynamo.Api.Database
type GlobalCsApiDb = TestDynamo.Api.GlobalDatabase

type private AmazonDynamoDBClientBuilder<'a>() =

    static let builder' withCredentials =
        let cs = ImmutableCredentials("""AAAAA7AAAAAAAAAAAAA""", Convert.ToBase64String (Array.create 30 0uy), null)

        let dummyCredentials =
            { new Amazon.Runtime.AWSCredentials() with
                member _.GetCredentials () = cs } |> Expression.Constant

        let constructor =
            [
                typeof<'a>.GetConstructors()
                typeof<'a>.GetConstructors(BindingFlags.NonPublic ||| BindingFlags.Instance)
            ]
            |> Seq.concat
            |> Seq.filter (fun c ->
                let param = c.GetParameters()
                let isRegion = param |> Array.filter (_.ParameterType >> ((=)typeof<RegionEndpoint>))
                let isCredentials = param |> Array.filter (_.ParameterType >> ((=)typeof<AWSCredentials>))
                let mandatory =
                    param
                    |> Seq.filter (_.IsOptional >> not)
                    |> Seq.filter (flip Array.contains isRegion >> not)
                    |> Seq.filter (flip Array.contains isCredentials >> not)
                    |> Seq.length

                let credLength = if withCredentials then 1 else 0
                isRegion.Length = 1 && isCredentials.Length = credLength && mandatory = 0)
            |> Collection.tryHead
            ?|>? fun _ ->
                [
                    $"Type {typeof<'a>} must have a constructor which accepts only an AWSCredentials argument and a RegionEndpoint argument"
                    "To omit the AWSCredentials argument, use the addAwsCredentials flag when creating a client"
                    "Use the TestDynamoClient.Attach method to attach to a custom built client"
                ] |> Str.join ". " |> notSupported

        let regionParam = System.Linq.Expressions.Expression.Parameter(typeof<RegionEndpoint>)
        let args =
            constructor.GetParameters()
            |> Seq.map (function
                | x when x.ParameterType = typeof<RegionEndpoint> -> regionParam :> Expression
                | x when x.ParameterType = typeof<AWSCredentials> -> dummyCredentials
                | x -> Expression.Constant(x.DefaultValue, x.ParameterType))

        Expression.Lambda<Func<RegionEndpoint, 'a>>(
            Expression.New(constructor, args), regionParam).Compile().Invoke

    static let builderWithoutCredentials = lazy(builder' false)
    static let builderWithCredentials = lazy(builder' true)
    
    static let regionName' =
        let client = Expression.Parameter typeof<'a>
        
        let systemName =
            Expression.PropertyOrField(
                Expression.PropertyOrField(
                    Expression.PropertyOrField(
                        client,
                        "Config"),
                    "RegionEndpoint"),
                "SystemName")
            
        Expression.Lambda<Func<'a, string>>(systemName, client).Compile().Invoke

    static member builder withCredentials =
        if withCredentials then builderWithCredentials.Value
        else builderWithoutCredentials.Value
        
    static member regionName = regionName'

    static member getRuntimePipeline: 'a -> Amazon.Runtime.Internal.RuntimePipeline =
        let param = System.Linq.Expressions.Expression.Parameter(typeof<'a>)
        let p = System.Linq.Expressions.Expression.PropertyOrField(param, "RuntimePipeline")

        System.Linq.Expressions.Expression
            .Lambda<System.Func<'a, Amazon.Runtime.Internal.RuntimePipeline>>(p, param)
            .Compile()
            .Invoke

    static member getOptionalInterceptor client =
        let inline cast (x: IPipelineHandler) = x :?> ObjPipelineInterceptor

        AmazonDynamoDBClientBuilder<'a>.getRuntimePipeline client
        |> _.Handlers
        |> Seq.filter (fun h -> h.GetType() = typeof<ObjPipelineInterceptor>)
        |> Collection.tryHead
        ?|> cast

    static member getRequiredInterceptor client =
        AmazonDynamoDBClientBuilder<'a>.getOptionalInterceptor client
        ?|>? fun _ -> invalidOp "Client does not have a database attached"

/// <summary>
/// Extensions to create a dynamodb db client from a Database or to attach a Database to
/// an existing dynamodb client.
/// Methods targeting C# have upper case names, methods targeting F# have lower case names
/// </summary>
[<Extension>]
type TestDynamoClient =

    static let defaultLogger: Either<Database,struct (GlobalDatabase * _)> -> _ =
        Either.map1Of2 _.DefaultLogger
        >> Either.map2Of2 (fstT >> _.DefaultLogger)
        >> Either.reduce

    static let validateRegion clientRegion (db: Api.FSharp.Database) =
        let clRegion = clientRegion
        if clRegion <> db.Id.regionId
        then invalidOp $"Cannot attach client from region {clRegion} to database from region {db.Id.regionId}. The regions must match"
        else db

    static member private attach'<'a> (logger: ILogger voption) struct (db, disposeDb) interceptor (client: 'a) =

        let regionId = AmazonDynamoDBClientBuilder.regionName client
        let db =
            db
            |> Either.map1Of2 (validateRegion regionId) 
            |> Either.map2Of2 (flip tpl ({ regionId = regionId }: Model.DatabaseId))

        let id =
            db
            |> Either.map1Of2 (fun (x: ApiDb) -> x.Id)
            |> Either.map2Of2 sndT
            |> Either.reduce

        let attachedAlready =
            // TODO: there are now 2 interceptors to test against
            AmazonDynamoDBClientBuilder<'a>.getOptionalInterceptor client
            ?|> (fun db' ->
                match db with
                | Either1 db when db'.Database = db -> true
                | Either1 db when db'.GlobalDatabase ?|> (fun x -> x.GetDatabase ValueNone id) = ValueSome db -> true
                | Either2 (db, _) when db'.GlobalDatabase = ValueSome db -> true
                | _ -> invalidOp "Client already has a TestDynamo database attached")
            ?|? false

        if not attachedAlready
        then
            let runtimePipeline = AmazonDynamoDBClientBuilder<'a>.getRuntimePipeline client
            runtimePipeline.AddHandler(
                new ObjPipelineInterceptor(
                    db,
                    Settings.DefaultClientResponseDelay,
                    logger ?|> ValueSome ?|? defaultLogger db,
                    disposeDb))

            match interceptor with
            | ValueNone -> ()
            | ValueSome i ->
                let db =  db |> Either.map2Of2 (fun struct (db, id) -> db.GetDatabase ValueNone id) |> Either.reduce
                runtimePipeline.AddHandler(
                    CustomPipelineInterceptor(db, i))

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given Database
    /// </summary>
    /// <param name="addAwsCredentials">
    /// <para>
    /// If true, will inject some dummy AWS credentials into the client. This will prevent the underlying
    /// AmazonServiceClient from automatically reading credentials configured in the user profile on the
    /// local machine, and attempting to assume a role
    /// </para>
    /// <para>
    /// If false, the typeof 'a does not need to have a constructor which accepts AwsCredentials
    /// </para>
    /// </param>
    [<Extension>]
    static member CreateClient<'a>(
        database: CsApiDb,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger,
        [<Optional; DefaultParameterValue(true)>] addAwsCredentials: bool) =

        let client = AmazonDynamoDBClientBuilder<'a>.builder addAwsCredentials (RegionEndpoint.GetBySystemName(database.Id.regionId))
        TestDynamoClient.Attach(database, client, interceptor, logger)
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase
    /// </summary>
    /// <param name="addAwsCredentials">
    /// <para>
    /// If true, will inject some dummy AWS credentials into the client. This will prevent the underlying
    /// AmazonServiceClient from automatically reading credentials configured in the user profile on the
    /// local machine, and attempting to assume a role
    /// </para>
    /// <para>
    /// If false, the typeof 'a does not need to have a constructor which accepts AwsCredentials
    /// </para>
    /// </param>
    [<Extension>]
    static member CreateClient<'a>(
        database: GlobalCsApiDb,
        databaseId: TestDynamo.Model.DatabaseId,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger,
        [<Optional; DefaultParameterValue(true)>] addAwsCredentials: bool) =

        let client = AmazonDynamoDBClientBuilder<'a>.builder addAwsCredentials (RegionEndpoint.GetBySystemName(databaseId.regionId))
        TestDynamoClient.Attach(database, client, interceptor, logger)
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase
    /// </summary>
    /// <param name="addAwsCredentials">
    /// <para>
    /// If true, will inject some dummy AWS credentials into the client. This will prevent the underlying
    /// AmazonServiceClient from automatically reading credentials configured in the user profile on the
    /// local machine, and attempting to assume a role
    /// </para>
    /// <para>
    /// If false, the typeof 'a does not need to have a constructor which accepts AwsCredentials
    /// </para>
    /// </param>
    [<Extension>]
    static member CreateClient<'a>(
        database: GlobalCsApiDb,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger,
        [<Optional; DefaultParameterValue(true)>] addAwsCredentials: bool) =
        TestDynamoClient.CreateClient<'a>(database, {regionId = Settings.DefaultRegion}, interceptor, logger, addAwsCredentials)

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new Database
    /// </summary>
    /// <param name="addAwsCredentials">
    /// <para>
    /// If true, will inject some dummy AWS credentials into the client. This will prevent the underlying
    /// AmazonServiceClient from automatically reading credentials configured in the user profile on the
    /// local machine, and attempting to assume a role
    /// </para>
    /// <para>
    /// If false, the typeof&lt;'a> does not need to have a constructor which accepts AwsCredentials
    /// </para>
    /// </param>
    static member CreateClient<'a>(
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger,
        [<Optional; DefaultParameterValue(true)>] addAwsCredentials: bool) =

        let database = new ApiDb()
        let client = AmazonDynamoDBClientBuilder<'a>.builder addAwsCredentials (RegionEndpoint.GetBySystemName(database.Id.regionId))
        TestDynamoClient.attach' (Maybe.Null.toOption logger) struct (Either1 database, true) (interceptor |> Maybe.Null.toOption) client
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new GlobalDatabase
    /// </summary>
    static member CreateGlobalClient<'a>(
        databaseId: TestDynamo.Model.DatabaseId,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger,
        [<Optional; DefaultParameterValue(true)>] addAwsCredentials: bool) =

        let database = new GlobalApiDb()
        let client = AmazonDynamoDBClientBuilder<'a>.builder addAwsCredentials (RegionEndpoint.GetBySystemName(databaseId.regionId))
        TestDynamoClient.attachGlobal' (logger |> Maybe.Null.toOption) struct (database, true) (interceptor |> Maybe.Null.toOption) client
        client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on a new GlobalDatabase
    /// </summary>
    /// <param name="addAwsCredentials">
    /// <para>
    /// If true, will inject some dummy AWS credentials into the client. This will prevent the underlying
    /// AmazonServiceClient from automatically reading credentials configured in the user profile on the
    /// local machine, and attempting to assume a role
    /// </para>
    /// <para>
    /// If false, the typeof 'a does not need to have a constructor which accepts AwsCredentials
    /// </para>
    /// </param>
    static member CreateGlobalClient<'a>(
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger,
        [<Optional; DefaultParameterValue(true)>] addAwsCredentials: bool): 'a =
        TestDynamoClient.CreateGlobalClient<'a>({regionId = Settings.DefaultRegion}, interceptor, logger, addAwsCredentials)

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given Database or a new Database
    /// </summary>
    /// <param name="addAwsCredentials">
    /// <para>
    /// If true, will inject some dummy AWS credentials into the client. This will prevent the underlying
    /// AmazonServiceClient from automatically reading credentials configured in the user profile on the
    /// local machine, and attempting to assume a role
    /// </para>
    /// <para>
    /// If false, the typeof 'a does not need to have a constructor which accepts AwsCredentials
    /// </para>
    /// </param>
    static member createClient<'a> logger addAwsCredentials (interceptor: IRequestInterceptor voption) (database: ApiDb voption) =

        match database with
        | ValueSome database ->
            let client = AmazonDynamoDBClientBuilder<'a>.builder addAwsCredentials (RegionEndpoint.GetBySystemName(database.Id.regionId))
            TestDynamoClient.attach logger database interceptor client
            client
        | ValueNone ->
            let database = new ApiDb()
            let client = AmazonDynamoDBClientBuilder<'a>.builder addAwsCredentials (RegionEndpoint.GetBySystemName(database.Id.regionId))
            TestDynamoClient.attach' logger (Either1 database, true) interceptor client
            client

    /// <summary>
    /// Create an AmazonDynamoDBClient which can execute operations on the given GlobalDatabase or a new GlobalDatabase
    /// </summary>
    /// <param name="addAwsCredentials">
    /// <para>
    /// If true, will inject some dummy AWS credentials into the client. This will prevent the underlying
    /// AmazonServiceClient from automatically reading credentials configured in the user profile on the
    /// local machine, and attempting to assume a role
    /// </para>
    /// <para>
    /// If false, the typeof 'a does not need to have a constructor which accepts AwsCredentials
    /// </para>
    /// </param>
    static member createGlobalClient<'a> logger addAwsCredentials (dbId: TestDynamo.Model.DatabaseId voption) (interceptor: IRequestInterceptor voption) (database: GlobalApiDb voption) =

        let regionId = dbId ?|> _.regionId ?|? Settings.DefaultRegion |> RegionEndpoint.GetBySystemName
        match database with
        | ValueSome database ->
            let client = AmazonDynamoDBClientBuilder<'a>.builder addAwsCredentials regionId
            TestDynamoClient.attachGlobal logger database interceptor client
            client
        | ValueNone ->
            let database = new GlobalApiDb()
            let client = AmazonDynamoDBClientBuilder<'a>.builder addAwsCredentials regionId
            TestDynamoClient.attachGlobal' logger struct (database, true) interceptor client
            client

    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given Database  
    /// </summary>
    static member attach<'a> (logger: ILogger voption) (db: Database) (interceptor: IRequestInterceptor voption) (client: 'a): unit =
        TestDynamoClient.attach' logger (Either1 db, false) interceptor client

    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given Database
    /// </summary>
    [<Extension>]
    static member Attach (
        db: TestDynamo.Api.Database,
        client: 'a,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        TestDynamoClient.attach (Maybe.Null.toOption logger) db.CoreDb (Maybe.Null.toOption interceptor) client

    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given GlobalDatabase
    /// </summary>
    static member attachGlobal<'a> (logger: ILogger voption) (db: GlobalDatabase) (interceptor: IRequestInterceptor voption) (client: 'a): unit =
        struct (Either2 db, false) |> flip1To3 (TestDynamoClient.attach'<'a> logger) interceptor client

    static member private attachGlobal'<'a> (logger: ILogger voption) (db: struct (GlobalDatabase * bool)) (interceptor: IRequestInterceptor voption) (client: 'a): unit =
        db |> mapFst Either2 |> flip1To3 (TestDynamoClient.attach'<'a> logger) interceptor client

    /// <summary>
    /// Alter an AmazonDynamoDBClient so that it executes on a given GlobalDatabase
    /// </summary>
    [<Extension>]
    static member Attach<'a> (
        db: TestDynamo.Api.GlobalDatabase,
        client: 'a,
        [<Optional; DefaultParameterValue(null: IRequestInterceptor)>] interceptor: IRequestInterceptor,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger): unit =

        TestDynamoClient.attachGlobal<'a> (Maybe.Null.toOption logger) db.CoreDb (Maybe.Null.toOption interceptor) client

    /// <summary>
    /// Set an artificial delay on all requests.  
    /// </summary>
    static member setProcessingDelay<'a> delay client =
        let interceptor = AmazonDynamoDBClientBuilder<'a>.getRequiredInterceptor client
        interceptor.ProcessingDelay <- delay

    /// <summary>
    /// Set an artificial delay on all requests.  
    /// </summary>
    static member SetProcessingDelay<'a>(client, delay) = TestDynamoClient.setProcessingDelay<'a> delay client

    /// <summary>
    /// Set limits on how much data can be scanned in a single page
    /// </summary>
    static member setScanLimits<'a> scanLimits client =
        let interceptor = AmazonDynamoDBClientBuilder<'a>.getRequiredInterceptor client
        interceptor.SetScanLimits scanLimits

    /// <summary>
    /// Set limits on how much data can be scanned in a single page
    /// </summary>
    static member SetScanLimits<'a>(client, scanLimits) = TestDynamoClient.setScanLimits<'a> scanLimits client

    /// <summary>
    /// Set the aws account id for an AmazonDynamoDBClient
    /// </summary>
    static member setAwsAccountId<'a> awsAccountId client =
        let interceptor = AmazonDynamoDBClientBuilder<'a>.getRequiredInterceptor client
        interceptor.AwsAccountId <- awsAccountId

    /// <summary>
    /// Set limits on how much data can be scanned in a single page
    /// </summary>
    static member SetAwsAccountId<'a>(client, awsAccountId) = TestDynamoClient.setAwsAccountId<'a> awsAccountId client

    /// <summary>
    /// Set the aws account id for an AmazonDynamoDBClient
    /// </summary>
    static member getAwsAccountId<'a> client =
        let interceptor = AmazonDynamoDBClientBuilder<'a>.getRequiredInterceptor client
        interceptor.AwsAccountId

    /// <summary>
    /// Set limits on how much data can be scanned in a single page
    /// </summary>
    static member GetAwsAccountId<'a>(client) = TestDynamoClient.getAwsAccountId<'a> client

    /// <summary>
    /// Get the underlying database from an AmazonDynamoDBClient
    /// </summary>
    static member getDatabase<'a> (client: 'a) =
        let interceptor = AmazonDynamoDBClientBuilder<'a>.getRequiredInterceptor client
        interceptor.Database

    /// <summary>
    /// Get the underlying database from an AmazonDynamoDBClient
    /// </summary>
    static member GetDatabase<'a>(client: 'a) =
        let db = TestDynamoClient.getDatabase<'a> client
        new Api.Database(db)

    /// <summary>
    /// Get the underlying global database from an AmazonDynamoDBClient
    /// Returns None if this client is attached to a non global database
    /// </summary>
    static member getGlobalDatabase<'a>(client: 'a) =
        let interceptor = AmazonDynamoDBClientBuilder<'a>.getRequiredInterceptor client
        interceptor.GlobalDatabase

    /// <summary>
    /// Get the underlying global database from an AmazonDynamoDBClient
    /// Returns null if this client is attached to a non global database
    /// </summary>
    static member GetGlobalDatabase<'a>(client: 'a) =

        TestDynamoClient.getGlobalDatabase<'a> client
        ?|> (fun x -> new Api.GlobalDatabase(x) |> box)
        |> Maybe.Null.fromOption
        :?> Api.GlobalDatabase

    /// <summary>
    /// Get a table from the underlying database
    /// </summary>
    static member getTable<'a> tableName (client: 'a) =
        let db = TestDynamoClient.getDatabase<'a> client
        db.GetTable ValueNone tableName

    /// <summary>
    /// Get a table from the underlying database
    /// </summary>
    static member GetTable<'a>(client: 'a, tableName) =
        TestDynamoClient.getTable tableName client

    /// <summary>
    /// Wait for all subscribers to complete from the underlying Database or GlobalDatabase
    /// </summary>
    static member awaitAllSubscribers<'a> logger cancellationToken (client: 'a) =
        TestDynamoClient.getGlobalDatabase<'a> client
        ?|> fun db -> db.AwaitAllSubscribers logger cancellationToken
        ?|>? fun db -> (TestDynamoClient.getDatabase client).AwaitAllSubscribers logger cancellationToken

    /// <summary>
    /// Wait for all subscribers to complete from the underlying Database or GlobalDatabase
    /// </summary>
    static member AwaitAllSubscribers<'a>(
        client: 'a,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger,
        [<Optional; DefaultParameterValue(CancellationToken())>] cancellationToken: CancellationToken) =

        TestDynamoClient.awaitAllSubscribers (Maybe.Null.toOption logger) cancellationToken client
