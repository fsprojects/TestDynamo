namespace TestDynamo.Serialization.CloudFormation

open System
open System.Runtime.InteropServices
open System.Text.Json
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open TestDynamo
open TestDynamo.Client
open TestDynamo.Data.Monads.Operators
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Api.FSharp
open TestDynamo.Serialization

type CloudFormationSettings =
    { ignoreUnsupportedResources: bool }
    with static member defaultSettings = { ignoreUnsupportedResources = true; }

type CloudFormationDbSettings =
    { /// <summary>
      /// If true, the builder will always return a global DB. If false, the builder may still
      /// return a global DB if CFN files target multiple regions
      /// </summary>
      alwaysCreateGlobal: bool
      settings: CloudFormationSettings }

type CloudFormationFile =
    { fileJson: string
      region: string }

type CloudFormationConstruct() =

    let mutable typ: string = null
    let mutable dependsOn: string = null
    let mutable properties: JsonObject = null

    member _.Type
        with get () = typ
        and set value = typ <- value

    member _.Properties
        with get () = properties
        and set value = properties <- value

    member _.DependsOn
        with get () = dependsOn
        and set value = dependsOn <- value

type PartialCloudFormationFile() =

    let mutable resources: Dictionary<string, CloudFormationConstruct> = null

    member _.Resources
        with get () = resources
        and set value = resources <- value

type ReplicaDetails() =

    let mutable globalSecondaryIndexes: MList<ReplicaGlobalSecondaryIndex> = null
    let mutable deletionProtectionEnabled: bool = false
    let mutable region: string = null

    member _.GlobalSecondaryIndexes
        with get () = globalSecondaryIndexes
        and set value = globalSecondaryIndexes <- value

    member _.DeletionProtectionEnabled
        with get () = deletionProtectionEnabled
        and set value = deletionProtectionEnabled <- value

    member _.Region
        with get () = region
        and set value = region <- value

type CloudFormationParser() =
    
    static member private noTask = ValueTask<_>(()).Preserve()

    static member private handle<'req, 'resp> (f: 'req -> ValueTask<'resp>) (json: JsonObject) =
        RawSerializers.Nodes.read<'req>(json)
        |> f |> Io.ignore<'resp>

    static member private handleTable' forceEnableStreams logger state =
        let logger' = logger ?|? Logger.notAnILogger
        logger'.LogInformation("Creating table")
        CloudFormationParser.handle<CreateTableRequest, CreateTableResponse>(fun c ->
            let c =
                match struct (forceEnableStreams, c.StreamSpecification) with
                | false, _ -> c
                | true, ValueNone ->
                    { c with StreamSpecification = ValueSome { StreamEnabled = ValueSome true; StreamViewType = ValueSome StreamViewType.NEW_AND_OLD_IMAGES } }
                | true, ValueSome x -> { c with StreamSpecification = ValueSome { x with StreamEnabled = ValueSome true } }

            DbCommandExecutor.execute state ValueNone CancellationToken.None c
            |%|> fun x -> x :?> CreateTableResponse)

    static member private handleTable = CloudFormationParser.handleTable' false

    static member private addReplicasAndGetDeletionProtection (logger: ILogger) state (json: JsonObject) =
        let replicaDetails =
            match json.TryGetPropertyValue("Replicas") with
            | false, _ -> Seq.empty
            | true, json when json.GetValueKind() = JsonValueKind.Null -> Seq.empty
            | true, json when json.GetValueKind() <> JsonValueKind.Array -> invalidOp "Invalid Replicas property. Expected an array"
            | true, json -> json.AsArray()
            |> Seq.map RawSerializers.Nodes.read<ReplicaDetails>
            |> Array.ofSeq

        let updates =
            replicaDetails
            |> Seq.map (fun x ->
                { Delete = ValueNone
                  Update = ValueNone
                  Create =
                      ({ RegionName = ValueSome x.Region
                         GlobalSecondaryIndexes = x.GlobalSecondaryIndexes |> Maybe.Null.toOption ?|> Array.ofSeq
                         KMSMasterKeyId = ValueNone
                         OnDemandThroughputOverride = ValueNone
                         ProvisionedThroughputOverride = ValueNone
                         TableClassOverride = ValueNone }: CreateReplicationGroupMemberAction) |> ValueSome }: ReplicationGroupUpdate)

        let deletionProtection =
            replicaDetails
            |> Seq.map (fun x -> struct (x.DeletionProtectionEnabled, ({ regionId = x.Region }: DatabaseId)))

        let updateReq =
            { RawSerializers.Nodes.read<UpdateTableRequest> json with 
                    DeletionProtectionEnabled = ValueSome true
                    ReplicaUpdates = updates |> Array.ofSeq |> ValueSome }

        logger.LogInformation("Adding replicas to regions {0}", updates |> Seq.map (_.Create ??|> _.RegionName) |> List.ofSeq)
        DbCommandExecutor.execute state ValueNone CancellationToken.None updateReq
        |%|> fun _ -> struct (deletionProtection, updateReq.TableName |> Maybe.expectSome)

    static member private scopeState logger (state: DbInterceptorUtils.DbInterceptorState) id =
        match state.parent with
        | ValueNone -> invalidOp "Unexpected error occurred"
        | ValueSome struct (parent, _) ->
            { state with parent = ValueSome struct (parent, id); db = parent.GetDatabase (ValueSome logger) id }
    
    static member private addDeletionProtection (logger: ILogger) state struct (deletionProtection, tableName) =

        deletionProtection
        |> Seq.map (fun struct (deletionProtection, region) ->
            task {
                logger.LogInformation("Adding deletion protection = {0} to region {1}", deletionProtection, region.regionId)
                
                let req =
                    { AttributeDefinitions = ValueNone
                      BillingMode = ValueNone
                      DeletionProtectionEnabled = ValueSome deletionProtection
                      GlobalSecondaryIndexUpdates = ValueNone
                      OnDemandThroughput = ValueNone
                      ProvisionedThroughput = ValueNone
                      ReplicaUpdates = ValueNone
                      SSESpecification = ValueNone
                      StreamSpecification = ValueNone
                      TableClass = ValueNone
                      TableName = ValueSome tableName }: UpdateTableRequest

                let localState = CloudFormationParser.scopeState logger state region
                do! DbCommandExecutor.execute localState ValueNone CancellationToken.None req |> Io.ignore<obj>
            } |> Io.fromTask)
        |> Io.traverse
        |> Io.ignore<unit list>

    static member private handleGlobalTable logger (state: DbInterceptorUtils.DbInterceptorState) json =
        let logger' = logger ?|? Logger.notAnILogger

        logger'.LogInformation("Creating global table")
        logger'.LogInformation("Adding root to region {0}", state.db.Id.regionId)
        
        CloudFormationParser.handleTable' true logger state json
        |%>>= fun _ -> CloudFormationParser.addReplicasAndGetDeletionProtection logger' state json
        |%>>= CloudFormationParser.addDeletionProtection logger' state

    static member private handlers =
        Map.empty
        |> Map.add "AWS::DynamoDB::Table" CloudFormationParser.handleTable
        |> Map.add "AWS::DynamoDB::GlobalTable" CloudFormationParser.handleGlobalTable

    static member private applyCfn settings logger db (json: CloudFormationConstruct) =
        if json.Type = null then invalidOp $"Found invalid cloudformation file. All constructs in a cfn file must have a \"Type\""
        if json.Properties = null then invalidOp $"Found invalid cloudformation file. All constructs in a cfn file must have \"Properties\""

        CloudFormationParser.handlers
        |> MapUtils.tryFind json.Type
        ?|> ValueSome
        ?|>? (fun _ ->
            if not settings.settings.ignoreUnsupportedResources
            then invalidOp $"Unsupported construct {json.Type}. You can ignore these constructs with the {nameof Unchecked.defaultof<CloudFormationSettings>.ignoreUnsupportedResources} setting"
            else ValueNone)
        ?|> (apply logger >> apply db >> apply json.Properties)
        ?|? CloudFormationParser.noTask

    static member orderJsonObjectDepends (j: Dictionary<string, CloudFormationConstruct>) =
        let struct (propsWithoutDeps, propsWithDeps) =
            j |> Collection.partition (fun x -> System.String.IsNullOrWhiteSpace(x.Value.DependsOn))

        let rec reOrder = function
            | struct (complete, []) -> complete
            | struct (complete, (head: KeyValuePair<string, CloudFormationConstruct>)::tail) when List.contains head.Value.DependsOn complete ->
                reOrder struct (head.Key::complete, tail)
            | struct (complete, head::tail) -> 
                let newComplete = reOrder struct (complete, tail)
                if List.length newComplete <= List.length complete
                then invalidOp $"Circular reference or missing dependency on for DependsOn: {head.Value.DependsOn}"

                reOrder struct (newComplete, [head])

        reOrder struct (propsWithoutDeps |> List.map _.Key, propsWithDeps)
        |> Seq.rev
        |> Seq.map (fun k -> j[k])

    static member private createDatabase settings (logger: ILogger voption) (constructs: struct (DatabaseId * CloudFormationConstruct list) list) =
        let hasGlobalTable =
            Seq.collect sndT constructs
            |> Seq.filter (_.Type >> ((=) "AWS::DynamoDB::GlobalTable"))
            |> Collection.tryHead
            |> ValueOption.isSome

        if not settings.alwaysCreateGlobal && not hasGlobalTable && List.length constructs = 1
        then
            let id = fstT constructs[0]
            logger ?|> (fun l -> new Database(id, l)) ?|? new Database(id)
            |> Either1
        else
            logger ?|> (fun l -> new GlobalDatabase(l)) ?|? new GlobalDatabase()
            |> Either2

    static member buildDatabase settings logger (cfnJsonStacks: CloudFormationFile seq) =

        let constructs =
            cfnJsonStacks
            |> Seq.map (
                tplDouble
                >> mapFst (_.region)
                >> mapSnd (_.fileJson >> RawSerializers.Strings.read<PartialCloudFormationFile>))
            |> Seq.collect (fun struct (region, json) ->
                CloudFormationParser.orderJsonObjectDepends json.Resources
                |> Seq.map (tpl ({regionId = region}: DatabaseId)))
            |> Collection.groupBy fstT
            |> Collection.mapSnd (Seq.map sndT >> List.ofSeq)
            |> List.ofSeq

        let database = CloudFormationParser.createDatabase settings logger constructs
        
        let applyToClient constructs (clientState: DbInterceptorUtils.DbInterceptorState) =
            List.fold (fun (s: ValueTask<unit>) x ->
                s
                |%|> fun _ -> clientState
                |%>>= flip (CloudFormationParser.applyCfn settings logger) x) CloudFormationParser.noTask constructs
        
        let dbState dbId db =
            match db with
            | Either1 (db': Database) when db'.Id <> dbId -> invalidOp "Unexpected error occurred"
            | Either1 db' ->
                { db = db'
                  parent = ValueNone
                  artificialDelay = TimeSpan.Zero
                  loggerOrDevNull = logger ?|? Logger.notAnILogger
                  defaultLogger = logger
                  scanSizeLimits =
                      { maxScanItems = Settings.ScanSizeLimits.DefaultMaxScanItems
                        maxPageSizeBytes = Settings.ScanSizeLimits.DefaultMaxPageSizeBytes }
                  awsAccountId = Settings.DefaultAwsAccountId}: DbInterceptorUtils.DbInterceptorState
            | Either2 (db': GlobalDatabase) ->
                { db = db'.GetDatabase ValueNone dbId
                  parent = ValueSome struct (db', dbId)
                  artificialDelay = TimeSpan.Zero
                  loggerOrDevNull = logger ?|? Logger.notAnILogger
                  defaultLogger = logger
                  scanSizeLimits =
                      { maxScanItems = Settings.ScanSizeLimits.DefaultMaxScanItems
                        maxPageSizeBytes = Settings.ScanSizeLimits.DefaultMaxPageSizeBytes }
                  awsAccountId = Settings.DefaultAwsAccountId}: DbInterceptorUtils.DbInterceptorState
        
        let apply struct (dbId, constructs) db =
            let state = dbState dbId db
            task { return! applyToClient constructs state } |> Io.fromTask |%|> asLazy db

        let execute _ =
            constructs
            |> Seq.fold (fun s x -> s |%>>= apply x) (ValueTask<_>(database))

        Io.onError execute (fun _ ->
            match database with
            | Either1 db -> db.Dispose()
            | Either2 db -> db.Dispose())
        |%>>= function
            | Either1 _ & db -> Io.retn db
            | Either2 db & db' ->
                db.AwaitAllSubscribers logger CancellationToken.None
                |> Io.normalizeVt
                |%|> asLazy db'

    static member BuildDatabase(
        cfnJsonStacks,
        settings,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let settings = {settings = settings; alwaysCreateGlobal = false }
        task {
            match! CloudFormationParser.buildDatabase settings (Maybe.Null.toOption logger) cfnJsonStacks with
            | Either1 db -> return new TestDynamo.Api.Database(db)
            | Either2 db ->
                db.Dispose()
                invalidOp $"Invalid cfnFiles. Files target multiple regions or have a global table creation. Use {nameof CloudFormationParser.BuildGlobalDatabase} instead"
                return Unchecked.defaultof<_>
        } |> Io.fromTask

    static member BuildGlobalDatabase(
        cfnJsonStacks,
        settings,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let settings = {settings = settings; alwaysCreateGlobal = true }
        task {
            match! CloudFormationParser.buildDatabase settings (Maybe.Null.toOption logger) cfnJsonStacks with
            | Either2 db -> return new TestDynamo.Api.GlobalDatabase(db)
            | Either1 db ->
                db.Dispose()
                invalidOp $"An unexpected error has occurred"
                return Unchecked.defaultof<_>
        } |> Io.fromTask
