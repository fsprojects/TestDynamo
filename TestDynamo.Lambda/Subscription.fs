namespace TestDynamo.Lambda

open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.GeneratedCode.Dtos
open TestDynamo.GenericMapper
open TestDynamo.Model
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils

#nowarn "3390"

module LambdaSubscriberUtils =
    
    /// <summary>
    /// Converts a null seq to empty and removes any null values
    /// </summary>
    let private orEmpty = function
        | null -> Seq.empty
        | xs -> xs
        
    /// <summary>
    /// Converts a null seq to empty and removes any null values
    /// </summary>
    let private sanitizeSeq xs = orEmpty xs |> Seq.filter ((<>)null)

    let private getEventNameAndKeyConfig keyConfig (x: ChangeResult) =
        match struct (x.Put, x.Deleted) with
        | ValueNone, ValueNone -> ValueNone
        | ValueSome x, ValueNone -> ValueSome struct ("INSERT", KeyConfig.keyAttributesOnly (Item.attributes x) keyConfig)
        | ValueNone, ValueSome x -> ValueSome struct ("REMOVE", KeyConfig.keyAttributesOnly (Item.attributes x) keyConfig)
        | ValueSome x, ValueSome _ -> ValueSome struct ("REMOVE", KeyConfig.keyAttributesOnly (Item.attributes x) keyConfig)

    let private size item =
        match item with
        | ValueSome x -> Item.size x
        | ValueNone -> 0

    let private mapChange awsAccountId regionId (streamViewType: StreamViewType) (ch: DatabaseSynchronizationPacket<TableCdcPacket>) (result: ChangeResult) =

        getEventNameAndKeyConfig ch.data.packet.changeResult.KeyConfig result
        ?|> (fun struct (evName, keys) ->
            
            { EventSourceArn = ch.data.tableArn struct (awsAccountId, regionId) |> ValueSome
              AwsRegion = ch.synchronizationPacketPath |> NonEmptyList.head |> _.regionId |> ValueSome
              EventID = result.UniqueEventId |> toString |> ValueSome
              EventName = evName |> ValueSome
              EventSource = "aws:dynamodb" |> ValueSome
              EventVersion = ValueNone
              UserIdentity = ValueNone 
              Dynamodb =
                  { ApproximateCreationDateTime = System.DateTime.UtcNow |> ValueSome
                    Keys = keys |> ValueSome
                    SequenceNumber = result.Id.Value.ToString() |> ValueSome
                    SizeBytes = (size result.Deleted + size result.Put) |> int64 |> ValueSome
                    StreamViewType = streamViewType.Value |> ValueSome
                    OldImage = result.Deleted ?|> Item.attributes
                    NewImage = result.Put ?|> Item.attributes } |> ValueSome }: DynamodbStreamRecord<AttributeValue>)

    let mapCdcPacket awsAccountId regionId dataType (x: DatabaseSynchronizationPacket<TableCdcPacket>) =
        let changes =
            x.data.packet.changeResult.OrderedChanges
            |> Seq.map (mapChange awsAccountId regionId dataType x)
            |> Maybe.traverse
            |> Array.ofSeq
            |> ValueSome

        { Records = changes }: DynamoDBEvent<_>

    let parseStreamConfig = function
        | (x: StreamViewType) when x.Value = StreamViewType.KEYS_ONLY.Value -> StreamDataType.KeysOnly
        | x when x.Value = StreamViewType.NEW_IMAGE.Value -> StreamDataType.NewImage 
        | x when x.Value = StreamViewType.OLD_IMAGE.Value -> StreamDataType.OldImage 
        | x when x.Value = StreamViewType.NEW_AND_OLD_IMAGES.Value -> StreamDataType.NewAndOldImages
        | x -> notSupported $"Invalid {nameof StreamViewType} \"{x.Value}\""

    let completedTask = ValueTask<_>(()).Preserve()

    let build<'DynamoDBEvent> (subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>) (streamViewType: StreamViewType) =

        mapCdcPacket
        >>>>> DtoMappers.mapDto<DynamoDBEvent<AttributeValue>, 'DynamoDBEvent>
        >>> fun mapper changeData c ->
            let funcInput = mapper streamViewType changeData

            // filter results which only have attempted deletes
            if List.isEmpty changeData.data.packet.changeResult.OrderedChanges
            then completedTask
            else subscriber.Invoke(funcInput, c) |> Io.normalizeVt

type Subscriptions() =

    static member private addSubscription<'DynamoDBEvent>
        (database: Either<TestDynamo.Api.Database, TestDynamo.Api.FSharp.Database>)
        tableName
        (subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>)
        behaviour
        (streamViewType: StreamViewType voption)
        (awsAccountId: string voption) =

        let awsAccountId = awsAccountId ?|? Settings.DefaultAwsAccountId
        let streamViewType = streamViewType ?|? StreamViewType.NEW_AND_OLD_IMAGES
        let streamConfig = LambdaSubscriberUtils.parseStreamConfig streamViewType
        
        database
        |> Either.map1Of2 (fun db ->
            let f = LambdaSubscriberUtils.build subscriber streamViewType awsAccountId db.Id.regionId
            db.SubscribeToStream(tableName, struct (behaviour, streamConfig), f))
        |> Either.map2Of2 (fun db ->
            LambdaSubscriberUtils.build subscriber streamViewType awsAccountId db.Id.regionId
            |> db.SubscribeToStream ValueNone tableName struct (behaviour, streamConfig))
        |> Either.reduce

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.FSharp.Database
    /// </summary>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.FSharp.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            (Either2 database)
            tableName
            subscriber
            SubscriberBehaviour.defaultOptions
            ValueNone
            (awsAccountId |> Maybe.Null.toOption)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.FSharp.Database
    /// </summary>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.FSharp.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            (Either2 database)
            tableName
            subscriber
            SubscriberBehaviour.defaultOptions
            (ValueSome streamViewType)
            (awsAccountId |> Maybe.Null.toOption)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.FSharp.Database
    /// </summary>
    /// <param name="behaviour">Define the synchronicity and error handling strategy for this subscriber</param>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.FSharp.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        behaviour,
        streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            (Either2 database)
            tableName
            subscriber
            behaviour
            (ValueSome streamViewType)
            (awsAccountId |> Maybe.Null.toOption)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.FSharp.Database
    /// </summary>
    /// <param name="behaviour">Define the synchronicity and error handling strategy for this subscriber</param>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.FSharp.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        behaviour,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            (Either2 database)
            tableName
            subscriber
            behaviour
            ValueNone
            (awsAccountId |> Maybe.Null.toOption)
    
    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            (Either1 database)
            tableName
            subscriber
            SubscriberBehaviour.defaultOptions
            ValueNone
            (awsAccountId |> Maybe.Null.toOption)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            (Either1 database)
            tableName
            subscriber
            SubscriberBehaviour.defaultOptions
            (ValueSome streamViewType)
            (awsAccountId |> Maybe.Null.toOption)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    /// <param name="behaviour">Define the synchronicity and error handling strategy for this subscriber</param>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        behaviour,
        streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            (Either1 database)
            tableName
            subscriber
            behaviour
            (ValueSome streamViewType)
            (awsAccountId |> Maybe.Null.toOption)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    /// <param name="behaviour">Define the synchronicity and error handling strategy for this subscriber</param>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        behaviour,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            (Either1 database)
            tableName
            subscriber
            behaviour
            ValueNone
            (awsAccountId |> Maybe.Null.toOption)
            
