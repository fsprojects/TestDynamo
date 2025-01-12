namespace TestDynamo.Lambda

open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open TestDynamo
open TestDynamo.Api.FSharp
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

            // filter out results which only have attempted deletes
            if List.isEmpty changeData.data.packet.changeResult.OrderedChanges
            then completedTask
            else subscriber.Invoke(funcInput, c) |> Io.normalizeVt
            
    let deFunc2 (f: System.Func<_, _, _>) x y = f.Invoke(x, y)

type SubscriptionDetails =
    { awsAccountId: string voption
      tableName: Model.Database.TableName
      behaviour: SubscriberBehaviour voption
      streamViewType: StreamViewType voption }
    
    with
    static member ofTableName tableName =
        { awsAccountId = ValueNone
          tableName = tableName
          behaviour = ValueNone
          streamViewType = ValueNone }

type Subscriptions() =
        
    /// <summary>
    /// Subscribe to a stream of lambda events
    /// Targets F#. See `AddSubscription` for C# version
    /// </summary>
    static member addSubscription<'DynamoDBEvent>
        (details: SubscriptionDetails)
        (subscriber: 'DynamoDBEvent -> CancellationToken -> ValueTask)
        (database: TestDynamo.Api.FSharp.Database): IStreamSubscriberDisposal =

        let awsAccountId = details.awsAccountId ?|? Settings.DefaultAwsAccountId
        let streamViewType = details.streamViewType ?|? StreamViewType.NEW_AND_OLD_IMAGES
        let streamConfig = LambdaSubscriberUtils.parseStreamConfig streamViewType
        let behaviour = details.behaviour ?|? SubscriberBehaviour.defaultOptions
        
        LambdaSubscriberUtils.build<'DynamoDBEvent> subscriber streamViewType awsAccountId database.Id.regionId
        |> database.SubscribeToStream ValueNone details.tableName struct (behaviour, streamConfig)
    
    /// <summary>
    /// Subscribe to a stream of lambda events
    /// Targets C#. See `addSubscription` for F# version
    /// </summary>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            { awsAccountId = (awsAccountId |> Maybe.Null.toOption)
              tableName = tableName
              behaviour = ValueNone
              streamViewType = ValueNone }
            (LambdaSubscriberUtils.deFunc2 subscriber)
            database.CoreDb

    /// <summary>
    /// Subscribe to a stream of lambda events
    /// Targets C#. See `addSubscription` for F# version
    /// </summary>
    [<Extension>]
    static member AddSubscription<'DynamoDBEvent> (
        database: TestDynamo.Api.Database,
        tableName,
        subscriber: System.Func<'DynamoDBEvent, CancellationToken, ValueTask>,
        streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.addSubscription
            { awsAccountId = (awsAccountId |> Maybe.Null.toOption)
              tableName = tableName
              behaviour = ValueNone
              streamViewType = ValueSome streamViewType }
            (LambdaSubscriberUtils.deFunc2 subscriber)
            database.CoreDb

    /// <summary>
    /// Subscribe to a stream of lambda events
    /// Targets C#. See `addSubscription` for F# version
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
            { awsAccountId = (awsAccountId |> Maybe.Null.toOption)
              tableName = tableName
              behaviour = ValueSome behaviour
              streamViewType = ValueSome streamViewType }
            (LambdaSubscriberUtils.deFunc2 subscriber)
            database.CoreDb

    /// <summary>
    /// Subscribe to a stream of lambda events
    /// Targets C#. See `addSubscription` for F# version
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
            { awsAccountId = (awsAccountId |> Maybe.Null.toOption)
              tableName = tableName
              behaviour = ValueSome behaviour
              streamViewType = ValueNone }
            (LambdaSubscriberUtils.deFunc2 subscriber)
            database.CoreDb
            
