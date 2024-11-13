namespace TestDynamo.Lambda

open System
open System.Collections.Generic
open System.IO
open System.Linq
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.Lambda.DynamoDBEvents
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.Model
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils

#nowarn "3390"

module LambdaSubscriberUtils =

    let rec private attributeFromDynamodb' name (attr: DynamoDBEvent.AttributeValue) =
        match struct (
            attr.BOOL.HasValue,
            attr.S,
            attr.N,
            attr.B,
            attr.NULL.HasValue && attr.NULL.Value,
            attr.M <> null,
            attr.L <> null,
            attr.SS <> null,
            attr.NS <> null,
            attr.BS <> null
            ) with
        | true, null, null, null, false, false, false, false, false, false -> attr.BOOL.Value |> AttributeValue.Boolean
        | false, str, null, null, false, false, false, false, false, false when str <> null -> str |> AttributeValue.String
        | false, null, num, null, false, false, false, false, false, false when num <> null -> num |> Decimal.Parse |> AttributeValue.Number
        | false, null, null, bin, false, false, false, false, false, false when bin <> null ->
            bin.ToArray() |> AttributeValue.Binary
        | false, null, null, null, true, false, false, false, false, false -> AttributeValue.Null
        | false, null, null, null, false, true, false, false, false, false ->
            if attr.M = null then TestDynamo.Utils.clientError "Map data not set"
            itemFromDynamodb' name attr.M |> AttributeValue.HashMap
        | false, null, null, null, false, false, true, false, false, false ->
            CSharp.sanitizeSeq attr.L 
            |> Seq.mapi (fun i -> attributeFromDynamodb' $"{name}[{i}]") |> Array.ofSeq |> CompressedList |> AttributeValue.AttributeList
        | false, null, null, null, false, false, false, true, false, false ->
            CSharp.sanitizeSeq attr.SS
            |> Seq.map String
            |> AttributeSet.create
            |> HashSet
        | false, null, null, null, false, false, false, false, true, false ->

            CSharp.sanitizeSeq attr.NS
            |> Seq.map (Decimal.Parse >> Number)
            |> AttributeSet.create
            |> HashSet
        | false, null, null, null, false, false, false, false, false, true ->

            CSharp.sanitizeSeq attr.BS
            |> Seq.map (fun (b: MemoryStream) -> b.ToArray() |> Binary)
            |> AttributeSet.create
            |> HashSet
        | pp -> clientError $"Unknown attribute type for \"{name}\""

    and attributeFromDynamodb (attr: DynamoDBEvent.AttributeValue) = attributeFromDynamodb' "$"

    and private mapAttribute name (attr: KeyValuePair<string, DynamoDBEvent.AttributeValue>) =
        struct (attr.Key, attributeFromDynamodb' $"{name}.{attr.Key}" attr.Value)

    and private itemFromDynamodb' name x = x |> Seq.map (mapAttribute name) |> MapUtils.fromTuple
    and itemFromDynamoDb: Dictionary<string,DynamoDBEvent.AttributeValue> -> Map<string,AttributeValue> = itemFromDynamodb' "$"

    let rec attributeToDynamoDbEvent = function
        | AttributeValue.String x ->
            let attr = DynamoDBEvent.AttributeValue()
            attr.S <- x
            attr
        | AttributeValue.Number x ->
            let attr = DynamoDBEvent.AttributeValue()
            attr.N <- x.ToString()
            attr
        | AttributeValue.Binary x ->
            let attr = DynamoDBEvent.AttributeValue()
            attr.B <- new MemoryStream(x)
            attr
        | AttributeValue.Boolean x ->
            let attr = DynamoDBEvent.AttributeValue()
            attr.BOOL <- x
            attr
        | AttributeValue.Null ->
            let attr = DynamoDBEvent.AttributeValue()
            attr.NULL <- true
            attr
        | AttributeValue.HashMap x ->
            let attr = DynamoDBEvent.AttributeValue()
            attr.M <- itemToDynamoDbEvent x
            attr
        | AttributeValue.HashSet x ->
            let attr = DynamoDBEvent.AttributeValue()
            match AttributeSet.getSetType x with
            | AttributeType.Binary ->
                attr.BS <-
                    AttributeSet.asBinarySeq x
                    |> Seq.map (fun x -> new MemoryStream(x))
                    |> Enumerable.ToList

            | AttributeType.String ->
                attr.SS <-
                    AttributeSet.asStringSeq x
                    |> Enumerable.ToList

            | AttributeType.Number ->
                attr.NS <-
                    AttributeSet.asNumberSeq x
                    |> Seq.map _.ToString()
                    |> Enumerable.ToList

            | x -> clientError $"Unknown set type {x}"

            attr
        | AttributeValue.AttributeList xs ->
            let attr = DynamoDBEvent.AttributeValue()
            attr.L <- xs |> AttributeListType.asSeq |> Seq.map attributeToDynamoDbEvent |> Enumerable.ToList
            attr

    and itemToDynamoDbEvent =
        CSharp.toDictionary (fun (x: string) -> x) attributeToDynamoDbEvent

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

    let private mapItem =
        ValueOption.map (Item.attributes >> itemToDynamoDbEvent) >> CSharp.fromOption

    let private mapChange awsAccountId regionId (streamViewType: StreamViewType) (ch: DatabaseSynchronizationPacket<TableCdcPacket>) (result: ChangeResult) =

        getEventNameAndKeyConfig ch.data.packet.changeResult.KeyConfig result
        ?|> (fun struct (evName, keys) -> 
            let r = DynamoDBEvent.DynamodbStreamRecord()
            r.EventSourceArn <- ch.data.tableArn struct (awsAccountId, regionId)
            r.AwsRegion <- ch.synchronizationPacketPath |> NonEmptyList.head |> _.regionId
            r.EventID <- result.UniqueEventId |> toString
            r.EventName <- evName
            r.EventSource <- "aws:dynamodb"

            r.Dynamodb <-
                let ddb = DynamoDBEvent.StreamRecord()
                ddb.ApproximateCreationDateTime <- System.DateTime.UtcNow
                ddb.Keys <- itemToDynamoDbEvent keys
                ddb.SequenceNumber <- result.Id.Value.ToString()
                ddb.SizeBytes <- (size result.Deleted + size result.Put) |> int64
                ddb.StreamViewType <- streamViewType.Value
                ddb.OldImage <- mapItem result.Deleted
                ddb.NewImage <- mapItem result.Put
                ddb

            r)

    let mapCdcPacket awsAccountId regionId dataType (x: DatabaseSynchronizationPacket<TableCdcPacket>) =
        let changes =
            x.data.packet.changeResult.OrderedChanges
            |> Seq.map (mapChange awsAccountId regionId dataType x)
            |> Maybe.traverse

        let record = DynamoDBEvent()
        record.Records <- List(changes)
        record

    let parseStreamConfig = function
        | (x: StreamViewType) when x.Value = StreamViewType.KEYS_ONLY.Value -> StreamDataType.KeysOnly
        | x when x.Value = StreamViewType.NEW_IMAGE.Value -> StreamDataType.NewImage 
        | x when x.Value = StreamViewType.OLD_IMAGE.Value -> StreamDataType.OldImage 
        | x when x.Value = StreamViewType.NEW_AND_OLD_IMAGES.Value -> StreamDataType.NewAndOldImages
        | x -> notSupported $"Invalid {nameof StreamViewType} \"{x.Value}\""

    let completedTask = ValueTask<_>(()).Preserve()

    let build (subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>) (streamViewType: StreamViewType) =

        mapCdcPacket
        >>> fun mapper changeData c ->
            let funcInput = mapper streamViewType changeData

            // filter results which only have attempted deletes
            if List.isEmpty changeData.data.packet.changeResult.OrderedChanges
            then completedTask
            else subscriber.Invoke(funcInput, c) |> Io.normalizeVt

[<Extension>]
type Subscriptions() =

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    [<Extension>]
    static member AddSubscription (
        database: TestDynamo.Api.FSharp.Database,
        tableName,
        subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.AddSubscription(database, tableName, subscriber, SubscriberBehaviour.defaultOptions, streamViewType, awsAccountId)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    /// <param name="behaviour">Define the synchronicity and error handling strategy for this subscriber</param>
    [<Extension>]
    static member AddSubscription (
        database: TestDynamo.Api.FSharp.Database,
        tableName,
        subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>,
        behaviour,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        let awsAccountId =
            match awsAccountId with
            | null -> Settings.DefaultAwsAccountId
            | x -> x

        let streamViewType =
            streamViewType
            |> CSharp.toOption
            ?|? StreamViewType.NEW_AND_OLD_IMAGES

        let streamConfig = LambdaSubscriberUtils.parseStreamConfig streamViewType

        LambdaSubscriberUtils.build subscriber streamViewType awsAccountId database.Id.regionId
        |> database.SubscribeToStream ValueNone tableName struct (behaviour, streamConfig)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    [<Extension>]
    static member AddSubscription (
        client: AmazonDynamoDBClient,
        tableName,
        subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.AddSubscription(client, tableName, subscriber, SubscriberBehaviour.defaultOptions, streamViewType, awsAccountId)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    /// <param name="behaviour">Define the synchronicity and error handling strategy for this subscriber</param>
    [<Extension>]
    static member AddSubscription (
        client: AmazonDynamoDBClient,
        tableName,
        subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>,
        behaviour,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.AddSubscription(client.GetDatabase(), tableName, subscriber, behaviour, streamViewType, awsAccountId)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    [<Extension>]
    static member AddSubscription (
        database: TestDynamo.Api.Database,
        tableName,
        subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        Subscriptions.AddSubscription(database, tableName, subscriber, SubscriberBehaviour.defaultOptions, streamViewType, awsAccountId)

    /// <summary>
    /// Create a stream subscriber that can be passed into the SubscribeToLambdaStream method on Api.Database
    /// </summary>
    /// <param name="behaviour">Define the synchronicity and error handling strategy for this subscriber</param>
    [<Extension>]
    static member  AddSubscription (
        database: TestDynamo.Api.Database,
        tableName,
        subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>,
        behaviour,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: string)>] awsAccountId: string) =

        let awsAccountId =
            match awsAccountId with
            | null -> Settings.DefaultAwsAccountId
            | x -> x

        let streamViewType =
            streamViewType
            |> CSharp.toOption
            ?|? StreamViewType.NEW_AND_OLD_IMAGES

        let streamConfig = LambdaSubscriberUtils.parseStreamConfig streamViewType

        let f = LambdaSubscriberUtils.build subscriber streamViewType awsAccountId database.Id.regionId
        database.SubscribeToStream(tableName, struct (behaviour, streamConfig), f)
