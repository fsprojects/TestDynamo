namespace TestDynamo.Api

open System
open System.IO
open System.Linq
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.Lambda.DynamoDBEvents

open TestDynamo
open System.Collections.Generic
open TestDynamo.Data.BasicStructures
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

[<Struct; IsReadOnly>]
type LambdaStreamSubscriber =
    private | Ss of struct (
        struct (SubscriberBehaviour * StreamDataType) *
        (AwsAccountId -> RegionId -> DatabaseSynchronizationPacket<TableCdcPacket> -> CancellationToken -> ValueTask<Unit>))

    static member private attributeFromDynamodb (attr: DynamoDBEvent.AttributeValue) = LambdaStreamSubscriber.attributeFromDynamodb' "$"
    
    static member private attributeFromDynamodb' name (attr: DynamoDBEvent.AttributeValue) =
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
            if attr.M = null then clientError "Map data not set"
            LambdaStreamSubscriber.itemFromDynamodb' name attr.M |> AttributeValue.HashMap
        | false, null, null, null, false, false, true, false, false, false ->
            CSharp.sanitizeSeq attr.L 
            |> Seq.mapi (fun i -> LambdaStreamSubscriber.attributeFromDynamodb' $"{name}[{i}]") |> Array.ofSeq |> CompressedList |> AttributeValue.AttributeList
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

    static member private mapAttribute name (attr: KeyValuePair<string, DynamoDBEvent.AttributeValue>) =
        struct (attr.Key, LambdaStreamSubscriber.attributeFromDynamodb' $"{name}.{attr.Key}" attr.Value)

    static member private itemFromDynamodb' name x = x |> Seq.map (LambdaStreamSubscriber.mapAttribute name) |> MapUtils.fromTuple
    static member itemFromDynamoDb: Dictionary<string,DynamoDBEvent.AttributeValue> -> Map<string,AttributeValue> = LambdaStreamSubscriber.itemFromDynamodb' "$"

    static member private attributeToDynamoDbEvent = function
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
            attr.M <- LambdaStreamSubscriber.itemToDynamoDbEvent x
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
            attr.L <- xs |> AttributeListType.asSeq |> Seq.map LambdaStreamSubscriber.attributeToDynamoDbEvent |> Enumerable.ToList
            attr

    static member private itemToDynamoDbEvent =
        CSharp.toDictionary (fun (x: string) -> x) LambdaStreamSubscriber.attributeToDynamoDbEvent

    static member private getEventNameAndKeyConfig keyConfig (x: ChangeResult) =
        match struct (x.Put, x.Deleted) with
        | ValueNone, ValueNone -> ValueNone
        | ValueSome x, ValueNone -> ValueSome struct ("INSERT", KeyConfig.keyAttributesOnly (Item.attributes x) keyConfig)
        | ValueNone, ValueSome x -> ValueSome struct ("REMOVE", KeyConfig.keyAttributesOnly (Item.attributes x) keyConfig)
        | ValueSome x, ValueSome _ -> ValueSome struct ("REMOVE", KeyConfig.keyAttributesOnly (Item.attributes x) keyConfig)

    static member private size item =
        match item with
        | ValueSome x -> Item.size x
        | ValueNone -> 0

    static member private mapItem =
        ValueOption.map (Item.attributes >> LambdaStreamSubscriber.itemToDynamoDbEvent) >> CSharp.fromOption

    static member private mapChange awsAccountId regionId (streamViewType: StreamViewType) (ch: DatabaseSynchronizationPacket<TableCdcPacket>) (result: ChangeResult) =

        LambdaStreamSubscriber.getEventNameAndKeyConfig ch.data.packet.changeResult.KeyConfig result
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
                ddb.Keys <- LambdaStreamSubscriber.itemToDynamoDbEvent keys
                ddb.SequenceNumber <- result.Id.Value.ToString()
                ddb.SizeBytes <- (LambdaStreamSubscriber.size result.Deleted + LambdaStreamSubscriber.size result.Put) |> int64
                ddb.StreamViewType <- streamViewType.Value
                ddb.OldImage <- LambdaStreamSubscriber.mapItem result.Deleted
                ddb.NewImage <- LambdaStreamSubscriber.mapItem result.Put
                ddb

            r)

    static member private mapCdcPacket awsAccountId regionId dataType (x: DatabaseSynchronizationPacket<TableCdcPacket>) =
        let changes =
            x.data.packet.changeResult.OrderedChanges
            |> Seq.map (LambdaStreamSubscriber.mapChange awsAccountId regionId dataType x)
            |> Maybe.traverse

        let record = DynamoDBEvent()
        record.Records <- List(changes)
        record

    static member private parseStreamConfig = function
        | (x: StreamViewType) when x.Value = StreamViewType.KEYS_ONLY.Value -> StreamDataType.KeysOnly
        | x when x.Value = StreamViewType.NEW_IMAGE.Value -> StreamDataType.NewImage 
        | x when x.Value = StreamViewType.OLD_IMAGE.Value -> StreamDataType.OldImage 
        | x when x.Value = StreamViewType.NEW_AND_OLD_IMAGES.Value -> StreamDataType.NewAndOldImages
        | x -> notSupported $"Invalid {nameof StreamViewType} \"{x.Value}\""

    static member private completedTask = ValueTask<_>(()).Preserve()
    static member Build (
        subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType) =
        
        LambdaStreamSubscriber.Build(subscriber, SubscriberBehaviour.defaultOptions, streamViewType)
        
    static member Build (
        subscriber: System.Func<DynamoDBEvent, CancellationToken, ValueTask>,
        behaviour,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType) =
        
        let streamConfig =
            streamViewType
            |> CSharp.toOption
            ?|? StreamViewType.NEW_AND_OLD_IMAGES
            |> LambdaStreamSubscriber.parseStreamConfig
        
        LambdaStreamSubscriber.mapCdcPacket
        >>> fun mapper changeData c ->
            let funcInput = mapper streamViewType changeData
            
            // filter results which only have attempted deletes
            if List.isEmpty changeData.data.packet.changeResult.OrderedChanges
            then LambdaStreamSubscriber.completedTask
            else subscriber.Invoke(funcInput, c) |> Io.normalizeVt
        |> tpl struct (behaviour, streamConfig)
        |> Ss

    static member internal getStreamConfig (Ss (x, _)) = x

    static member internal getStreamSubscriber awsAccountId regionId (Ss (_, f)) = f awsAccountId regionId