module Tests.Utils

open System
open System.IO
open System.Linq
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Lambda.DynamoDBEvents
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.GenericMapper
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open Tests.Table
open Xunit
open Xunit.Abstractions
open TestDynamo.Api.FSharp
open Tests.ClientLoggerContainer
open Tests.Loggers

type ApiDb = TestDynamo.Api.FSharp.Database
let asFunc2 (f: 'a -> 'b -> 'c): System.Func<'a, 'b, 'c> = f 

let attributeFromDynamodb (attr: DynamoAttributeValue) =
    DtoMappers.mapDto<DynamoAttributeValue, AttributeValue> attr
    
let itemFromDynamodb (x: Dictionary<string,DynamoAttributeValue>) =
    x |> Seq.map (fun x -> KeyValuePair<_, _>(x.Key, attributeFromDynamodb x.Value)) |> MapUtils.fromKvp
    
let attributeToDynamoDb = DtoMappers.mapDto<AttributeValue, DynamoAttributeValue>

let itemToDynamoDb: KeyValuePair<string, AttributeValue> seq -> Dictionary<string, DynamoAttributeValue> =
    Seq.map kvpToTuple
    >> Collection.mapSnd attributeToDynamoDb
    >> Seq.map tplToKvp
    >> kvpToDictionary
    
let recordSubscription writer tableName (database: ApiDb) behaviour subscriber =

    let record = System.Collections.Generic.List<_>()
    let subscription =
        behaviour ?|? SubscriberBehaviour.defaultOptions
        |> flip tpl StreamDataType.NewAndOldImages
        |> database.SubscribeToStream writer tableName
        <| fun x c ->
            record.Add(struct (DateTimeOffset.UtcNow, x))
            subscriber
            ?|> (fun s -> s x c)
            |> ValueOption.defaultValue (ValueTask<_>(()))

    struct (subscription, record)

type OutputCollector() =
    let logs = System.Collections.Generic.List<string>()

    member this.Emit(output: ITestOutputHelper) =
        let logsCopy = lock logs (fun _ -> Array.ofSeq logs)
        for log in logsCopy do
            output.WriteLine(log)

    interface ITestOutputHelper with
        member this.WriteLine(message) =
            logs.Add(message)

        member this.WriteLine(format, args) =
            logs.Add(System.String.Format(format, args))

let bits (count: int) =
    let (>>>) = shiftRight
    let total = (Math.Round(Math.Pow(2, count)) |> int) - 1
    [0..total]
    |> Seq.map (
        (>>>)
        >> fun shiftableNumber -> shiftableNumber >> flip (%) 2 >> (=) 0
        >> fun bitForPosition -> [0..count - 1] |> Seq.map bitForPosition)

let private oneHotFilter ignore =
    let spF =
        Seq.ofList
        >> Seq.mapi tpl
        >> Seq.filter (fstT >> flip Array.contains ignore >> not)
        >> Seq.filter sndT
        >> Seq.length
        >> ((=)1)

    ValueOption.map (fun inputFilter xs -> inputFilter xs && spF xs)
    >> ValueOption.defaultValue spF

type TestData<'a>(generator: unit -> 'a seq) =

    interface IEnumerable<obj array> with
        member this.GetEnumerator(): IEnumerator<obj array> =
            (generator() |> Seq.map (box >> Array.singleton)).GetEnumerator()

        member this.GetEnumerator(): Collections.IEnumerator =
            (this :> IEnumerable<obj array>).GetEnumerator()

type Flags (count, filter: (bool list -> bool) voption) =
    new(count) =
        Flags(count, ValueNone)

    interface IEnumerable<obj array> with
        member this.GetEnumerator(): IEnumerator<obj array> =
            let flags = bits count |> Seq.map List.ofSeq 
            let result =
                filter
                ?|> (flip Seq.filter flags)
                |> ValueOption.defaultValue flags

            let flg = result |> Seq.map (Seq.map box >> Array.ofSeq)
            flg.GetEnumerator()

        member this.GetEnumerator(): Collections.IEnumerator =
            (this :> IEnumerable<obj array>).GetEnumerator()

type OneFlag private(filter: (bool list -> bool) voption) =
    inherit Flags(1, filter)
    new() = OneFlag(ValueNone)
    new(filter: bool list -> bool) = OneFlag(ValueSome filter)

type TwoFlags private(filter: (bool list -> bool) voption) =
    inherit Flags(2, filter)
    new() = TwoFlags(ValueNone)
    new(filter: bool list -> bool) = TwoFlags(ValueSome filter)

/// <summary>One hot flags are flags with one hot encoding (i.e. exactly 1 flag will be true at any time)</summary>
type TwoOneHotFlags private(filter: (bool list -> bool) voption, ignoreInOneHotCalculation: int array) =
    inherit TwoFlags(oneHotFilter ignoreInOneHotCalculation filter)
    new() = TwoOneHotFlags(ValueNone, Array.empty)
    new(filter: bool list -> bool) = TwoOneHotFlags(ValueSome filter, Array.empty)
    new(filter: bool list -> bool, ignore) = TwoOneHotFlags(ValueSome filter, ignore)
    new(ignore) = TwoOneHotFlags(ValueNone, ignore)

type ThreeFlags private(filter: (bool list -> bool) voption) =
    inherit Flags(3, filter)
    new() = ThreeFlags(ValueNone)
    new(filter: bool list -> bool) = ThreeFlags(ValueSome filter)

/// <summary>One hot flags are flags with one hot encoding (i.e. exactly 1 flag will be true at any time)</summary>
type ThreeOneHotFlags private(filter: (bool list -> bool) voption, ignoreInOneHotCalculation: int array) =
    inherit ThreeFlags(oneHotFilter ignoreInOneHotCalculation filter)
    new() = ThreeOneHotFlags(ValueNone, Array.empty)
    new(filter: bool list -> bool) = ThreeOneHotFlags(ValueSome filter, Array.empty)
    new(filter: bool list -> bool, ignore) = ThreeOneHotFlags(ValueSome filter, ignore)
    new(ignore) = ThreeOneHotFlags(ValueNone, ignore)

type FourFlags private(filter: (bool list -> bool) voption) =
    inherit Flags(4, filter)
    new() = FourFlags(ValueNone)
    new(filter: bool list -> bool) = FourFlags(ValueSome filter)

/// <summary>One hot flags are flags with one hot encoding (i.e. exactly 1 flag will be true at any time)</summary>
type FourOneHotFlags private(filter: (bool list -> bool) voption, ignoreInOneHotCalculation: int array) =
    inherit FourFlags(oneHotFilter ignoreInOneHotCalculation filter)
    new() = FourOneHotFlags(ValueNone, Array.empty)
    new(filter: bool list -> bool) = FourOneHotFlags(ValueSome filter, Array.empty)
    new(filter: bool list -> bool, ignore) = FourOneHotFlags(ValueSome filter, ignore)
    new(ignore) = FourOneHotFlags(ValueNone, ignore)

type FiveFlags private(filter: (bool list -> bool) voption) =
    inherit Flags(5, filter)
    new() = FiveFlags(ValueNone)
    new(filter: bool list -> bool) = FiveFlags(ValueSome filter)

/// <summary>One hot flags are flags with one hot encoding (i.e. exactly 1 flag will be true at any time)</summary>
type FiveOneHotFlags private(filter: (bool list -> bool) voption, ignoreInOneHotCalculation: int array) =
    inherit FiveFlags(oneHotFilter ignoreInOneHotCalculation filter)
    new() = FiveOneHotFlags(ValueNone, Array.empty)
    new(filter: bool list -> bool) = FiveOneHotFlags(ValueSome filter, Array.empty)
    new(filter: bool list -> bool, ignore) = FiveOneHotFlags(ValueSome filter, ignore)
    new(ignore) = FiveOneHotFlags(ValueNone, ignore)

type SixFlags private(filter: (bool list -> bool) voption) =
    inherit Flags(6, filter)
    new() = SixFlags(ValueNone)
    new(filter: bool list -> bool) = SixFlags(ValueSome filter)

type SevenFlags private(filter: (bool list -> bool) voption) =
    inherit Flags(7, filter)
    new() = SevenFlags(ValueNone)
    new(filter: bool list -> bool) = SevenFlags(ValueSome filter)

type EightFlags private(filter: (bool list -> bool) voption) =
    inherit Flags(8, filter)
    new() = EightFlags(ValueNone)
    new(filter: bool list -> bool) = EightFlags(ValueSome filter)

type NineFlags private(filter: (bool list -> bool) voption) =
    inherit Flags(9, filter)
    new() = NineFlags(ValueNone)
    new(filter: bool list -> bool) = NineFlags(ValueSome filter)

type EnumValues<'a when 'a :> Enum and 'a : (new: unit -> 'a) and 'a : struct>() =
    interface IEnumerable<obj array> with
        member this.GetEnumerator(): IEnumerator<obj array> =
            let values =
                Enum.GetValues<'a>()
                |> Seq.map (box >> Array.singleton)

            values.GetEnumerator()

        member this.GetEnumerator(): Collections.IEnumerator =
            (this :> IEnumerable<obj array>).GetEnumerator()

let uniqueId = IncrementingId.next >> _.Value

let commonHost = new Database()

let buildClient output = new ClientContainer(commonHost, new TestLogger(output), true)

let buildTempClient() = new ClientContainer(commonHost, Logger.notAnILogger, false)

let buildClientWithlogLevel logLevel logger = new ClientContainer(commonHost, new TestLogger(logger, level = logLevel), true)

let buildClientFromLogger logger =
    TestDynamoClient.createClient<AmazonDynamoDBClient> (ValueSome logger) true ValueNone (ValueSome commonHost)
let printFullList xs =
    Seq.mapi (fun i -> if i = 0 then id else sprintf "  %s;") xs
    |> Str.join "\n"
    |> sprintf "[ %s ]"

let printFullMap xs =
    Seq.map (
        sprintf "%s: %s;" |> uncurry
        >> Str.indent "  ") xs
    |> Str.join "\n"
    |> sprintf "{ %s }"

// let cloneHost logger changeSubscriberOpts =
//     match struct (logger, changeSubscriberOpts) with
//     | logger, ValueNone -> commonHost.Clone logger
//     | ValueNone, ValueSome _ & x ->
//         let cloneData = commonHost.BuildCloneData()
//         new Database(
//             cloneData = { cloneData with streamSubscriberOptions = x })
//     | ValueSome logger, ValueSome _ & x ->
//         let cloneData = commonHost.BuildCloneData()
//         new Database(
//             logger = logger,
//             cloneData = { cloneData with streamSubscriberOptions = x })

let cloneHost logger = commonHost.Clone logger

let cloneHostUnlogged () = commonHost.Clone()

let cloneGlobalHost () =
    let host = commonHost.BuildCloneData()
    struct (new GlobalDatabase(host), commonHost.Id)

let seededRandomBuilder seed (t: ITestOutputHelper) =
    t.WriteLine($"Seed {seed}")
    System.Random(seed)

let randomBuilder (t: ITestOutputHelper) =
    let seed =
        Environment.GetEnvironmentVariable("TEST_SEED")
        |> function
            | null
            | "" -> Random.Shared.Next()
            | x ->
                match Int32.TryParse x with
                | false, _ -> invalidOp $"TEST_SEED must be a number \"{x}\""
                | true, seed -> seed

    seededRandomBuilder seed t

let addTable (client: IAmazonDynamoDB) enableStreams =
    task {
        let name = (System.Guid.NewGuid()).ToString()

        do!
            TableBuilder.empty
            |> TableBuilder.withTableName name
            |> TableBuilder.withAttribute "PartitionKey" "S"
            |> TableBuilder.withAttribute "SortKey" "N"
            |> TableBuilder.withKeySchema "PartitionKey" (ValueSome "SortKey")
            |> if enableStreams
               then TableBuilder.withStreamAction (CreateOrDelete.Create ())
               else id
            |> TableBuilder.req
            |> client.CreateTableAsync
            |> Io.ignoreTask

        return name
    }

let assertErrors (output: ITestOutputHelper) msgs (e: exn) =
    Assert.NotEmpty(msgs)

    let str = e.ToString()

    msgs
    |> List.fold (fun _ msg ->
        try
            Assert.Contains(msg, str)
        with _ ->
            output.WriteLine($"Expected: {msg}\nActual: {str}")
            reraise()
        ()) ()

let assertError output = List.singleton >> assertErrors output

type ScanBuilder =
    { tableName: string voption
      indexName: string voption }

    with

    static member empty =
        { tableName = ValueNone
          indexName = ValueNone }
    static member withTableName name sb = {sb with tableName = ValueSome name}: ScanBuilder
    static member withIndexName name sb = {sb with indexName = ValueSome name}: ScanBuilder
    static member req sb =
        let req = ScanRequest()
        req.TableName <- sb.tableName |> Maybe.expectSome
        req.IndexName <- sb.indexName |> ValueOption.defaultValue null

        req

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
            if attr.M = null then ClientError.clientError "Map data not set"
            itemFromDynamodb' name attr.M |> AttributeValue.HashMap
        | false, null, null, null, false, false, true, false, false, false ->
            sanitizeSeq attr.L 
            |> Seq.mapi (fun i -> attributeFromDynamodb' $"{name}[{i}]") |> Array.ofSeq |> CompressedList |> AttributeValue.AttributeList
        | false, null, null, null, false, false, false, true, false, false ->
            sanitizeSeq attr.SS
            |> Seq.map String
            |> AttributeSet.create
            |> HashSet
        | false, null, null, null, false, false, false, false, true, false ->
    
            sanitizeSeq attr.NS
            |> Seq.map (Decimal.Parse >> Number)
            |> AttributeSet.create
            |> HashSet
        | false, null, null, null, false, false, false, false, false, true ->
    
            sanitizeSeq attr.BS
            |> Seq.map (fun (b: MemoryStream) -> b.ToArray() |> Binary)
            |> AttributeSet.create
            |> HashSet
        | pp -> ClientError.clientError $"Unknown attribute type for \"{name}\""
    
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
    
            | x -> ClientError.clientError $"Unknown set type {x}"
    
            attr
        | AttributeValue.AttributeList xs ->
            let attr = DynamoDBEvent.AttributeValue()
            attr.L <- xs |> AttributeListType.asSeq |> Seq.map attributeToDynamoDbEvent |> Enumerable.ToList
            attr
    
    and itemToDynamoDbEvent =
        Seq.map kvpToTuple
        >> Collection.mapSnd attributeToDynamoDbEvent
        >> Seq.map tplToKvp
        >> kvpToDictionary
