module Tests.Utils

open System
open System.Threading.Tasks
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open DynamoDbInMemory.Client
open DynamoDbInMemory.Data.BasicStructures
open DynamoDbInMemory.Model
open DynamoDbInMemory.Utils
open DynamoDbInMemory.Data.Monads.Operators
open Tests.Table
open Xunit
open Xunit.Abstractions
open DynamoDbInMemory.Api
open Tests.ClientLoggerContainer
open Tests.Loggers

let asFunc2 (f: 'a -> 'b -> 'c): System.Func<'a, 'b, 'c> = f 

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
        for log in logs do
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

let buildClient = function
    | ValueNone ->  InMemoryDynamoDbClient.Create(commonHost)
    | ValueSome x -> new ClientContainer(commonHost, new TestLogger(x)) :> IInMemoryDynamoDbClient

let buildClientWithlogLevel logLevel = function
    | ValueNone ->  InMemoryDynamoDbClient.Create(commonHost)
    | ValueSome x -> new ClientContainer(commonHost, new TestLogger(x, level = logLevel)) :> IInMemoryDynamoDbClient

let buildClientFromLogger logger =
    InMemoryDynamoDbClient.Create(commonHost, logger)
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

let cloneDistributedHost () =
    let host = commonHost.BuildCloneData()
    struct (new DistributedDatabase(host), commonHost.Id)

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
