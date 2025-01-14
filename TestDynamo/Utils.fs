module TestDynamo.Utils

open System
open System.Diagnostics.CodeAnalysis
open System.Runtime.CompilerServices
open System.Text
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks

type RegionId = string
type AwsAccountId = string
type KeyValuePair<'k, 'v> =  System.Collections.Generic.KeyValuePair<'k, 'v>
type Dictionary<'k, 'v> =  System.Collections.Generic.Dictionary<'k, 'v>
type MList<'a> =  System.Collections.Generic.List<'a>
type IEnumerable<'a> =  System.Collections.Generic.IEnumerable<'a>
type IEnumerator =  System.Collections.IEnumerator
type IEnumerator<'a> =  System.Collections.Generic.IEnumerator<'a>
type IEqualityComparer<'a> =  System.Collections.Generic.IEqualityComparer<'a>
type IComparer<'a> =  System.Collections.Generic.IComparer<'a>
type IReadOnlyList<'a> =  System.Collections.Generic.IReadOnlyList<'a>
type ICollection<'a> =  System.Collections.Generic.ICollection<'a>
type IReadOnlyCollection<'a> =  System.Collections.Generic.IReadOnlyCollection<'a>
type IList<'a> =  System.Collections.Generic.IList<'a>

#if NETSTANDARD2_0
// quick and dirty hash code calculation
// copied from https://stackoverflow.com/questions/1646807/quick-and-simple-hash-code-combinations
type internal HashCode() =
    static member Combine<'a when 'a: equality>(h1: int, h2: 'a) =
        // unchecked operators
        let (+) = Operators.(+)
        let (*) = Operators.(*)

        h1 * 31 + h2.GetHashCode()

let kvpToDictionary (xs: KeyValuePair<_, _> seq) =
    let d = Dictionary<_, _>()
    for x in xs do
        d.Add(x.Key, x.Value)
    d
type internal IsReadOnlyAttribute() = inherit System.Attribute()
#else
let kvpToDictionary (xs: KeyValuePair<_, _> seq) = Dictionary<_, _> xs
#endif

let shiftRight = (>>>)
[<ExcludeFromCodeCoverage>]
let inline (>>>) f g x = f x >> g
[<ExcludeFromCodeCoverage>]
let inline (>>>>) f g x = f x >>> g
[<ExcludeFromCodeCoverage>]
let inline (>>>>>) f g x = f x >>>> g
[<ExcludeFromCodeCoverage>]
let inline mapFst f struct (x, y) = struct (f x, y)
[<ExcludeFromCodeCoverage>]
let inline mapSnd f struct (x, y) = struct (x, f y)
let mapFstFst f = mapFst (mapFst f)
let mapFstSnd f = mapFst (mapSnd f)
let mapSndFst f = mapSnd (mapFst f)
let mapSndSnd f = mapSnd (mapSnd f)
let inline toString x = x.ToString()
[<ExcludeFromCodeCoverage>]
let inline mapTpl fl fr struct (x, y) = struct (fl x, fr y)
[<ExcludeFromCodeCoverage>]
let inline fstT struct (x, _) = x
[<ExcludeFromCodeCoverage>]
let inline sndT<'a, 'b> struct (_: 'a, x: 'b) = x
[<ExcludeFromCodeCoverage>]
let inline curry f x y = f struct (x, y)
[<ExcludeFromCodeCoverage>]
let inline curryRef f x y = f (x, y)
[<ExcludeFromCodeCoverage>]
let inline curry3 f x y z = f struct (x, y, z)
[<ExcludeFromCodeCoverage>]
let inline uncurry f struct (x, y) = f x y
[<ExcludeFromCodeCoverage>]
let inline uncurry3 f struct (x, y, z) = f x y z
[<ExcludeFromCodeCoverage>]
let inline tplDouble x = struct (x, x)
[<ExcludeFromCodeCoverage>]
let inline tpl x y = struct (x, y)
[<ExcludeFromCodeCoverage>]
let inline tpl3 x y z = struct (x, y, z)
[<ExcludeFromCodeCoverage>]
let inline tpl4 w x y z = struct (w, x, y, z)
[<ExcludeFromCodeCoverage>]
let inline structTpl (x, y) = struct (x, y)
[<ExcludeFromCodeCoverage>]
let inline applyTpl struct (f, x) = f x
[<ExcludeFromCodeCoverage>]
let inline tplToList struct (x, y) = [x; y]

/// <summary>
/// Just like ignore, but adds a type check to mitigate future errors due to currying 
/// </summary>
[<ExcludeFromCodeCoverage>]
let inline ignoreTyped<'a> (_: ^a) = ()

/// <summary>
/// converts a value to a function, or
/// adds an arg to the start of a function
/// </summary>
[<ExcludeFromCodeCoverage>]
let inline asLazy x _ = x
[<ExcludeFromCodeCoverage>]
let inline asLazy2 f x _ = f x
[<ExcludeFromCodeCoverage>]
let inline asLazy3 f x y _ = f x y
[<ExcludeFromCodeCoverage>]
let inline lazyF f x = lazy(f x)
[<ExcludeFromCodeCoverage>]
let inline flip f x y = f y x
[<ExcludeFromCodeCoverage>]
let inline flip3To1 f x y z = f y z x
[<ExcludeFromCodeCoverage>]
let inline flip1To3 f x y z = f z x y
[<ExcludeFromCodeCoverage>]
let inline flip2To3 f x y z = f x z y
[<ExcludeFromCodeCoverage>]
let inline flip4To1 f w x y z = f x y z w
[<ExcludeFromCodeCoverage>]
let inline flip1To4 f w x y z = f z w x y
[<ExcludeFromCodeCoverage>]
let inline flipTpl struct (x, y) = struct (y, x)
let kvpToRefTuple (kvp: KeyValuePair<'a, 'b>) = (kvp.Key, kvp.Value)
let kvpToTuple (kvp: KeyValuePair<'a, 'b>) = struct (kvp.Key, kvp.Value)
let tplToKvp struct (k, v) = KeyValuePair<_, _>(k, v)
[<ExcludeFromCodeCoverage>]
let inline toRefTuple struct (x, y) = (x, y)
let apply = (|>)

#if DEBUG
let debug x =
    if System.Diagnostics.Debugger.IsAttached then
        System.Diagnostics.Debugger.Break()
    x

let debug2 y x =
    if System.Diagnostics.Debugger.IsAttached then
        System.Diagnostics.Debugger.Break()
    x
#else
[<ExcludeFromCodeCoverage>]
let inline debug x = x
[<ExcludeFromCodeCoverage>]
let inline debug2 _ x = x
#endif


let notSupported msg = msg |> NotSupportedException |> raise
let serverError msg = msg |> Exception |> raise

module AwsUtils =
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
    let tableNameRx = Regex(@"^[0-9A-Za-z\.\-_]{3,255}$", RegexOptions.Compiled)
    [<ExcludeFromCodeCoverage>]
    let inline parseTableName (name: string) =
        if tableNameRx.IsMatch name then ValueSome name else ValueNone

    let parseIndexName = parseTableName

    type Region = string
    type Account = string
    type TableName = string

    let private tableArnParser = Regex(@"^arn:aws:dynamodb:(?<region>[\w\-]+?):(?<account>\d+?):table/(?<table>[0-9A-Za-z\.\-_]+)/?$", RegexOptions.Compiled)
    let parseTableArn (arn: string): struct (Region * Account * TableName) voption =
        match tableArnParser.Match arn with
        | x when not x.Success -> ValueNone
        | x ->
            struct (x.Groups["region"].Captures[0].Value, x.Groups["account"].Captures[0].Value, x.Groups["table"].Captures[0].Value)
            |> ValueSome

/// <summary>An id which increments over time. Similar to a SQL identity</summary>
type IncrementingId =
    private
    | Iid of int

    with
    static member inline value (x: IncrementingId) = x.Value
    member this.Value = match this with | Iid x -> x
    override this.ToString() = match this with | Iid x -> x.ToString()

module IncrementingId =

    let next =
        let mutable id = 100
        let inline f () = Interlocked.Increment(&id) |> Iid
        f

let private hashMapTryFind key map =
    match Map.containsKey key map with
    | true -> Map.find key map |> ValueSome
    | false -> ValueNone

type CacheHit = bool

/// <summary>
/// Memoze a function. Return a cached value from a previous invocation if possible
/// If the memoized function is called twice in parallel with the same args,
/// then a cache hit will not occur until at least one of the invocations complete 
/// </summary>
/// <param name="cacheInvalidationSettings">
/// Cache invalidation settings.
/// If SOME (low, high), the cache will be invalidated when it contains `high` number of items.
/// Items are purged based on their last access time until `low` number of items remain.
/// The greater difference between `low` and `high`, the less expensive cache invalidations are performed
/// If NONE, never invalidate the cache
/// </param>
/// <param name="keySelector">
/// The cache key, mapped from the input args.
/// Cache keys are compared using System.Collections.Generic.Comparer&lt;'key>.Default</param>
/// <param name="f">The function to memoize</param>
let memoize cacheInvalidationSettings (keySelector: 'a -> 'key) (f: 'a -> 'b) : 'a -> struct (CacheHit * 'b) =

    let cache = Dictionary()
    let struct (min, max) = ValueOption.defaultValue (Int32.MaxValue, Int32.MaxValue) cacheInvalidationSettings
    if (max < min) then invalidOp "Min cache size must be less than max size"

    let cacheUsageTime: KeyValuePair<_, _> -> DateTime = _.Value >> sndT 

    let invalidateCache () =
        if cache.Count <= max then ()
        else

        let vals = cache |> Seq.sortByDescending cacheUsageTime |> Seq.truncate min 
        let intermediate = System.Collections.Generic.List<_>(min)
        intermediate.AddRange vals
        cache.Clear()
        for x in intermediate do
            cache.Add(x.Key, x.Value)

    let addToCache' struct (k, v) _ =
        cache[k] <- struct (v, DateTime.UtcNow)
        invalidateCache()

    let threadLock = obj()
    let inline addToCache x = lock threadLock (addToCache' x)

    let executeCached x =
        let key = keySelector x

        match cache.TryGetValue(key) with
        | true, struct (data, _) ->
            addToCache struct (key, data)
            struct (true, data)
        | false, _ ->
            let y = f x
            addToCache struct (key, y)
            struct (false, y)

    executeCached

[<RequireQualifiedAccess>]
module Str =

    let emptyStringToNull = function
        | null -> null
        | (x: string) when System.String.IsNullOrWhiteSpace x -> null
        | x -> x

    type StringBuilderRental() =

        static let lockObj = obj()
        [<ThreadStatic; DefaultValue>]
        static val mutable private rental: StringBuilder

        static member private rent' () =
            let mutable sb = StringBuilderRental.rental
            StringBuilderRental.rental <- null

            if sb = null
            then sb <- StringBuilder()
            else sb.Clear() |> ignoreTyped<StringBuilder>

            sb

        static member Rent () =  lock lockObj StringBuilderRental.rent'

        static member Return (returned: StringBuilder) =
            returned.Clear() |> ignoreTyped<StringBuilder>
            StringBuilderRental.rental <- returned

    let split (separator: string) (s: string) =
#if NETSTANDARD2_0
        s.Split([|separator|], System.StringSplitOptions.None)
#else
        s.Split separator
#endif

    [<ExcludeFromCodeCoverage>]
    let inline join (separator: string) (xs: string seq) = System.String.Join(separator, xs)

    [<ExcludeFromCodeCoverage>]
    let inline concat (x: string) (y: string) = x + y
    
    [<ExcludeFromCodeCoverage>]
    let inline contains (search: string) (str: string) = str.Contains search

    let indentComplex firstIndentation indentation (str: string) =
        split "\n" str
        |> Seq.mapi (fun i -> sprintf "%s%s" (if i = 0 then firstIndentation else indentation))
        |> join "\n"

    [<ExcludeFromCodeCoverage>]
    let inline indent indentation = indentComplex indentation indentation

    let indentSpace =
        let indentation i =
            let sb = StringBuilderRental.Rent()
            let mutable i = i
            while i > 0 do
                sb.Append ' ' |> ignoreTyped<StringBuilder>
                i <- i - 1

            let indent = sb.ToString()
            StringBuilderRental.Return sb
            indent

        let inline k (x: int) = x    
        let indentCache = memoize (ValueSome (100, 200)) k indentation >> sndT

        fun i ->
            if i < 100 then sprintf "%s%s" (indentCache i)
            else sprintf "%s%s" (indentation i)

[<RequireQualifiedAccess>]
module SetUtils =

    let add x xs =
        match Set.contains x xs with
        | true -> struct (false, xs)
        | false -> struct (true, Set.add x xs)

[<RequireQualifiedAccess>]
module Collection =
    type MList<'a> = System.Collections.Generic.List<'a>

    [<Struct; IsReadOnly>]
    type MapCombination<'k> =
        { intersection: 'k list
          inLOnly: 'k list
          inROnly: 'k list }

    [<ExcludeFromCodeCoverage>]
    let inline tryGetArr arr i =
        if i < 0 || Array.length arr <= i then ValueNone
        else Array.get arr i |> ValueSome

    [<ExcludeFromCodeCoverage>]
    let inline replace arr i x =
        if i < 0 || Array.length arr <= i then ArgumentOutOfRangeException() |> raise
        else Array.mapi (fun i' x' -> if i' = i then x else x') arr

    [<ExcludeFromCodeCoverage>]
    let inline tryAddOrReplaceArr arr i x =
        if i < 0 || Array.length arr < i then ValueNone
        elif i = Array.length arr then Array.insertAt i x arr |> ValueSome
        else replace arr i x |> ValueSome

    /// <summary>Like regular fold back but with args like fold</summary>
    [<ExcludeFromCodeCoverage>]
    let inline foldBack f s xs = Seq.foldBack (flip f) xs s

    /// <summary>Like regular fold back but with args like fold</summary>
    [<ExcludeFromCodeCoverage>]
    let inline foldBackL f s xs = List.foldBack (flip f) xs s

    // single use type. has mutable state, so do not call peek/pop more than once
    // on the same instance
    type private IEnumeratorQ<'a> =
        { mutable head: 'a voption
          mutable tail: System.Collections.Generic.IEnumerator<'a> }
        with

        member this.tryPop() =
            match this.head with
            | ValueSome _ & x ->
                this.head <- ValueNone
                x
            | ValueNone when this.tail.MoveNext() ->
                ValueSome this.tail.Current
            | ValueNone & x -> x

        member this.tryPeek() =
            match this.head with
            | ValueSome _ & x -> x
            | ValueNone when this.tail.MoveNext() ->
                this.head <- ValueSome this.tail.Current
                this.head
            | ValueNone & x -> x

    /// <summary>The second half of a merge sort</summary>
    let mergeSortedSeq compare (xs: _ seq) (ys: _ seq) =
        seq {
            use xE = xs.GetEnumerator()
            use yE = ys.GetEnumerator()

            let xQ = { head = ValueNone; tail = xE }
            let yQ = { head = ValueNone; tail = yE }

            let mutable brk = false
            while not brk do
                match struct (xQ.tryPeek(), yQ.tryPeek()) with
                | ValueSome x, ValueSome y when compare x y <= 0 ->
                    xQ.tryPop() |> ignore
                    yield x
                | ValueSome _, ValueSome y ->
                    yQ.tryPop() |> ignore
                    yield y
                | ValueSome x, ValueNone ->
                    xQ.tryPop() |> ignore
                    yield x
                | ValueNone, ValueSome y ->
                    yQ.tryPop() |> ignore
                    yield y
                | ValueNone, ValueNone -> brk <- true
        }

    let window windowSize (xs: 'a seq): 'a seq seq =
        if windowSize < 1 then invalidOp "Size < 1"
        seq {
            use enm = xs.GetEnumerator()

            while enm.MoveNext() do
                // MList is most efficient collection here and mutation is kept
                // to within the bounds of the function
                let output = MList<_>(windowSize)
                output.Add(enm.Current)
                let mutable currentWindowSize = 1
                while currentWindowSize < windowSize && enm.MoveNext() do
                    output.Add(enm.Current)
                    currentWindowSize <- currentWindowSize + 1

                yield output
        }

    /// <summary>
    /// Windows data with breakpoints before the item which passes the predicate
    /// </summary>
    let windowDynamic predicate (xs: 'a seq): 'a seq seq =
        seq {
            use enm = xs.GetEnumerator()

            let mutable output = ValueNone
            while enm.MoveNext() do
                if predicate enm.Current && ValueOption.isSome output
                then
                    yield output.Value
                    output <- ValueNone

                if ValueOption.isNone output
                then output <- ValueSome (MList<_>())

                output.Value.Add(enm.Current)

            if ValueOption.isSome output
            then yield output.Value
        }

    let unzip xs =
        Seq.fold (fun struct (xs, ys) struct (x, y) -> struct (x::xs, y::ys)) struct ([], []) xs
        |> mapTpl List.rev List.rev

    let rec private zipL' acc = function
        | struct ([], []) -> acc
        | [], _ -> acc
        | _, [] -> acc
        | headX::tailX, headY::tailY -> zipL' (struct (headX, headY)::acc) struct (tailX, tailY)

    let zipL xs ys = zipL' [] struct (xs, ys) |> List.rev

    let zip (xs: 'a seq) (ys: 'b seq) =
        seq {
            use enmX = xs.GetEnumerator()
            use enmY = ys.GetEnumerator()

            while enmX.MoveNext() && enmY.MoveNext() do
                yield struct (enmX.Current, enmY.Current)
        }

    let rec private zipStrict' acc = function
        | struct ([], []) -> ValueSome acc
        | [], _ -> ValueNone
        | _, [] -> ValueNone
        | headX::tailX, headY::tailY -> zipStrict' (struct (headX, headY)::acc) struct (tailX, tailY)

    let zipStrictL xs ys = zipStrict' [] struct (xs, ys) |> ValueOption.map List.rev

    let partition predicate xs =
        Seq.foldBack (fun x struct (l, r) ->
            match predicate x with
            | true -> struct (x::l, r)
            | false -> struct (l, x::r)) xs struct ([], [])

    [<ExcludeFromCodeCoverage>]
    let inline private tryHeadIList (l: IList<_>) =
        if l.Count = 0 then ValueNone
        else l[0] |> ValueSome

    [<ExcludeFromCodeCoverage>]
    let inline private tryHeadIReadOnlyList (l: IReadOnlyList<_>) =
        if l.Count = 0 then ValueNone
        else l[0] |> ValueSome

    let private tryHeadL = function
        | [] -> ValueNone
        | head::_ -> ValueSome head

    let tryHead (xs: 'a seq) =
        match xs with
        | :? List<'a> as l -> tryHeadL l
        | :? IList<'a> as l -> tryHeadIList l
        | :? IReadOnlyList<'a> as l -> tryHeadIReadOnlyList l
        | xs ->
            use enm = xs.GetEnumerator()
            match enm.MoveNext() with
            | true -> ValueSome enm.Current
            | false -> ValueNone

    /// <summary>
    /// Have your cake and eat it!
    /// Return the first element of the seq and then the entire seq, without having consumed the first
    ///
    /// The downside is, if the original seq is not empty, then the returned seq MUST be consumed in some form
    /// in order to dispose of the original seq enumerator. 
    /// </summary>
    let tryHeadAndReturnUnmodified: 'a seq -> struct ('a voption * 'a seq) = function
        | :? List<'a> as l -> struct (tryHeadL l, l)
        | :? IList<'a> as l -> struct (tryHeadIList l, l)
        | :? IReadOnlyList<'a> as l -> struct (tryHeadIReadOnlyList l, l)
        | xs ->
            let enm = xs.GetEnumerator()

            if enm.MoveNext() |> not
            then
                enm.Dispose()
                struct (ValueNone, Seq.empty)
            else
                let first = enm.Current
                seq {
                    use _ = enm
                    yield first
                    while enm.MoveNext() do
                        yield enm.Current
                } |> tpl (ValueSome first)

    /// <summary>
    /// If the seq is not empty, return it. Otherwise return the default value
    ///
    /// Uses tryHeadAndReturnUnmodified. Read method comments there to see restrictions 
    /// </summary>
    let atLeast1 defaultValue seq =
        match tryHeadAndReturnUnmodified seq with
        | ValueNone, _ -> Seq.singleton defaultValue
        | ValueSome _, xs -> xs

    let tryMax (xs: 'a seq) =
        use enm = xs.GetEnumerator()
        match enm.MoveNext() with
        | false -> ValueNone
        | true ->
            let mutable max = enm.Current
            while enm.MoveNext() do
                if compare max enm.Current < 0
                then max <- enm.Current

            ValueSome max

    let rec private tryFindL' = function
        | struct (_, []) -> ValueNone
        | predicate, head::_ when predicate head -> ValueSome head
        | predicate, _::tail -> tryFindL' struct (predicate, tail)

    let private tryFindReadOnlyList' struct (predicate, xs: IReadOnlyList<_>) =
        let mutable i = 0
        let mutable result = ValueNone
        while ValueOption.isNone result && i < xs.Count do
            if predicate xs[i] then result <- ValueSome xs[i]
            else i <- i + 1

        result

    let tryFind predicate (xs: 'a seq) =
        match xs with
        | :? List<'a> as l -> tryFindL' struct (predicate, l)
        | :? IReadOnlyList<'a> as l -> tryFindReadOnlyList' struct (predicate, l)
        | xs -> 
            let mutable result = ValueNone
            use enm = xs.GetEnumerator()
            while ValueOption.isNone result && enm.MoveNext() do
                if predicate enm.Current
                then result <- ValueSome enm.Current

            result

    let emptyToNone (xs: _ seq) =
        match xs with
        | :? List<'a> as [] -> ValueNone
        | :? List<'a> -> ValueSome xs
        | :? IList<'a> as l when l.Count = 0 -> ValueNone
        | :? IList<'a> -> ValueSome xs
        | :? IReadOnlyList<'a> as l when l.Count = 0 -> ValueNone
        | :? IReadOnlyList<'a> -> ValueSome xs
        | xs ->
            let enm = xs.GetEnumerator()
            match enm.MoveNext() with
            | false ->
                enm.Dispose()
                ValueNone
            | true ->
                seq {
                    use _ = enm
                    yield enm.Current
                    while enm.MoveNext() do yield enm.Current
                } |> ValueSome

    let concat2 (xs: 'a seq) (ys: 'a seq) =
        match struct (emptyToNone xs, emptyToNone ys) with
        | ValueNone, ValueNone -> Seq.empty
        | ValueNone, ValueSome ys -> ys
        | ValueSome xs, ValueNone -> xs
        | ValueSome xs, ValueSome ys -> Seq.append xs ys

    // NOTE: not more efficient than normal group by. Just standardises a bit
    let groupBy f =
        Seq.groupBy f
        >> Seq.map structTpl

    // NOTE: not more efficient than normal group by. Just standardises a bit
    let groupByL f =
        List.groupBy f
        >> List.map structTpl

    [<ExcludeFromCodeCoverage>]
    let inline prependL x xs = x::xs

    let prepend (x: 'a) (xs: 'a seq) =
        seq {
            yield x
            for x' in xs do
                yield x'
        }

    let append (x: 'a) (xs: 'a seq) =
        seq {
            for x' in xs do
                yield x'
            yield x
        }

    let mapFst f xs =
        Seq.map (mapFst f) xs

    let mapSnd f xs =
        Seq.map (mapSnd f) xs

    [<ExcludeFromCodeCoverage>]
    let inline ofSeq<'a> (xs: ^a seq) = xs

    let singleOrDefault (xs: 'a seq) =
        use enm = xs.GetEnumerator()

        if enm.MoveNext()
        then
            let x = enm.Current
            if enm.MoveNext()
            then
                invalidOp "Expected 1 or 0 elements"
            else ValueSome x
        else ValueNone

    type ICachableEnumerable<'a> =
        inherit IEnumerable<'a>
        inherit IDisposable
        abstract member AsList: IReadOnlyList<'a>

[<RequireQualifiedAccess>]
module MapUtils =
        
    let fromKvp xs =
        xs
        |> Seq.map kvpToRefTuple
        |> Map.ofSeq

    let fromTuple xs =
        xs
        |> Seq.map toRefTuple
        |> Map.ofSeq

    let toSeq map =
        seq {
            use keys = (Map.keys map).GetEnumerator()
            while keys.MoveNext() do
                yield struct (keys.Current, Map.find keys.Current map)
        }

    let ofSeq seq =
        Seq.fold (fun map struct (k, v) -> Map.add k v map) Map.empty seq

    let tryFind = hashMapTryFind

    let tryPop key map =
        let value = tryFind key map
        struct (Map.remove key map, value)

    /// <summary>Adds if there is no record for the key added already. Returns true if an item was added</summary>
    let tryAdd key value map =
        match tryFind key map with
        | ValueNone -> Map.add key value map |> tpl true
        | ValueSome _ -> map |> tpl false

    let change f key value map =
        match tryFind key map with
        | ValueNone -> Map.add key (f key ValueNone value) map
        | ValueSome x ->
            Map.remove key map
            |> Map.add key (f key (ValueSome x) value)

    let private concat2Folder s struct (k, v) =
        match s with
        | ValueNone -> ValueNone
        | ValueSome s ->
            match tryAdd k v s with
            | false, _ -> ValueNone
            | true, s -> ValueSome s

    /// <summary>Returns None if there is a duplicate key</summary>    
    let rec concat2 map1 map2 =
        if Map.count map1 = 0 then ValueSome map2
        elif Map.count map2 = 0 then ValueSome map1
        elif Map.count map2 > Map.count map1 then concat2 map2 map1
        else toSeq map2 |> Seq.fold concat2Folder (ValueSome map1)

    let collectionMapAdd k v m =
        match tryFind k m with
        | ValueNone -> Map.add k [v] m
        | ValueSome xs -> Map.add k (v::xs) m

[<Struct; IsReadOnly>]
type Either<'a, 'b> =
    | Either1 of a: 'a
    | Either2 of 'b

[<RequireQualifiedAccess>]
module Either =
    [<ExcludeFromCodeCoverage>]
    let inline map1Of2 f x =
        match x with
        | Either1 x -> f x |> Either1
        | Either2 x -> x |> Either2

    [<ExcludeFromCodeCoverage>]
    let inline map2Of2 f x = 
        match x with
        | Either1 x -> x |> Either1
        | Either2 x -> f x |> Either2

    [<ExcludeFromCodeCoverage>]
    let inline reduce x = 
        match x with
        | Either1 x -> x
        | Either2 x -> x

    [<ExcludeFromCodeCoverage>]
    let inline ignore1 x = 
        match x with
        | Either1 _ -> ValueNone
        | Either2 x -> ValueSome x

    [<ExcludeFromCodeCoverage>]
    let inline ignore2 x = 
        match x with
        | Either1 x -> ValueSome x
        | Either2 _ -> ValueNone
        
    [<ExcludeFromCodeCoverage>]
    let inline expect1 x = match x with | Either1 x -> x | Either2 _ -> invalidOp "Expected 1"
    [<ExcludeFromCodeCoverage>]
    let inline expect2 x = match x with | Either2 x -> x | Either1 _ -> invalidOp "Expected 2"
    
    [<ExcludeFromCodeCoverage>]
    let inline take1 x = match x with | Either1 x -> ValueSome x | Either2 _ -> ValueNone
    [<ExcludeFromCodeCoverage>]
    let inline take2 x = match x with | Either1 _ -> ValueNone | Either2 x -> ValueSome x

    let partition xs =
        Collection.partition (function
            | Either1 _ -> true
            | Either2 _ -> false) xs
        |> mapFst (List.map (function | Either1 x -> x | _ -> invalidOp ""))
        |> mapSnd (List.map (function | Either2 x -> x | _ -> invalidOp ""))

    let partitionL xs =
        Collection.foldBackL (fun struct (e1, e2) -> function
            | Either1 x -> struct (x::e1, e2)
            | Either2 x -> struct (e1, x::e2)) struct ([], []) xs

[<RequireQualifiedAccess>]
module Maybe =
    
    [<RequireQualifiedAccess>]
    module Null =
        let toOption = function
            | null -> ValueNone
            | x -> ValueSome x

        let valToOption (x: System.Nullable<_>) =
            if x.HasValue then ValueSome x.Value
            else ValueNone
            
        let fromOption = function
            | ValueNone -> null
            | ValueSome x -> x
            
        let valFromOption x =
            match x with
            | ValueSome x -> Nullable<_>(x)
            | ValueNone -> Nullable<_>()
    
    [<ExcludeFromCodeCoverage>]
    let inline expectSomeErr err errState =
        ValueOption.defaultWith (fun _ -> sprintf err errState |> invalidOp)

    [<ExcludeFromCodeCoverage>]
    let inline defaultWith defThunk arg voption =
        match voption with
        | ValueNone -> defThunk arg
        | ValueSome v -> v

    [<ExcludeFromCodeCoverage>]
    let inline expectSome x = expectSomeErr "Expected some%s" "" x

    [<ExcludeFromCodeCoverage>]
    let inline traverseFn f arg =
        ValueOption.map (apply arg) f

    let tpl x y = ValueOption.bind (fun x -> ValueOption.map (tpl x) y) x

    [<ExcludeFromCodeCoverage>]
    let inline apply f x =
        match struct (f, x) with
        | ValueSome f', ValueSome x -> f' x |> ValueSome
        | _ -> ValueNone

    let private traverse' (xs: 'a voption seq) =
        seq {
            use enm = xs.GetEnumerator()
            while enm.MoveNext() do
                match enm.Current with
                | ValueSome x -> yield x
                | ValueNone -> ()
        }

    let traverse (xs: 'a voption seq) =
        match xs with
        | :? List<'a voption> as l when Collection.tryHead l |> ValueOption.isNone -> Seq.empty
        | :? IList<'a voption> as l when l.Count = 0 -> Seq.empty
        | :? IReadOnlyList<'a voption> as l when l.Count = 0 -> Seq.empty
        | xs -> traverse' xs

    let toList = function
        | ValueNone -> []
        | ValueSome x -> [x]

    let fromRef = function
        | None -> ValueNone
        | Some x -> ValueSome x

[<RequireQualifiedAccess>]
module Io =
    let private caf (x: Task<'a>) = x.ConfigureAwait(false)
    let private caf0 (x: Task) = x.ConfigureAwait(false)
    let cafV (x: ValueTask<'a>) = x.ConfigureAwait(false)
    let private cafV0 (x: ValueTask) = x.ConfigureAwait(false)

    /// <summary>Use sparingly. Map/bind/task {} etc... are more performant abstractions</summary>
    let execute (vt: ValueTask<_>) =
        if vt.IsCompletedSuccessfully then vt.Result
        else vt.AsTask().ConfigureAwait(false).GetAwaiter().GetResult()

    let retn (x: 'a) = ValueTask<'a>(x)

    let trySyncResult (task: ValueTask<'a>) =
        if task.IsCompletedSuccessfully then ValueSome task.Result
        elif task.IsCompleted then ValueSome (task.AsTask().Result)
        else ValueNone

    let fromTask (task: Task<'a>) = ValueTask<'a> task

    let toTask (task: ValueTask<'a>) = task.AsTask()

    /// <summary>Retains synchronicity if input task is complete</summary>
    let map (f: 'a -> 'b) (x: ValueTask<'a>): ValueTask<'b> =
        match trySyncResult x with
        | ValueSome x -> ValueTask<'b>(f x)
        | ValueNone ->
            task {
                let! x' = x |> cafV
                return (f x')
            } |> ValueTask<'b>

    /// <summary>Retains synchronicity if input task is complete</summary>
    let bind f (x: ValueTask<'a>): ValueTask<'b> =
        match trySyncResult x with
        | ValueSome x -> f x
        | ValueNone ->
            task {
                let! x' = x |> cafV
                return! (f x') |> cafV
            } |> ValueTask<'b>

    /// <summary>Retains synchronicity if input task is complete</summary>
    let ignore<'a> (x: ValueTask<'a>): ValueTask<unit> =
        match trySyncResult x with
        | ValueSome _ -> ValueTask<unit>(())
        | ValueNone ->
            task {
                let! _ = x |> cafV
                return ()
            } |> ValueTask<unit>

    let onError (action: unit -> ValueTask<'b>) onErr =
        // This function is just an awkward way of logging and re-throwing an error
        // Can't figure out how to combine task/try-with/reraise

        let syncError =
            try
                action()
            with
            | e ->
                onErr e
                reraise()

        if syncError.IsCompleted then syncError
        else
            let task = syncError.AsTask()
            do
                task.ContinueWith(fun (t: Task<'b>) -> if t.IsFaulted then onErr t.Exception)
                |> caf0
                |> ignoreTyped<ConfiguredTaskAwaitable>

            ValueTask<'b>(task)

    /// <summary>Recover from an exception in an async operation</summary>
    let recover (f: unit -> ValueTask<'a>) (fRecovery: exn -> 'a) =
        try
            let x = f()
            match trySyncResult x with
            | ValueSome x -> ValueTask<'a>(x)
            | ValueNone ->
                task {
                    try return! x |> cafV
                    with | e -> return fRecovery e
                } |> ValueTask<'a>
        with
        | e -> fRecovery e |> ValueTask<'a>

    type MList<'a> = System.Collections.Generic.List<'a>
    let private processTraverseResult = function
        | struct (err: MList<exn>, output: MList<'a>) when err.Count = 0 ->
            List.ofSeq output
        | err, _ -> AggregateException err |> raise

    /// <summary>Retains synchronicity if all input tasks are complete. Thrown exceptions might be out of order</summary>
    let traverse (x: ValueTask<'a> seq): ValueTask<'a list> =
        let err = MList<exn>()
        let await = MList<Task>()
        let output = MList<'a>()
        let enm = x.GetEnumerator()

        while enm.MoveNext() do
            if enm.Current.IsCompletedSuccessfully
            then output.Add(enm.Current.Result)
            elif enm.Current.IsCompleted
            then
                try
                    output.Add(enm.Current.AsTask().Result)
                with
                | e -> err.Add(e)
            else
                let i = output.Count
                output.Add(Unchecked.defaultof<'a>)
                let t =
                    task {
                        let! x = enm.Current |> cafV
                        try
                            output[i] <- x
                        with
                        | e -> err.Add(e)

                        return ()
                    }

                await.Add(t)

        if await.Count = 0
        then ValueTask<'a list>(processTraverseResult struct (err, output))
        else
            task {
                let! _ = Task.WhenAll await |> caf0
                return processTraverseResult struct (err, output)
            } |> ValueTask<'a list>

    let private vtU = ValueTask<unit>(()).Preserve() 
    let normalizeVt (valueTask: ValueTask) =
        if valueTask.IsCompletedSuccessfully then ValueTask<unit>(())
        elif valueTask.IsCompleted then
            valueTask.AsTask().Wait()
            vtU
        else
            task {
                do! valueTask |> cafV0
                return ()
            } |> ValueTask<unit>

    let deNormalizeVt (valueTask: ValueTask<unit>) =
        match trySyncResult valueTask with
#if NETSTANDARD2_0
        | ValueSome _ -> ValueTask()
#else
        | ValueSome _ -> ValueTask.CompletedTask
#endif
        | ValueNone ->
            task {
                do! valueTask |> cafV
                return ()
            } |> ValueTask

    let private asAction (f: unit -> unit): System.Action = f

    // should not be public, but needs some complex testing
    // very difficult (niche) case where it might be used organically
    /// <summary>Less efficient version of Task.WaitAsync</summary>
    let addCancellationTokenNetStandard (c: CancellationToken) (t: Task<'a>) =
        let source = TaskCompletionSource<'a>(c)
        let mutable i = 0

        let mutable unRegister = Unchecked.defaultof<CancellationTokenRegistration>
        let cancel () = 
            if Interlocked.Increment(&i) = 1
            then
                source.SetCanceled()
                unRegister.Dispose()

        unRegister <- c.Register(cancel)

        task {
            try
                let! x = t |> caf
                if Interlocked.Increment(&i) = 1
                then
                    source.SetResult x
                    unRegister.Dispose()
            with
            | e -> 
                if Interlocked.Increment(&i) = 1
                then
                    source.SetException e
                    unRegister.Dispose()
        } |> ignoreTyped<Task<unit>>

        source.Task

    let addCancellationToken (c: CancellationToken) (t: ValueTask<'a>) =
        if t.IsCompleted || c = CancellationToken.None then t
#if NETSTANDARD2_0
        else t.AsTask() |> addCancellationTokenNetStandard c |> ValueTask<'a>
#else
        else t.AsTask().WaitAsync(c) |> ValueTask<'a>
#endif

    let apply x = x |> flip map |> bind

    [<ExcludeFromCodeCoverage>]
    let inline ignoreTask<'a> (x: Task<^a>): Task = x

module Comparison =
    
    let propertyComparer<'a, 'prop> prop (eq: IEqualityComparer<'prop>) =
        
        { new IEqualityComparer<'a> with
            override this.Equals(x, y) = eq.Equals(prop x, prop y)

            override this.GetHashCode(obj) = eq.GetHashCode(prop obj) }
    
    let tplFstComparer (eq: IEqualityComparer<'a>) = propertyComparer fstT eq
    let tplSndComparer (eq: IEqualityComparer<'a>) = propertyComparer sndT eq

    let rec compareArrays fromI toI xs ys =
        match struct (xs, ys) with
        | _ when fromI > toI -> true
        | x, y when fromI >= Array.length x && fromI >= Array.length y -> true
        | x, y when fromI >= Array.length x || fromI >= Array.length y -> false
        | x, y ->
            if Array.get x fromI <> Array.get y fromI
            then false
            else compareArrays (fromI + 1) toI xs ys

    let private arrayComparer'<'a when 'a: equality> hashElements =
        if hashElements < 0 then invalidArg (nameof hashElements) "< 0"
        let hashElements = Math.Ceiling(float hashElements / 2.0) |> int

        { new IEqualityComparer<'a array> with
            override this.Equals(x, y) =
                if Array.length x <> Array.length y
                then false
                else compareArrays 0 Int32.MaxValue x y

            override this.GetHashCode(obj) =
                let mutable h = Array.length obj
                let mutable i = 0

                while i < hashElements && i < Array.length obj do
                    h <- HashCode.Combine(h, Array.get obj i)
                    i <- i + 1

                i <- Array.length obj - 1
                while i >= hashElements do
                    h <- HashCode.Combine(h, Array.get obj i)
                    i <- i - 1

                h
        }

    type ArrayComparerCache<'a when 'a: equality> =
        static member create =
            let inline kSelector (x: int) = x
            memoize (ValueSome (50, 100)) kSelector arrayComparer'<'a>

    [<ExcludeFromCodeCoverage>]
    let inline arrayComparer<'a when 'a: equality> hashElements = ArrayComparerCache<'a>.create hashElements |> sndT

    let private seqComparer'<'a when 'a: comparison> () =

        { new IComparer<'a seq> with
            override this.Compare(x, y) =
                use enmX = x.GetEnumerator()
                use enmY = y.GetEnumerator()

                let mutable cmp = 0
                while cmp = 0 && enmX.MoveNext() do
                    if enmY.MoveNext() |> not
                    then cmp <- 1
                    else cmp <- compare enmX.Current enmY.Current

                if cmp = 0 && enmY.MoveNext() then -1
                else 0
        }

    type SeqComparerCache<'a when 'a: comparison> =
        static member value = seqComparer'<'a>()

    let seqComparer<'a when 'a: comparison> = SeqComparerCache<'a>.value

module Disposable =
    let nothingToDispose =
        { new IDisposable with
            member this.Dispose() = () }

    let rec private disposeList' = function
        | struct ([], []) -> ()
        | [], errs ->
            AggregateException (List.rev errs) |> raise
        | head:IDisposable::tail, errs ->
            let errs =
                try
                    head.Dispose()
                    errs
                with
                | e -> e::errs

            disposeList' struct (tail, errs)

    let disposeList = flip tpl [] >> disposeList'

    let combine (d1: IDisposable) (d2: IDisposable) =
        { new IDisposable with
            member this.Dispose() = disposeList [d1; d2] }

    let conditional<'a when 'a :> IDisposable> condition (x: 'a) =
        { new IDisposable with
            member this.Dispose() =
                if condition x then x.Dispose() }

    /// <summary>
    /// Dispose of items in a catch block with error handling
    /// Any disposables that throw will be encapsulated in an aggregate exception and re-thrown
    /// If an exception is re-thrown, the original exception will be included in the aggregate
    /// </summary>
    let handleCatch (e: exn) disposables =
        try
            disposeList disposables
            []
        with
        | :? AggregateException as e -> e.InnerExceptions |> List.ofSeq
        | e -> [e]
        |> tpl e
        |> function
            | _, [] -> []
            | :? AggregateException as e, es -> (e.InnerExceptions |> List.ofSeq) @ es
            | e, es -> e::es
        |> function
            | [] -> ()
            | es -> es |> AggregateException |> raise

module CSharp =
    
    type private MList<'a> = System.Collections.Generic.List<'a>