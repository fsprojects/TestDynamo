﻿namespace TestDynamo.Model

open System
open System.Collections.Generic
open System.Diagnostics.CodeAnalysis
open System.Runtime.InteropServices
open System.Text
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open System.Runtime.CompilerServices

type AttributeType =
    | String
    | Number
    | Boolean
    | Binary
    | HashMap
    | StringHashSet
    | BinaryHashSet
    | NumberHashSet
    | AttributeList
    | Null

    static member describeHashSet =
        let desc = AttributeType.describe >> sprintf "%sS"
        let inline k (t: AttributeType) = t 
        memoize (ValueSome (100, 200)) k desc >> sndT

    static member describe = function
        | String -> "S"
        | Number -> "N"
        | Boolean -> "BOOL"
        | Binary -> "B"
        | HashMap -> "M"
        | AttributeList -> "L"
        | Null -> "NULL"
        | StringHashSet -> "SS"
        | NumberHashSet -> "NS"
        | BinaryHashSet -> "BS"

    static member tryParse value =
        match value with
        | "S" -> AttributeType.String |> ValueSome
        | "N" -> AttributeType.Number |> ValueSome
        | "BOOL" -> AttributeType.Boolean |> ValueSome
        | "B" -> AttributeType.Binary |> ValueSome
        | "M" -> AttributeType.HashMap |> ValueSome
        | "SS" -> AttributeType.StringHashSet |> ValueSome
        | "NS" -> AttributeType.NumberHashSet |> ValueSome
        | "BS" -> AttributeType.BinaryHashSet |> ValueSome
        | "L" -> AttributeType.AttributeList |> ValueSome
        | "NULL" -> AttributeType.Null |> ValueSome
        | _ -> ValueNone

    static member parse value =
        AttributeType.tryParse value
        |> Maybe.expectSomeErr "Invalid AttributeType %s" value

/// <summary>
/// A set of attribute values which have a specified type
/// </summary>
[<CustomEquality; CustomComparison>]
type AttributeSet =
    private
    | As of struct (AttributeType * Set<AttributeValue>)

    with
    static member create strs =
        Seq.fold (fun struct (t, s) x ->
            let ``type`` = t ?|? AttributeValue.getType x

            match AttributeValue.asType ``type`` x with
            | ValueNone -> ClientError.clientError "Found multiple types in single set"
            | ValueSome x ->
                match SetUtils.add x s with
                | false, _ -> ClientError.clientError $"Duplicate value in {``type``} set"
                | true, s -> struct (ValueSome ``type``, s)) struct (ValueNone, Set.empty) strs
        |> function
            | _, xs when xs = Set.empty -> ClientError.clientError $"Empty set not supported"
            | ValueNone, _ -> ClientError.clientError $"Empty set not supported"
            | ValueSome t, xs -> struct (t, xs)
        |> As

    static member asSet (As x) = sndT x
    static member getSetType (As x) = fstT x

    static member asBinarySeq = function
        | As struct (t, x) when t <> AttributeType.Binary ->
            ClientError.clientError $"Set has type {t}, not {AttributeType.Binary}"
        | As struct (_, xs) ->
            Seq.map (function
                | Binary x -> x
                | x -> ClientError.clientError $"Set item {x} is not {AttributeType.Binary}") xs
    static member asNumberSeq = function
        | As struct (t, x) when t <> AttributeType.Number ->
            ClientError.clientError $"Set has type {t}, not {AttributeType.Number}"
        | As struct (_, xs) ->
            Seq.map (function
                | Number x -> x
                | x -> ClientError.clientError $"Set item {x} is not {AttributeType.Number}") xs
    static member asStringSeq = function
        | As struct (t, x) when t <> AttributeType.String ->
            ClientError.clientError $"Set has type {t}, not {AttributeType.String}"
        | As struct (_, xs) ->
            Seq.map (function
                | String x -> x
                | x -> ClientError.clientError $"Set item {x} is not {AttributeType.String}") xs

    static member contains value (As struct (_, set)) = Set.contains value set

    /// <summary>Returns none if union types do not match</summary>
    static member private trySetOperation op = function
        | struct (As (t1, set1), As (t2, set2)) when t1 = t2 ->
            op set1 set2 |> tpl t1 |> ValueSome
        | _ -> ValueNone

    /// <summary>Returns none if union types do not match</summary>
    static member tryOperation =
        AttributeSet.trySetOperation >>> ValueOption.map As

    /// <summary>
    /// Evaluates to "true" if all elements of the first set are in the second
    /// Returns none if set types do not match
    /// </summary>
    static member isSubset = AttributeSet.trySetOperation Set.isSubset >> ValueOption.map sndT

    /// <summary>Returns none if set types do not match</summary>
    static member tryUnion = AttributeSet.tryOperation Set.union

    /// <summary>Outer None signals set types do not match, inner None signals the result is an empty set</summary>
    static member tryXOr xs =
        AttributeSet.isSubset xs
        ?>>= (function
            | true -> ValueSome ValueNone
            | false -> AttributeSet.tryOperation Set.difference xs ?|> ValueSome)

    interface IComparable with
        [<ExcludeFromCodeCoverage>] // no need to test all cases here
        member this.CompareTo obj =
            match struct (this, obj) with
            | As (tX, x), (:? AttributeSet as (As (tY, y))) ->
                match struct (tX, tY) with
                | AttributeType.Binary, AttributeType.Binary
                | AttributeType.String, AttributeType.String
                | AttributeType.Number, AttributeType.Number -> compare x y
                | AttributeType.String, _ -> -1
                | _, AttributeType.String -> 1
                | AttributeType.Number, _ -> -1
                | _, AttributeType.Number -> 1
                | AttributeType.Boolean, _ -> -1
                | _, AttributeType.Boolean -> 1
                | AttributeType.Binary, _ -> -1
                | _, AttributeType.Binary -> 1
                | AttributeType.StringHashSet, _ -> -1
                | _, AttributeType.StringHashSet -> 1
                | AttributeType.NumberHashSet, _ -> -1
                | _, AttributeType.NumberHashSet -> 1
                | AttributeType.BinaryHashSet, _ -> -1
                | _, AttributeType.BinaryHashSet -> 1
                | AttributeType.HashMap, _ -> -1
                | _, AttributeType.HashMap -> 1
                | AttributeType.AttributeList, _ -> -1
                | _, AttributeType.AttributeList -> 1
                | AttributeType.Null, _ -> -1
            | _ -> invalidArg "obj" "cannot compare value of different types"

    override this.Equals(yobj) =
        match struct (this, yobj) with
        | As x, (:? AttributeSet as (As y)) -> x = y
        | _ -> false

    override this.GetHashCode() =
        match this with | As x -> x.GetHashCode()

    override this.ToString() =
        match this with | As (_, x) -> x.ToString()

and RequiredConversion = bool

and    
    /// <summary>
    /// NoComparison with IComparable so as to avoid boxing
    /// Use static compare method instead
    /// </summary>
    [<IsReadOnly; Struct; NoComparison; CustomEquality>]
    AttributeListType =
    | SparseList of m: Map<uint, AttributeValue>
    | CompressedList of AttributeValue array

    with

    /// <summary>Returns None if the index is out of range</summary>
    static member set i value ``to`` =
        match ``to`` with
        | SparseList t ->
            Map.add (uint i) value t |> SparseList |> ValueSome
        | CompressedList t ->
            Collection.tryAddOrReplaceArr t i value ?|> CompressedList

    /// <summary>Does nothing if the index is out of range</summary>
    static member remove i ``to`` =
        match ``to`` with
        | SparseList t -> Map.remove (uint i) t |> SparseList
        | CompressedList t & t' when Array.length t <= i -> t'
        | CompressedList t -> Array.removeAt i t |> CompressedList

    [<ExcludeFromCodeCoverage>] // coverage doesn't seem to pick this up
    static member private emptySparse = Map.empty |> SparseList
    [<ExcludeFromCodeCoverage>] // coverage doesn't seem to pick this up
    static member private emptyCompressed = [||] |> CompressedList

    static member asSparse = function
        | CompressedList xs when xs.Length = 0 -> AttributeListType.emptySparse
        | CompressedList _ & xs -> xs |> AttributeListType.asMap |> SparseList
        | SparseList _ & xs -> xs

    static member asCompressed = function
        | CompressedList _ & xs -> xs
        | SparseList xs when Map.isEmpty xs -> AttributeListType.emptyCompressed
        | SparseList _ & xs -> xs |> AttributeListType.asArray |> CompressedList

    static member asSeq = AttributeListType.asSeqWithMetadata >> sndT

    static member asSeqWithMetadata: AttributeListType -> struct (RequiredConversion * AttributeValue seq) = function
        | CompressedList xs -> struct (false, xs)
        | SparseList xs -> MapUtils.toSeq xs |> Seq.sortBy fstT |> Seq.map sndT |> tpl true

    static member asArray = AttributeListType.asArrayWithMetadata >> sndT

    static member asArrayWithMetadata: AttributeListType -> struct (RequiredConversion * AttributeValue array) = function
        | CompressedList xs -> struct (false, xs)
        | SparseList _ & xs -> struct (true, AttributeListType.asSeq xs |> Array.ofSeq)

    static member asMap = AttributeListType.asMapWithMetadata >> sndT

    static member asMapWithMetadata: AttributeListType -> struct (RequiredConversion * Map<uint,AttributeValue>) = function
        | CompressedList xs -> struct (true, xs |> Seq.mapi (tpl >>> mapFst uint) |> MapUtils.ofSeq)
        | SparseList xs -> struct (false, xs)

    static member length = function
        | CompressedList xs -> Array.length xs
        | SparseList xs -> Map.count xs

    static member tryFind i = function
        | CompressedList xs -> Collection.tryGetArr xs i
        | SparseList xs -> MapUtils.tryFind (uint i) xs

    static member find = AttributeListType.tryFind >>> ValueOption.defaultWith (fun _ -> IndexOutOfRangeException() |> raise)

    static member private attributeValueArrayComparer: IEqualityComparer<AttributeValue array> = Comparison.arrayComparer<AttributeValue> 10

    static member compare = function
        | struct (SparseList v1, SparseList v2) -> compare v1 v2
        | CompressedList v1, CompressedList v2 -> compare v1 v2
        | SparseList _ & v1, CompressedList v2 -> Comparison.seqComparer<AttributeValue>.Compare(AttributeListType.asSeq v1, v2)
        | CompressedList v1, SparseList _ & v2 -> Comparison.seqComparer<AttributeValue>.Compare(v1, AttributeListType.asSeq v2)

    static member equals = function
        | struct (SparseList this, SparseList ys) -> this = ys
        | CompressedList this, CompressedList ys -> AttributeListType.attributeValueArrayComparer.Equals(this, ys)
        | SparseList _ & sl, CompressedList l
        | CompressedList l, SparseList _ & sl -> Comparison.seqComparer<AttributeValue>.Compare(l, AttributeListType.asSeq sl) = 0

    static member getHashCode = function
        | CompressedList v1 -> HashCode.Combine(1, AttributeListType.attributeValueArrayComparer.GetHashCode(v1))
        | SparseList _ & v1 -> HashCode.Combine(2, AttributeListType.attributeValueArrayComparer.GetHashCode(
            AttributeListType.asArray v1))

    static member private appendKeyOffset = Map.keys >> Collection.tryMax >> ValueOption.map ((+)1u) >> ValueOption.defaultValue 0u
    static member append = function
        | struct (CompressedList xs, CompressedList ys) -> Array.concat [xs; ys] |> CompressedList
        | SparseList xs, SparseList ys ->
            let offset = AttributeListType.appendKeyOffset xs
            MapUtils.toSeq ys
            |> Seq.fold (fun s struct (k, v) -> Map.add (k + offset) v s) xs
            |> SparseList
        | CompressedList xsl & xs, SparseList ys ->
            let xsSparse = AttributeListType.asMap xs
            MapUtils.toSeq ys
            |> Seq.fold (fun s struct (k, v) -> Map.add (k + uint xsl.Length) v s) xsSparse
            |> SparseList
        | SparseList xs, CompressedList ys ->
            let offset = AttributeListType.appendKeyOffset xs
            Seq.mapi tpl ys
            |> Seq.fold (fun s struct (k, v) -> Map.add (uint k + offset) v s) xs
            |> SparseList

    override this.Equals(yobj) =
        match yobj with
        | :? AttributeListType as y -> AttributeListType.equals struct(this, y)
        | _ -> false

    override this.GetHashCode() = AttributeListType.getHashCode this

    override this.ToString() =
        AttributeListType.asSeq this
        |> Seq.map toString
        |> Str.join " "
        |> sprintf "[%s]"

and        
    /// <summary>
    /// A dynamodb attribute of any valid type
    /// Implements equality and comparison
    /// </summary>
    [<CustomEquality; CustomComparison>]
    AttributeValue =
    | Null
    | String of string
    | Number of decimal
    | Binary of byte array
    | Boolean of bool
    | HashMap of Map<string, AttributeValue>
    | HashSet of AttributeSet
    | AttributeList of AttributeListType

    with

    /// <summary>
    /// The type shorthand of the dynamodb object.
    /// Values are "S", "N", "B", "BOOL", "M", "SS", "NS", "BS", "L", "NULL" 
    /// </summary>
    member this.AttributeType = this |> AttributeValue.getType |> toString

    member this.TryString([<Out>] value: byref<string>) =
        match this with
        | String x ->
            value <- x
            true
        | _ -> false

    member this.TryNumber([<Out>] value: byref<decimal>) =
        match this with
        | Number x ->
            value <- x
            true
        | _ -> false

    member this.TryBinary([<Out>] value: byref<IReadOnlyList<byte>>) =
        match this with
        | Binary x ->
            value <- x
            true
        | _ -> false

    member this.TryBoolean([<Out>] value: byref<bool>) =
        match this with
        | Boolean x ->
            value <- x
            true
        | _ -> false

    member this.TryMap([<Out>] value: byref<IReadOnlyDictionary<string, AttributeValue>>) =
        match this with
        | HashMap x ->
            value <- x
            true
        | _ -> false

    member this.TryStringSet([<Out>] value: byref<Set<AttributeValue>>) =
        match this with
        | HashSet x when AttributeSet.getSetType x = AttributeType.String ->
            value <- AttributeSet.asSet x
            true
        | _ -> false

    member this.TryNumberSet([<Out>] value: byref<Set<AttributeValue>>) =
        match this with
        | HashSet x when AttributeSet.getSetType x = AttributeType.Number ->
            value <- AttributeSet.asSet x
            true
        | _ -> false

    member this.TryBinarySet([<Out>] value: byref<Set<AttributeValue>>) =
        match this with
        | HashSet x when AttributeSet.getSetType x = AttributeType.Binary ->
            value <- AttributeSet.asSet x
            true
        | _ -> false

    member this.TryList([<Out>] value: byref<IReadOnlyList<AttributeValue>>) =
        match this with
        | AttributeList x ->
            // should not have to do any conversion here
            // data at reset should always be in array format
            value <- AttributeListType.asArray x
            true
        | _ -> false

    /// <summary>Return this as a string or throw an exception</summary>
    member this.S =
        let mutable x = Unchecked.defaultof<_>
        match this.TryString(&x) with
        | true -> x
        | false -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not a string"

    /// <summary>Return this as a number or throw an exception</summary>
    member this.N =
        let mutable x = Unchecked.defaultof<_>
        match this.TryNumber(&x) with
        | true -> x
        | false -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not a number"

    /// <summary>Return this as binary data or throw an exception</summary>
    member this.B =
        let mutable x = Unchecked.defaultof<IReadOnlyList<byte>>
        match this.TryBinary(&x) with
        | true -> x
        | false -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not binary data"

    /// <summary>Return this as a boolean or throw an exception</summary>
    member this.BOOL =
        let mutable x = Unchecked.defaultof<_>
        match this.TryBoolean(&x) with
        | true -> x
        | false -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not a boolean"

    /// <summary>Return this as a map or throw an exception</summary>
    member this.M =
        let mutable x = Unchecked.defaultof<_>
        match this.TryMap(&x) with
        | true -> x
        | false -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not a map"

    /// <summary>Return this as a set or throw an exception. All of the values in the set will be strings</summary>
    member this.SS =
        let mutable x = Unchecked.defaultof<_>
        match this.TryStringSet(&x) with
        | true -> x
        | x -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not a string set"

    /// <summary>Return this as a set or throw an exception. All of the values in the set will be numbers</summary>
    member this.NS =
        let mutable x = Unchecked.defaultof<_>
        match this.TryNumberSet(&x) with
        | true -> x
        | false -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not a number set"

    /// <summary>Return this as a set or throw an exception. All of the values in the set will be binary data</summary>
    member this.BS =
        let mutable x = Unchecked.defaultof<_>
        match this.TryBinarySet(&x) with
        | true -> x
        | x -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not a binary set"

    /// <summary>Return this as a list or throw an exception</summary>
    member this.L =
        let mutable x = Unchecked.defaultof<_>
        match this.TryList(&x) with
        | true -> x
        | false -> invalidOp $"Attribute value of type {AttributeValue.getType this} is not a list"

    interface IComparable with
        [<ExcludeFromCodeCoverage>] // no need to test all cases here
        member this.CompareTo obj =
            match obj with
            | :? AttributeValue as y ->
                match struct (this, y) with
                | String v1, String v2 -> compare v1 v2
                | Number v1, Number v2 -> compare v1 v2
                | Boolean v1, Boolean v2 -> compare v1 v2
                | Binary v1, Binary v2 -> compare v1 v2
                | HashMap v1, HashMap v2 -> compare v1 v2
                | HashSet v1, HashSet v2 -> compare v1 v2
                | AttributeList v1, AttributeList v2 -> AttributeListType.compare struct (v1, v2)
                | Null, Null -> 0
                | String _, _ -> -1
                | _, String _ -> 1
                | Number _, _ -> -1
                | _, Number _ -> 1
                | Boolean _, _ -> -1
                | _, Boolean _ -> 1
                | Binary _, _ -> -1
                | _, Binary _ -> 1
                | HashSet _, _ -> -1
                | _, HashSet _ -> 1
                | HashMap _, _ -> -1
                | _, HashMap _ -> 1
                | AttributeList _, _ -> -1
                | _, AttributeList _ -> 1
            | _ -> invalidArg "obj" "cannot compare value of different types"

    override this.ToString() =
        match this with
        | Null -> "NULL"
        | String x -> x
        | Number x -> x.ToString()
        | Binary x -> x.ToString()
        | Boolean x -> x.ToString()
        | HashMap x -> x.ToString()
        | HashSet x -> x.ToString()
        | AttributeList x -> x.ToString()

    override this.Equals(yobj) =
        match yobj with
        | :? AttributeValue as y ->
            match struct (this, y) with
            | Binary this, Binary ys ->
                AttributeValue.byteArrayComparer.Equals(this, ys)
            | this, ys -> (this :> IComparable).CompareTo ys = 0
        | _ -> false

    override this.GetHashCode() =
        match this with
        | Null -> 0
        | String v1 -> HashCode.Combine(1, v1)
        | Number v1 -> HashCode.Combine(2, v1)
        | Binary v1 -> HashCode.Combine(4, AttributeValue.byteArrayComparer.GetHashCode v1)
        | Boolean v1 -> HashCode.Combine(3, v1)
        | HashMap v1 -> HashCode.Combine(5, v1)
        | HashSet v1 -> HashCode.Combine(6, v1)
        | AttributeList v1 -> HashCode.Combine(7, AttributeListType.getHashCode v1)

    static member private byteArrayComparer: IEqualityComparer<byte array> = Comparison.arrayComparer<byte> 10

    static member private getHashSetType = function
        | AttributeType.String -> StringHashSet
        | AttributeType.Number -> NumberHashSet
        | AttributeType.Binary -> BinaryHashSet
        | x -> invalidOp $"{x} is not a valid Set type"

    static member getType (value: AttributeValue) =
        match value with
        | Null -> AttributeType.Null
        | String _ -> AttributeType.String
        | Number _ -> AttributeType.Number
        | Binary _ -> AttributeType.Binary
        | Boolean _ -> AttributeType.Boolean
        | HashMap _ -> AttributeType.HashMap
        | HashSet x -> AttributeSet.getSetType x |> AttributeValue.getHashSetType
        | AttributeList _ -> AttributeType.AttributeList

    static member private compressSparseLists': AttributeValue -> struct (RequiredConversion * AttributeValue) = function
        | Null & x
        | Binary _ & x
        | String _ & x
        | Number _ & x
        | Boolean _ & x
        | HashSet _ & x -> struct (false, x)
        | HashMap map & x ->
            Map.fold (fun (struct (modified, map) & s) k v ->
                match AttributeValue.compressSparseLists' v with
                | true, v' -> Map.add k v' map |> tpl true
                | false, _ -> s) struct (false, map) map
            |> mapSnd HashMap
        | AttributeList list & fullList ->
            // quite optimised to avoid allocations, includes mutations
            let struct (arrChanged, arr') = AttributeListType.asArrayWithMetadata list

            // if arrChanged, then this is a new array, not referenced by anything else: safe to mutate
            let mutable mutableArray = if arrChanged then arr' else  Unchecked.defaultof<AttributeValue array>
            let mutable i = 0

            while i < arr'.Length do
                match AttributeValue.compressSparseLists' arr'[i] with
                | false, _ -> ()
                | true, x ->
                    if mutableArray = null then mutableArray <- Array.copy arr'
                    Array.set mutableArray i x

                i <- i + 1

            match mutableArray with
            | null -> struct (false, fullList)
            | x -> struct (true, x |> CompressedList |> AttributeList)

    static member compressSparseLists = AttributeValue.compressSparseLists' >> sndT

    static member describe v =
        let typ = AttributeValue.getType v |> AttributeType.describe
        match v with
        | Null -> typ
        | Binary _ -> $"{typ}:BINARY_DATA"
        | String x -> $"{typ}:{x}"
        | Number x -> $"{typ}:{x}"
        | Boolean x -> $"{typ}:{x}"
        | HashMap _ -> $"{typ}:MAP"
        | HashSet _ -> $"{typ}:SET"
        | AttributeList _ -> $"{typ}:LIST"

    [<ExcludeFromCodeCoverage>] // extensive testing here is a bit of a waste of time
    static member asType ``type`` value =
        match value with
        | Null -> if ``type`` = AttributeType.Null then ValueSome value else ValueNone
        | String _ -> if ``type`` = AttributeType.String then ValueSome value else ValueNone
        | Number _ -> if ``type`` = AttributeType.Number then ValueSome value else ValueNone
        | Binary _ -> if ``type`` = AttributeType.Binary then ValueSome value else ValueNone
        | Boolean _ -> if ``type`` = AttributeType.Boolean then ValueSome value else ValueNone
        | HashMap _ -> if ``type`` = AttributeType.HashMap then ValueSome value else ValueNone
        | HashSet hs when ``type`` = AttributeType.StringHashSet && AttributeSet.getSetType hs = AttributeType.String -> ValueSome value
        | HashSet hs when ``type`` = AttributeType.NumberHashSet && AttributeSet.getSetType hs = AttributeType.Number -> ValueSome value
        | HashSet hs when ``type`` = AttributeType.BinaryHashSet && AttributeSet.getSetType hs = AttributeType.Binary -> ValueSome value
        | HashSet _ -> ValueNone
        | AttributeList _ -> if ``type`` = AttributeType.AttributeList then ValueSome value else ValueNone

    static member compare x y =
        match struct (x, y) with
        | Null, Null -> 0 |> ValueSome
        | String x', String y' -> compare x' y' |> ValueSome
        | Boolean x', Boolean y' -> compare x' y' |> ValueSome
        | Number x', Number y' -> compare x' y' |> ValueSome
        | Binary x', Binary y' -> compare x' y' |> ValueSome
        | AttributeList x', AttributeList y' -> AttributeListType.compare struct (x', y') |> ValueSome
        | HashSet x', HashSet y' -> compare x' y' |> ValueSome
        | HashMap x', HashMap y' -> compare x' y' |> ValueSome
        | String _, _ -> ValueNone
        | Boolean _, _ -> ValueNone
        | Number _, _ -> ValueNone
        | Binary _, _ -> ValueNone
        | AttributeList _, _ -> ValueNone
        | HashSet _, _ -> ValueNone
        | HashMap _, _ -> ValueNone
        | Null, _ -> ValueNone

/// <summary>
/// A lookup result on a typed value
/// </summary>
[<Struct; IsReadOnly>]
type AttributeLookupResult =
    /// <summary>
    /// Typed value was found
    /// </summary>
    | HasValue of a: AttributeValue

    /// <summary>
    /// Typed value was not found
    /// </summary>
    | NoValue

    /// <summary>
    /// A value was found but the type is incorrect
    /// </summary>
    | ExpectedType of AttributeType

    with

    static member asOption = function
        | HasValue x -> ValueSome x
        | _ -> ValueNone

[<Struct; IsReadOnly>]
type AttributeSelector =
    { attributeDescription: string
      attributeName: string
      attributeType: AttributeType }

    with

    static member describe selector =
        $"{selector.attributeDescription} - {selector.attributeName}: {AttributeType.describe selector.attributeType}"

    static member test item selector =
        match MapUtils.tryFind selector.attributeName item with
        | ValueNone -> NoValue
        | ValueSome x when AttributeValue.asType selector.attributeType x |> ValueOption.isSome -> HasValue x
        | ValueSome _ -> ExpectedType selector.attributeType

    static member get item selector =
        match AttributeSelector.test item selector with
        | NoValue -> ClientError.clientError $"""Missing {selector.attributeDescription} "{selector.attributeName}" for item {item}"""
        | ExpectedType t -> ClientError.clientError $"""Expected {selector.attributeDescription} "{selector.attributeName}" to have type "{t}" for item {item}"""
        | HasValue x -> x

    static member asTpl selector = struct (selector.attributeName, selector.attributeType)

type ItemSize =
    private
    | Isz of uint16

module ItemSize =

    let rec private fractionalSize acc (x: decimal): int =
        if Math.Truncate x = x then acc
        else fractionalSize (acc + 1) (x * 10M)

    let rec private wholeSize (x: decimal): int =
        if x < 0M then wholeSize -x
        else
            let truncated = Math.Truncate x
            if truncated = 0M then 1
            else
                truncated
                |> double
                // hack, add a tiny number (10^-10) so that (log10 9 = 1, log10 10 = 2)
                |> (+) 0.0000000001
                |> Math.Log10
                |> Math.Ceiling
                |> int

    let rec countDigits (x: decimal): int =
        if x = 0M then 1
        elif x < 0M then 1 + (countDigits (-x))
        else
            let fS = fractionalSize 0 x
            let decimal = if fS > 0 then 1 else 0
            wholeSize x + fS + decimal

    let rec private numberSize =
        countDigits >> float >> (flip (/) 2.0) >> Math.Ceiling >> int >> (+) 1

    [<ExcludeFromCodeCoverage>]
    let inline private stringSize (x: string) = Encoding.UTF8.GetByteCount x

    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
    let private mapAndListInfoAndOverhead = 3 + 1
    let rec private attrSize = function
        | AttributeValue.String x -> stringSize x
        | AttributeValue.Number x -> numberSize x
        | AttributeValue.Boolean _
        | AttributeValue.Null -> 1
        | AttributeValue.Binary x -> Array.length x
        | AttributeValue.HashMap m -> mapAndListInfoAndOverhead + attrsSize m
        | AttributeValue.AttributeList (CompressedList xs) -> mapAndListInfoAndOverhead + Array.sumBy attrSize xs
        | AttributeValue.AttributeList xs -> mapAndListInfoAndOverhead + (AttributeListType.asSeq xs |> Seq.sumBy attrSize)
        | AttributeValue.HashSet x ->
            AttributeSet.asSet x
            |> Set.fold (fun s x -> s + attrSize x) mapAndListInfoAndOverhead

    and private attrsSize x =
        Map.fold (fun s k v -> stringSize k + attrSize v + s) 0 x

    let rec tryCreate =
        attrsSize
        >> function
            | x when x > 400_000 -> ValueNone
            // using / 10 so that 400_000 can fit in a uint16
            | x -> float x / 10.0 |> Math.Ceiling |> uint16 |> ValueSome
        >> ValueOption.map Isz

    // usig * 10 so that 400_000 can fit in a uint16
    let size (Isz x) = int x |> ((*)10)

    let calculate = attrsSize

[<IsReadOnly; Struct>]
type private ItemData =
    { itemName: string
      itemId: IncrementingId
      size: ItemSize
      attributes: Map<string, AttributeValue> }

/// <summary>
/// A dynamodb item.
/// Contains a map of attributes and in internalId.
/// The internal id references a single item which is copied to multiple indexes 
/// </summary>
type Item =
    private
    | It of ItemData

    override this.ToString() =
        match this with
        | It x -> $"{x.itemName} ({x.itemId})"

    member this.Item
        with get attributeName =
            match this with | It {attributes = x } -> x[attributeName]

module Item =

    let internalId = function | It {itemId=x} -> x

    let itemName = function | It {itemName=x} -> x

    let attributes = function
        | It {attributes = x} -> x

    let compareByCreationOrder x y =
        let id1 = internalId x |> IncrementingId.value
        let id2 = internalId y |> IncrementingId.value
        id1 - id2

    let private createItem name internalId item =

        let data =
            { itemId = internalId
              itemName = name
              size =
                  ItemSize.tryCreate item
                  |> ValueOption.defaultWith (fun _ -> ClientError.clientError "Maximum item size if 400kB") 
              attributes = item }

        It data

    let empty = createItem "## Empty" (IncrementingId.next()) Map.empty

    let private validateMap =
        let dot = "."
        let rec getAttrErrors = function
            | struct (struct (depth, path), _) when depth > 32 ->
                $"Maximum nested attribute depth is 32: \"{path |> List.rev |> Str.join dot}\""
                |> Seq.singleton 
            | struct (depth, prop), HashMap item ->

                let invalidNested =
                    item
                    |> MapUtils.toSeq
                    |> Seq.collect (
                        mapFst (
                            flip Collection.prependL prop
                            >> tpl (depth + 1))
                        >> getAttrErrors)

                match struct (Map.containsKey null item || Map.containsKey "" item, prop) with
                | false, _ -> invalidNested
                | true, name -> Collection.prepend $"Item has map or property attribute with null or empty name: \"{prop |> List.rev |> Str.join dot}\"" invalidNested
            | (depth, prop), HashSet xs -> AttributeSet.asSet xs |> Seq.collect (fun attr -> getAttrErrors ((depth + 1, "[]"::prop), attr))
            | (depth, prop), AttributeList xs -> AttributeListType.asSeq xs |> Seq.collect (fun attr -> getAttrErrors ((depth + 1, "[]"::prop), attr))
            | _, Null -> Seq.empty
            | _, String _ -> Seq.empty
            | _, Number _ -> Seq.empty
            | _, Binary _ -> Seq.empty
            | _, Boolean _ -> Seq.empty

        fun item ->
            match getAttrErrors ((1, []), HashMap item) |> Str.join "; " with
            | "" -> ()
            | e -> ClientError.clientError $"Invalid item - {e}"

    let create name item =
        validateMap item
        createItem name (IncrementingId.next()) item

    let size (It {size = sz}) = ItemSize.size sz

    let reIndex =
        let project item = function
            | ValueNone -> item
            | ValueSome (projections: string array) -> Map.filter (fun k _ -> Array.contains k projections) item

        fun newName projections (It {attributes = x; itemId = id}) ->
            project x projections
            // empty logger should be fine. The resulting item will be equal or smaller than
            // initial item
            |> createItem newName id
