namespace TestDynamo.Model

open System
open System.Buffers
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

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
        match value with
        | "S" -> AttributeType.String
        | "N" -> AttributeType.Number
        | "BOOL" -> AttributeType.Boolean
        | "B" -> AttributeType.Binary
        | "M" -> AttributeType.HashMap
        | "SS" -> AttributeType.StringHashSet
        | "NS" -> AttributeType.NumberHashSet
        | "BS" -> AttributeType.BinaryHashSet
        | "L" -> AttributeType.AttributeList
        | "NULL" -> AttributeType.Null
        | x -> invalidOp $"Invalid AttributeType {x}"

type NullConst private() =
    static member Value = NullConst()
    
    override _.Equals(x: obj) =
        match x with
        | :? NullConst -> true
        | _ -> false
        
    override _.GetHashCode() = 0
    
type BoolConst private(v: bool) =
    static member True = BoolConst(true)
    static member False = BoolConst(false)
    
    member private _.v = v 
    
    override _.Equals(x: obj) =
        match x with
        | :? BoolConst as x' -> x'.v = v
        | _ -> false
        
    override _.GetHashCode() = if v then 1 else 0
        

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
            | ValueNone -> clientError "Found multiple types in single set"
            | ValueSome x ->
                match SetUtils.add x s with
                | false, _ -> clientError $"Duplicate value in {``type``} set"
                | true, s -> struct (ValueSome ``type``, s)) struct (ValueNone, Set.empty) strs
        |> function
            | _, xs when xs = Set.empty -> clientError $"Empty set not supported"
            | ValueNone, _ -> clientError $"Empty set not supported"
            | ValueSome t, xs -> struct (t, xs)
        |> As

    static member asSet (As x) = sndT x
    static member getSetType (As x) = fstT x
    
    static member asBinarySeq = function
        | As struct (t, x) when t <> AttributeType.Binary ->
            clientError $"Set has type {t}, not {AttributeType.Binary}"
        | As struct (_, xs) ->
            Seq.map (AttributeValue.value >> function
                | BinaryX x -> x
                | x -> clientError $"Set item {x} is not {AttributeType.Binary}") xs
    static member asNumberSeq = function
        | As struct (t, x) when t <> AttributeType.Number ->
            clientError $"Set has type {t}, not {AttributeType.Number}"
        | As struct (_, xs) ->
            Seq.map (AttributeValue.value >> function
                | NumberX x -> x
                | x -> clientError $"Set item {x} is not {AttributeType.Number}") xs
    static member asStringSeq = function
        | As struct (t, x) when t <> AttributeType.String ->
            clientError $"Set has type {t}, not {AttributeType.String}"
        | As struct (_, xs) ->
            Seq.map (AttributeValue.value >> function
                | StringX x -> x
                | x -> clientError $"Set item {x} is not {AttributeType.String}") xs
    static member contains =
        function
        | struct (value, As struct (_, set)) -> Set.contains value set
        |> curry

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

    /// <summary>Returns none if set types do not match</summary>
    static member tryIntersect = AttributeSet.tryOperation Set.intersect

    interface IComparable with
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

    /// <summary>Returns None if the index is out of range for either input</summary>
    static member copyTo i from ``to`` =
        match struct (from, ``to``) with
        | SparseList f, SparseList t ->
            let i = uint i
            MapUtils.tryFind i f
            ?|> (flip (Map.add i) t >> SparseList)
        | CompressedList f, SparseList t ->
            Collection.tryGetArr f i
            ?|> (flip (Map.add (uint i)) t >> SparseList)
        | SparseList f, CompressedList t ->
            MapUtils.tryFind (uint i) f
            ?>>= (Collection.tryAddOrReplaceArr t i)
            ?|> CompressedList
        | CompressedList f, CompressedList t ->
            Collection.tryGetArr f i
            ?>>= (Collection.tryAddOrReplaceArr t i)
            ?|> CompressedList

    static member private emptySparse = Map.empty |> SparseList
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
    [<CustomEquality; CustomComparison; Struct; IsReadOnly>]
    WorkingAttributeValue =
    | NullX
    | StringX of s: string
    | NumberX of d: decimal
    | BinaryX of b: byte array
    | BooleanX of bl: bool
    | HashMapX of m: Map<string, AttributeValue>
    | HashSetX of hs: AttributeSet
    | AttributeListX of al: AttributeListType

    with

    interface IComparable with
            // TODO perf, boxing
        member this.CompareTo obj = (AttributeValue.create this :> IComparable).CompareTo obj

    override this.ToString() = (AttributeValue.create this).ToString()

    override this.Equals(yobj) = (AttributeValue.create this).Equals(yobj)

    override this.GetHashCode() = (AttributeValue.create this).GetHashCode()

and        
    /// <summary>
    /// A dynamodb attribute of any valid type
    /// Implements equality and comparison
    /// </summary>
    [<CustomEquality; CustomComparison; Struct; IsReadOnly>]
    AttributeValue =
    private
    | AttVal of obj

    with
    
    static member createNull () = AttVal AttributeValue._null
    static member createString (x: string) = AttVal x
    static member createNumber (x: decimal) = AttVal (box x)// todo: create new object rather than box
    static member createBinary (x: byte array) = AttVal x
    static member createBoolean (x: bool) = (if x then AttributeValue._true else AttributeValue._false) |> AttVal
    static member createHashMap (x: Map<string, AttributeValue>) = AttVal x
    static member createHashSet (x: AttributeSet) = AttVal x
    static member createAttributeList (x: AttributeListType) = AttVal x
    
    static member private _null = NullConst.Value
    static member private _false = BoolConst.False
    static member private _true = BoolConst.True
    
    // TODO: delete in favor of specific create methods
    static member create worker =
        match worker with
        | NullX -> AttVal AttributeValue._null
        | StringX x -> AttVal x
        | NumberX x -> AttVal (box x)// todo: create new object rather than box
        | BinaryX x -> AttVal x
        | BooleanX x when x -> AttVal AttributeValue._true
        | BooleanX x -> AttVal AttributeValue._false
        | HashMapX x -> AttVal x
        | HashSetX x -> AttVal x
        | AttributeListX x -> AttVal x
    
    static member value x =
        match x with
        | AttVal x when x = AttributeValue._null -> NullX
        | AttVal (:? string as x) -> StringX x
        | AttVal (:? decimal as x) -> NumberX x// todo: create new object rather than unbox
        | AttVal (:? (byte array) as x) -> BinaryX x
        | AttVal x when x = AttributeValue._false -> BooleanX false
        | AttVal x when x = AttributeValue._true -> BooleanX true
        | AttVal (:? Map<string, AttributeValue> as x) -> HashMapX x
        | AttVal (:? AttributeSet as x) -> HashSetX x
        | AttVal (:? AttributeListType as x) -> AttributeListX x
        | AttVal x -> invalidOp $"Unknown type {x.GetType()}"
        
    member this.Value = AttributeValue.value this

    /// <summary>
    /// The type shorthand of the dynamodb object.
    /// Values are "S", "N", "B", "BOOL", "M", "SS", "NS", "BS", "L", "NULL" 
    /// </summary>
    member this.AttributeType = this |> AttributeValue.getType |> toString

    member this.IsNull =
        match this with
        | AttVal x -> x = AttributeValue._null

    member this.TryString([<Out>] value: byref<string>) =
        match this with
        | AttVal (:? string as x) ->
            value <- x
            true
        | _ -> false

    member this.TryNumber([<Out>] value: byref<decimal>) =
        match this with
        | AttVal (:? decimal as x) -> // todo: create new object rather than unbox
            value <- x
            true
        | _ -> false

    member this.TryBinary([<Out>] value: byref<IReadOnlyList<byte>>) =
        match this with
        | AttVal (:? (byte array) as x) ->
            value <- x
            true
        | _ -> false

    member this.TryBoolean([<Out>] value: byref<bool>) =
        match this with
        | AttVal x when x = AttributeValue._false ->
            value <- false
            true
        | AttVal x when x = AttributeValue._true ->
            value <- true
            true
        | _ -> false

    member this.TryMap([<Out>] value: byref<IReadOnlyDictionary<string, AttributeValue>>) =
        match this with
        | AttVal (:? Map<string, AttributeValue> as x) ->
            value <- x
            true
        | _ -> false

    member this.TryStringSet([<Out>] value: byref<Set<AttributeValue>>) =
        match this with
        | AttVal (:? AttributeSet as x) when AttributeSet.getSetType x = AttributeType.String ->
            value <- AttributeSet.asSet x
            true
        | _ -> false

    member this.TryNumberSet([<Out>] value: byref<Set<AttributeValue>>) =
        match this with
        | AttVal (:? AttributeSet as x) when AttributeSet.getSetType x = AttributeType.Number ->
            value <- AttributeSet.asSet x
            true
        | _ -> false

    member this.TryBinarySet([<Out>] value: byref<Set<AttributeValue>>) =
        match this with
        | AttVal (:? AttributeSet as x) when AttributeSet.getSetType x = AttributeType.Binary ->
            value <- AttributeSet.asSet x
            true
        | _ -> false

    member this.TryList([<Out>] value: byref<IReadOnlyList<AttributeValue>>) =
        match this with
        | AttVal (:? AttributeListType as x) ->
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
        member this.CompareTo obj =
            match obj with
            | :? AttributeValue as y -> AttributeValue.compareTo this y
            | _ -> invalidArg "obj" "cannot compare value of different types"

    override this.ToString() =
        match this with
        | AttVal x when x = AttributeValue._null -> "NULL"
        | AttVal (:? string as x) -> x
        | AttVal (:? decimal as x) -> x.ToString()
        | AttVal (:? (byte array) as x) -> x.ToString()  // todo How does this work?
        | AttVal x when x = AttributeValue._false -> "false"
        | AttVal x when x = AttributeValue._true -> "true"
        | AttVal (:? Map<string, AttributeValue> as x) -> x.ToString()
        | AttVal (:? AttributeSet as x) -> x.ToString()
        | AttVal (:? AttributeListType as x) -> x.ToString()
        | AttVal x -> x.ToString()

    override this.Equals(yobj) =
        match yobj with
        | :? AttributeValue as y ->
            match struct (this, y) with
            | AttVal (:? (byte array) as this), AttVal (:? (byte array) as ys) ->
                AttributeValue.byteArrayComparer.Equals(this, ys)
            | this, ys -> (this :> IComparable).CompareTo ys = 0
        | _ -> false

    override this.GetHashCode() =
        match this with
        | AttVal x when x = AttributeValue._null -> 0
        | AttVal (:? string as v1) -> HashCode.Combine(1, v1)
        | AttVal (:? decimal as v1) -> HashCode.Combine(2, v1)
        | AttVal (:? (byte array) as v1) -> HashCode.Combine(4, AttributeValue.byteArrayComparer.GetHashCode v1)
        | AttVal x when x = AttributeValue._false -> HashCode.Combine(3, false)
        | AttVal x when x = AttributeValue._true -> HashCode.Combine(3, true)
        | AttVal (:? Map<string, AttributeValue> as v1) -> HashCode.Combine(5, v1)
        | AttVal (:? AttributeSet as v1) -> HashCode.Combine(6, v1)
        | AttVal (:? AttributeListType as v1) -> HashCode.Combine(7, AttributeListType.getHashCode v1)
        | AttVal x -> HashCode.Combine(-1, x)

    static member private byteArrayComparer: IEqualityComparer<byte array> = Comparison.arrayComparer<byte> 10

    static member private getHashSetType = function
        | AttributeType.String -> StringHashSet
        | AttributeType.Number -> NumberHashSet
        | AttributeType.Binary -> BinaryHashSet
        | x -> invalidOp $"{x} is not a valid Set type"

    static member getType (value: AttributeValue) =
        match value with
        | AttVal x when x = AttributeValue._null -> AttributeType.Null
        | AttVal (:? string) -> AttributeType.String
        | AttVal (:? decimal) -> AttributeType.Number
        | AttVal (:? (byte array)) -> AttributeType.Binary
        | AttVal x when x = AttributeValue._false -> AttributeType.Boolean
        | AttVal x when x = AttributeValue._true -> AttributeType.Boolean
        | AttVal (:? Map<string, AttributeValue>) -> AttributeType.HashMap
        | AttVal (:? AttributeSet as x) -> AttributeSet.getSetType x |> AttributeValue.getHashSetType
        | AttVal (:? AttributeListType) -> AttributeType.AttributeList
        | AttVal x -> invalidOp $"Invalid attr type {x.GetType()}"

    static member private compressSparseLists': AttributeValue -> struct (RequiredConversion * AttributeValue) = function
        | AttVal x & x' when x = AttributeValue._null -> struct (false, x')
        | AttVal x & x' when x = AttributeValue._true -> struct (false, x')
        | AttVal x & x' when x = AttributeValue._false -> struct (false, x')
        | AttVal (:? (byte array)) & x
        | AttVal (:? string) & x
        | AttVal (:? decimal) & x
        | AttVal (:? AttributeSet) & x -> struct (false, x)
        | AttVal (:? Map<string, AttributeValue> as map) & x ->
            Map.fold (fun (struct (modified, map) & s) k v ->
                match AttributeValue.compressSparseLists' v with
                | true, v' -> Map.add k v' map |> tpl true
                | false, _ -> s) struct (false, map) map
            |> mapSnd (HashMapX >> AttributeValue.create)
        | AttVal (:? AttributeListType as list) & fullList ->
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
            | x -> struct (true, x |> CompressedList |> AttributeListX |> AttributeValue.create)
        | AttVal _ & x -> struct (false, x)

    static member compressSparseLists = AttributeValue.compressSparseLists' >> sndT

    static member describe v =
        let typ = AttributeValue.getType v |> AttributeType.describe
        match v with
        | AttVal x when x = AttributeValue._null -> typ
        | AttVal (:? (byte array)) -> $"{typ}:BINARY_DATA"
        | AttVal (:? string as x) -> $"{typ}:{x}"
        | AttVal (:? decimal as x) -> $"{typ}:{x}"
        | AttVal x when x = AttributeValue._false -> $"{typ}:false"
        | AttVal x when x = AttributeValue._true -> $"{typ}:true"
        | AttVal (:? Map<string, AttributeValue>) -> $"{typ}:MAP"
        | AttVal (:? AttributeSet) -> $"{typ}:SET"
        | AttVal (:? AttributeListType) -> $"{typ}:LIST"
        | AttVal x -> $"{x}:UNKNOWN"

    static member asType ``type`` value =
        match value with
        | AttVal x when x = AttributeValue._null -> if ``type`` = AttributeType.Null then ValueSome value else ValueNone
        | AttVal (:? string) -> if ``type`` = AttributeType.String then ValueSome value else ValueNone
        | AttVal (:? decimal) -> if ``type`` = AttributeType.Number then ValueSome value else ValueNone
        | AttVal (:? (byte array)) -> if ``type`` = AttributeType.Binary then ValueSome value else ValueNone
        | AttVal x when x = AttributeValue._false -> if ``type`` = AttributeType.Boolean then ValueSome value else ValueNone
        | AttVal x when x = AttributeValue._true -> if ``type`` = AttributeType.Boolean then ValueSome value else ValueNone
        | AttVal (:? Map<string, AttributeValue>) -> if ``type`` = AttributeType.HashMap then ValueSome value else ValueNone
        | AttVal (:? AttributeSet as hs) when ``type`` = AttributeType.StringHashSet && AttributeSet.getSetType hs = AttributeType.String -> ValueSome value
        | AttVal (:? AttributeSet as hs) when ``type`` = AttributeType.NumberHashSet && AttributeSet.getSetType hs = AttributeType.Number -> ValueSome value
        | AttVal (:? AttributeSet as hs) when ``type`` = AttributeType.BinaryHashSet && AttributeSet.getSetType hs = AttributeType.Binary -> ValueSome value
        | AttVal (:? AttributeListType) -> if ``type`` = AttributeType.AttributeList then ValueSome value else ValueNone
        | AttVal _ -> ValueNone

    static member compare x y = // TODO: duplication with IComparable interface
        match struct (x, y) with
        | AttVal x, AttVal y when x = y -> 0 |> ValueSome
        | AttVal (:? string as x'), AttVal (:? string as y') -> compare x' y' |> ValueSome
        | AttVal x', AttVal y' when x' = AttributeValue._false && y' = AttributeValue._true  -> -1 |> ValueSome
        | AttVal x', AttVal y' when x' = AttributeValue._true && y' = AttributeValue._false  -> 1 |> ValueSome
        | AttVal (:? decimal as x'), AttVal (:? decimal as y') -> x' - y' |> int |> ValueSome
        | AttVal (:? (byte array) as x'), AttVal (:? (byte array) as y') -> compare x' y' |> ValueSome    // TODO: does this work???
        | AttVal (:? AttributeListType as x'), AttVal (:? AttributeListType as y') -> AttributeListType.compare struct (x', y') |> ValueSome
        | AttVal (:? AttributeSet as x'), AttVal (:? AttributeSet as y') -> compare x' y' |> ValueSome
        | AttVal (:? Map<string, AttributeValue> as x'), AttVal (:? Map<string, AttributeValue> as y') -> compare x' y' |> ValueSome
        | AttVal _, AttVal _ -> ValueNone
        
    static member compareTo x y =
        match struct (x, y) with
        | AttVal v1, AttVal v2 when v1 = v2 -> 0
        | AttVal (:? string as v1), AttVal (:? string as v2) -> compare v1 v2
        | AttVal (:? decimal as v1), AttVal (:? decimal as v2) -> v1.CompareTo v2 |> int
        | AttVal v1, AttVal v2 when v1 = AttributeValue._true && v2 = AttributeValue._false -> 1
        | AttVal v1, AttVal v2 when v1 = AttributeValue._false && v2 = AttributeValue._true -> -1
        | AttVal (:? (byte array) as v1), AttVal (:? (byte array) as v2) -> compare v1 v2 // TODO: how does this work?
        | AttVal (:? Map<string, AttributeValue> as v1), AttVal (:? Map<string, AttributeValue> as v2) -> compare v1 v2
        | AttVal (:? AttributeSet as v1), AttVal (:? AttributeSet as v2) -> compare v1 v2
        | AttVal (:? AttributeListType as v1), AttVal (:? AttributeListType as v2) -> AttributeListType.compare struct (v1, v2)
        | AttVal x, AttVal y when x = AttributeValue._null && x = y -> 0
        | AttVal (:? string), _ -> -1
        | _, AttVal (:? string) -> 1
        | AttVal (:? decimal), _ -> -1
        | _, AttVal (:? decimal) -> 1
        | AttVal x, _ when x = AttributeValue._false || x = AttributeValue._true -> -1
        | _, AttVal x when x = AttributeValue._false || x = AttributeValue._true -> 1
        | AttVal (:? (byte array)), _ -> -1
        | _, AttVal (:? (byte array)) -> 1
        | AttVal (:? AttributeSet), _ -> -1
        | _, AttVal (:? AttributeSet) -> 1
        | AttVal (:? Map<string, AttributeValue>), _ -> -1
        | _, AttVal (:? Map<string, AttributeValue>) -> 1
        | AttVal (:? AttributeListType), _ -> -1
        | _, AttVal (:? AttributeListType) -> 1
        | AttVal x, AttVal y -> invalidOp $"Invalid attr type {x.GetType()} {y.GetType()}"
        
    static member comparer =
        { new IComparer<AttributeValue> with
            member _.Compare(x: AttributeValue, y: AttributeValue) = AttributeValue.compareTo x y }

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
        | NoValue -> clientError $"""Missing {selector.attributeDescription} "{selector.attributeName}" for item {item}"""
        | ExpectedType t -> clientError $"""Expected {selector.attributeDescription} "{selector.attributeName}" to have type "{t}" for item {item}"""
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
                // hack, add a tiny number so that (log10 9 = 1, log10 10 = 2)
                |> (+) 0.00000001
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

    let inline private stringSize (x: string) = Encoding.UTF8.GetByteCount x

    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
    let private mapAndListInfoAndOverhead = 3 + 1
    let rec private attrSize x =
        match AttributeValue.value x with
        | WorkingAttributeValue.StringX x -> stringSize x
        | WorkingAttributeValue.NumberX x -> numberSize x
        | WorkingAttributeValue.BooleanX _
        | WorkingAttributeValue.NullX -> 1
        | WorkingAttributeValue.BinaryX x -> Array.length x
        | WorkingAttributeValue.HashMapX m -> mapAndListInfoAndOverhead + attrsSize m
        | WorkingAttributeValue.AttributeListX (CompressedList xs) -> mapAndListInfoAndOverhead + Array.sumBy attrSize xs
        | WorkingAttributeValue.AttributeListX xs -> mapAndListInfoAndOverhead + (AttributeListType.asSeq xs |> Seq.sumBy attrSize)
        | WorkingAttributeValue.HashSetX x ->
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
                  |> ValueOption.defaultWith (fun _ -> clientError "Maximum item size if 400kB") 
              attributes = item }

        It data

    let empty = createItem "## Empty" (IncrementingId.next()) Map.empty

    let private validateMap =
        let dot = "."
        let rec getAttrErrors x =
            match mapSnd AttributeValue.value x with
            | struct (struct (depth, path), _) when depth > 32 ->
                $"Maximum nested attribute depth is 32: \"{path |> List.rev |> Str.join dot}\""
                |> Seq.singleton 
            | struct (depth, prop), HashMapX item ->

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
            | (depth, prop), HashSetX xs -> AttributeSet.asSet xs |> Seq.collect (curry getAttrErrors (depth + 1, "[]"::prop))
            | (depth, prop), AttributeListX xs -> xs |> AttributeListType.asSeq |> Seq.collect (curry getAttrErrors (depth + 1, "[]"::prop))
            | _, NullX -> Seq.empty
            | _, StringX _ -> Seq.empty
            | _, NumberX _ -> Seq.empty
            | _, BinaryX _ -> Seq.empty
            | _, BooleanX _ -> Seq.empty

        fun item ->
            match getAttrErrors ((1, []), HashMapX item |> AttributeValue.create) |> Str.join "; " with
            | "" -> ()
            | e -> clientError $"Invalid item - {e}"

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
