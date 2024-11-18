
// Copy of f# map https://github.com/dotnet/fsharp/blob/main/src/FSharp.Core/map.fs
// This is an AVL tree. Copy includes code to get values where key between x and y

namespace TestDynamo.Data

open TestDynamo.Utils
open System
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.CompilerServices
open Microsoft.FSharp.Core.LanguagePrimitives.IntrinsicOperators

module public AvlTreeInternal =

    [<NoEquality; NoComparison>]
    [<AllowNullLiteral>]
    type Tree<'Key, 'Value>(k: 'Key, v: 'Value, h: int) =
        member _.Height = h
        member _.Key = k
        member _.Value = v
        new(k: 'Key, v: 'Value) = Tree(k, v, 1)

    [<NoEquality; NoComparison>]
    [<Sealed>]
    [<AllowNullLiteral>]
    type internal TreeNode<'Key, 'Value>
        (k: 'Key, v: 'Value, left: Tree<'Key, 'Value>, right: Tree<'Key, 'Value>, h: int) =
        inherit Tree<'Key, 'Value>(k, v, h)
        member _.Left = left
        member _.Right = right

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module internal AvlTree =

        let empty = null

        let inline isEmpty (m: Tree<'Key, 'Value>) =
            isNull m

        let inline private asNode (value: Tree<'Key, 'Value>) : TreeNode<'Key, 'Value> =
            value :?> TreeNode<'Key, 'Value>

        let rec sizeAux acc (m: Tree<'Key, 'Value>) =
            if isEmpty m then
                acc
            else if m.Height = 1 then
                acc + 1
            else
                let mn = asNode m
                sizeAux (sizeAux (acc + 1) mn.Left) mn.Right

        let size x =
            sizeAux 0 x

        let inline height (m: Tree<'Key, 'Value>) =
            if isEmpty m then 0 else m.Height

        [<Literal>]
        let tolerance = 2

        let mk l k v r : Tree<'Key, 'Value> =
            let hl = height l
            let hr = height r
            let m = if hl < hr then hr else hl

            if m = 0 then // m=0 ~ isEmpty l && isEmpty r
                Tree(k, v)
            else
                TreeNode(k, v, l, r, m + 1) :> Tree<'Key, 'Value> // new map is higher by 1 than the highest

        let rebalance t1 (k: 'Key) (v: 'Value) t2 : Tree<'Key, 'Value> =
            let t1h = height t1
            let t2h = height t2

            if t2h > t1h + tolerance then (* right is heavier than left *)
                let t2' = asNode (t2)
                (* one of the nodes must have height > height t1 + 1 *)
                if height t2'.Left > t1h + 1 then (* balance left: combination *)
                    let t2l = asNode (t2'.Left)
                    mk (mk t1 k v t2l.Left) t2l.Key t2l.Value (mk t2l.Right t2'.Key t2'.Value t2'.Right)
                else (* rotate left *)
                    mk (mk t1 k v t2'.Left) t2'.Key t2'.Value t2'.Right
            else if t1h > t2h + tolerance then (* left is heavier than right *)
                let t1' = asNode (t1)
                (* one of the nodes must have height > height t2 + 1 *)
                if height t1'.Right > t2h + 1 then
                    (* balance right: combination *)
                    let t1r = asNode (t1'.Right)
                    mk (mk t1'.Left t1'.Key t1'.Value t1r.Left) t1r.Key t1r.Value (mk t1r.Right k v t2)
                else
                    mk t1'.Left t1'.Key t1'.Value (mk t1'.Right k v t2)
            else
                mk t1 k v t2

        let rec add (comparer: IComparer<'Key>) k (v: 'Value) (m: Tree<'Key, 'Value>) : struct (struct ('Key * 'Value) voption * Tree<'Key, 'Value>) =
            if isEmpty m then
                ValueNone, Tree(k, v)
            else
                let c = comparer.Compare(k, m.Key)

                if m.Height = 1 then
                    if c < 0 then
                        ValueNone, TreeNode(k, v, empty, m, 2) :> Tree<'Key, 'Value>
                    elif c = 0 then
                        ValueSome (m.Key, m.Value), Tree(k, v)
                    else
                        ValueNone, TreeNode(k, v, m, empty, 2) :> Tree<'Key, 'Value>
                else
                    let mn = asNode m

                    if c < 0 then
                        let struct (removed, left) = add comparer k v mn.Left
                        removed, rebalance left mn.Key mn.Value mn.Right
                    elif c = 0 then
                        ValueSome (m.Key, m.Value), TreeNode(k, v, mn.Left, mn.Right, mn.Height) :> Tree<'Key, 'Value>
                    else
                        let struct (removed, right) = add comparer k v mn.Right
                        removed, rebalance mn.Left mn.Key mn.Value right

        let rec tryGetValue (comparer: IComparer<'Key>) k (v: byref<'Value>) (m: Tree<'Key, 'Value>) =
            if isEmpty m then
                false
            else
                let c = comparer.Compare(k, m.Key)

                if c = 0 then
                    v <- m.Value
                    true
                else if m.Height = 1 then
                    false
                else
                    let mn = asNode m
                    tryGetValue comparer k &v (if c < 0 then mn.Left else mn.Right)

        [<MethodImpl(MethodImplOptions.NoInlining)>]
        let throwKeyNotFound () =
            raise (KeyNotFoundException())

        [<MethodImpl(MethodImplOptions.NoInlining)>]
        let find (comparer: IComparer<'Key>) k (m: Tree<'Key, 'Value>) =
            let mutable v = Unchecked.defaultof<'Value>

            if tryGetValue comparer k &v m then
                v
            else
                throwKeyNotFound ()

        let tryFind (comparer: IComparer<'Key>) k (m: Tree<'Key, 'Value>) =
            let mutable v = Unchecked.defaultof<'Value>

            if tryGetValue comparer k &v m then
                ValueSome v
            else
                ValueNone

        let rec spliceOutSuccessor (m: Tree<'Key, 'Value>) =
            if isEmpty m then
                failwith "internal error: Map.spliceOutSuccessor"
            else if m.Height = 1 then
                m.Key, m.Value, empty
            else
                let mn = asNode m

                if isEmpty mn.Left then
                    mn.Key, mn.Value, mn.Right
                else
                    let k3, v3, l' = spliceOutSuccessor mn.Left in k3, v3, mk l' mn.Key mn.Value mn.Right

        let rec remove (comparer: IComparer<'Key>) k (m: Tree<'Key, 'Value>) =
            if isEmpty m then
                empty
            else
                let c = comparer.Compare(k, m.Key)

                if m.Height = 1 then
                    if c = 0 then empty else m
                else
                    let mn = asNode m

                    if c < 0 then
                        rebalance (remove comparer k mn.Left) mn.Key mn.Value mn.Right
                    elif c = 0 then
                        if isEmpty mn.Left then
                            mn.Right
                        elif isEmpty mn.Right then
                            mn.Left
                        else
                            let sk, sv, r' = spliceOutSuccessor mn.Right
                            mk mn.Left sk sv r'
                    else
                        rebalance mn.Left mn.Key mn.Value (remove comparer k mn.Right)

        let rec change
            (comparer: IComparer<'Key>)
            k
            (u: 'Value voption -> 'Value voption)
            (m: Tree<'Key, 'Value>)
            : Tree<'Key, 'Value> =
            if isEmpty m then
                match u ValueNone with
                | ValueNone -> m
                | ValueSome v -> Tree(k, v)
            else if m.Height = 1 then
                let c = comparer.Compare(k, m.Key)

                if c < 0 then
                    match u ValueNone with
                    | ValueNone -> m
                    | ValueSome v -> TreeNode(k, v, empty, m, 2) :> Tree<'Key, 'Value>
                elif c = 0 then
                    match u (ValueSome m.Value) with
                    | ValueNone -> empty
                    | ValueSome v -> Tree(k, v)
                else
                    match u ValueNone with
                    | ValueNone -> m
                    | ValueSome v -> TreeNode(k, v, m, empty, 2) :> Tree<'Key, 'Value>
            else
                let mn = asNode m
                let c = comparer.Compare(k, mn.Key)

                if c < 0 then
                    rebalance (change comparer k u mn.Left) mn.Key mn.Value mn.Right
                elif c = 0 then
                    match u (ValueSome mn.Value) with
                    | ValueNone ->
                        if isEmpty mn.Left then
                            mn.Right
                        elif isEmpty mn.Right then
                            mn.Left
                        else
                            let sk, sv, r' = spliceOutSuccessor mn.Right
                            mk mn.Left sk sv r'
                    | ValueSome v -> TreeNode(k, v, mn.Left, mn.Right, mn.Height) :> Tree<'Key, 'Value>
                else
                    rebalance mn.Left mn.Key mn.Value (change comparer k u mn.Right)

        let rec mem (comparer: IComparer<'Key>) k (m: Tree<'Key, 'Value>) =
            if isEmpty m then
                false
            else
                let c = comparer.Compare(k, m.Key)

                if m.Height = 1 then
                    c = 0
                else
                    let mn = asNode m

                    if c < 0 then
                        mem comparer k mn.Left
                    else
                        (c = 0 || mem comparer k mn.Right)

        let rec map (f: 'Value -> 'Result) (m: Tree<'Key, 'Value>) : Tree<'Key, 'Result> =
            if isEmpty m then
                empty
            else if m.Height = 1 then
                Tree(m.Key, f m.Value)
            else
                let mn = asNode m
                let l2 = map f mn.Left
                let v2 = f mn.Value
                let r2 = map f mn.Right
                TreeNode(mn.Key, v2, l2, r2, mn.Height) :> Tree<'Key, 'Result>

        let rec mapiOpt (f: OptimizedClosures.FSharpFunc<'Key, 'Value, 'Result>) (m: Tree<'Key, 'Value>) =
            if isEmpty m then
                empty
            else if m.Height = 1 then
                Tree(m.Key, f.Invoke(m.Key, m.Value))
            else
                let mn = asNode m
                let l2 = mapiOpt f mn.Left
                let v2 = f.Invoke(mn.Key, mn.Value)
                let r2 = mapiOpt f mn.Right
                TreeNode(mn.Key, v2, l2, r2, mn.Height) :> Tree<'Key, 'Result>

        let mapi f m =
            mapiOpt (OptimizedClosures.FSharpFunc<_, _, _>.Adapt f) m

        let rec foldBackOpt (f: OptimizedClosures.FSharpFunc<_, _, _, _>) (m: Tree<'Key, 'Value>) x =
            if isEmpty m then
                x
            else if m.Height = 1 then
                f.Invoke(m.Key, m.Value, x)
            else
                let mn = asNode m
                let x = foldBackOpt f mn.Right x
                let x = f.Invoke(mn.Key, mn.Value, x)
                foldBackOpt f mn.Left x

        let foldBack f m x =
            foldBackOpt (OptimizedClosures.FSharpFunc<_, _, _, _>.Adapt f) m x

        let rec foldOpt (f: OptimizedClosures.FSharpFunc<_, _, _, _>) x (m: Tree<'Key, 'Value>) =
            if isEmpty m then
                x
            else if m.Height = 1 then
                f.Invoke(x, m.Key, m.Value)
            else
                let mn = asNode m
                let x = foldOpt f x mn.Left
                let x = f.Invoke(x, mn.Key, mn.Value)
                foldOpt f x mn.Right

        let fold f x m =
            foldOpt (OptimizedClosures.FSharpFunc<_, _, _, _>.Adapt f) x m

        let ofList comparer l =
            List.fold (fun acc struct (k, v) -> add comparer k v acc |> sndT) empty l

        let rec mkFromEnumerator comparer acc (e: IEnumerator<_>) =
            if e.MoveNext() then
                let struct (x, y) = e.Current
                mkFromEnumerator comparer (add comparer x y acc |> sndT) e
            else
                acc

        let ofArray comparer (arr: struct ('Key * 'Value) array) =
            let mutable res = empty

            for (x, y) in arr do
                res <- add comparer x y res |> sndT

            res

        let ofSeq comparer (c: struct ('Key * 'T) seq) =
            match c with
            | :? (struct ('Key * 'T) array) as xs -> ofArray comparer xs
            | :? (struct ('Key * 'T) list) as xs -> ofList comparer xs
            | _ ->
                use ie = c.GetEnumerator()
                mkFromEnumerator comparer empty ie

        /// Imperative left-to-right iterators.
        [<NoEquality; NoComparison>]
        type MapIterator<'Key, 'Value when 'Key: comparison> =
            {
                /// invariant: always collapseLHS result
                mutable stack: Tree<'Key, 'Value> list

                /// true when MoveNext has been called
                mutable started: bool
            }

        // collapseLHS:
        // a) Always returns either [] or a list starting with MapOne.
        // b) The "fringe" of the set stack is unchanged.
        let rec collapseLHS (stack: Tree<'Key, 'Value> list) =
            match stack with
            | [] -> []
            | m :: rest ->
                if isEmpty m then
                    collapseLHS rest
                else if m.Height = 1 then
                    stack
                else
                    let mn = asNode m
                    collapseLHS (mn.Left :: Tree(mn.Key, mn.Value) :: mn.Right :: rest)

        let mkIterator m =
            {
                stack = collapseLHS [ m ]
                started = false
            }

        let notStarted () =
            raise (InvalidOperationException("enumerationNotStarted"))

        let alreadyFinished () =
            raise (InvalidOperationException("enumerationAlreadyFinished"))

        let unexpectedStackForCurrent () =
            failwith "Please report error: Map iterator, unexpected stack for current"

        let unexpectedStackForMoveNext () =
            failwith "Please report error: Map iterator, unexpected stack for moveNext"

        let current i =
            if i.started then
                match i.stack with
                | [] -> alreadyFinished ()
                | m :: _ ->
                    if m.Height = 1 then
                        struct (m.Key, m.Value)
                    else
                        unexpectedStackForCurrent ()
            else
                notStarted ()

        let rec moveNext i =
            if i.started then
                match i.stack with
                | [] -> false
                | m :: rest ->
                    if m.Height = 1 then
                        i.stack <- collapseLHS rest
                        not i.stack.IsEmpty
                    else
                        unexpectedStackForMoveNext ()
            else
                i.started <- true (* The first call to MoveNext "starts" the enumeration. *)
                not i.stack.IsEmpty

        let mkIEnumerator m =
            let mutable i = mkIterator m

            { new IEnumerator<_> with
                member _.Current = current i
              interface System.Collections.IEnumerator with
                  member _.Current = box (current i)

                  member _.MoveNext() =
                      moveNext i

                  member _.Reset() =
                      i <- mkIterator m
              interface System.IDisposable with
                  member _.Dispose() =
                      ()
            }

        let rec leftmost m =
            if isEmpty m then
                throwKeyNotFound ()
            else if m.Height = 1 then
                (m.Key, m.Value)
            else
                let nd = asNode m

                if isNull nd.Left then
                    (m.Key, m.Value)
                else
                    leftmost nd.Left

        let rec rightmost m =
            if isEmpty m then
                throwKeyNotFound ()
            else if m.Height = 1 then
                (m.Key, m.Value)
            else
                let nd = asNode m

                if isNull nd.Right then
                    (m.Key, m.Value)
                else
                    rightmost nd.Right
open AvlTreeInternal

module private AvlTreeSeek =

    let notNull x = x|> box |> ((<>)null)

    /// <summary>
    /// Walk down one side of a tree
    /// </summary>
    let rec private walkOutside next (tree: Tree<'k, 'v>) =
        let mutable t = tree
        seq {
            while t |> notNull do
                yield t

                t <-
                    match t with
                    | :? TreeNode<'k, 'v> as tn -> next tn
                    | _ -> null
        }

    let inline returnTrue _ = true

    let private includeNodeLess inclusive =
        ValueOption.map (fun key node ->
            compare key node
            |> tpl inclusive
            |> function
                | true, 0 -> true
                | _, cmp -> cmp > 0)
        >> ValueOption.defaultValue returnTrue

    let private includeNodeGreater inclusive =
        ValueOption.map (fun key node ->
            compare key node
            |> tpl inclusive
            |> function
                | true, 0 -> true
                | _, cmp -> cmp < 0)
        >> ValueOption.defaultValue returnTrue

    let rec private getPathToStart includeNode getPrev getNext (tree: Tree<_, _>) acc =

        match tree with
        | null -> acc
        | tree ->
            match struct (includeNode tree.Key, tree) with
            | true, (:? TreeNode<_, _> as tn) & n -> getPathToStart includeNode getPrev getNext (getPrev tn) (n::acc)
            | true, tn -> tn::acc
            | false, (:? TreeNode<_, _> as tn) -> getPathToStart includeNode getPrev getNext (getNext tn) acc
            | false, _ -> acc

    and private enumerateNode includeNode getPrev getNext (tree: struct (int * Tree<_, _>)) =

        match tree with
        | _, null -> Seq.empty
        | _, (:? TreeNode<_, _> as tree) ->
            seq {
                yield struct (tree.Key, tree.Value)
                for x in getSorted includeNode getPrev getNext (getNext tree) [] do
                    yield x
            }
        | _, (tree: Tree<_, _>) -> Seq.singleton struct (tree.Key, tree.Value)

    and private getSorted includeNode getPrev getNext (tree: Tree<_, _>) acc =

        getPathToStart includeNode getPrev getNext tree acc
        |> Seq.mapi tpl
        |> Seq.collect (enumerateNode includeNode getPrev getNext)

    let getRange from ``to`` inclusive tree =
        let filter =
            match ``to`` with
            | ValueNone -> id
            | ValueSome ``to`` ->
                Seq.takeWhile (fun struct (k, _) ->
                    match struct (compare k ``to``, inclusive) with
                    | 0, true -> true
                    | c, _ -> c < 0)

        getSorted (includeNodeGreater inclusive from) _.Left _.Right tree []
        |> filter

    let getRangeDescending from ``to`` inclusive tree =
        let filter =
            match ``to`` with
            | ValueNone -> id
            | ValueSome ``to`` ->
                Seq.takeWhile (fun struct (k, _) ->
                    match struct (compare k ``to``, inclusive) with
                    | 0, true -> true
                    | c, _ -> c > 0)

        getSorted (includeNodeLess inclusive from) _.Right _.Left tree []
        |> filter
open AvlTreeSeek

type AvlTree<[<EqualityConditionalOn>] 'Key, [<EqualityConditionalOn; ComparisonConditionalOn>] 'Value when 'Key: comparison>
    (comparer: IComparer<'Key>, tree: Tree<'Key, 'Value>) =

    [<System.NonSerialized>]
    // This type is logically immutable. This field is only mutated during deserialization.
    let mutable comparer = comparer

    [<System.NonSerialized>]
    // This type is logically immutable. This field is only mutated during deserialization.
    let mutable tree = tree

    // We use .NET generics per-instantiation static fields to avoid allocating a new object for each empty
    // set (it is just a lookup into a .NET table of type-instantiation-indexed static fields).
    static let empty comparer =
        //let comparer = LanguagePrimitives.FastGenericComparer<'Key>
        new AvlTree<'Key, 'Value>(comparer, AvlTree.empty)

    // Do not set this to null, since concurrent threads may also be serializing the data
    //[<System.Runtime.Serialization.OnSerializedAttribute>]
    //member _.OnSerialized(context: System.Runtime.Serialization.StreamingContext) =
    //    serializedData <- null

    static member Empty comparer: AvlTree<'Key, 'Value> = empty comparer

    static member Create(ie: IEnumerable<_>) : AvlTree<'Key, 'Value> =
        let comparer = LanguagePrimitives.FastGenericComparer<'Key>
        AvlTree<_, _>(comparer, AvlTree.ofSeq comparer ie)

    new(elements: seq<_>) =
        let comparer = LanguagePrimitives.FastGenericComparer<'Key>
        new AvlTree<_, _>(comparer, AvlTree.ofSeq comparer elements)

    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    member internal m.Comparer = comparer

    //[<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    member internal m.Tree = tree

    member m.AddOrReplace(key, value) =
        let struct (replaced, t) = AvlTree.add comparer key value tree
        struct (replaced, new AvlTree<'Key, 'Value>(comparer, t))

    member m.Change(key, f) : AvlTree<'Key, 'Value> =
        new AvlTree<'Key, 'Value>(comparer, AvlTree.change comparer key f tree)

    [<DebuggerBrowsable(DebuggerBrowsableState.Never)>]
    member m.IsEmpty = AvlTree.isEmpty tree

    member m.Seek(from, ``to``, inclusive, forwards) =
        match forwards with
        | true -> getRange from ``to`` inclusive m.Tree
        | false -> getRangeDescending from ``to`` inclusive m.Tree

    member m.Item
        with get (key: 'Key) =
            AvlTree.find comparer key tree

    member m.Fold f acc =
        AvlTree.foldBack f tree acc

    member m.MapRange(f: 'Value -> 'Result) =
        new AvlTree<'Key, 'Result>(comparer, AvlTree.map f tree)

    member m.Map f =
        new AvlTree<'Key, 'b>(comparer, AvlTree.mapi f tree)

    member m.Count = AvlTree.size tree

    member m.ContainsKey key =
        AvlTree.mem comparer key tree

    member m.Remove key =
        new AvlTree<'Key, 'Value>(comparer, AvlTree.remove comparer key tree)

    member m.TryGetValue(key, [<System.Runtime.InteropServices.Out>] value: byref<'Value>) =
        AvlTree.tryGetValue comparer key &value tree

    member m.TryFind key =
        AvlTree.tryFind comparer key tree

    member m.MinKeyValue = AvlTree.leftmost tree
    member m.MaxKeyValue = AvlTree.rightmost tree

    static member ofList l : AvlTree<'Key, 'Value> =
        let comparer = LanguagePrimitives.FastGenericComparer<'Key>
        new AvlTree<_, _>(comparer, AvlTree.ofList comparer l)

    interface IEnumerable<struct ('Key * 'Value)> with
        member _.GetEnumerator() =
            AvlTree.mkIEnumerator tree

    interface System.Collections.IEnumerable with
        member _.GetEnumerator() =
            (AvlTree.mkIEnumerator tree :> System.Collections.IEnumerator)

[<RequireQualifiedAccess>]
module AvlTree =

    let isEmpty (table: AvlTree<_, _>) =
        table.IsEmpty

    let add key value (table: AvlTree<_, _>) =
        table.AddOrReplace(key, value) |> sndT

    let addOrReplace key value (table: AvlTree<_, _>) =
        table.AddOrReplace(key, value)

    let change key f (table: AvlTree<_, _>) =
        table.Change(key, f)

    let find key (table: AvlTree<_, _>) =
        table.[key]

    let tryFind key (table: AvlTree<_, _>) =
        table.TryFind key

    let remove key (table: AvlTree<_, _>) =
        table.Remove key

    let containsKey key (table: AvlTree<_, _>) =
        table.ContainsKey key

    let map mapping (table: AvlTree<_, _>) =
        table.Map mapping

    let fold<'Key, 'T, 'State when 'Key: comparison> folder (state: 'State) (table: AvlTree<'Key, 'T>) =
        AvlTree.fold folder state table.Tree

    let foldBack<'Key, 'T, 'State when 'Key: comparison> folder (table: AvlTree<'Key, 'T>) (state: 'State) =
        AvlTree.foldBack folder table.Tree state

    let toSeq (table: AvlTree<_, _>): struct (_ * _) seq = table

    let ofList (elements: struct ('Key * 'Value) list) =
        AvlTree<_, _>.ofList elements

    let ofSeq elements =
        AvlTree<_, _>.Create elements

    let ofArray (elements: struct ('Key * 'Value) array) =
        let comparer = LanguagePrimitives.FastGenericComparer<'Key>
        new AvlTree<_, _>(comparer, AvlTree.ofArray comparer elements)

    let empty<'Key, 'Value when 'Key: comparison> = AvlTree<'Key, 'Value>.Empty

    let count (table: AvlTree<_, _>) =
        table.Count

    let minKeyValue (table: AvlTree<_, _>) =
        table.MinKeyValue

    let maxKeyValue (table: AvlTree<_, _>) =
        table.MaxKeyValue

    let seek from ``to`` inclusive forwards (table: AvlTree<_, _>) = table.Seek(from, ``to``, inclusive, forwards)