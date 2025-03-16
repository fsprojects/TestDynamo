namespace TestDynamo.Data.BasicStructures

open System
open TestDynamo.Utils
open System.Runtime.CompilerServices

[<IsReadOnly; Struct>]
type CreateOrDelete<'a> =
    | Create of c: 'a
    | Delete of 'a

[<IsReadOnly; Struct>]
type NonEmptyList<'a> =
    private | L of 'a list

module NonEmptyList =
    let ofHeadTail head tail = head::tail |> L
    let prepend head (L tail) = head::tail |> L
    let singleton x = [x] |> L
    let ofList x =
        match x with
        | [] -> invalidOp "List cannot be empty"
        | xs -> L xs

    let unwrap (L xs) = xs

    let head (L xs) = List.head xs

    let pop = function
        | L (head::(_::_ & tail)) -> struct (head, ofList tail |> ValueSome)
        | xs -> struct (head xs, ValueNone)

    let concat (L xs) (L ys) = xs @ ys |> L
    
[<IsReadOnly; Struct;>]
type SortedList<'a> =
    private | Sl of 'a list
    
module SortedList =
    
    let create (vals: _ seq) =
        Seq.sort vals
        |> List.ofSeq
        |> Sl

    type SortedListCache<'a> =
        static member empty: SortedList<'a> = Sl []
        
    let empty<'a> = SortedListCache<'a>.empty
    
    let rec private add' (c: IComparer<_>) i value = function
        | [] -> struct ([value], i)
        | head::_ & vs when c.Compare(value, head) < 0 -> value::vs, i
        | _::tail -> add' c (i + 1) value tail
        
    let add value (Sl vs) =
        let c = System.Collections.Generic.Comparer<'a>.Default
        match add' c 0 value vs with
        | vs', 0 -> Sl vs'
        | vs', 1 -> (List.head vs)::vs' |> Sl
        | vs', i ->
            Seq.truncate i vs
            |> flip Collection.concat2 vs'
            |> List.ofSeq
            |> Sl
