namespace TestDynamo.Data

open System.Collections.Generic
open System.Linq
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

type Lookup<'k, 'v> (eq: IEqualityComparer<'k>, items: struct ('k * 'v) seq) =
    
    let cmp = Comparison.tplFstComparer eq
    
    let filter k x = eq.Equals(k, fstT x) 
    
    let validateUniqueness (xs: struct ('k * 'v) array) =
        let unq = Enumerable.Distinct(xs, cmp).Count()
        if unq <> xs.Length then invalidOp $"Duplicate key found in lookup"
        xs
    
    let items =
        match items with
        | :? (struct ('k * 'v) array) as a -> a
        | xs -> Array.ofSeq xs
        |> validateUniqueness
    
    
    new(items: struct ('k * 'v) seq) = Lookup(EqualityComparer.Default, items)
    
    member _.tryGetValue k =
        Array.filter (filter k) items
        |> Collection.tryHead
        ?|> sndT

[<RequireQualifiedAccess>]
module Lookup =
    let create eq items =
        match eq with
        | ValueNone -> Lookup items
        | ValueSome e -> Lookup(e, items)
        
    let tryGetValue k (l: Lookup<_, _>) = l.tryGetValue(k)

