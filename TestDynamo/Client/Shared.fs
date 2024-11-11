module TestDynamo.Client.Shared

open System.Net
open Amazon.DynamoDBv2.Model
open Amazon.Runtime
open System
open System.Linq.Expressions
open System.Reflection
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators

let amazonWebServiceResponse<'a when 'a : (new : unit -> 'a) and 'a :> AmazonWebServiceResponse> () =
    let x = new 'a()
    x.ResponseMetadata <-
        let m = ResponseMetadata()
        m.ChecksumAlgorithm <- TestDynamo.Settings.AmazonWebServiceResponse.ChecksumAlgorithm
        m.ChecksumValidationStatus <- TestDynamo.Settings.AmazonWebServiceResponse.ChecksumValidationStatus
        m.RequestId <- TestDynamo.Settings.AmazonWebServiceResponse.RequestId.Invoke()
        m
    x.ContentLength <- TestDynamo.Settings.AmazonWebServiceResponse.ResponseContentLength
    x.HttpStatusCode <- HttpStatusCode.OK
    x

let getOptionalBool<'obj, 'value> name =
    let isSet =
        let expr =
            let param = Expression.Parameter typedefof<'obj>
            let method =
                typedefof<'obj>.GetMethods(BindingFlags.Instance ||| BindingFlags.NonPublic)
                |> Seq.filter (fun x ->
                    x.Name = $"IsSet{name}"
                        && x.ReturnType = typedefof<bool>
                        && (x.GetParameters() |> Array.length = 0))
                |> Seq.head

            let call = Expression.Call(param, method)
            Expression.Lambda<Func<'obj, bool>>(call, [|param|])

        expr.Compile()

    let value =
        let expr =
            let param = Expression.Parameter typedefof<'obj>
            let prop = Expression.PropertyOrField(param, name)
            Expression.Lambda<Func<'obj, 'value>>(prop, [|param|])

        expr.Compile()

    fun x ->
        match isSet.Invoke x with
        | false -> ValueNone
        | true -> value.Invoke x |> ValueSome

type MList<'a> = System.Collections.Generic.List<'a>

/// <summary>
/// Tools to generate a list of names like: (#t1, :t1), (#t2, :t2), ...
/// The value of this component is in cached strings
/// </summary>
module NameValueEnumerator =
        
    let private next' struct (prefix, i) =
        let t = i + 1
        struct (struct ($"#{prefix}{t}", $":{prefix}{t}"), t)
    
    let private cacheMax = 2000
    let private next =
        let k (x: struct (string * int)) = x
        memoize (ValueSome (cacheMax / 2, cacheMax + 1)) k next' >> sndT

    let infiniteNames prefix =
        seq {
            // allow for about 10 prefixes to be cached @ 200 items each
            let mutable i = cacheMax / 10
            let mutable tkn = next struct (prefix, 0)
            while true do
                yield fstT tkn
                tkn <- (if i > 0 then next else next') (struct (prefix, sndT tkn))
                i <- i - 1
        }

module GetUtils =

    let buildProjection projectionExpression (attributesToGet: IReadOnlyList<string>) =
        if attributesToGet <> null && attributesToGet.Count > 0 && ValueOption.isSome projectionExpression
        then clientError $"Cannot use {nameof Unchecked.defaultof<GetItemRequest>.ProjectionExpression} and {nameof Unchecked.defaultof<GetItemRequest>.AttributesToGet} in the same request"
        
        if attributesToGet <> null && attributesToGet.Count > 0
        then
            attributesToGet
            |> Collection.zip (NameValueEnumerator.infiniteNames "p")
            |> Seq.map (fun struct (struct (id, _), attr) -> struct (id, struct (id, attr)))
            |> Collection.unzip
            |> mapFst (Str.join ",")
            |> mapSnd (flip (Seq.fold (fun s struct (k, v) -> Map.add k v s)))
            |> ValueSome
        else
            projectionExpression
            ?|> flip tpl id

// [<IsReadOnly; Struct>]
// type NullFixer<'a> =
//     private
//     | F of 'a
//
//     with
//     member this.value = match this with | F x -> x
//
//         // let x = <@ fun x -> "" @>
//     member this.property (f: Quotations.Expr<'a -> 'b>): NullFixer<'b> =
//         match this with
//         | F x ->
//             f. x |> CSharp.notNull name |> F
//
//     member this.list (f: Quotations.Expr<'a -> MList<'b>>): NullFixer<MList<'b>> =
//         match this with
//         | F x -> f x |> CSharp.list name |> F
//
//     member this.enumerable f =
//         match this with
//         | F x -> f x |> CSharp.enumerable |> F
//
//
//     // member this.value () =
//     //     match this with
//     //     | F
//
// and NullFixer() =
//     static member ``val``<'a when 'a : struct> (x: 'a) = F x
//     static member ref<'a when 'a : null> name (x: 'a) =
//         CSharp.notNull name x |> F
//
// // module Nulls =
// //     let obj name root f =
// //         let data =
// //         CSharp.notNull
// //     let list = CSharp.noNullsOrEmpty
// //