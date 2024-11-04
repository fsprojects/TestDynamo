module TestDynamo.Client.Shared

open System.Net
open Amazon.Runtime
open System
open System.Linq.Expressions
open System.Reflection

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