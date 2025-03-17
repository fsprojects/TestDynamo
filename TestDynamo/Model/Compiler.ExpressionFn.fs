
namespace TestDynamo.Model.Compiler

open System.Diagnostics.CodeAnalysis
open System.Runtime.CompilerServices
open TestDynamo.Utils
open TestDynamo.Data.BasicStructures
open TestDynamo.Model

[<Struct; IsReadOnly>]
type ItemData =
    { item: Item
      filterParams: FilterTools }
    with
    static member getItem {item=item} = item
    
type ExpressionFnResult<'a> = Result<'a voption, NonEmptyList<string>>
// TODO: rename
type ExpressionFnX<'a> = ItemData -> ExpressionFnResult<'a>
type ExpressionFnResult = ExpressionFnResult<AttributeValue>
type ExpressionFnX = ItemData -> ExpressionFnResult

[<RequireQualifiedAccess>]
module ExpressionFnX =
    
    let resultOk x: ExpressionFnResult<_> = x |> Ok
    
    let result x: ExpressionFnResult<_> = x |> ValueSome |> Ok
    
    let inline retn x: ExpressionFnX<_> = x |> result |> asLazy
    
    let inline retnOk x: ExpressionFnX<_> = x |> resultOk |> asLazy
    
    let inline create (f: ItemData -> 'a): ExpressionFnX<'a> = f >> result
    
    let inline createOk (f: ItemData -> 'a voption): ExpressionFnX<'a> = f >> resultOk
    
    let prependError err (x: ExpressionFnX<_>) itemData: ExpressionFnResult<_> =
        match x itemData with
        | Ok _ & x' -> x'
        | Error errs -> NonEmptyList.prepend err errs |> Error
    
    // TODO: is this really needed
    let throwResult (x: ExpressionFnResult<_>) =
        match x with
        | Error xs -> NonEmptyList.unwrap xs |> Str.join "\n" |> ClientError.clientError 
        | Ok x -> x
    
    // TODO: is this really needed
    let throw (x: ExpressionFnX<_>) itemData =
        x itemData |> throwResult
    
    let defaultWith (f: ItemData -> 'a) (x: ExpressionFnX<'a>) itemData: ExpressionFnResult<'a> =
        match x itemData with
        | Ok ValueNone -> f itemData |> result
        | Ok (ValueSome _) & x
        | Error _ & x -> x
    
    let defaultFn (f: ExpressionFnX<'a>) (x: ExpressionFnX<'a>) itemData: ExpressionFnResult<'a> =
        match x itemData with
        | Ok ValueNone -> f itemData
        | Ok (ValueSome _) & x
        | Error _ & x -> x
    
    let defaultResult (d: ExpressionFnResult<'a>) (x: ExpressionFnX<'a>) itemData: ExpressionFnResult<'a> =
        match x itemData with
        | Ok ValueNone -> d
        | Ok (ValueSome _) & x
        | Error _ & x -> x
    
    let defaultValue d (x: ExpressionFnX<'a>) itemData: ExpressionFnResult<'a> =
        match x itemData with
        | Ok ValueNone -> d |> result
        | Ok (ValueSome _) & x
        | Error _ & x -> x
    
    let bind (f: _ -> ExpressionFnX<_>) (x: ExpressionFnX<_>): ExpressionFnX<_> =
        fun itemData ->
            match x itemData with
            | Ok (ValueSome x) -> f x itemData
            | Ok ValueNone -> Ok ValueNone
            | Error x -> Error x
        
    let map f (x: ExpressionFnX<_>) itemData: ExpressionFnResult<_> =
        match x itemData with
        | Ok (ValueSome x) -> f x |> ValueSome |> Ok
        | Ok ValueNone -> Ok ValueNone
        | Error x -> Error x
        
    let binaryOp
        (f: 'a -> 'b -> ExpressionFnResult<'c>)
        (x: ExpressionFnX<'a>)
        (y: ExpressionFnX<'b>)
        itemData: ExpressionFnResult<_> =
            
        match struct (x itemData, y itemData) with
        | Ok (ValueSome x'), Ok (ValueSome y') -> f x' y'
        | Ok _, Ok ValueNone
        | Ok ValueNone, Ok _ -> Ok ValueNone
        | Error exs, Error eys -> NonEmptyList.concat exs eys |> Error
        | Error es, _
        | _, Error es -> Error es
        
    type private TraverseCache<'a> =
        
        static member traverse: ExpressionFnX<'a> seq -> ExpressionFnX<'a list> =
            let prepend' = Collection.prependL >>> ValueSome >>> Ok
            let traverseStart = [] |> ValueSome |> Ok |> asLazy
            let folder = flip (binaryOp prepend')
            Seq.fold folder traverseStart
            
    let traverse<'a>: ExpressionFnX<'a> seq -> ExpressionFnX<'a list> = TraverseCache<'a>.traverse
        
    type private Nones<'a> =
        
        static member value: ExpressionFnResult<'a> = ValueNone |> Ok
        static member fn: ExpressionFnX<'a> = ValueNone |> Ok |> asLazy
        
    let noneValue<'a> = Nones<'a>.value
    
    let noneFn<'a> = Nones<'a>.fn
    
    let errResult msg: ExpressionFnResult<'a> = NonEmptyList.ofList [msg] |> Error
    
    let errFn msg: ExpressionFnX<'a> = errResult msg |> asLazy
    
    let tpl struct (l: ExpressionFnX<'a>, r: ExpressionFnX<'b>): ExpressionFnX<struct ('a * 'b)> =
        bind (fun l' -> map (tpl l') r) l 

module ExpressionFnXOps =

    /// <summary>Functor map on Result</summary>
    [<ExcludeFromCodeCoverage>]
    let inline (&?|>) x f = ExpressionFnX.map f x
    /// <summary>Extended functor map on Result</summary>
    [<ExcludeFromCodeCoverage>]
    let inline (&??|>) f g x = ExpressionFnX.map g (f x)
    /// <summary>Functor apply on Result</summary>
    [<ExcludeFromCodeCoverage>]
    let inline (<|?&) (f: ExpressionFnX<('a -> 'b)>) (x: ExpressionFnX<'a>): ExpressionFnX<'b> = ExpressionFnX.bind (fun f' -> ExpressionFnX.map f' x) f
    /// <summary>Extended functor map on Result</summary>
    [<ExcludeFromCodeCoverage>]
    let inline (&???|>) f g x y = ExpressionFnX.map g (f x y)
    /// <summary>Monad bind on Result</summary>
    [<ExcludeFromCodeCoverage>]
    let inline (&?>>=) x f = ExpressionFnX.bind f x
    /// <summary>Monad Kleisli operator on Result</summary>
    [<ExcludeFromCodeCoverage>]
    let inline (&?>=>) f g x = f x &?>>= g
    /// <summary>Extended monad Kleisli operator on Result</summary>
    [<ExcludeFromCodeCoverage>]
    let inline (&?>==>) f g x y = f x y &?>>= g
    
    