module TestDynamo.CSharp

open System.Collections.Concurrent
open System.Collections.Generic
open System.Runtime.CompilerServices
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open System.Linq.Expressions
open TestDynamo.Utils

type MList<'a> = System.Collections.Generic.List<'a>

let notNull name = function
    | null -> nullArg $"{name} is null"
    | x -> x

let kvpToTpl (kvp: KeyValuePair<_, _>) = struct (kvp.Key, kvp.Value)

/// <summary>
/// Converts a null seq to empty and removes any null values
/// </summary>
let orEmpty = function
    | null -> Seq.empty
    | xs -> xs

/// <summary>
/// Converts a null seq to empty and removes any null values
/// </summary>
let sanitizeSeq xs = orEmpty xs |> Seq.filter ((<>)null)

let list name = function
    | null -> List<'a>()
    | (xs: List<'a>) ->
        use mutable vals = xs.GetEnumerator()
        while vals.MoveNext() do
            match vals.Current with
            | null -> nullArg $"{name} is has null values"
            | _ -> ()

        xs

let toOption = function
    | null -> ValueNone
    | x -> ValueSome x
    
let toNullOrEmptyOption = function
    | ""
    | null -> ValueNone
    | x -> ValueSome x

let fromOption = function
    | ValueNone -> null
    | ValueSome x -> x

let emptyStringToNull = function
    | null -> null
    | (x: string) when System.String.IsNullOrWhiteSpace x -> null
    | x -> x

let mandatory msg = function
    | null -> clientError msg
    | x -> x

let toMap kf vf (dictionary: Dictionary<'k, 'v>) =

    dictionary
    |> Seq.map (fun x -> struct (kf x.Key, vf x.Value))
    |> MapUtils.fromTuple

let toDictionary kf vf (map: Map<'k, 'v>) =

    map
    |> Seq.map (fun x -> KeyValuePair.Create (kf x.Key, vf x.Value))
    |> Dictionary

let tplToDictionary (items: struct ('k * 'v) seq) =
    items
    |> Seq.map (fun x -> KeyValuePair.Create (fstT x, sndT x))
    |> Dictionary

let arrayToList (items: 'a array) =
    let list = System.Collections.Generic.List<_>(items.Length)
    if items.Length > 0 then list.AddRange(items)
    list

module private Quotes =

    let private someIfFalse tf x =
        match tf with
        | false -> ValueSome x
        | true -> ValueNone

    let rec private asExpression dryRun (param: struct (Var * Expression)) expr: struct (string list voption * Expression voption) =
        match struct (param, expr) with
        | (var, expr), Var value when value = var -> struct (ValueSome ["INPUT"], someIfFalse dryRun expr)
        | _, Var x -> clientError $"Unexpected Var {x}"
        | _, Value (value, _) ->
            struct (ValueNone, someIfFalse dryRun (Expression.Constant value))
        | _, PropertyGet (context, prop, []) ->
            let struct (ctxtPath, ctxt) =
                Option.map (asExpression dryRun param) context
                |> Option.defaultValue (ValueSome [], ValueSome null)

            let path = ValueOption.map (fun x -> prop.Name::x) ctxtPath
            let expr = ValueOption.map (fun x -> Expression.PropertyOrField(x, prop.Name) :> Expression) ctxt
            struct (path, expr)
        | _, FieldGet (context, field) ->
            let struct (ctxtPath, ctxt) =
                Option.map (asExpression dryRun param) context
                |> Option.defaultValue (ValueSome [], ValueSome null)

            let path = ValueOption.map (fun x -> field.Name::x) ctxtPath
            let expr = ValueOption.map (fun x -> Expression.PropertyOrField(x, field.Name) :> Expression) ctxt
            struct (path, expr)
        | _, PropertyGet (_, _, args) -> clientError $"Unknown property args {args}"
        | _, x -> clientError $"Only a subset of quotations are supported; {x.GetType()}: {x}"

    let private compileQuotation' (lambda: Expr<'a -> 'b>) =
        match lambda with
        | Lambda (var, expr) ->
            let param = Expression.Parameter var.Type
            let struct (_, body) = asExpression false struct (var, param) expr

            Expression.Lambda<System.Func<'a, 'b>>(body |> Maybe.expectSome, [|param|]) |> _.Compile()
        | x -> clientError $"Only a subset of quotations are supported: {x}"

    let private compileCache = ConcurrentDictionary<obj, obj>()
    let compileQuotation (lambda: Expr<'a -> 'b>) =
        match lambda with
        | Lambda (var, expr) ->
            let struct (key', _) = asExpression true struct (var, null) expr
            key'
            |> ValueOption.map (fun x -> compileCache.TryGetValue x |> tpl x)
            |> ValueOption.map (function
                | _, (true, x) -> x :?> System.Func<'a, 'b>
                | key, (false, _) ->
                    let compiled = compileQuotation' lambda
                    compileCache.TryAdd(key, compiled) |> ignore
                    compiled)
            |> ValueOption.defaultWith (fun _ -> compileQuotation' lambda)
        | x -> clientError $"Only a subset of quotations are supported: {x}"

let nullSafe (lambda: Expr<'a -> 'b>) x =
    let func = Quotes.compileQuotation lambda
    func.Invoke x

[<IsReadOnly; Struct>]
type NullFixer<'a> =
    private
    | N of struct (string list * 'a)

module NullFixer =
     let create context x =
         match context with
         | ValueNone -> N struct ([], x)
         | ValueSome c -> N struct ([c], x)

     let map expr = function
         | N struct (ctxt, x) ->
             let result = (Quotes.compileQuotation expr).Invoke x
             N struct (ctxt, result)

     let value = function
         | N struct (_, x) -> x

// [<IsReadOnly; Struct>]
// type NullFixer<'a> =
//     private
//     | N of struct (string list * 'a)
//
//      with
//      member this.value = match this with | N x -> x
//
//      member private this.reScope validate (prop: Expr<('a -> 'b)>) =
//          let getter = Exprs.compileQuotation prop
//          match this with
//          | N (scope, x) ->
//              let newScope = prop.ToString()::scope
//              getter.Invoke x |> validate ((List.rev newScope).ToString()) |> tpl newScope |> N
//
//      member this.property (prop: Expr<('a -> 'b)>): NullFixer<'b> =
//          this.reScope CSharp.notNull prop
//
//      member this.list (prop: Expr<('a -> MList<'b>)>): NullFixer<MList<'b>> =
//          this.reScope CSharp.list prop
//
//      member this.enumerable (prop: Expr<('a -> IEnumerable<'b>)>): NullFixer<IEnumerable<'b>> =
//          this.reScope (asLazy id) prop
//
// module NullFixer =
//      let ``val``<'a when 'a : struct> (x: 'a) = N struct ([(typedefof<'a>).Name], x)
//      let ref<'a when 'a : null> name (x: 'a) =
//          let name = (typedefof<'a>).Name
//          CSharp.notNull name x |> tpl [name] |> N
//
//      let value = function | N struct (_, x) -> x
//
//      let scope = function | N struct (x, _) -> x
//
//      let property (prop: Expr<('a -> 'b)>) (x: NullFixer<'a>): NullFixer<'b> =
//          x.property prop
//
//      let list (prop: Expr<('a -> MList<'b>)>) (x: NullFixer<'a>): NullFixer<MList<'b>> =
//          x.list prop
//
//      let enumerable (prop: Expr<('a -> IEnumerable<'b>)>) (x: NullFixer<'a>): NullFixer<IEnumerable<'b>> =
//          x.enumerable prop
//
//      let expand (x: NullFixer<MList<'a>>) =
//          let struct (context, validated) = list <@fun x -> x@> x |> function | N x -> x
//          validated
//          |> Seq.mapi tpl
//          |> Seq.map (mapFst (fun x -> $"[{x}]"::context) >> N)