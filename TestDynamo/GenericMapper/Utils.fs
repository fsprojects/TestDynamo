module TestDynamo.GenericMapper.Utils

open System
open System.Reflection
open System.Text.RegularExpressions
open Microsoft.FSharp.Core
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils

let traverseTpl1 = function
    | struct (ValueNone, _) -> ValueNone
    | ValueSome x, y -> ValueSome struct (x, y)
let traverseTpl2 = function
    | struct (_, ValueNone) -> ValueNone
    | x, ValueSome y -> ValueSome struct (x, y)

let allOrNone (xs: _ voption seq) =
    let fold xs x =
        ValueSome Collection.prependL
        <|? x
        <|? xs

    Collection.foldBack fold (ValueSome []) xs

module Reflection =

    let rec typeName (t: Type) =
        if not t.IsGenericType then t.Name
        else
            let gens = t.GetGenericArguments() |> Seq.map typeName |> Str.join ", "
            match Regex.Replace(t.Name, @"`.*", "") with
            | "FSharpValueOption" -> sprintf "%s voption" gens
            | "FSharpMap" -> sprintf "Map<%s>" gens
            | "FSharpList" -> sprintf "%s list" gens
            | "IEnumerable" -> sprintf "%s seq" gens
            | x -> sprintf "%s<%s>" x gens

    let voidType = typeof<IDisposable>.GetMethod("Dispose").ReturnType

    type MethodDescription =
        { isStatic: bool
          t: Type
          name: string
          args: Type list
          returnType: Type
          classGenerics: Type list
          methodGenerics: Type list
          caseInsensitiveName: bool }

    let rec private argsLike' = function
        | struct ([], []) -> true
        | [], _ -> false
        | _, [] -> false
        | (argType: Type)::tail1, (arg: Type)::tail2 ->
            argType.IsAssignableFrom arg && argsLike' (tail1, tail2)

    let argsLike = curry argsLike'

    let createInstance (t: Type) (args: obj seq) = Activator.CreateInstance(t, args |> Array.ofSeq)

    let makeGeneric (generics: Type list) (methodInfo: MethodInfo) =
        match struct (generics, methodInfo.IsGenericMethod) with
        | [], true -> ValueNone
        | xs, true when methodInfo.GetGenericArguments().Length = xs.Length ->
            // bug here. Not taking into account generic constraints
            methodInfo.MakeGenericMethod(Array.ofList xs) |> ValueSome
        | _, true -> ValueNone
        | [], false -> methodInfo |> ValueSome
        | _, false -> ValueNone

    let tryMethod description =

        let instanceFlag = if description.isStatic then BindingFlags.Static else BindingFlags.Instance

        let t =
            if description.classGenerics = []
            then description.t
            else description.t.MakeGenericType(description.classGenerics |> Array.ofList)

        t.GetMethods(instanceFlag ||| BindingFlags.Public)
        |> Collection.concat2 (t.GetMethods(instanceFlag ||| BindingFlags.NonPublic))
        |> if description.caseInsensitiveName
           then Seq.filter (_.Name >> _.Equals(description.name, StringComparison.OrdinalIgnoreCase))
           else Seq.filter (_.Name >> (=)description.name)
        |> Seq.map (makeGeneric description.methodGenerics)
        |> Maybe.traverse
        |> Seq.filter (_.ReturnType >> (=)description.returnType)
        |> Seq.filter (_.GetParameters() >> Seq.map _.ParameterType >> List.ofSeq >> flip argsLike description.args)
        |> Collection.tryHead

    let private descriptionErr description =
        $"Could not find method - {description}"
    let method description =
        try
            tryMethod description
            ?|>? fun _ -> descriptionErr description |> invalidOp
        with
        | e ->
            InvalidOperationException(descriptionErr description, e) |> raise

    let piName (p: PropertyInfo) = p.Name

    module Elevated =
        let getNullableType (t: Type) =
            if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Nullable<_>>
            then t.GetGenericArguments() |> Array.head |> ValueSome
            else ValueNone
            
        let getValueOptionType (t: Type) =
            if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<_ voption>
            then t.GetGenericArguments() |> Array.head |> ValueSome
            else ValueNone

        let getEnumerableType (t: Type) =
            t.GetInterfaces()
            |> Seq.ofArray
            |> if t.IsInterface then Collection.prepend t else id
            |> Seq.filter (fun x -> x.IsGenericType && x.GetGenericTypeDefinition() = typedefof<IEnumerable<_>>)
            |> Seq.map (_.GetGenericArguments() >> Array.head)
            |> Collection.tryHead

        let getKeyValuePairType (t: Type) =
            if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<KeyValuePair<_, _>>
            then t.GetGenericArguments() |> function | [|x; y|] -> struct (x, y) |> ValueSome | _ -> ValueNone
            else ValueNone

        let funcType (ts: Type list) =
            match ts with
            | [] -> invalidOp "Invalid func type"
            | [_] -> invalidOp "Invalid func type"
            | [x1; y] -> typedefof<_ -> _>.MakeGenericType(Array.ofList ts)
            | [x1; x2; y] -> typedefof<_ -> _ -> _>.MakeGenericType(Array.ofList ts)
            | [x1; x2; x3; y] -> typedefof<_ -> _ -> _ -> _>.MakeGenericType(Array.ofList ts)
            | [x1; x2; x3; x4; y] -> typedefof<_ -> _ -> _ -> _ -> _>.MakeGenericType(Array.ofList ts)
            | [x1; x2; x3; x4; x5; y] -> typedefof<_ -> _ -> _ -> _ -> _ -> _>.MakeGenericType(Array.ofList ts)
            | _ -> notSupported "Only supports up to 5 args"

module Tuple =
    let values (t: Type) =
        if not t.IsConstructedGenericType
            || t.GetGenericTypeDefinition() <> typedefof<System.ValueTuple<_, _>>
        then invalidOp $"Type {t} is not a ValueTuple`2"
        
        struct (t.GetField("Item1"), t.GetField("Item2"))

module Converters =
    open Reflection
    open Reflection.Elevated

    /// <summary>Some static methods that make reflection easier (more C# like)</summary>
    type private UtilHelpers() =

        static member fromFunc<'a, 'b>(f: Func<'a, 'b>): 'a -> 'b = f.Invoke
        static member toFunc<'a, 'b>(f: 'a -> 'b): Func<'a, 'b> = f
        static member valueNone<'a> (): 'a voption = ValueNone
        static member defaultOrNull<'a> () = Unchecked.defaultof<'a>

    let fromFunc (f: obj) =
        let t = f.GetType()
        if not t.IsGenericType || t.GetGenericTypeDefinition() <> typedefof<System.Func<_, _>>
        then invalidOp $"Value of type {t} is not a func"

        ({ isStatic = true
           t = typeof<UtilHelpers>
           name = nameof UtilHelpers.fromFunc
           args = [t]
           returnType = t.GetGenericArguments() |> List.ofArray |> funcType
           methodGenerics = t.GetGenericArguments() |> List.ofArray
           classGenerics = []
           caseInsensitiveName = false } |> method).Invoke(null, [|f|])

    let toFunc (f: obj) =
        let t = f.GetType()
        if not t.IsGenericType || t.GetGenericTypeDefinition() <> typedefof<_ -> _>
        then invalidOp $"Value of type {t} is not a fun"

        ({ isStatic = true
           t = typeof<UtilHelpers>
           name = nameof UtilHelpers.toFunc
           args = [t]
           returnType = typedefof<Func<_, _>>.MakeGenericType(t.GetGenericArguments())
           methodGenerics = t.GetGenericArguments() |> List.ofArray
           classGenerics = []
           caseInsensitiveName = false } |> method).Invoke(null, [|f|])

    let valueNone t =
        { isStatic = true
          t = typeof<UtilHelpers>
          name = nameof UtilHelpers.valueNone
          args = []
          returnType = typedefof<_ voption>.MakeGenericType([|t|])
          methodGenerics = [t]
          classGenerics = []
          caseInsensitiveName = false }
        |> method
        |> _.Invoke(null, [||])

    let defaultOrNull t =
        match getValueOptionType t with
        | ValueSome t -> valueNone t
        | ValueNone ->
            { isStatic = true
              t = typeof<UtilHelpers>
              name = nameof UtilHelpers.defaultOrNull
              args = []
              returnType = t
              methodGenerics = [t]
              classGenerics = []
              caseInsensitiveName = false }
            |> method
            |> _.Invoke(null, [||])