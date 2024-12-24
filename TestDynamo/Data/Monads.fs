namespace TestDynamo.Data.Monads
open TestDynamo
open TestDynamo.Data.BasicStructures
open Utils

// Writer monad. Might come in useful later
type Writer<'emit, 'a> = | W of struct ('emit list * 'a)
[<RequireQualifiedAccess>]
module Writer =
    let inline create e x = struct ([e], x) |> W
    let inline retn x = struct([], x) |> W
    let inline map f = function | W w -> mapSnd f w |> W
    let inline mapEmits f = function | W w -> mapFst f w |> W
    let inline mapEmit f = List.map f |> mapEmits
    let inline bind f = function
        | W struct (e, x) -> match f x with | W (e', y) -> struct (e'@e, y) |> W
    let inline apply x f =
        match struct (f, x) with
        | W ([], f'), W (xe, x') -> struct (xe, f' x') |> W
        | W (fe, f'), W (xe, x') -> struct (xe @ fe, f' x') |> W
    let inline traverseTpl struct (x, y) =
        retn tpl |> apply x |> apply y
    let inline traverse ws =
        let prepend wx = bind (fun s -> map (fun x -> x::s) wx)
        retn [] |> List.foldBack prepend ws

    let inline execute (W w') = mapFst List.rev w'

    // let inline executeNested w =
    //     let struct (e1, r1) = execute w
    //     let struct (e2, r2) = execute r1
    //     struct (struct (e1, e2), r2)

    let traverseOpt = function
        | W struct (_, ValueNone) -> ValueNone
        | W struct (emit, ValueSome x) -> W struct (emit, x) |> ValueSome

    let traverseResult = function
        | W struct (_, Error x) -> Error x
        | W struct (emit, Ok x) -> W struct (emit, x) |> Ok

    let inline traverseResultF (W struct (emit, f)) =
        f >> Result.map (tpl emit >> W)

type Reader<'env, 'a> = 'env -> 'a

[<RequireQualifiedAccess>]
module Reader =

    let retn: 'a -> Reader<'env, 'a> = asLazy

    let inline create (x: 'env -> 'a): Reader<'env, 'a> = x

    let execute: 'env -> Reader<'env, 'a> -> 'a = apply

    let inline map (f: 'a -> 'b) (x: Reader<'env, 'a>): Reader<'env, 'b> = x >> f

    let inline invokeTwice f x = f x x

    let inline bind (f: 'a -> Reader<'env, 'b>) (x: Reader<'env, 'a>): Reader<'env, 'b> =
        x >> f |> invokeTwice

    let inline apply (x: Reader<'env, 'a>) (f: Reader<'env, 'a -> 'b>): Reader<'env, 'b> =
        bind (flip map x) f

    let inline traverse (x: Reader<'env, 'a> seq): Reader<'env, 'a list> =
        Seq.map (map Collection.prependL) x
        |> Collection.foldBack apply (retn [])

    let inline traverseSeq (x: Reader<'env, 'a> seq): Reader<'env, 'a seq> =
        create (fun env ->
            seq {
                for r in x do
                    yield execute env r
            })

type ReaderResult<'env, 'a> = 'env -> Result<'a, NonEmptyList<string>>

[<RequireQualifiedAccess>]    
module ReaderResult =

    let inline retn (x: 'a): ReaderResult<'env, 'a> = x |> Ok |> asLazy

    let inline fromReader (x: Reader<'env, 'a>): ReaderResult<'env, 'a> = Reader.map Ok x

    let inline fromResult (x: 'env -> 'a): ReaderResult<'env, 'a> = Reader.map Ok x

    let inline create (x: 'env -> Result<'a, NonEmptyList<string>>): ReaderResult<'env, 'a> = x

    let execute: 'env -> ReaderResult<'env, 'a> -> Result<'a, NonEmptyList<string>> = apply

    let inline map (f: 'a -> 'b) (x: ReaderResult<'env, 'a>): ReaderResult<'env, 'b> = x >> Result.map f

    let inline mapEnv (f: 'env2 -> 'env1) (x: ReaderResult<'env1, 'a>): ReaderResult<'env2, 'a> = f >> x

    let inline invokeTwice f x = f x x

    let inline bind (f: 'a -> ReaderResult<'env, 'b>) (x: ReaderResult<'env, 'a>): ReaderResult<'env, 'b> =
        fun env -> x env |> Result.bind (flip f env)

    let private applyResult = function
        | struct (Error errF, Error errX) -> NonEmptyList.concat errF errX |> Error
        | f, x -> Result.bind (flip Result.map x) f

    let private curriedApplyResult x f =  applyResult struct (f, x)

    /// <summary>Not strictly monadic</summary>
    let apply (x: ReaderResult<'env, 'a>) (f: ReaderResult<'env, 'a -> 'b>): ReaderResult<'env, 'b> =
        tplDouble
        >> mapFst f
        >> mapSnd x
        >> applyResult

    let traverse (x: ReaderResult<'env, 'a> seq): ReaderResult<'env, 'a list> =
        Seq.map (map Collection.prependL) x
        |> Collection.foldBack apply (retn [])

    let traverseTpl struct (x: ReaderResult<'env, 'a>, y: ReaderResult<'env, 'b>): ReaderResult<'env, struct ('a * 'b)> =
        tplDouble
        >> mapFst (x >> curriedApplyResult)
        >> mapSnd (y >> curriedApplyResult)
        >> fun struct (x, y) -> Ok tpl |> x |> y

type State<'state, 'a> = 'state -> struct ('state * 'a)

[<RequireQualifiedAccess>]
module State =

    let inline retn x: State<'state, 'a> = flip tpl x

    let inline create (x: 'state -> struct ('state * 'a)): State<'state, 'a> = x

    let inline fromFn (f: 'state -> 'a): State<'state, 'a> = tplDouble >> mapSnd f

    let execute: 'state -> State<'state, 'a> -> struct ('state * 'a) = apply

    let inline map (f: 'a -> 'b) (x: State<'state, 'a>): State<'state, 'b> = x >> mapSnd f

    let inline invokeTwice f x = f x x

    let inline bind (f: 'a -> State<'state, 'b>) (x: State<'state, 'a>): State<'state, 'b> =
        x >> mapSnd f >> flipTpl >> applyTpl

    let private fnApply = apply
    /// <summary>NOTE: state is applied to function first, arg second</summary>
    let inline apply (x: State<'state, 'a>) (f: State<'state, 'a -> 'b>): State<'state, 'b> =
        bind (flip map x) f

    let collect (xs: State<'state, 'a> seq): State<'state, 'a list> =
        let folded =
            flip (Seq.fold (fun struct (state, acc) ->
                fnApply state
                >> mapSnd (flip Collection.prependL acc))) xs
            >> mapSnd List.rev

        create (flip tpl [] >> folded)

module Result =
    type private Cache<'a, 'b>() =
        static member throw fErr title: Result<'a, NonEmptyList<string>> -> 'a =
            Result.mapError (
                NonEmptyList.unwrap
                >> Seq.map (sprintf " * %s")
                >> Str.join "\n"
                >> sprintf title)
            >> function | Ok x -> x | Error x -> fErr x

        static member traverse: Result<'a, NonEmptyList<'b>> seq -> Result<'a list, NonEmptyList<'b>> =
            Collection.foldBack (fun struct (ok, success, failure) -> function
                | Ok x -> struct (ok, x::success, failure)
                | Error err -> struct (false, success, err::failure)) struct (true, [], [])
            >> function
                | false, _, failure ->
                    let errs = List.collect NonEmptyList.unwrap failure
                    NonEmptyList.ofList errs |> Error
                | true, success, _ -> Ok success

    // not strictly monadic
    let traverse xs = Cache<_, _>.traverse xs

    let throw fErr title = Cache<_, _>.throw fErr title

    let unwrapTpl1 = function
        | struct (Ok x, y) -> struct (x, y) |> Ok
        | Error x, _ -> Error x

    let unwrapTpl2 = function
        | struct (x, Ok y) -> struct (x, y) |> Ok
        | _, Error y -> Error y

    let tryRecover f = function
        | Ok _ & x -> x
        | Error y -> f y