namespace TestDynamo.Data.Monads

open TestDynamo.Data.BasicStructures
open TestDynamo.Data.Monads
open TestDynamo.Utils

type MaybeLazy<'env, 'a> = Either<'a, Reader<'env, 'a>>

[<RequireQualifiedAccess>]    
module MaybeLazy =

    let inline retn (x: 'a): MaybeLazy<'env, 'a> = x |> Either1

    let inline ``lazy`` (x: Reader<'env, 'a>): MaybeLazy<'env, 'a> = x |> Either2

    let inline execute env (x: MaybeLazy<'env, 'a>) =
        match x with
        | Either1 x -> x
        | Either2 f -> f env

    let inline map (f: 'a -> 'b) (x: MaybeLazy<'env, 'a>): MaybeLazy<'env, 'b> =
        match x with
        | Either1 x -> f x |> Either1
        | Either2 x -> (x >> f) |> Either2

    let inline bind (f: 'a -> MaybeLazy<'env, 'b>) (x: MaybeLazy<'env, 'a>): MaybeLazy<'env, 'b> =
        match x with
        | Either1 x -> f x
        | Either2 x ->
            Either2 (fun env -> x env |> f |> execute env)

    let inline apply (x: MaybeLazy<'env, 'a>) (f: MaybeLazy<'env, 'a -> 'b>): MaybeLazy<'env, 'b> =
        bind (flip map x) f

    let inline traverse (x: MaybeLazy<'env, 'a> seq): MaybeLazy<'env, 'a list> =
        Seq.map (map Collection.prependL) x
        |> Collection.foldBack apply (retn [])

type MaybeLazyResult<'env, 'a> = Either<Result<'a, NonEmptyList<string>>, ReaderResult<'env, 'a>>
module MaybeLazyResult =
    let inline fromReaderResult f: MaybeLazyResult<'env, 'a> = Either2 f
    let inline fromResult (x: Result<'a, NonEmptyList<string>>): MaybeLazyResult<'env, 'a> = Either1 x
    let inline fromReader f: MaybeLazyResult<'env, 'a> = Reader.map Ok f |> fromReaderResult
    let inline retn (x: 'a): MaybeLazyResult<'env, 'a> = x |> Ok |> fromResult

    let inline mapEnv f (x: MaybeLazyResult<'env1, 'a>): MaybeLazyResult<'env2, 'a> =
        match x with
        | Either1 x -> Either1 x
        | Either2 x -> ReaderResult.mapEnv f x |> Either2

    let inline execute args (x: MaybeLazyResult<'env, 'a>) =
        MaybeLazy.execute args x

    let inline private sanitize x =
        match x with
        | Ok x -> x
        | Error err -> err |> Error |> Either1

    let inline map (f: 'a -> 'b) (x: MaybeLazyResult<'env, 'a>): MaybeLazyResult<'env, 'b> =
        match x with
        | Either1 x -> Result.map f x |> Either1
        | Either2 x -> x >> Result.map f |> Either2

    let inline bind (f: 'a -> MaybeLazyResult<'env, 'b>) (x: MaybeLazyResult<'env, 'a>): MaybeLazyResult<'env, 'b> =        
        match x with
        | Either1 x -> Result.map f x |> sanitize
        | Either2 x ->
            fun args -> x args |> Result.map f |> sanitize |> Either.map2Of2 (apply args) |> Either.reduce
            |> Either2

    let private applyResult = function
        | struct (Error errF, Error errX) -> NonEmptyList.concat errF errX |> Error
        | f, x -> Result.bind (flip Result.map x) f

    /// <summary>Not strictly monadic</summary>      
    let apply (x: MaybeLazyResult<'env, 'a>) (f: MaybeLazyResult<'env, 'a -> 'b>): MaybeLazyResult<'env, 'b> =
        match struct (x, f) with
        | Either1 x, Either1 f -> struct (f, x) |> applyResult |> Either1
        | Either2 x, Either2 f ->
            tplDouble
            >> mapFst f
            >> mapSnd x
            >> applyResult
            |> Either2
        | _ -> bind (flip map x) f

    let inline traverse (x: MaybeLazyResult<'env, 'a> seq): MaybeLazyResult<'env, 'a list> =
        Seq.map (map Collection.prependL) x
        |> Collection.foldBack apply (retn [])