/// <summary>
/// <para>
/// Operators consist of a generic operator (e.g. >>=) and a prefix (e.g. &amp;).
/// The generic operators describe the operation and the prefix describes the type
/// </para>
/// <ul>
///     <li>map: "|>", haskell "$", take inspiration from pipe (|>) which is like a map in non-elevated space
///         <ul><li>extended by duplicating the prefix if first function has more args (e.g. ???|>)</li></ul></li>
///     <li>bind: ">>=", haskell ">>="</li>
///     <li>apply: "&lt;|", haskell "&lt;*>", take inspiration from reverse pipe (&lt;|) which is like an apply in non-elevated space</li>
///     <li>kleisli arrows: ">=>", haskell ">=>"
///         <ul><li>extended if first function has more args (>====>)</li></ul></li>
/// </ul>
/// <para>
/// Different monad types have prefixes to keep the F# compiler happy. FSharpPlus is not imported to keep dependencies down, and because it makes compliation slow
/// <ul>
///     <li>ValueOption: "?"</li>
///     <li>Result: "&amp;"</li>
///     <li>ValueTask: "|%"</li>
/// </ul>
/// </para>
/// </summary>
module TestDynamo.Data.Monads.Operators

// NOTE: operators for streams (lists) not added, as f# is a bit messy with lists, seqs and arrays

open System.Diagnostics.CodeAnalysis
open TestDynamo.Utils

/// <summary>Functor map on ValueOption</summary>
[<ExcludeFromCodeCoverage>]
let inline (?|>) x f = ValueOption.map f x
/// <summary>Extended functor map on ValueOption</summary>
[<ExcludeFromCodeCoverage>]
let inline (??|>) f g x = ValueOption.map g (f x)
/// <summary>Functor apply on ValueOption</summary>
[<ExcludeFromCodeCoverage>]
let inline (<|?) f x = ValueOption.bind (fun f' -> ValueOption.map f' x) f
/// <summary>Monad bind on ValueOption</summary>
[<ExcludeFromCodeCoverage>]
let inline (?>>=) x f = ValueOption.bind f x
/// <summary>Monad Kleisli operator on ValueOption</summary>
[<ExcludeFromCodeCoverage>]
let inline (?>=>) f g x = f x ?>>= g
/// <summary>ValueOption.defaultValue</summary>
[<ExcludeFromCodeCoverage>]
let inline (?|?) opt x = ValueOption.defaultValue x opt
/// <summary>ValueOption.defaultValue</summary>
[<ExcludeFromCodeCoverage>]
let inline (??|?) f x opt = ValueOption.defaultValue x (f opt)
/// <summary>ValueOption.defaultWith</summary>
[<ExcludeFromCodeCoverage>]
let inline (?|>?) opt f = ValueOption.defaultWith f opt

/// <summary>Functor map on Result</summary>
[<ExcludeFromCodeCoverage>]
let inline (&|>) x f = Result.map f x
/// <summary>Extended functor map on Result</summary>
[<ExcludeFromCodeCoverage>]
let inline (&&|>) f g x = Result.map g (f x)
/// <summary>Extended functor map on Result</summary>
[<ExcludeFromCodeCoverage>]
let inline (&&&|>) f g x y = Result.map g (f x y)
/// <summary>Functor apply on Result</summary>
[<ExcludeFromCodeCoverage>]
let inline (<|&) f x = Result.bind (fun f' -> Result.map f' x) f
/// <summary>Monad bind on Result</summary>
[<ExcludeFromCodeCoverage>]
let inline (&>>=) x f = Result.bind f x
/// <summary>Monad Kleisli operator on Result</summary>
[<ExcludeFromCodeCoverage>]
let inline (&>=>) f g x = f x &>>= g
/// <summary>Extended monad Kleisli operator on Result</summary>
[<ExcludeFromCodeCoverage>]
let inline (&>==>) f g x y = f x y &>>= g

/// <summary>Functor map on ValueTask&lt;_></summary>
[<ExcludeFromCodeCoverage>]
let inline (|%|>) x f = Io.map f x
/// <summary>Extended functor map on ValueTask&lt;_></summary>
[<ExcludeFromCodeCoverage>]
let inline (|%%|>) f g x = Io.map g (f x)
/// <summary>Extended functor map on ValueTask&lt;_></summary>
[<ExcludeFromCodeCoverage>]
let inline (|%%%|>) f g x y = Io.map g (f x y)
/// <summary>Functor apply on ValueTask&lt;_></summary>
[<ExcludeFromCodeCoverage>]
let inline (<|%|) f x = Io.bind (fun f' -> Io.map f' x) f
/// <summary>Monad bind on ValueTask&lt;_></summary>
[<ExcludeFromCodeCoverage>]
let inline (|%>>=) x f = Io.bind f x
/// <summary>Monad Kleisli operator on ValueTask&lt;_></summary>
[<ExcludeFromCodeCoverage>]
let inline (|%>=>) f g x = f x |%>>= g
/// <summary>Extended monad Kleisli operator on ValueTask&lt;_></summary>
[<ExcludeFromCodeCoverage>]
let inline (|%>==>) f g x y = f x y |%>>= g