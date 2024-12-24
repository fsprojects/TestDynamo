
namespace TestDynamo.Model.Compiler

open System
open System.Text
open TestDynamo.Data.Monads
open TestDynamo.Data.Monads.Operators
open TestDynamo.Data.BasicStructures
open TestDynamo.Model
open TestDynamo.Model.Compiler.Lexer
open TestDynamo.Utils
open TestDynamo
open System.Runtime.CompilerServices

type FilterTools =
    { logger: Logger
      expressionAttributeNameLookup: string -> string voption
      expressionAttributeValueLookup: string -> AttributeValue voption }

[<Struct; IsReadOnly; CustomComparison; CustomEquality>]
type ResolvedAccessorType =
    | ResolvedAttribute of s: string
    | ResolvedListIndex of uint

    with
    override this.ToString() =
        match this with
        | ResolvedAttribute x -> x
        | ResolvedListIndex x -> ResolvedAccessorType.indexToString x

    override this.GetHashCode() =
        match this with
        | ResolvedAttribute x -> HashCode.Combine(1, x)
        | ResolvedListIndex x -> HashCode.Combine(2, x)

    override this.Equals(obj) =
        match obj with
        | :? ResolvedAccessorType as y -> ResolvedAccessorType.equals struct (this, y)
        | _ -> false

    static member indexToString =
        let keySelector (i: uint) = i
        let toString = sprintf "[{%iu}]"
        memoize (ValueSome (100, 200)) keySelector toString >> sndT

    static member equals = function
        | struct (ResolvedAttribute x, ResolvedAttribute y) -> x = y
        | ResolvedListIndex x, ResolvedListIndex y -> x = y
        | _ -> false

    static member compare = function
        | struct (ResolvedListIndex x', ResolvedListIndex y') -> x' - y' |> int
        | ResolvedAttribute x', ResolvedAttribute y' -> compare x' y'
        | ResolvedAttribute _, ResolvedListIndex _ -> 1
        | ResolvedListIndex _, ResolvedAttribute _ -> -1

    interface IComparable with
        member this.CompareTo obj =
            match obj with
            | :? ResolvedAccessorType as y -> ResolvedAccessorType.compare struct (this, y)
            | _ -> invalidArg "obj" "cannot compare value of different types"

[<Struct; IsReadOnly>]
type ValidatedPath =
    private
    | Vp of ResolvedAccessorType list

    with
    override this.ToString() =
        match this with
        | Vp [] -> ""
        | Vp (valsH::valsT) ->
            let builder = Str.StringBuilderRental.Rent()
            builder.Append(valsH) |> ignoreTyped<StringBuilder>

            for v in valsT do
                match v with
                | ResolvedListIndex _ & x -> builder.Append(valsH) |> ignoreTyped<StringBuilder>
                | ResolvedAttribute _ & x -> builder.AppendFormat(".{0}", valsH) |> ignoreTyped<StringBuilder>

            let output = builder.ToString()
            Str.StringBuilderRental.Return builder
            output

type ResolvedDescription =
    | ResolvedPath of ValidatedPath
    | ResolvedCsv of ResolvedDescription list
    | ResolvedExpressionAttrValue of struct (string * AttributeValue)
    | BooleanLogic of string
    | Arithmetic of string
    | Comparer of string
    | FunctionCall of string
    // NONE is all updates
    | UpdatePlaceholder of UpdateExpressionVerb voption
    | Projection

type UnresolvedDescription =
    | UnresolvedPath of AccessorType list
    | UnresolvedCsv of UnresolvedDescription list
    | UnresolvedExpressionAttrValue of string
    | Resolved of ResolvedDescription

type ValidationResult<'a> = MaybeLazyResult<FilterTools, 'a>    
type ValidationResult = ValidationResult<ResolvedDescription>
type OpValidationFn<'input> = 'input -> ValidationResult

// Important: this class is used in building a cached value ExpressionCompilers.
//  ExpressionCompilers also contains a lot of lazily evaluated props, so values of this class
//  are cached themselves, and not just used to build a cached value
// Make sure there is nothing context specific (e.g. an AttributeValue or an ILogger) trapped in the
// lexical scope of this obj
type CodeGenSettings =
    { isInvalidUpdateKeyAttribute: string -> bool
      isInvalidFilterKeyAttribute: string -> bool
      isTableScalarAttribute: string -> bool
      parserSettings: ParserSettings }

module VR = MaybeLazyResult

/// <summary>
/// A wrapper around a validator for a function
/// </summary>
[<IsReadOnly; Struct>]
type Validator<'input> =
    private
    | Av of OpValidationFn<'input>

[<RequireQualifiedAccess>]
module ValidatedPath =
    let unwrap (Vp x) = x

    let create settings =
        let valdateFirstAttr =
            function
            | [] -> NonEmptyList.singleton "Invalid empty path" |> Error
            | ResolvedListIndex i::_ -> NonEmptyList.singleton $"Invalid list index at beginning of path [{i}]" |> Error
            | ResolvedAttribute attr::ResolvedListIndex _::_ when settings.isTableScalarAttribute attr -> NonEmptyList.singleton $"Table scalar attribute {attr} is not a list" |> Error
            | ResolvedAttribute attr::_ when settings.isInvalidFilterKeyAttribute attr -> NonEmptyList.singleton $"Cannot use key attribute {attr} in filter expression" |> Error
            | [_] & xs -> xs |> Ok
            | ResolvedAttribute attr::_ when settings.isTableScalarAttribute attr -> NonEmptyList.singleton $"Table scalar attribute {attr} is not a map" |> Error
            | xs -> xs |> Ok

        valdateFirstAttr >> Result.map Vp

    let rec private overlap' = function
        | struct (overlapping, [], _)
        | overlapping, _, [] -> overlapping
        | _, x::_, y::_ when x <> y -> false
        | _, _::xTail, _::yTail -> overlap' struct (true, xTail, yTail)

    let overlap (Vp x) (Vp y) = overlap' struct (false, x, y)

    let length = unwrap >> List.length

[<RequireQualifiedAccess>]
module Validator =
    let inline private(=+) x d = ValueOption.defaultValue d x
    let inline private(=+>) x d = ValueOption.defaultWith d x

    let private lookupExprAttrValue name vArgs =
        vArgs.expressionAttributeValueLookup name
        ?|> (tpl name >> ResolvedExpressionAttrValue >> Ok)
        =+> fun _ -> NonEmptyList.singleton $"Cannot find attribute expression value {name}" |> Error

    let private lookupExprAttrName name vArgs =
        vArgs.expressionAttributeNameLookup name
        ?|> (ResolvedAttribute >> Ok)
        =+> fun _ -> NonEmptyList.singleton $"Cannot find attribute expression name {name}" |> Error

    let private resolvePath': AccessorType list -> ValidationResult<ResolvedAccessorType list> =
        Collection.foldBackL (fun s -> function
            | AccessorType.RawAttribute x
            | AccessorType.Attribute x -> ResolvedAttribute x |> Collection.prependL |> flip VR.map s
            | AccessorType.ListIndex x -> ResolvedListIndex x |> Collection.prependL |> flip VR.map s
            | AccessorType.ExpressionAttrName x ->
                lookupExprAttrName x
                |> VR.fromReaderResult
                |> VR.map Collection.prependL
                |> VR.apply s) (VR.retn [])

    let validatePath settings: AccessorType list -> ValidationResult<ValidatedPath> =
        resolvePath'
        >> VR.bind (ValidatedPath.create settings >> VR.fromResult)

    let private resolvePath settings: AccessorType list -> ValidationResult =
        validatePath settings
        >> VR.map ResolvedPath

    let unresolve = Resolved

    let private resolveAttrName = lookupExprAttrValue >> VR.fromReaderResult

    let private resolve settings =
        let rec resolve' = function
            | Resolved x -> VR.retn x
            | UnresolvedPath xs -> resolvePath settings xs
            | UnresolvedExpressionAttrValue x -> resolveAttrName x
            | UnresolvedCsv csv ->
                List.map resolve' csv
                |> VR.traverse
                |> VR.map ResolvedCsv

        resolve'

    let private invalidArgLength =
        let invalid = sprintf "Invalid arg count. Expected %i, got %i" >>> NonEmptyList.singleton >>> Error |> uncurry
        let inline key (k: struct (int * int)) = k
        memoize (ValueSome (100, 200)) key invalid >> sndT

    let private buildReducer settings argReducer =

        argReducer
        >> VR.fromResult
        >> VR.bind (resolve settings)
        |> VR.bind

    let private create
        settings
        (argMappers: (ResolvedDescription -> Result<UnresolvedDescription, NonEmptyList<string>>) list)
        (argReducer: ResolvedDescription list -> Result<UnresolvedDescription, NonEmptyList<string>>) =

        let argLErr =
            List.length
            >> tpl (List.length argMappers)
            >> invalidArgLength
            >> VR.fromResult
            >> asLazy

        let map args =
            Collection.zipStrictL argMappers args
            ?|> List.map (
                applyTpl
                >> VR.fromResult
                >> VR.bind (resolve settings))
            ?|> VR.traverse
            =+> argLErr args

        map >> buildReducer settings argReducer |> Av

    /// <summary>
    /// Create a Validator which accepts a list of args, passes each
    /// arg into the same arg mapper, and then passes the resolved descriptions into
    /// a reducer
    /// </summary>
    let createStar
        settings
        (argMapper: ResolvedDescription -> Result<UnresolvedDescription, NonEmptyList<string>>)
        (argReducer: ResolvedDescription list -> Result<UnresolvedDescription, NonEmptyList<string>>) =

        let map =
            List.map (
                argMapper
                >> VR.fromResult
                >> VR.bind (resolve settings))
            >> VR.traverse

        map >> buildReducer settings argReducer |> Av

    let expressionAttributeName settings: string -> ValidationResult =
        AccessorType.ExpressionAttrName
        >> List.singleton
        >> resolvePath settings

    let attributeName settings: string -> ValidationResult =
        AccessorType.Attribute
        >> List.singleton
        >> resolvePath settings

    let listIndex settings: uint -> ValidationResult =
        AccessorType.ListIndex
        >> List.singleton
        >> resolvePath settings

    let expressionAttributeValue settings: string -> ValidationResult =
        UnresolvedExpressionAttrValue
        >> resolve settings

    let create0 settings = Ok >> asLazy >> create settings []

    let create1 settings = List.singleton >> flip (create settings) (List.head >> unresolve >> Ok)

    let createRoot settings f: Validator<AstNode> =
        f >> MaybeLazyResult.bind (resolve settings) |> Av

    let create2 settings validator1 validator2 groupValidator =
        let gValidator = function
            | [a1; a2] -> groupValidator struct (a1, a2)
            | xs -> ClientError.clientError "Unexpected argument count"

        create settings [validator1; validator2] gValidator

    let create3 settings validator1 validator2 validator3 groupValidator =
        let gValidator = function
            | [a1; a2; a3] -> groupValidator struct (a1, a2, a3)
            | xs -> ClientError.clientError "Unexpected argument count"

        create settings [validator1; validator2; validator3] gValidator

    let validate (args: ValidationResult list) (Av f): ValidationResult =
        VR.traverse args
        |> VR.bind f

    let validateRoot (args: AstNode) (Av f): ValidationResult = f args