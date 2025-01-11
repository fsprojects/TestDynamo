
namespace TestDynamo.Model.Compiler

open System.Diagnostics.CodeAnalysis
open TestDynamo.Data.BasicStructures
open TestDynamo.Data.Monads
open TestDynamo.Model
open TestDynamo.Model.Compiler
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open System.Runtime.CompilerServices

[<Struct; IsReadOnly>]
type ItemData =
    { item: Item
      filterParams: FilterTools }
    with
    static member getItem {item=item} = item

type ExpressionAttributeName = string
type AttributeName = string

type ProjectionTools = ItemData

module ProjectionTools =

    let private resolvePath' (lookups: FilterTools) =
        Collection.foldBackL (fun s x ->
            match struct (s, x) with
            | ValueNone, _ -> ValueNone
            | ValueSome acc, RawAttribute s
            | ValueSome acc, Attribute s -> ResolvedAttribute s::acc |> ValueSome
            | ValueSome acc, ListIndex i -> ResolvedListIndex i::acc |> ValueSome
            | ValueSome acc, ExpressionAttrName s ->
                lookups.expressionAttributeNameLookup s
                ?|> (ResolvedAttribute >> flip Collection.prependL acc)) (ValueSome [])

    let resolvePath (path: AccessorType list) (tools: ProjectionTools) = resolvePath' tools.filterParams path

type ItemMutationState = Map<string, AttributeValue>

type MutatedAttributeValue = AttributeValue

[<Struct; IsReadOnly>]
type ItemMutation =
    { valueGetter: ProjectionTools -> MutatedAttributeValue voption
      mutation: struct (ProjectionTools * MutatedAttributeValue voption) -> State<ItemMutationState, ValidatedPath list> }

module ItemMutation =
    let create valueGetter mutation = { mutation = mutation; valueGetter = valueGetter }

    let private maybePrependL collection = function
        | [] -> collection
        | x -> x::collection

    let executable tools item pathAcc =
        item.valueGetter tools
        |> tpl tools
        |> item.mutation
        |> State.map (maybePrependL pathAcc)

type ExpressionFn = ItemData -> AttributeValue voption
type ExpressionReaderWriter = Writer<ItemMutation, ExpressionFn>
type CompilerResult = (struct (ExpressionReaderWriter * ValidationResult))
type CompilerOutput = Result<CompilerResult, NonEmptyList<string>>
type private ArgumentIndex = int
type private ArgCompiler = ArgumentIndex -> AstNode -> CompilerOutput
type private ArgPreCompiler = AstNode list -> Result<AstNode list, NonEmptyList<string>>

type private ExpressionPartCompilerData =
    { argPreProcessor: ArgPreCompiler
      compiler: ArgCompiler
      name: string
      argsValidator: Validator<ResolvedDescription list>
      expression: ExpressionReaderWriter list -> ExpressionReaderWriter }

type private RootExpressionPartCompilerData =
    { argPreProcessor: ArgPreCompiler
      rootCompiler: AstNode -> ExpressionReaderWriter
      name: string
      argValidator: Validator<AstNode> }

/// <summary>
/// Compiles a specific operation with specific expected args
/// e.g. an operation compiler for an AND expression expects 2 args 
/// </summary>
type ExpressionPartCompiler =
    private
    | Root of RootExpressionPartCompilerData
    | E0 of ExpressionPartCompilerData
    | E1 of ExpressionPartCompilerData
    | E2 of ExpressionPartCompilerData
    | E3 of ExpressionPartCompilerData
    | EStar of ExpressionPartCompilerData
    | Err of string

module ExpressionPartCompiler =

    let buildErr = Err

    let private opToExpressionFn: (ItemData -> AttributeValue voption) -> ExpressionReaderWriter = Writer.retn

    let build0 name argPreProcessor buildDescription expression settings compiler =
        { argPreProcessor = argPreProcessor
          compiler = compiler
          name = name
          argsValidator = Validator.create0 settings buildDescription 
          expression = asLazy (opToExpressionFn expression) } |> E0

    let oneArg name f = function
        | [x] -> f x
        | xs -> ClientError.clientError $"Expected 1 arg for {name}, got {List.length xs}"

    let private oneArgOpFromExpressionFn = Writer.map

    let build1WithWriter name argPreProcessor argValidator expression settings compiler =
        { argPreProcessor = argPreProcessor
          compiler = asLazy compiler
          name = name
          argsValidator = Validator.create1 settings argValidator
          expression = oneArg name expression } |> E1

    let build1 name argPreProcessor argValidator =
        oneArgOpFromExpressionFn >> build1WithWriter name argPreProcessor argValidator

    let twoArgs name f = function
        | [x1; x2] -> f struct (x1, x2)
        | xs -> ClientError.clientError $"Expected 2 args for {name}, got {List.length xs}"

    let private twoArgOpFromExpressionFn op =
        Writer.traverseTpl >> Writer.map op

    let build2iWithWriter name argPreProcessor arg1 arg2 buildDescription expression settings compiler =
        { argPreProcessor = argPreProcessor
          compiler = compiler
          name = name
          argsValidator = Validator.create2 settings arg1 arg2 buildDescription
          expression = twoArgs name expression } |> E2

    let build2i name argPreProcessor arg1 arg2 buildDescription =
        twoArgOpFromExpressionFn >> build2iWithWriter name argPreProcessor arg1 arg2 buildDescription

    let build2WithWriter name argPreProcessor arg1 arg2 buildDescription expression settings =
        asLazy >> build2iWithWriter name argPreProcessor arg1 arg2 buildDescription expression settings

    let build2 name argPreProcessor arg1 arg2 buildDescription expression settings =
        asLazy >> build2i name argPreProcessor arg1 arg2 buildDescription expression settings

    let threeArgs name f = function
        | [x1; x2; x3] -> f struct (x1, x2, x3)
        | xs -> ClientError.clientError $"Expected 3 args for {name}, got {List.length xs}"

    [<ExcludeFromCodeCoverage>]
    let inline private traverseTrpl struct (x, y, z) =
        Writer.retn tpl3 |> Writer.apply x |> Writer.apply y |> Writer.apply z

    [<ExcludeFromCodeCoverage>]
    let inline private threeArgOpFromExpressionFn op =
        traverseTrpl >> Writer.map op

    let build3 name argPreProcessor arg1 arg2 arg3 buildDescription expression settings compiler =
        { argPreProcessor = argPreProcessor
          compiler = asLazy compiler
          name = name
          argsValidator = Validator.create3 settings arg1 arg2 arg3 buildDescription
          expression = threeArgs name (threeArgOpFromExpressionFn expression) } |> E3

    let buildRoot name argPreProcessor buildDescription rootCompiler settings =
        { argPreProcessor = argPreProcessor
          rootCompiler = rootCompiler
          name = name
          argValidator = Validator.createRoot settings buildDescription } |> Root

    [<ExcludeFromCodeCoverage>]
    let inline private argListOpFromExpressionFn op =
        Writer.traverse >> Writer.map op

    /// <summary>
    /// Create an ExpressionPartCompiler which accepts a list of args, compiles them as 1
    /// and returns a result
    /// </summary>
    let buildStarWithWriter name argPreProcessor argDescription buildDescription expression settings compiler =
        { argPreProcessor = argPreProcessor
          compiler = asLazy compiler
          name = name
          argsValidator = Validator.createStar settings argDescription buildDescription
          expression = expression } |> EStar

    /// <summary>
    /// Create an ExpressionPartCompiler which accepts a list of args, compiles them as 1
    /// and returns a result
    /// </summary>
    let buildStar name argPreProcessor argDescription buildDescription =
        argListOpFromExpressionFn >> buildStarWithWriter name argPreProcessor argDescription buildDescription

    let private compile' struct (args, exp): CompilerOutput =

        List.mapi exp.compiler args
        |> Result.traverse
        &|> (
            Collection.unzip
            >> mapFst (exp.expression >> tpl)
            >> mapSnd (
                flip Validator.validate exp.argsValidator)
            >> applyTpl)

    let private compileRoot' struct (rootAst, exp: RootExpressionPartCompilerData): CompilerOutput =

        Validator.validateRoot rootAst exp.argValidator
        |> tpl (exp.rootCompiler rootAst)
        |> Ok

    let preProcessArgs =
        function
        | Err _ -> Ok
        | E0 typ -> typ.argPreProcessor
        | E1 typ -> typ.argPreProcessor
        | E2 typ -> typ.argPreProcessor
        | E3 typ -> typ.argPreProcessor
        | EStar typ -> typ.argPreProcessor
        | Root typ -> typ.argPreProcessor
        |> flip

    let compile: AstNode list -> ExpressionPartCompiler -> CompilerOutput =
        tpl
        >>> (fun struct (ast, compiler) ->
            preProcessArgs ast compiler
            &|> (flip tpl compiler))
        >>> Result.bind (function
            | struct (_, Err msg) -> NonEmptyList.singleton msg |> Error
            | [] & args, E0 typ
            | [_] & args, E1 typ
            | [_; _] & args, E2 typ
            | [_; _; _] & args, E3 typ -> compile' (args, typ)
            | args, EStar typ -> compile' (args, typ)
            | [arg], Root typ -> compileRoot' (arg, typ)
            | args, E0 typ -> NonEmptyList.singleton $"{typ.name} operation expects no arguments, got {List.length args}" |> Error
            | args, E1 typ -> NonEmptyList.singleton $"{typ.name} Operation expects 1 argument, got {List.length args}" |> Error
            | args, E2 typ -> NonEmptyList.singleton $"{typ.name} Operation expects 2 arguments, got {List.length args}" |> Error
            | args, E3 typ -> NonEmptyList.singleton $"{typ.name} Operation expects 3 arguments, got {List.length args}" |> Error
            | args, Root typ -> NonEmptyList.singleton $"{typ.name} operation expects 1 arguments, got {List.length args}" |> Error)
