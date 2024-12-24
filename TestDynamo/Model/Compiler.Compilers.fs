/// <summary>
/// Compiles an AST from a projection expression into an executable function 
/// </summary>
module TestDynamo.Model.Compiler.Compilers

open TestDynamo
open TestDynamo.Model
open TestDynamo.Model.Compiler
open TestDynamo.Model.Compiler.Parser
open TestDynamo.Utils
open System.Runtime.CompilerServices

module private CompileUtils =

    let private buildCompiler compilerFactory =
        compilerFactory
        >>> flip3To1 ExpressionCompiler.compile

    let private compileAst' buildFilterCompiler =
        let compiler = buildCompiler buildFilterCompiler

        fun (logger: Logger) (tableSettings: CompilerSettings) ->
            tplDouble
            >> mapSnd (compiler logger tableSettings)

    let private compile' astCompiler buildFilterCompiler =
        let compiler = compileAst' buildFilterCompiler

        fun (logger: Logger) (tableSettings: CompilerSettings) (expression: string) ->
            expression
            |> astCompiler logger
            |> compiler logger tableSettings

    let compile (cacheHitMessage: Printf.StringFormat<CacheHit -> string>) astCompiler buildFilterCompiler =

        let debug = flip (Logger.debug1 cacheHitMessage)
        let compile = compile' astCompiler buildFilterCompiler

        let inline keySelector struct (_: Logger, compilerSettings: CompilerSettings, expr: string) = struct (compilerSettings, expr)
        let memoed = uncurry3 compile |> memoize Settings.QueryCacheSize keySelector

        let memoedWithLogging struct (logger, settings) expr =
            try
                memoed struct (logger, settings, expr)
                |> mapFst (debug logger)
                |> sndT
            with
            | e -> ClientError.clientErrorWithInnerException $"Error compiling expression: \"{expr}\"" e

        memoedWithLogging

    let compileFromAst = compileAst' >> uncurry

    let compileAst (logMsg: Printf.StringFormat<string -> string>) =
        fun settings logger expression ->
            expression
            |> Logger.logFn1 logMsg logger
            |> Lexer.tokenize
            |> Parser.generateAstFromExpression { logger = logger; settings = settings } expression

open CompileUtils

[<RequireQualifiedAccess>]
module Updates =

    let private parserSettings =
        { updateFunctionVerbs = true
          functionNames =
            [|
                "if_not_exists"
                "list_append"
            |] }

    // AST caching used for update only as AST for update needs to be built
    // at 2 different points in execution
    let compileAst =
        let inline keySelector struct (_: Logger, expr: string) = expr
        let compile = CompileUtils.compileAst "Compiling update %A" parserSettings
        let memoed = uncurry compile |> memoize Settings.QueryCacheSize keySelector |> curry

        fun logger ->
            memoed logger
            >> mapFst (flip (Logger.debug1 "Pre-compiled update AST cache hit: %A") logger)
            >> sndT

    let private buildUpdateCompiler = ExpressionCompiler.build "Pre-built update compiler cache hit: %A"

    let private compile' =
        CompileUtils.compile
            "Pre-compiled update expression cache hit: %A"
            compileAst
            buildUpdateCompiler

    let inline private createItemMutator f item = f item (Item.attributes item)

    let compile logger tableKeys =
        let settings =
            { parserSettings = parserSettings
              tableSettings =
                  { invalidTableKeyUpdateAttributes = ValueSome tableKeys
                    invalidIndexKeyAttributes = ValueNone
                    tableScalarAttributes = TableKeyAttributeList.empty } }

        compile' struct (logger, settings)
        >> mapSnd (
            flip (>>) _.asWriter
            >>> createItemMutator)

[<RequireQualifiedAccess>]
module Projections =

    let private parserSettings = { functionNames = [||]; updateFunctionVerbs = false }
    let private compileAst =
        CompileUtils.compileAst "Compiling projection %A" parserSettings
        >>> (Projections >> Synthetic)

    let private buildProjectionCompiler = ExpressionCompiler.build "Pre-built projection compiler cache hit: %A"

    let private compile' =
        CompileUtils.compile
            "Pre-compiled projection expression cache hit: %A"
            compileAst
            buildProjectionCompiler

    let private createItemProjector = flip >> apply Map.empty

    let private buildSettings tableScalarAttributes =
        { parserSettings = parserSettings
          tableSettings =
              { invalidIndexKeyAttributes = ValueNone
                invalidTableKeyUpdateAttributes = ValueNone 
                tableScalarAttributes = tableScalarAttributes } }

    let compile logger tableScalarAttributes =
        let settings = buildSettings tableScalarAttributes

        compile' struct (logger, settings)
        >> mapSnd (
            flip (>>) _.asWriter
            >>> createItemProjector)

    let compileFromAst =
        mapSnd buildSettings
        >> CompileUtils.compileFromAst buildProjectionCompiler
        |> curry

[<RequireQualifiedAccess>]
module Filters =
    let private parserSettings =
        { functionNames =
            [|
                "begins_with"
                "attribute_exists"
                "attribute_not_exists"
                "attribute_type"
                "contains"
                "size"
            |]
          updateFunctionVerbs = false }

    let private buildFilterCompiler = ExpressionCompiler.build "Pre-built filter compiler cache hit: %A"
    let private compileAst = compileAst "Compiling filter %A" parserSettings

    let private compile' =
        CompileUtils.compile
            "Pre-compiled filter expression cache hit: %A"
            compileAst
            buildFilterCompiler

    let private booleanAnswer = function
        | ValueSome (Boolean x) -> x
        | ValueNone
        | ValueSome _ -> false

    let compile logger tableSettings =
        let settings =
            { parserSettings = parserSettings
              tableSettings = tableSettings }

        compile' struct (logger, settings)
        >> mapSnd (
            flip (>>) _.asReader
            >>>> booleanAnswer)

module OpenIndex =

    let private parserSettings =
        { functionNames = [| "begins_with" |]
          updateFunctionVerbs = false }

    let compileQuery =
        let compileAst = compileAst "Compiling query %A" parserSettings

        let compile' struct (logger, expr, keyConfig) =
            let ast = compileAst logger expr

            let query =
                ast
                |> Logger.logFn1 "Compiled query %A" logger
                |> QueryExpressionCompiler.compile keyConfig

            // logger is convenient for cache hitter to use
            fun (logger: Logger) -> struct (ast, query)

        let inline memoKey struct (_: Logger, expr: string, keyConfig: KeyConfig) = struct (expr, keyConfig)
        let compiler =
            compile'
            |> memoize Settings.QueryCacheSize memoKey
            >> fun struct (cacheHit, f) logger ->
                Logger.debug1 "Pre-compiled index opener cache hit: %A" cacheHit logger
                f logger

        fun logger exp key ->
            try
                compiler struct (logger, exp, key) logger
            with
            | e -> ClientError.clientErrorWithInnerException $"Error compiling query: \"{exp}\"" e
