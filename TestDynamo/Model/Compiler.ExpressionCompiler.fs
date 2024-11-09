namespace TestDynamo.Model.Compiler

open System
open System.Runtime.CompilerServices
open TestDynamo.Data.Monads
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.Model
open TestDynamo.Model.Compiler
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model.Compiler.Lexer

type private MutableContainer<'a> =
    { mutable value: 'a }

type private UpdateCompilers =
    { updates: ExpressionPartCompiler
      add: ExpressionPartCompiler
      addClause: ExpressionPartCompiler
      remove: ExpressionPartCompiler
      removeClause: ExpressionPartCompiler
      delete: ExpressionPartCompiler
      deleteClause: ExpressionPartCompiler
      set: ExpressionPartCompiler
      setClause: ExpressionPartCompiler
      plus: ExpressionPartCompiler
      minus: ExpressionPartCompiler }

type private DynamoDbQueryLangCompilers =
    { csvList: ExpressionPartCompiler
      between: ExpressionPartCompiler
      ``in``: ExpressionPartCompiler }

type private BinaryOperationCompilers =
    { ``and``: ExpressionPartCompiler
      ``or``: ExpressionPartCompiler
      eq: ExpressionPartCompiler
      neq: ExpressionPartCompiler
      lt: ExpressionPartCompiler
      lte: ExpressionPartCompiler
      gt: ExpressionPartCompiler
      gte: ExpressionPartCompiler
      not: ExpressionPartCompiler }

type private ReadFunctionCompilerss =
    { size: ExpressionPartCompiler
      contains: ExpressionPartCompiler
      begins_with: ExpressionPartCompiler
      attribute_type: ExpressionPartCompiler
      attribute_exists: ExpressionPartCompiler
      attribute_not_exists: ExpressionPartCompiler }

type private WriteFunctionCompilers =
    { if_not_exists: ExpressionPartCompiler
      list_append: ExpressionPartCompiler }

type private ExpressionCompilers =
    { isUpdateExpression: bool

      // errors
      invalidFunction: string -> CompilerOutput
      invalidSpace: CompilerOutput
      unexpectedListIndex: uint -> CompilerOutput
      unexpectedCsv: CompilerOutput
      emptyParenthesis: CompilerOutput

      // root compilers
      expressionAttributeValueExpression: string -> CompilerOutput
      rootAttributeExpression: ExpressionPartCompiler
      rootExpressionAttributeNameExpression: ExpressionPartCompiler
      accessor: ExpressionPartCompiler
      accessorPath: ExpressionPartCompiler

      dynamoDbQueryLang: Lazy<DynamoDbQueryLangCompilers>
      binaryOperations: Lazy<BinaryOperationCompilers>
      readFunctions: Lazy<ReadFunctionCompilerss>
      writeFunctions: Lazy<WriteFunctionCompilers>
      updates: Lazy<UpdateCompilers>
      projectionCompiler: Lazy<ExpressionPartCompiler> }

    with
    member this.csvList = this.dynamoDbQueryLang.Value.csvList
    member this.between = this.dynamoDbQueryLang.Value.between
    member this.``in`` = this.dynamoDbQueryLang.Value.``in``
    member this.``and`` = this.binaryOperations.Value.``and``
    member this.``or`` = this.binaryOperations.Value.``or``
    member this.eq =
        if this.isUpdateExpression
        then this.updates.Value.setClause
        else this.binaryOperations.Value.eq
    member this.neq = this.binaryOperations.Value.neq
    member this.lt = this.binaryOperations.Value.lt
    member this.lte = this.binaryOperations.Value.lte
    member this.gt = this.binaryOperations.Value.gt
    member this.gte = this.binaryOperations.Value.gte
    member this.not = this.binaryOperations.Value.not
    member this.size = this.readFunctions.Value.size
    member this.contains = this.readFunctions.Value.contains
    member this.begins_with = this.readFunctions.Value.begins_with
    member this.attribute_type = this.readFunctions.Value.attribute_type
    member this.attribute_exists = this.readFunctions.Value.attribute_exists
    member this.attribute_not_exists = this.readFunctions.Value.attribute_not_exists
    member this.if_not_exists = this.writeFunctions.Value.if_not_exists
    member this.list_append = this.writeFunctions.Value.list_append
    member this.updateGroup = this.updates.Value.updates
    member this.set = this.updates.Value.set
    member this.setClause = this.updates.Value.setClause
    member this.add = this.updates.Value.add
    member this.addClause = this.updates.Value.addClause
    member this.remove = this.updates.Value.remove
    member this.removeClause = this.updates.Value.removeClause
    member this.delete = this.updates.Value.delete
    member this.deleteClause = this.updates.Value.deleteClause
    member this.plus = this.updates.Value.plus
    member this.minus = this.updates.Value.minus
    member this.projections = this.projectionCompiler.Value

type private CompiledWithDescription = FilterTools -> struct (ExpressionReaderWriter * ResolvedDescription)

[<Struct; IsReadOnly>]
type CompilerTableSettings =
    { invalidTableKeyUpdateAttributes: KeyConfig voption
      invalidIndexKeyAttributes: KeyConfig voption
      tableScalarAttributes: TableKeyAttributeList }

type CompilerSettings =
    { parserSettings: ParserSettings
      tableSettings: CompilerTableSettings }

type private ExpressionCompilerData =
    { compiler: AstNode -> CompiledWithDescription
      settings: CodeGenSettings }

/// <summary>
/// Compiles an expression with an AstNode and FilterParams  
/// </summary>    
type ExpressionCompiler =
    private
    | Ec of ExpressionCompilerData

[<RequireQualifiedAccess>]
module ExpressionCompiler =
    type private AstCompiler = AstNode -> CompilerOutput

    [<Struct; IsReadOnly>]
    type WriterResult =
        { result: Map<string,AttributeValue>
          mutatedPaths: ValidatedPath list }

    [<Struct; IsReadOnly>]
    type CompiledExpression =
        { asWriter: Item -> Map<string,AttributeValue> -> WriterResult
          asReader: ExpressionFn }

        with
        static member writer x = x.asWriter

    module private Build =
        let private expandOpt = ValueOption.map AstNode.expand >> ValueOption.defaultValue []

        let private compilePart = ExpressionPartCompiler.compile
        let rec private generateCode' (compiler: MutableContainer<ExpressionCompilers>): AstCompiler =
            function
            | AstNode.Call ("size", args) -> compilePart (expandOpt args) compiler.value.size
            | AstNode.Call ("contains", args) -> compilePart (expandOpt args) compiler.value.contains
            | AstNode.Call ("begins_with", args) -> compilePart (expandOpt args) compiler.value.begins_with
            | AstNode.Call ("list_append", args) -> compilePart (expandOpt args) compiler.value.list_append
            | AstNode.Call ("if_not_exists", args) -> compilePart (expandOpt args) compiler.value.if_not_exists
            | AstNode.Call ("attribute_type", args) -> compilePart (expandOpt args) compiler.value.attribute_type
            | AstNode.Call ("attribute_exists", args) -> compilePart (expandOpt args) compiler.value.attribute_exists
            | AstNode.Call ("attribute_not_exists", args) -> compilePart (expandOpt args) compiler.value.attribute_not_exists
            | AstNode.Call (name, _) -> compiler.value.invalidFunction name
            | AstNode.Update (Add, updates) -> compilePart (AstNode.expand updates) compiler.value.add
            | AstNode.Update (Delete, updates) -> compilePart (AstNode.expand updates) compiler.value.delete
            | AstNode.Update (Set, updates) -> compilePart (AstNode.expand updates) compiler.value.set
            | AstNode.Update (Remove, updates) -> compilePart (AstNode.expand updates) compiler.value.remove
            | AstNode.Updates updates -> compilePart updates compiler.value.updateGroup
            | AstNode.Between (v, (l, r)) -> compilePart [v; l; r] compiler.value.between
            | AstNode.Accessor (AccessorType.RawAttribute _) & a
            | AstNode.Accessor (AccessorType.Attribute _) & a -> compilePart [a] compiler.value.rootAttributeExpression
            | AstNode.Accessor (AccessorType.ListIndex i) -> compiler.value.unexpectedListIndex i
            | AstNode.Accessor (AccessorType.ExpressionAttrName _) & a -> compilePart [a] compiler.value.rootExpressionAttributeNameExpression
            | AstNode.Synthetic (CsvList acc) -> compilePart acc compiler.value.csvList
            | AstNode.Synthetic (Projections _) & acc -> compilePart [acc] compiler.value.projections
            | AstNode.Synthetic (AccessorPath _) & acc -> compilePart [acc] compiler.value.accessorPath
            | AstNode.Synthetic (IndividualAddUpdate (l, r)) -> compilePart [l; r] compiler.value.addClause
            | AstNode.Synthetic (IndividualSetUpdate (l, r)) -> compilePart [l; r] compiler.value.setClause
            | AstNode.Synthetic (IndividualDeleteUpdate (l, r)) -> compilePart [l; r] compiler.value.deleteClause
            | AstNode.Synthetic (IndividualRemoveUpdate x) -> compilePart [x] compiler.value.removeClause
            | AstNode.UnaryOperator (Not, x) -> compilePart [x] compiler.value.``not``
            | AstNode.BinaryOperator (Single And, (l, r)) -> compilePart [l;r] compiler.value.``and``
            | AstNode.BinaryOperator (Single Or, (l, r)) -> compilePart [l;r] compiler.value.``or``
            | AstNode.BinaryOperator (Single Eq, (l, r)) -> compilePart [l;r] compiler.value.eq
            | AstNode.BinaryOperator (Single Neq, (l, r)) -> compilePart [l;r] compiler.value.neq
            | AstNode.BinaryOperator (Single Lt, (l, r)) -> compilePart [l;r] compiler.value.lt
            | AstNode.BinaryOperator (Single Lte, (l, r)) -> compilePart [l;r] compiler.value.lte
            | AstNode.BinaryOperator (Single Gt, (l, r)) -> compilePart [l;r] compiler.value.gt
            | AstNode.BinaryOperator (Single Gte, (l, r)) -> compilePart [l;r] compiler.value.gte
            | AstNode.BinaryOperator (Single In, (l, r)) -> compilePart [l;r] compiler.value.``in``
            | AstNode.BinaryOperator (Single Acc, (l, r)) -> compilePart [l;r] compiler.value.accessor
            | AstNode.BinaryOperator (Single Plus, (l, r)) -> compilePart [l;r] compiler.value.plus
            | AstNode.BinaryOperator (Single Minus, (l, r)) -> compilePart [l;r] compiler.value.minus
            | AstNode.BinaryOperator (Single WhiteSpace, (l, r)) -> compiler.value.invalidSpace
            | AstNode.BinaryOperator (Multi Comma, (l, r)) -> compiler.value.unexpectedCsv
            | AstNode.EmptyParenthesis -> compiler.value.emptyParenthesis
            | AstNode.ExpressionAttrValue p -> compiler.value.expressionAttributeValueExpression p

        let private allowFunction onSucess name (settings: CodeGenSettings) =
            if Array.contains name settings.parserSettings.functionNames
            then onSucess settings
            else $"Unsupported function {name}" |> ExpressionPartCompiler.buildErr |> asLazy

        let private notValidOnUpdateExpression name onSuccess (settings: CodeGenSettings) =
            if settings.parserSettings.updateFunctionVerbs
            then $"Unsupported operation in update expression {name}" |> ExpressionPartCompiler.buildErr |> asLazy
            else onSuccess settings

        /// <summary>
        /// awkward way of getting around a circular reference
        /// Returns a compiler with everthing set to null, as well as a function to initialize them
        /// </summary>
        let private buildEmptyCompiler (settings: CodeGenSettings) =

            let compiler = { value = Unchecked.defaultof<ExpressionCompilers> }
            let build codeGen =
                compiler.value <-
                    // ReSharper disable FSharpRedundantParens
                    { isUpdateExpression = settings.parserSettings.updateFunctionVerbs

                      unexpectedCsv = AstOps.Errors.unexpectedCsv
                      invalidSpace =  AstOps.Errors.invalidSpace
                      emptyParenthesis = AstOps.Errors.emptyParenthesis
                      unexpectedListIndex = AstOps.Value.unexpectedListIndex
                      rootAttributeExpression = AstOps.Value.rootAttributeExpression settings
                      rootExpressionAttributeNameExpression = AstOps.Value.rootExpressionAttributeNameExpression settings
                      expressionAttributeValueExpression =  AstOps.Value.expressionAttributeValueExpression settings
                      accessor = AstOps.Value.accessorExpression settings codeGen
                      accessorPath = AstOps.Value.accessorPathExpression settings
                      invalidFunction = AstOps.Call.invalidFunction

                      binaryOperations = lazy (
                          { ``and`` = notValidOnUpdateExpression "AND" AstOps.BinaryOps.andExpression settings codeGen
                            ``or`` = notValidOnUpdateExpression "OR" AstOps.BinaryOps.orExpression settings codeGen
                            eq = AstOps.BinaryOps.eqExpression settings codeGen
                            neq = notValidOnUpdateExpression "<>" AstOps.BinaryOps.neqExpression settings codeGen
                            lt = notValidOnUpdateExpression "<" AstOps.BinaryOps.ltExpression settings codeGen
                            lte = notValidOnUpdateExpression "<=" AstOps.BinaryOps.lteExpression settings codeGen
                            gt = notValidOnUpdateExpression ">" AstOps.BinaryOps.gtExpression settings codeGen
                            gte = notValidOnUpdateExpression ">=" AstOps.BinaryOps.gteExpression settings codeGen
                            not = notValidOnUpdateExpression "NOT" AstOps.BinaryOps.notExpression settings codeGen })

                      dynamoDbQueryLang = lazy (
                          { between = notValidOnUpdateExpression "BETWEEN" AstOps.DynamoDbQueryLang.betweenExpression settings codeGen
                            csvList = notValidOnUpdateExpression "CsvList" AstOps.DynamoDbQueryLang.csvListExpression settings codeGen 
                            ``in`` = notValidOnUpdateExpression "IN" AstOps.DynamoDbQueryLang.inExpression settings codeGen })

                      readFunctions = lazy (
                          { size = allowFunction AstOps.Call.size "size" settings codeGen
                            contains = allowFunction AstOps.Call.contains "contains" settings codeGen
                            begins_with = allowFunction AstOps.Call.begins_with "begins_with" settings codeGen
                            attribute_type = allowFunction AstOps.Call.attribute_type "attribute_type" settings codeGen
                            attribute_exists = allowFunction AstOps.Call.attribute_exists "attribute_exists" settings codeGen
                            attribute_not_exists = allowFunction AstOps.Call.attribute_not_exists "attribute_not_exists" settings codeGen })

                      writeFunctions = lazy (
                          {  if_not_exists = allowFunction AstOps.Call.if_not_exists "if_not_exists" settings codeGen
                             list_append =  allowFunction AstOps.Call.list_append "list_append" settings codeGen  })

                      updates = lazy (
                          { updates = AstOps.Updates.all settings codeGen 
                            add = AstOps.Updates.add settings codeGen
                            addClause = AstOps.Updates.addClause settings codeGen
                            remove = AstOps.Updates.remove settings codeGen
                            removeClause = AstOps.Updates.removeClause settings codeGen
                            delete = AstOps.Updates.delete settings codeGen
                            deleteClause = AstOps.Updates.deleteClause settings codeGen
                            set = AstOps.Updates.set settings codeGen
                            setClause = AstOps.Updates.setClause settings codeGen
                            plus = AstOps.BinaryOps.addExpression settings codeGen
                            minus = AstOps.BinaryOps.subtractExpression settings codeGen })

                      projectionCompiler =  lazy(AstOps.Projections.projectionExpression settings codeGen) }

            struct (compiler, build)

        let private returnFalse = asLazy false
        let private keyNamesContain = KeyConfig.keyNames >> flip List.contains
        let private toCompiled = function
            | Error xs -> Error xs |> asLazy
            | Ok struct (fn, ``lazy``) ->
                MaybeLazyResult.map (tpl fn) ``lazy``
                |> flip MaybeLazyResult.execute

        let private throw = Result.throw "Error compiling expression\n%s"
        let buildCompiler (compilerSettings: CompilerSettings) =

            // IMPORTANT: if modifying any code related to CodeGenSettings, see comments in CodeGenSettings
            let isInvalidFilterKeyAttribute =
                compilerSettings.tableSettings.invalidIndexKeyAttributes
                ?|> keyNamesContain
                |> ValueOption.defaultValue returnFalse

            let isInvalidUpdateKeyAttribute =
                compilerSettings.tableSettings.invalidIndexKeyAttributes
                ?>>= (function
                    | x when ValueSome x = compilerSettings.tableSettings.invalidTableKeyUpdateAttributes -> ValueSome isInvalidFilterKeyAttribute
                    | _ -> ValueNone)
                |> ValueOption.defaultWith (fun _ ->
                    compilerSettings.tableSettings.invalidTableKeyUpdateAttributes
                    ?|> keyNamesContain
                    |> ValueOption.defaultValue returnFalse)

            let settings =
                { parserSettings = compilerSettings.parserSettings
                  isInvalidUpdateKeyAttribute = isInvalidUpdateKeyAttribute
                  isInvalidFilterKeyAttribute = isInvalidFilterKeyAttribute
                  isTableScalarAttribute = flip TableKeyAttributeList.contains compilerSettings.tableSettings.tableScalarAttributes }

            let inline build compiler = { compiler = compiler; settings = settings }
            let struct (compiler, completeSetup) = buildEmptyCompiler settings
            let generate = generateCode' compiler
            completeSetup generate
            generate >> toCompiled >>> throw |> build |> Ec

    module private Compile =
        let private validateDuplicatePaths =
            Seq.sort
            >> Seq.windowed 2
            >> Seq.map List.ofSeq
            >> Seq.filter (function
                | [x; y] -> ValidatedPath.overlap x y
                | _ -> false)
            >> Seq.collect id
            >> Seq.distinct
            >> Seq.map (sprintf " * %O")
            >> Str.join "\n"
            >> function
                | "" -> Ok ()
                | err -> NonEmptyList.singleton (sprintf "An attribute or attribute path was updated or projected multiple times\n%s" err) |> Error

        let private validateAtLeast1Mutation =
            let good = Ok()
            let bad = NonEmptyList.singleton "No updates found in expression" |> Error

            function | [] -> bad | _ -> good

        let private validateKeyAttributes (settings: CodeGenSettings) =
            Seq.filter (ValidatedPath.unwrap >> function
                | ResolvedAttribute head::_ -> settings.isInvalidUpdateKeyAttribute head
                | _ -> false)
            >> Seq.map (sprintf " * %O")
            >> Str.join "\n"
            >> function
                | "" -> Ok ()
                | err -> sprintf "Key attributes cannot be updated\n%s" err |> NonEmptyList.singleton |> Error

        let private throw = Result.throw "Error compiling expression\n%s"
        let private validateMutatedAttributes struct (mutations, settings) (xs: ValidatedPath list) =
            [ validateKeyAttributes settings xs
              validateDuplicatePaths xs
              validateAtLeast1Mutation mutations ]
            |> Result.traverse
            &|> (asLazy xs)
            |> throw

        let stateMutatorInit = State.retn []
        let inline private buildWriterResult struct (result, mutatedPaths) = { result = result; mutatedPaths = mutatedPaths }
        let private buildWriter' struct (settings, lookups, mutations: ItemMutation list) =

            fun item ->
                let itemData =
                    { item = item
                      filterParams = lookups }

                mutations
                |> Seq.map (ItemMutation.executable itemData)
                |> Seq.fold (flip State.bind) stateMutatorInit
                |> flip State.execute
                >> mapSnd (
                    List.collect id
                    >> validateMutatedAttributes struct (mutations, settings))
                >> buildWriterResult

        let private noWriter = Item.attributes >> curry buildWriterResult |> flip |> apply [] |> asLazy |> flip
        let buildWriter settings (lookups: FilterTools) (mutations: ItemMutation list) =
            match mutations with
            | [] ->
                Logger.debug0 "No mutations" lookups.logger
                noWriter
            | ms -> 
                Logger.debug0 "Building mutations" lookups.logger
                buildWriter' struct (settings, lookups, ms)

        let asCompiledExpression struct (writer, struct (reader, _)) =
            { asWriter = writer; asReader = reader }

    let build =
        let inline keySelector (x: CompilerSettings) = x
        let built = memoize Settings.QueryCacheSize keySelector Build.buildCompiler

        fun (debugMsg: Printf.StringFormat<CacheHit -> string>) ->
            let debug = Logger.debug1 debugMsg |> flip
            fun logger ->
                built
                >> mapFst (debug logger)
                >> sndT

    let private operatorLimitError = $"Maximum number of operators has been reached. You can change this value by modifying {nameof Settings}.{nameof Settings.ExpressionOperatorLimit} for configuration"
    let compile node lookups (Ec c) =

        let operatorCount =
            AstNode.depthFirstSearch (fun _ s -> function
                | AstNode.Call _
                | AstNode.Between _
                | AstNode.BinaryOperator _
                | AstNode.UnaryOperator _ -> s + 1
                | _ -> s) 0 node
        
        if operatorCount > Settings.ExpressionOperatorLimit
        then clientError operatorLimitError

        c.compiler node lookups
        |> mapFst (flip Writer.map)
        |> mapSnd (flip tpl)
        |> applyTpl
        |> Writer.execute
        |> mapFst (Compile.buildWriter c.settings lookups)
        |> Compile.asCompiledExpression
