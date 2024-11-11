
/// <summary>
/// Generate an AST from an array of tokens
/// </summary>
module TestDynamo.Model.Compiler.Parser

open System
open System.Runtime.CompilerServices
open TestDynamo.Data.BasicStructures
open TestDynamo.Model.Compiler.Lexer
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators
open TestDynamo.Data.Monads

type ParserInputs =
    { logger: Logger
      settings: ParserSettings }

[<IsReadOnly; Struct>]
type ParserState =
    { inputs: ParserInputs
      depth: int
      cycle: int }

    with

    override this.ToString () = sprintf "cycle: %i, depth: %i" this.cycle this.depth
    static member down this = { this with depth = this.depth + 1 }
    static member inside this = { this with depth = this.depth + 1; cycle = 0 }
    static member across this = { this with cycle = this.cycle + 1; depth = 0 } 

let tokenDescriber = Logger.describable (fun struct (operation: string, state: ParserState, token: struct (struct (uint16 * uint16) * MaybeProcessedToken<AstNode>)) ->
    let tokenDesc =
        match token with
        | x, Processed y -> struct (x, AstNode.name y)
        | x, y -> struct (x, y.ToString())

    sprintf "as %s: %A (%O)" operation tokenDesc state)

let private debug struct (operationName, token) f state =
    Logger.debug1 "Processing token %O" (tokenDescriber (operationName, state, token)) state.inputs.logger
    f state

type private ParseError =
    /// <summary>
    /// This parser is a recursve descent parser with constant time complexity O(N). For small => large queries
    /// the stack will handle this depth fine
    /// For XL queries, this error allows the compiler to break and become an LL2 compiler
    /// which is not stack dependant, but also has some redundant calculations with worst case complexity O(N^2) / maxDepth
    /// </summary>
    | MultiplePassesRequired of PrecedenceProcessor<AstNode>
    | Errs of NonEmptyList<string>

// try not to throw in this class
// throwing in a deep stack might trigger a stack overflow itself
let private error msg = NonEmptyList.singleton msg |> Errs |> Error

let private appendErr token result =
    match struct (token, result) with
    | struct (_, _), Ok _ & x
    | (_, Processed _), x
    | _, Error (MultiplePassesRequired _) & x -> x
    | (_, Tkn token), Error (Errs errs) -> NonEmptyList.prepend $"Error processing token @ position {Token.position token}" errs |> Errs |> Error

let rec private processToken state token collection =

    let state = (ParserState.down state)

    // recursion limit. Stack overflow risk if it goes any higher
    if state.depth > 400
    then
        Logger.debug1 "Max depth reached %O" state state.inputs.logger
        Result.Error (MultiplePassesRequired collection)
    else processToken' state token collection

and private processToken' state token collection =

    let tkn = PrecedenceProcessor.get state.inputs.logger token collection

    match tkn with
    | p, Processed x -> debug struct ("Processed", tkn) processAst state p x collection
    | _, Tkn (Token.Space _) -> debug struct ("whitespace", tkn) processSpace state ()
    | (p, _), Tkn (Token.SyntheticToken struct (UpdateExpressionLabel (Set & verb), x))
    | (p, _), Tkn (Token.SyntheticToken struct (UpdateExpressionLabel (Remove & verb), x)) ->
        debug struct (verb.ToString(), tkn) processConformingUpdateExpression state p verb collection
    | (p, _), Tkn (Token.SyntheticToken struct (UpdateExpressionLabel (Add & verb), x))
    | (p, _), Tkn (Token.SyntheticToken struct (UpdateExpressionLabel (Delete & verb), x)) ->
        debug struct (verb.ToString(), tkn) processNonConformingUpdateExpression state p verb collection
    | (p, _), Tkn (Token.Text x) when Array.contains x.string state.inputs.settings.functionNames ->
        debug struct ("CALL", tkn) processCall state p x.string collection
    | (p, _), Tkn (Token.Text x) when tryTokenIsOpen (p + 1us) collection ->
        // this is a failure case. At this state we know it is an invalid function name
        debug struct ("CALL", tkn) processCall state p x.string collection
    | (p, _), Tkn (Token.Text attr) -> debug struct ("Symbol", tkn) processText state attr.string p collection
    | (p, _), Tkn (Token.ExpressionAttrValue attr) -> debug struct ("\":val\"", tkn) processExpressionAttrValue state attr.string p collection |> Ok
    | (p, _), Tkn (Token.ExpressionAttrName attr) -> debug struct ("\"#name\"", tkn) processExpressionAttrName state attr.string p collection |> Ok
    | (p, _), Tkn (Token.ListIndex (index, _)) -> debug struct ("[i]", tkn) processListIndex state p index collection
    | (p, _), Tkn (Token.BinaryOp (op, _)) -> debug struct ("BinaryOp", tkn) processBinaryOp state p op collection
    | (p, _), Tkn (Token.UnaryOp (op, _)) -> debug struct ("UnaryOp", tkn) processUnaryOp state p op collection
    | (p, _), Tkn (Token.Between _) -> debug struct ("BETWEEN", tkn) processBetween state p collection
    | (p, _), Tkn (Token.Open _) -> debug struct ("( [open parentheses]", tkn) processOpen state p collection
    | (p, _), Tkn (Token.Close _) -> debug struct (") [close parentheses]", tkn) processClose state p collection
    |> appendErr tkn

and private processAst _ struct (l, r) ast collection = Ok struct (collection, ProcessedTokenPointer.create l r ast)

and private processSpace _ () = serverError "Spaces should have been removed"

and tryTokenIsOpen p (collection: PrecedenceProcessor<_>) =
    PrecedenceProcessor.tryGetToken p collection
    ?|> function
        | Token.Open _ -> true
        | _ -> false
    ?|? false

/// <summary>
/// Conforming update expressions are expressions which have the same syntax as filters, queries etc
/// SET and REMOVE are conforming update expressions 
/// </summary>
and private processConformingUpdateExpression state p verb collection =
    if PrecedenceProcessor.isOutOfRange (p + 1us) collection
    then error $"{verb} expression must have at least 1 clause"
    else
        processToken (ParserState.down state) (p + 1us) collection
        &|> (fun struct (collection, result) ->
            let processed =
                struct (verb, result.node)
                |> AstNode.Update
                |> ProcessedTokenPointer.create p result.toToken

            PrecedenceProcessor.put state.inputs.logger processed collection
            |> flip tpl processed)

/// <summary>
/// Non-conforming update expressions are expressions which have a different syntax to filters, queries etc
/// They have a much simpler syntax, e.g. "DELETE LHS RHS, LHS RHS, etc..."
/// ADD and DELETE are non-conforming update expressions 
/// </summary>
and private processNonConformingUpdateExpression state p verb collection =

     getNonConformingUpdateClauses [] (ParserState.down state) (p + 1us) verb collection
     &|> (
         mapFst (
             List.rev
             >> AstNode.toCsv
             >> ValueOption.defaultWith (fun _ ->
                 clientError $"{verb} expression must have at least 1 clause")))
     &|> (fun struct (clauses, endP) ->

         let processed =
             struct (verb, clauses)
             |> AstNode.Update
             |> ProcessedTokenPointer.create p endP

         PrecedenceProcessor.put state.inputs.logger processed collection
         |> flip tpl processed)

and private getNonConformingUpdateClauses acc state p verb collection =

     Logger.debug1 "Get part 1 @ %i" p state.inputs.logger
     nextNonConformingUpdatePart (ParserState.down state) p verb collection
     &>>= (fun struct (collection, lhs) ->

         Logger.debug1 "Found part 1 %A, getting part 2" lhs state.inputs.logger
         nextNonConformingUpdatePart (ParserState.down state) (lhs.toToken + 1us) verb collection
         &|> (tpl lhs))
     &>>= (fun struct (lhs, struct (collection, rhs)) ->
         let acc' = AstNode.BinaryOperator (Single WhiteSpace, (lhs.node, rhs.node))::acc

         match PrecedenceProcessor.tryGet (rhs.toToken + 1us) collection with
         | ValueNone
         | ValueSome (_, Tkn (Token.SyntheticToken (UpdateExpressionLabel _, _)))
         | ValueSome (_, Processed (AstNode.Update _)) -> struct (acc', rhs.toToken) |> Ok
         | ValueSome ((_, endP), Tkn (Token.BinaryOp (BinaryOpToken.Multi Comma, _))) ->
             getNonConformingUpdateClauses acc' (ParserState.down state) (endP + 1us) verb collection
         | ValueSome x ->
             Logger.debug1 "Unexpected next item %A" x state.inputs.logger
             error $"Invalid {verb} update expression")

and private nextNonConformingUpdatePart state p verb collection =
    if PrecedenceProcessor.isOutOfRange p collection
    then
        Logger.debug0 "No next item found" state.inputs.logger
        error $"Invalid {verb} update expression"
    else processToken state p collection

and private processCall state p txt collection =

    Logger.debug1 "From call args %A" txt state.inputs.logger
    processToken (ParserState.down state) (p + 1us) collection
    &|> (fun struct (collection, result) ->
        let node =
            AstNode.Call struct (txt, ValueSome result.node)
            |> ProcessedTokenPointer.create p result.toToken

        PrecedenceProcessor.put state.inputs.logger node collection
        |> flip tpl node)

and private processTextToken logger astNode position collection =

    let node = ProcessedTokenPointer.create position position astNode
    PrecedenceProcessor.put logger node collection
    |> flip tpl node

and private processText state text: _ -> _ -> Result<_, _> =
    if ReservedWords.isReservedWord text then $"Cannot use reserved word {text}" |> error |> asLazy |> asLazy
    else processTextToken state.inputs.logger (AstNode.Accessor (AccessorType.Attribute text)) >>> Ok

and private processExpressionAttrValue state text =
    processTextToken state.inputs.logger (AstNode.ExpressionAttrValue text)

and private processExpressionAttrName state text =
    processTextToken state.inputs.logger (AstNode.Accessor (AccessorType.ExpressionAttrName text))

and private processBinaryOp state position op collection =

    Logger.debug1 "From binary op L %A" op state.inputs.logger
    processToken (ParserState.down state) (position - 1us) collection
    &>>= (fun struct (collection, l) ->
        Logger.debug1 "From binary op R %A" op state.inputs.logger
        processToken (ParserState.down state) (position + 1us) collection
        &|> (tpl l))
    &|> (fun struct ({fromToken = s; node = l}, struct (collection, {toToken = e; node = r})) ->
        let node =
            BinaryOperator struct (op, struct (l, r))
            |> ProcessedTokenPointer.create s e

        node
        |> flip (PrecedenceProcessor.put state.inputs.logger) collection
        |> flip tpl node)

and private processListIndex state position index collection =

    Logger.debug0 "From list index LHS" state.inputs.logger
    processToken (ParserState.down state) (position - 1us) collection
    &|> (fun struct (collection, {fromToken = s; node = l}) ->

        Logger.debug1 "From list index RHS [%d]" index state.inputs.logger
        let struct (collection, {toToken = e; node = r}) =
            processTextToken state.inputs.logger (AstNode.Accessor (AccessorType.ListIndex index)) position collection

        let node =
            BinaryOperator struct (BinaryOpToken.Single SingletonBinaryOpToken.Acc, struct (l, r))
            |> ProcessedTokenPointer.create s e

        node
        |> flip (PrecedenceProcessor.put state.inputs.logger) collection
        |> flip tpl node)

and private processUnaryOp state position op collection =

    Logger.debug1 "From unary operand %A" op state.inputs.logger
    processToken (ParserState.down state) (position + 1us) collection
    &|> (fun struct (collection, {toToken = e; node = r}) ->

        let node =
            UnaryOperator struct (op, r)
            |> ProcessedTokenPointer.create position e

        node
        |> flip (PrecedenceProcessor.put state.inputs.logger) collection
        |> flip tpl node)

and private processBetween state position collection =

    Logger.debug0 "From BETWEEN target" state.inputs.logger
    processToken (ParserState.down state) (position - 1us) collection
    &>>= (fun struct (collection, propStart) ->
        Logger.debug0 "From BETWEEN lower" state.inputs.logger
        processToken (ParserState.down state) (position + 1us) collection
        &|> (mapSnd _.toToken >> tpl propStart))
    &>>= (fun struct (propStart, struct (collection, andLhsEnd)) ->
        Logger.debug0 "From BETWEEN upper" state.inputs.logger
        processToken (ParserState.down state) (andLhsEnd + 1us) collection
        &>>= (
            mapSnd (function
                | { node = BinaryOperator struct (Single And, struct (l, r)); toToken = toToken } ->
                    struct (toToken, struct (l, r)) |> Ok
                | _ -> error "Invalid BETWEEN statement. Expected form \"x BETWEEN y and z\"")
            >> Result.unwrapTpl2)
        &|> (tpl struct (propStart, andLhsEnd)))
    &|> (fun struct (struct ({node = prop; fromToken = propStart}, andLhsEnd), struct (collection, struct (andEnd, andPart))) ->
        let node =
            AstNode.Between (prop, andPart)
            |> ProcessedTokenPointer.create propStart andEnd

        node
        |> flip (PrecedenceProcessor.put state.inputs.logger) collection
        |> flip tpl node)

and private findClose opens = function
    | Token.Close _ when opens = 1 -> struct (0, true)
    | Token.Close _ -> struct (opens - 1, false)
    | Token.Open _ -> struct (opens + 1, false)
    | _ -> struct (opens, false)

and private findOpen closes = function
    | Token.Open _ when closes = 1 -> struct (0, true)
    | Token.Open _ -> struct (closes - 1, false)
    | Token.Close _ -> struct (closes + 1, false)
    | _ -> struct (closes, false)

and private processParenthesis state ``open`` closed collection =
    let start = ``open`` + 1us
    let ``end`` = closed - 1us

    match compare start ``end`` with
    | i when i > 0 -> Ok struct (collection, ValueNone)
    | _ ->
        PrecedenceProcessor.alreadyProcessed start ``end`` collection
        ?|> (tpl collection >> Ok)
        |> ValueOption.defaultWith (fun _ ->
            Logger.debug1 "From sub collection: %A" struct (start, ``end``) state.inputs.logger
            PrecedenceProcessor.subCollection state.inputs.logger start ``end`` (processCollectionCycles (ParserState.inside state)) collection
            &>>= (function
                | struct ([x], c) -> struct (c, x) |> Ok
                | struct (_, c) ->
                    Logger.debug0 "Parenthesis depth reached" state.inputs.logger
                    MultiplePassesRequired c |> Error))
        &|> (mapSnd ValueSome)
    |> Result.map (
        mapSnd (
            ValueOption.map _.node
            >> ValueOption.defaultValue EmptyParenthesis)
        >> fun struct (collection, innerPointer) ->

            let pointer = ProcessedTokenPointer.create ``open`` closed innerPointer

            PrecedenceProcessor.put state.inputs.logger pointer collection
            |> flip tpl pointer)

and private processOpen state p collection =
    match PrecedenceProcessor.foldFindNextToken (p + 1us) findClose 1 collection with
    | _, ValueNone -> error "Could not find \")\" token correspondnig to \"(\""
    | _, ValueSome close -> processParenthesis state p close collection

and private processClose state p collection =
    match PrecedenceProcessor.foldFindPreviousToken (p - 1us) findOpen 1 collection with
    | x, ValueNone -> error "Could not find \"(\" token correspondnig to \")\""
    | _, ValueSome ``open`` ->  processParenthesis state ``open`` p collection

and private processQueue' state q collection =
    match q with
    | [] -> collection |> Ok
    | tokenId::q' ->
        match PrecedenceProcessor.get state.inputs.logger tokenId collection with
        | _, Processed _ -> processQueue' state q' collection
        | struct (p, _), Tkn _ ->
            let result = 
                processToken (ParserState.down state) p collection
                |> Result.map fstT
                |> Result.tryRecover (function
                    | MultiplePassesRequired x ->
                        Logger.debug0 "Queue item max depth reached" state.inputs.logger
                        Ok x
                    | Errs _ & x -> Error x)

            match result with
            | Ok x -> processQueue' state q' x
            | Error _ & x -> x

and private processCollectionError = Logger.describable (fun struct (oldC: PrecedenceProcessor<_>, newC: PrecedenceProcessor<_>) ->
    $"versionCompare: {PrecedenceProcessor.compareVersions oldC newC}, old count: {PrecedenceProcessor.rootNodeCount oldC}, new count: {PrecedenceProcessor.rootNodeCount newC}")

and private processCollectionCycles (state: ParserState) collection =

    Logger.debug1 "Executing cycle %O" state state.inputs.logger

    let q = PrecedenceProcessor.pQueue collection |> List.ofSeq
    match processQueue' (ParserState.down state) q collection with
    | Ok x & x' when PrecedenceProcessor.rootNodeCount x = 1 -> x'
    | Ok x when PrecedenceProcessor.compareVersions collection x >= 0 || PrecedenceProcessor.rootNodeCount x = 0 ->
        Logger.debug1 "Nothing processed in collection - %O" (processCollectionError struct (collection, x)) state.inputs.logger
        MultiplePassesRequired x |> Error
    | Ok x -> processCollectionCycles (ParserState.across state) x
    | Error _ & x -> x

let private removeSpaces: Token seq -> Token seq = Seq.filter (function | Token.Space _ -> false | _ -> true)

let private updateExpressionPreProcessor: Token seq -> Token seq seq =

    removeSpaces
    >> Seq.map (function
        | Token.Text ({string = s} & t) & tk ->
            UpdateExpressionVerb.tryParse s
            ?|> (
                UpdateExpressionLabel
                >> flip tpl t
                >> SyntheticToken)
            |> ValueOption.defaultValue tk
        | x -> x)

    // Assumes that verbs ("ADD", "REMOVE", "DELETE", "SET") are reserved terms and cannot be the names
    // of properties. This is not the case for "REMOVE"
    // Update expression: "SET x.remove = :p" is now not allowed
    >> Collection.windowDynamic (function | SyntheticToken (UpdateExpressionLabel _, _) -> true | _ -> false)

let rec private validateUpdateClauses logger astNode =
    AstNode.depthFirstSearch (fun _ acc ->
        function
        | AstNode.Update (verb, _) when List.contains verb acc ->
            clientError $"Invalid update expression, multiple {verb} expressions found"
        | AstNode.Update (verb, _) -> verb::acc
        | _ -> acc) [] astNode
    |> ignoreTyped<UpdateExpressionVerb list>

    astNode

let private addUpdateVerb = function
    | AstNode.Update (v, _) & x  -> struct (v, x)
    | x -> clientError $"{x} is not a valid upate expression"

let private combine: ProcessedTokenPointer<AstNode> seq -> ProcessedTokenPointer<AstNode> voption =
    Collection.foldBack (fun struct (struct (start, ``end``), acc) x ->
        struct (struct (Math.Min(x.fromToken, start), Math.Max(x.toToken, ``end``)), x.node::acc)) struct (struct (UInt16.MaxValue, UInt16.MinValue), [])
    >> function
        | struct (_, []) -> ValueNone
        | struct (struct (start, ``end``), nodes) ->
            { fromToken = start
              toToken = ``end``
              node = AstNode.Updates nodes }
            |> ValueSome

let private postProcessUpdateClauses logger expression =

    Seq.map (
        PrecedenceProcessor.complete logger expression
        >> addUpdateVerb)
    >> Seq.fold (fun s (struct (v, _) & x) ->
        if Collection.tryFind (fstT >> ((=)v)) s |> ValueOption.isSome
        then clientError $"Duplicate {v} in update expression"
        else x::s) []
    >> List.rev
    >> List.map sndT
    >> AstNode.Updates

type ClientOrServerError = Either<string list, unit>

let private combineErrors: _ -> Result<AstNode, ClientOrServerError> =
    Result.mapError (
        NonEmptyList.unwrap
        >> List.collect (function
            | MultiplePassesRequired _ -> [Either2 ()]
            | Errs errs -> NonEmptyList.unwrap errs |> List.map Either1)
        >> Either.partition
        >> function
            | struct ([], _) -> Either2 ()
            | xs, _ -> Either1 xs)

let private handleError =
    combineErrors
    >> function
        | Ok x -> x
        | Error (Either2 _) -> clientError "Query cannot be compiled"
        | Error (Either1 client) ->
            client
            |> Seq.mapi (fun i -> Str.indentSpace (i * 2))
            |> Str.join "\n"
            |> clientError

let singleOrServerError = function
    | [x] -> x
    | xs -> serverError $"Expected 1, got {List.length xs}"

let generateAstFromExpression inputs (expression: string) =

    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html
    let size = System.Text.Encoding.UTF8.GetByteCount(expression)
    if size > 4_000
    then clientError $"Max expression size is 4KB; received ({size}B)"

    let state = { inputs = inputs; depth = 0; cycle = 0 }
    let struct (preProcessor, postProcessor, validator) =
        if inputs.settings.updateFunctionVerbs
        then struct (
            updateExpressionPreProcessor,
            postProcessUpdateClauses inputs.logger expression,
            validateUpdateClauses inputs.logger)
        else struct (
            removeSpaces >> Seq.singleton,
            singleOrServerError >> PrecedenceProcessor.complete inputs.logger expression,
            id)

    let asArray i tokens =
        let tokens' = Array.ofSeq tokens
        Logger.debug2 "Processing expression part %i with %i tokens" (i + 1) tokens'.Length inputs.logger
        tokens'

    let (>>>) f g x y = f x y |> g
    preProcessor
    >> Seq.mapi (
        asArray
        >>> PrecedenceProcessor.create
        >>> processCollectionCycles state
        >>> Result.mapError NonEmptyList.singleton)
    >> Result.traverse
    >> Result.map (
        postProcessor
        >> validator
        >> Logger.debugFn1 "Constructed AST %A" inputs.logger)
    >> handleError

type Path = Either<string, uint> list

let private accessorBuilder =
    function
    | Either1 attr -> AccessorType.Attribute attr
    | Either2 i -> AccessorType.ListIndex i
    >> AstNode.Accessor

let private accessorPathBuilder = List.fold (fun s ->
    accessorBuilder >> tpl s >> tpl (Single Acc) >> AstNode.BinaryOperator)

let generateAstFromPaths: Path seq -> AstNode voption =

    Seq.map (function
        | [] -> ValueNone
        | head::tail -> struct (head, tail) |> ValueSome)
    >> Maybe.traverse
    >> Seq.map (
        flipTpl
        >> mapFst (flip accessorPathBuilder)
        >> mapSnd accessorBuilder
        >> applyTpl)
    >> List.ofSeq
    >> AstNode.toCsv