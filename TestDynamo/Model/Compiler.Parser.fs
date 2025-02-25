
/// <summary>
/// Generate an AST from an array of tokens
/// </summary>
module TestDynamo.Model.Compiler.Parser

open TestDynamo.Model
open TestDynamo.Model.Compiler.Lexer
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Model.Compiler
open TestDynamo.Data.Monads.Operators
open System.Runtime.CompilerServices

type ExpressionType =
    /// <summary>
    /// Conforming expressions are expressions which have the same syntax as filters, queries etc
    /// Projection, Condition, Key condition, Filters and Update SET and REMOVE are conforming update expressions 
    /// </summary>
    | Conforming
    
    /// <summary>
    /// Non-conforming update expressions are expressions which have a different syntax to filters, queries etc
    /// They have a much simpler syntax, e.g. "DELETE LHS RHS, LHS RHS, etc..."
    /// ADD and DELETE are non-conforming update expressions 
    /// </summary>
    | NonConforming

type ParserInputs =
    { logger: Logger
      settings: ParserSettings }
    
type ParserSettings =
    { inputs: ParserInputs
      exprType: ExpressionType }
    
[<IsReadOnly; Struct>]
type ParserState =
    { settings: ParserSettings
      processedThisCycle: bool
      depth: byte
      collection: PrecedenceProcessor<AstNode> }
    
let tokenDescriber = Logger.describable (fun struct (operation: string, state: ParserState, token: struct (struct (uint16 * uint16) * MaybeProcessedToken<AstNode>)) ->
    let tokenDesc =
        match token with
        | x, Processed y -> struct (x, AstNode.name y)
        | x, y -> struct (x, y.ToString())

    sprintf "as %s: %A (%O)" operation tokenDesc state)

let private debug struct (operationName, token) f state =
    Logger.debug1 "Processing token %O" (tokenDescriber (operationName, state, token)) state.settings.inputs.logger
    f state

let private pointer struct (struct (l, r), x) =
    { node = x
      fromToken = l
      toToken = r }

let private returnSnd = asLazy id

let private processSpace _ = serverError "Spaces should have been removed"

let private getProcessed p state =
    match PrecedenceProcessor.get state.settings.inputs.logger p state.collection with
    | x, Processed y -> struct (x, y) |> ValueSome
    | _ -> ValueNone

let private processBinaryOp state p op =
    
    let l = p - 1us
    let r = p + 1us
    
    ValueSome tpl
    <|? getProcessed l state
    <|? getProcessed r state
    ?|> (fun struct (((start, _), l'), ((_, ``end``), r')) ->
        BinaryOperator struct (op, struct (l', r'))
        |> tpl struct (start, ``end``)
        |> Either1)
    ?|? Either2 [|l; r|]

let private processText state p text =
    if ReservedWords.isReservedWord text then $"Cannot use reserved word {text}" |> ClientError.clientError
    
    AstNode.Accessor (AccessorType.Attribute text) 
    |> tpl struct (p, p)
    |> Either1

let private processExpressionAttrValue state p text =
    AstNode.ExpressionAttrValue text
    |> tpl struct (p, p)
    |> Either1

let private processExpressionAttrName state p text =
    AstNode.Accessor (AccessorType.ExpressionAttrName text) 
    |> tpl struct (p, p)
    |> Either1

let private processCall state p txt =

    Logger.debug1 "From call args %A" txt state.settings.inputs.logger
    
    let args = p + 1us
    getProcessed args state
    ?|> (fun struct ((_, ``end``), args) ->
        AstNode.Call struct (txt, args)
        |> tpl struct (p, ``end``)
        |> Either1)
    ?|? Either2 [|args|]

let private processBetween state position =
    Logger.debug0 "From BETWEEN target" state.settings.inputs.logger

    let target = position - 1us
    let range = position + 1us
    
    let rangeParts =
        getProcessed range state
        ?>>= (fun struct ((_, ``lower end``), lower) ->
                match PrecedenceProcessor.get state.settings.inputs.logger (``lower end`` + 1us) state.collection with
                | (_, endAnd), Tkn t ->
                    getProcessed (endAnd + 1us) state
                    ?|> fun struct ((_, ``upper end``), upper) -> struct (``upper end``, struct (lower, upper))
                | _ -> ValueNone)
    
    ValueSome tpl
    <|? getProcessed target state
    <|? rangeParts
    ?>>= (fun struct (((start, _), target), (``upper end``, (lower, upper))) ->
            struct (struct (start, ``upper end``), AstNode.Between struct (target, struct (lower, upper))) |> ValueSome)
    ?|> Either1
    ?|? Either2 [|target; range|]

let private processUnaryOp state position op =

    Logger.debug1 "From unary operand %A" op state.settings.inputs.logger
    
    let operand = position + 1us
    getProcessed operand state
    ?|> (fun struct ((_, ``end``), operand) ->
        UnaryOperator struct (op, operand)
        |> tpl struct (position, ``end``)
        |> Either1)
    ?|? Either2 [|operand|]

let private findClose opens = function
    | Token.Close _ when opens = 1 -> struct (0, true)
    | Token.Close _ -> struct (opens - 1, false)
    | Token.Open _ -> struct (opens + 1, false)
    | _ -> struct (opens, false)

let private findOpen closes = function
    | Token.Open _ when closes = 1 -> struct (0, true)
    | Token.Open _ -> struct (closes - 1, false)
    | Token.Close _ -> struct (closes + 1, false)
    | _ -> struct (closes, false)

let private tryTokenIsOpen p (collection: PrecedenceProcessor<_>) =
    PrecedenceProcessor.tryGetToken p collection
    ?|> function
        | Token.Open _ -> true
        | _ -> false
    ?|? false

let private processListIndex state position index =

    Logger.debug0 "From list index LHS" state.settings.inputs.logger
    
    let parent = position - 1us
    getProcessed parent state
    ?|> (fun struct ((from, _), indexed) ->
        Logger.debug1 "From list index RHS [%d]" index state.settings.inputs.logger

        AstNode.Accessor (AccessorType.ListIndex index)
        |> tpl indexed
        |> tpl (BinaryOpToken.Single SingletonBinaryOpToken.Acc)
        |> BinaryOperator
        |> tpl struct (from, position)
        |> Either1)
    ?|? Either2 [|parent|]

[<Struct; IsReadOnly>]
type FailedToken =
    { id: uint16
      // tokens which belong to this token
      claimed: uint16 array }
    
[<Struct; IsReadOnly>]
type TokenQueue =
    { queue: uint16 list
      failed: FailedToken list }
    
    static member areClaimed currentToken first last {failed = failed} =
        let range = []
        
        // the current token cannot be claimed
        let first = if first = currentToken then first + 1us else first
        let last = if last = currentToken then last - 1us else last
        
        failed
        |> Seq.collect _.claimed
        |> Seq.filter (flip Array.contains [|first..last|])
        |> Seq.isEmpty
        |> not
    
    static member create ids = {queue = ids; failed = [] }

let rec private processQueue (tokens: TokenQueue) (state: ParserState) =
        
    match tokens with
    | { queue = []; failed = []} -> state
    | { queue = []; failed = failed} when not state.processedThisCycle -> invalidOp "Cannot parse expression"
    | { queue = []; failed = failed} ->
        processQueue
            { queue = Seq.rev failed |> Seq.map _.id |> List.ofSeq; failed =  []}
            { state with processedThisCycle = false }
    | { queue = head::tail; failed = failed} ->

        let tkn = PrecedenceProcessor.get state.settings.inputs.logger head state.collection
        let struct (queue, state') =
            match tkn with
            | p, Processed x -> debug struct ("Processed", tkn) returnSnd state struct (p, x) |> tpl false |> Either1
            | _, Tkn (Token.Space _) -> debug struct ("whitespace", tkn) processSpace state |> Either1
            | (p, _), Tkn (Token.BinaryOp (op, _)) -> debug struct ("BinaryOp", tkn) processBinaryOp state p op |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.ExpressionAttrValue attr) -> debug struct ("\":val\"", tkn) processExpressionAttrValue state p attr.string |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.ExpressionAttrName attr) -> debug struct ("\"#name\"", tkn) processExpressionAttrName state p attr.string |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.Open _) -> debug struct ("( [open parentheses]", tkn) processOpen state p |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.Close _) -> debug struct (") [close parentheses]", tkn) processClose state p |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.Text x) when Array.contains x.string state.settings.inputs.settings.functionNames ->
                debug struct ("CALL", tkn) processCall state p x.string |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.Text x) when tryTokenIsOpen (p + 1us) state.collection ->
                // this is a failure case. At this state we know it is an invalid function name
                debug struct ("CALL", tkn) processCall state p x.string |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.UnaryOp (op, _)) -> debug struct ("UnaryOp", tkn) processUnaryOp state p op |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.Text attr) -> debug struct ("Symbol", tkn) processText state p attr.string |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.Between _) -> debug struct ("BETWEEN", tkn) processBetween state p |> Either.map1Of2 (tpl true)
            | (p, _), Tkn (Token.ListIndex (index, _)) -> debug struct ("[i]", tkn) processListIndex state p index |> Either.map1Of2 (tpl true)
            | _, Tkn (Token.SyntheticToken _) -> ClientError.clientError "Cannot parse expression"
            |> function
                // if a node is resolved which contains multiple tokens, and one of those tokens
                // is claimed by a previous node: abort
                | Either1 (true, ((from, ``to``), _)) when TokenQueue.areClaimed head from ``to`` tokens ->
                    Logger.debug2 "Nodes %i - %i claimed"  from ``to`` state.settings.inputs.logger
                    Either2 [|from..``to``|]
                | x -> x
            |> Either.map1Of2 (fun struct (put, pointerParts) ->
                let newCollection =
                    if put
                    then
                        let x = pointer pointerParts
                        PrecedenceProcessor.put state.settings.inputs.logger x state.collection
                    else state.collection
                    
                { state with collection = newCollection; processedThisCycle = state.processedThisCycle || put }
                |> tpl { queue = tail; failed = failed })
            |> Either.map2Of2 (fun claimed ->
                struct ({ queue = tail; failed = { id = head; claimed = claimed } ::failed}, state))
            |> Either.reduce
                
        processQueue queue state'

and private processOpen state p =
    match PrecedenceProcessor.foldFindNextToken (p + 1us) findClose 1 state.collection with
    | _, ValueNone -> ClientError.clientError "Could not find \")\" token corresponding to \"(\""
    | _, ValueSome close -> processParenthesis true state p close

and private processClose state p =
    match PrecedenceProcessor.foldFindPreviousToken (p - 1us) findOpen 1 state.collection with
    | x, ValueNone -> ClientError.clientError "Could not find \"(\" token corresponding to \")\""
    | _, ValueSome ``open`` -> processParenthesis false state ``open`` p

and private processParenthesis processingOpen state ``open`` closed =
    
    let start = ``open`` + 1us
    let ``end`` = closed - 1us

    match compare start ``end`` with
    | i when i > 0 -> ClientError.clientError "Cannot parse expression"
    | _ ->
        PrecedenceProcessor.alreadyProcessed start ``end`` state.collection
        ?|> (_.node >> ValueSome)
        ?|>? (fun _ ->
            Logger.debug1 "From sub collection: %A" struct (start, ``end``) state.settings.inputs.logger
            
            let processor = processCollection state.depth state.settings >> ValueSome
            PrecedenceProcessor.subCollection state.settings.inputs.logger start ``end`` processor state.collection
            ?>>= (function
                | struct ([x], c) -> ValueSome x.node
                | struct (_, c) -> ClientError.clientError "Cannot parse expression"))
        ?|> tpl struct (``open``, closed)
        ?|> Either1
        ?|>? fun _ -> 
            [| (if processingOpen then start else ``open``)..(if processingOpen then closed else ``end``)|]
            |> Either2

and processCollection' depth (settings: ParserSettings) (collection: PrecedenceProcessor<AstNode>): ParserState =
    
    // should not ever get above 2. Precedence processor is good at processing the inner
    // parentheses first
    if depth > 200uy then ClientError.clientError "Recursion limit reached"
            
    let state =
        { settings = settings
          depth = depth + 1uy 
          processedThisCycle = false
          collection = collection }
    
    PrecedenceProcessor.pQueue settings.inputs.settings.functionNames collection
    |> List.ofSeq
    |> TokenQueue.create
    |> flip processQueue state

and processCollection = processCollection' >>>> _.collection

let private removeSpaces: Token seq -> Token seq = Seq.filter (function | Token.Space _ -> false | _ -> true)

module private UpdateExpression =

    let preProcessor: Token seq -> struct (UpdateExpressionVerb voption * Token seq) seq =

        Seq.map (function
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
        >> Seq.map (
            List.ofSeq
            >> function
                | SyntheticToken (UpdateExpressionLabel head, _)::tail -> struct (ValueSome head, tail)
                | x -> invalidOp $"Invalid update expression")    // should not happen
    
    let rec validate logger astNode =
        AstNode.depthFirstSearch (fun _ acc ->
            function
            | AstNode.Update (verb, _) when List.contains verb acc ->
                ClientError.clientError $"Invalid update expression, multiple {verb} expressions found"
            | AstNode.Update (verb, _) -> verb::acc
            | _ -> acc) [] astNode
        |> ignoreTyped<UpdateExpressionVerb list>
    
        astNode
        
    let private makeUpdate struct (update, expression) =
        struct (update, AstNode.Update (update, expression))
        
    /// <summary>
    /// Non confirming expressions will end up 1 of 2 ways:
    ///     1) Expr: "Attr :value" => ["Attr", ":value"]
    ///     2) Expr: "Attr1 :value1, Attr2 :value2" => ["Attr", [":value1", "Attr2"], ":value2"]
    /// Need to re-format the second way so that related terms are grouped together
    /// </summary>
    let private reProcessCsv' astNode =
        let expanded =
            AstNode.expand astNode
            |> List.collect (fun x -> [Either2 (); Either1 x])
            
        match expanded with
        | [] -> []
        // remove leading comma
        | _::tail -> tail
            
    let private reProcessCsv exp (nodes: AstNode list) =
        nodes
        |> List.collect reProcessCsv'
        |> Collection.windowDynamic (function | Either2 _ -> true | Either1 _ -> false)
        |> Seq.map (
            Either.partition
            >> fstT
            >> function
                | [l; r] -> AstNode.BinaryOperator (Single WhiteSpace, (l, r))
                | _ -> ClientError.clientError $"{exp} is not a valid update expression")
    
    let private complete logger exp = function
        // conforming
        | struct (ValueSome (UpdateExpressionVerb.Set & upd), processor)
        | struct (ValueSome (UpdateExpressionVerb.Remove & upd), processor) ->
            processor
            |> PrecedenceProcessor.complete logger exp
            |> tpl upd
        // non conforming
        | struct (ValueSome (UpdateExpressionVerb.Add & upd), processor)
        | struct (ValueSome (UpdateExpressionVerb.Delete & upd), processor) ->
            PrecedenceProcessor.completeMulti logger exp processor
            |> reProcessCsv exp
            |> List.ofSeq
            |> AstNode.toCsv
            |> Maybe.expectSome
            |> tpl upd
        | _, expression -> ClientError.clientError $"{exp} is not a valid update expression"
    
    let postProcessor =
        complete
        >>> Seq.map
        >>>> Seq.map makeUpdate
        >>>> Seq.fold (fun s (struct (v, _) & x) ->
            if Collection.tryFind (fstT >> ((=)v)) s |> ValueOption.isSome
            then ClientError.clientError $"Duplicate {v} in update expression"
            else x::s) []
        >>>> List.rev
        >>>> List.map sndT
        >>>> AstNode.Updates

module private NonUpdateExpression =

    let private singleOrServerError = function
        | [x] -> x
        | xs -> serverError $"Expected 1, got {List.length xs}"
    
    let preProcessor = tpl ValueNone >> Seq.singleton
    let postProcessor logger expression: struct (_ * PrecedenceProcessor<_>) seq -> _ =
        Seq.map sndT
        >> List.ofSeq
        >> singleOrServerError
        >> PrecedenceProcessor.complete logger expression

let private expressionType =
    ValueOption.map (function
        | UpdateExpressionVerb.Set -> Conforming
        | UpdateExpressionVerb.Remove -> Conforming
        | UpdateExpressionVerb.Delete -> NonConforming
        | UpdateExpressionVerb.Add -> NonConforming)
    >> ValueOption.defaultValue Conforming

let private asArray logger i struct (upd, tokens) =
    let tokens' = Array.ofSeq tokens
    Logger.debug2 "Processing expression part %i with %i tokens" (i + 1) tokens'.Length logger
    struct (upd, struct (expressionType upd, tokens'))
    
let generateAstFromExpression (inputs: ParserInputs) (expression: string) =

    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html
    let size = System.Text.Encoding.UTF8.GetByteCount(expression)
    if size > 4_000
    then ClientError.clientError $"Max expression size is 4KB; received ({size}B)"

    let struct (preProcessor, postProcessor, validator) =
        if inputs.settings.updateFunctionVerbs
        then struct (
            UpdateExpression.preProcessor,
            UpdateExpression.postProcessor inputs.logger expression,
            UpdateExpression.validate inputs.logger)
        else struct (
            NonUpdateExpression.preProcessor,
            NonUpdateExpression.postProcessor inputs.logger expression,
            id)

    removeSpaces
    >> preProcessor
    >> Seq.mapi (
        asArray inputs.logger
        >>> mapSnd (
            mapFst (fun t -> { inputs = inputs; exprType = t })
            >> mapSnd PrecedenceProcessor.create
            >> uncurry (processCollection 0uy)))
    >> postProcessor
    >> validator
    >> Logger.debugFn1 "Constructed AST %A" inputs.logger

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