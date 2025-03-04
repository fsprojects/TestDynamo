﻿namespace TestDynamo.Model.Compiler

open System
open TestDynamo.Model
open TestDynamo.Model.Compiler.Lexer
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators
open System.Runtime.CompilerServices

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Precedence
module private NodePrecedence =

    [<Literal>]
    let nonFunctionSymbol = 0F

    // . is one step away from a symbol. Not mentioned in dynamodb spec
    [<Literal>]
    let dot = 0.1F

    // Parentheses (dynamodb spec places these in position 5)
    [<Literal>]
    let closeParentheses = 0.2F

    // Parentheses (dynamodb spec places these in position 5)
    // Process close before open. This will have the effect of ignoring open
    // entirely, and processing the inner most parenthesis block first
    [<Literal>]
    let openParentheses = 0.21F
    
    // Needs to be after parenthesis (args)
    [<Literal>]
    let functionSymbol = 0.25F

    // UPDATE expression verb
    // Not mentioned in dynamodb spec. Different syntax to anything else
    // causes parser rules to change. Process after symbols and before comma
    [<Literal>]
    let addOrDeleteUpdate = 0.3F

    // +, -. Needs to be done before equality/assignment
    [<Literal>]
    let aritmetic = 0.4F

    // = <> < <= > >=
    [<Literal>]
    let binaryOp = 1F

    // Not mentioned in dynamodb spec. Seems to be directly after binary ops
    [<Literal>]
    let comma = 1.1F

    // IN
    [<Literal>]
    let ``in`` = 2F

    // BETWEEN
    [<Literal>]
    let between = 3F

    // attribute_exists attribute_not_exists begins_with contains
    [<Literal>]
    let namedFunction = 4F

    // NOT
    [<Literal>]
    let not = 6F

    // AND
    [<Literal>]
    let ``and`` = 7F

    // OR
    [<Literal>]
    let ``or`` = 8F

    // UPDATE expression verb
    // Not mentioned in dynamodb spec. Should be processed last
    [<Literal>]
    let setOrRemoveUpdate = 100F

    // Synthetic binary operator. Should not be encouuntered
    // in precedence preprocessor
    [<Literal>]
    let spaceOperator = 100F

/// <summary>
/// A token processed result, including
/// indexes for the range of the tokens that it replaces
/// </summary>
[<Struct; IsReadOnly>]
type ProcessedTokenPointer<'a> =
    { node: 'a
      fromToken: uint16
      toToken: uint16 }

module ProcessedTokenPointer =

    let create f t n = {fromToken = f; toToken = t; node = n }

    let compare x y =
        match x.fromToken - y.fromToken with
        | 0us -> x.toToken - y.toToken
        | x -> x
        |> int

[<Struct; IsReadOnly>]
type MaybeProcessedToken<'processed> =
    | Processed of a: 'processed
    | Tkn of Token

[<Struct; IsReadOnly>]
type private PrecedenceProcessorState<'processed> =
    { processed: ProcessedTokenPointer<'processed> list
      tokens: Token array }

/// <summary>
/// A token processor
/// </summary>
[<Struct; IsReadOnly>]
type PrecedenceProcessor<'processed> =
    private
    | Pp of PrecedenceProcessorState<'processed>

module PrecedenceProcessor =

    let create tokens = { processed = []; tokens = tokens } |> Pp

    let isUpdateFunctionVerb = UpdateExpressionVerb.tryParse >> ValueOption.isSome

    let private prioritize functionNames = function
        | Token.Text x when Array.contains x.string functionNames -> NodePrecedence.functionSymbol
        | Token.Text _ -> NodePrecedence.nonFunctionSymbol
        | Token.Space _ -> NodePrecedence.nonFunctionSymbol
        | Token.BinaryOp (Single Eq, _)
        | Token.BinaryOp (Single Neq, _)
        | Token.BinaryOp (Single Lt, _)
        | Token.BinaryOp (Single Lte, _)
        | Token.BinaryOp (Single Gt, _)
        | Token.BinaryOp (Single Gte, _) -> NodePrecedence.binaryOp
        | Token.BinaryOp (Single Plus, _)
        | Token.BinaryOp (Single Minus, _) -> NodePrecedence.aritmetic
        | Token.BinaryOp (Multi Comma, _) -> NodePrecedence.comma
        | Token.BinaryOp (Single Acc, _) -> NodePrecedence.dot
        | Token.BinaryOp (Single And, _) -> NodePrecedence.``and``
        | Token.BinaryOp (Single In, _) -> NodePrecedence.``in``
        | Token.BinaryOp (Single Or, _) -> NodePrecedence.``or``
        | Token.UnaryOp (Not, _) -> NodePrecedence.``not``
        | Token.Between _ -> NodePrecedence.between
        | Token.Open _ -> NodePrecedence.openParentheses
        | Token.Close _ -> NodePrecedence.closeParentheses
        | Token.ExpressionAttrValue _ -> NodePrecedence.nonFunctionSymbol
        | Token.ExpressionAttrName _ -> NodePrecedence.nonFunctionSymbol
        | Token.SyntheticToken (UpdateExpressionLabel Set, _)
        | Token.SyntheticToken (UpdateExpressionLabel Remove, _) -> NodePrecedence.setOrRemoveUpdate
        | Token.SyntheticToken (UpdateExpressionLabel Add, _)
        | Token.SyntheticToken (UpdateExpressionLabel Delete, _) -> NodePrecedence.addOrDeleteUpdate
        | Token.ListIndex _ -> NodePrecedence.nonFunctionSymbol  // using same precedence as expr
        | Token.BinaryOp (Single WhiteSpace, _) ->
            assert false
            NodePrecedence.spaceOperator

    /// <summary>
    /// Create a priority queue from this processor. Priority is based on precedence and index
    /// </summary>
    let pQueue functionNames (Pp { tokens = tokens }) =
        Seq.mapi (tpl >>> mapFst uint16) tokens
        |> Seq.sortBy (fun struct (i, t: Token) ->
            let i' =
                match t with
                // process deepest nesting first
                | Open _ -> UInt16.MaxValue - i
                | _ -> i

            struct (prioritize functionNames t, i'))
        |> Seq.map fstT

    type private ForErrorsCache<'a>() =
        static member private forErrors' (Pp x) =
            let unmergedAst = x.processed |> List.sortBy _.fromToken
            let unmerged =
                unmergedAst
                |> Seq.map _.node
                |> Seq.mapi (sprintf " %d. %A")
                |> Str.join "\n"

            let unmergedFull =
                unmergedAst
                |> Seq.mapi (sprintf " %d. %A")
                |> Str.join "\n"

            let tokens =
                x.tokens
                |> Seq.mapi (sprintf " %d. %A")
                |> Str.join "\n"

            sprintf "Unmerged:\n%s\nUnmerged full:\n%s\nTokens:\n%s\n" unmerged unmergedFull tokens
        
        static member forErrors: _ -> Describable<PrecedenceProcessor<'a>> =
            Logger.describable ForErrorsCache<'a>.forErrors'
        
        static member forErrorsWithUnProcessedTokens: _ -> Describable<struct (PrecedenceProcessor<'a> * int list)> =
            Logger.describable (fun struct (x, tokens) ->
                ForErrorsCache<'a>.forErrors' x
                |> sprintf "Unprocessed: %A\n%s" tokens)

    let asSeq (Pp { processed = results }) = results |> Seq.sortWith ProcessedTokenPointer.compare

    let rootNodeCount (Pp { processed = p }) = List.length p

    /// <summary>
    /// Assert that the processor is complete and return the completed result
    /// </summary>
    let completeMulti logger expression (Pp { processed = processed; tokens = tkn } & processor) =
        
        if tkn.Length = 0 then processed |> List.map _.node
        else
            
        let processed =
            processed
            |> List.sortBy _.fromToken
        
        let processedFrom =
            List.head processed
            |> _.fromToken
            |> int
        
        let processedTo =
            Collection.lastL processed
            |> _.toToken
            |> int
            
        let unProcessed =
            processed
            |> Seq.windowed 2
            |> Seq.collect (function
                | [|l; r|] ->
                    [int l.toToken + 1 .. int r.fromToken - 1]
                | (* can't happen *) _ -> invalidOp "An unexpected error has occurred")
            |> Collection.concat2 [0..processedFrom - 1]
            |> flip Collection.concat2 [processedTo + 1 .. tkn.Length - 1]
            |> List.ofSeq
            
        match unProcessed with
        | [] -> processed |> List.map _.node
        | xs ->
            Logger.log2 "Error processing expression \"%s\"\nunmerged AST\n%O" expression (ForErrorsCache<_>.forErrorsWithUnProcessedTokens struct (processor, unProcessed)) logger
            ClientError.clientError $"Error processing expression \"{expression}\"."        

    /// <summary>
    /// Assert that the processor is complete and return the completed result
    /// </summary>
    let complete logger expression processor =
        match completeMulti logger expression processor with
        | [result] -> result
        | [] -> ClientError.clientError $"Error processing expression \"{expression}\"."
        | x ->
            Logger.log1 "Error processing expression: unmerged AST\n%O" (ForErrorsCache<_>.forErrors processor) logger
            ClientError.clientError $"Error processing expression \"{expression}\"."

    let rec private isProcessed' from ``to`` = function
        | [] -> ValueNone
        | head::_ when head.fromToken = from && head.toToken = ``to`` -> ValueSome head
        | _::tail -> isProcessed' from ``to`` tail

    // O(N) operation on an ever shrinking number of nodes
    // probably does not need to be optimised for small collections
    let alreadyProcessed from ``to`` (Pp { processed = processed }) =
        isProcessed' from ``to`` processed

    /// <summary>
    /// Put a tokenization result into the processor, replacing
    /// any tokens or other results which were in its place
    /// </summary>
    let put =
        let removeSubsumed struct (logger, pointer) =
            List.filter (function
                // unrelated
                | head when head.fromToken > pointer.toToken || head.toToken < pointer.fromToken -> true

                // overlaps exactly
                | head when head.fromToken = pointer.fromToken && head.toToken = pointer.toToken ->
                    Logger.log1 "Exactly overlapping elements %A" struct (head, pointer) logger
                    ClientError.clientError  $"Cannot parse expression"

                // subsumes
                | head when head.fromToken >= pointer.fromToken && head.toToken <= pointer.toToken -> false

                // everything else
                | head  ->
                    Logger.log1 "Overlapping elements %A" struct (head, pointer) logger
                    ClientError.clientError $"Cannot parse expression")

        let outOfRange tkn pointer =
            pointer.fromToken < 0us || pointer.fromToken > pointer.toToken || int pointer.toToken >= Array.length tkn

        fun logger pointer ->
            function
            | Pp { tokens = tkn } when outOfRange tkn pointer ->
                Logger.log1 "Ast node out of range %A" pointer logger
                ClientError.clientError $"Cannot parse expression"
            | Pp ({ processed = ast; } & data) ->
                let result = removeSubsumed struct (logger, pointer) ast
                Pp { data with processed = pointer::result }

    let private foldFindToken move  (start: uint16) filter s (Pp { tokens = ts }) =
        let mutable s' = s
        let mutable i = int start
        let mutable result = ValueNone
        while i < Array.length ts && i >= 0 && ValueOption.isNone result do
            let x = Array.get ts i
            let struct (s'', ok) = filter s' x

            if ok then
                result <- uint16 i |> ValueSome
            else
                i <- move i

            s' <- s''

        struct (s', result)

    let private findToken move =
        let find = foldFindToken move
        fun start filter -> find start (filter >> tpl true |> asLazy) true >> sndT

    /// <summary>
    /// Walk up the processor to find the next token that matches a predicate
    /// </summary>
    let foldFindNextToken start filter state = foldFindToken ((+)1) start filter state

    /// <summary>
    /// Walk down the processor to find the previous token that matches a predicate
    /// </summary>
    let foldFindPreviousToken start filter state = foldFindToken (flip (-)1) start filter state

    /// <summary>
    /// Get a token at a given index, along with its processed version if available
    /// </summary>
    let tryGet =
        // O(N) operation on an ever shrinking number of nodes
        // probably does not need to be optimised for small collections
        let rec tryAst index = function
            | [] -> ValueNone
            | head::_ when head.fromToken <= index && head.toToken >= index ->
                struct (struct (head.fromToken, head.toToken), head.node) |> ValueSome
            | _::tail -> tryAst index tail

        fun (i: uint16) collection ->
            match struct (i, collection) with
            | i, Pp { tokens = tkn } when int i >= Array.length tkn -> ValueNone
            | i, Pp { tokens = tkn; processed = ast } ->
                tryAst i ast
                ?|> (mapSnd Processed)
                |> ValueOption.defaultValue (Array.get tkn (int i) |> Tkn |> tpl struct (i, i))
                |> ValueSome

    /// <summary>
    /// Get a token at a given index. Even if the token is processed, return it's unprocessed version
    /// </summary>
    let tryGetToken (i: uint16) (Pp { tokens = tkn }) =
        let i = int i
        if i >= Array.length tkn then ValueNone
        else ValueSome tkn[i]

    /// <summary>
    /// Specify whether a given index is out of range
    /// </summary>    
    let isOutOfRange (i: UInt16) (Pp { tokens = collection }) = int i >= Array.length collection

    /// <summary>
    /// Get a token at a given index, along with its processed version if available
    /// </summary>
    let get logger i = 
        tryGet i >> ValueOption.defaultWith (fun _ ->
            Logger.log1 "Ast node out of range %i" i logger
            ClientError.clientError $"Cannot parse expression")

    /// <summary>
    /// Process a sub collection of tokens
    /// Given the start and end tokens and a processing function
    ///     create a new processor with the specified subset
    ///     pass the new processor into the processing function. This function can process as many nodes as possible
    ///     merge the results back into the current processor
    ///     return any processed nodes
    /// </summary>
    let subCollection =
        let createSub from ``to`` = function
            | Pp ({ tokens = tk; processed = pointers } & data) ->
                let tokens =
                    Seq.skip (int from) tk
                    |> Seq.truncate (int (``to`` + 1us - from))
                    |> Array.ofSeq

                let pointers =
                    Seq.filter (function
                        | x when x.fromToken >= from && x.toToken <= ``to`` -> true
                        | x when x.fromToken > ``to`` || x.toToken < from -> false
                        | x -> ClientError.clientError $"Overlapping element {from}-{``to``}, {x}.") pointers
                    |> Seq.map (fun x -> {x with fromToken = x.fromToken - from; toToken =  x.toToken - from })
                    |> List.ofSeq

                Pp { tokens = tokens; processed = pointers }

        let mergeBack logger parent from ``to`` = function
            | Pp { processed = processed } ->
                processed
                |> List.fold (fun struct (acc, p) x ->
                    let added = { x with fromToken = x.fromToken + from; toToken = x.toToken + from }
                    put logger added p
                    |> tpl (added::acc)) struct ([], parent)

        fun logger from ``to`` f collection ->
            createSub from ``to`` collection
            |> (f >> ValueOption.map (mergeBack logger collection from ``to``))