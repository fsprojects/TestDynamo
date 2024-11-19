
/// <summary>
/// Parses an expression string into tokens
/// </summary>
module TestDynamo.Model.Compiler.Lexer

open System
open System.Runtime.CompilerServices
open System.Text.RegularExpressions
open TestDynamo.Utils

[<Struct; IsReadOnly>]
type TokenData =
    { string: string
      position: int }

/// <summary>A binary op which resolves to a single item</summary>
type SingletonBinaryOpToken =
    | Eq
    | Neq
    | Lt
    | Lte
    | Gt
    | Gte
    | And
    | Or
    | In
    | Plus
    | Minus
    | Acc
    /// <summary>Currently exclusively used in update expressions as an operator: "ADD x :y"</summary>
    | WhiteSpace

    with
    override this.ToString() =
        match this with
        | Eq -> "=" 
        | Neq -> "<>"
        | Lt -> "<"
        | Lte -> "<="
        | Gt -> ">"
        | Gte -> ">="
        | And -> "AND"
        | Or -> "OR"
        | In -> "IN"
        | Plus -> "+"
        | Minus -> "-"
        | Acc -> "."
        | WhiteSpace -> " "

/// <summary>A binary op which resolves to a multiple items</summary>
type MultiBinaryOpToken =
    | Comma

    with
    override this.ToString() =
        match this with
        | Comma -> "," 

type BinaryOpToken =
    | Single of SingletonBinaryOpToken
    | Multi of MultiBinaryOpToken

    with
    override this.ToString() =
        match this with
        | Single x -> x.ToString()
        | Multi x -> x.ToString()

[<RequireQualifiedAccess>]
module private BinaryOps =
    let eq = Single Eq
    let neq = Single Neq
    let lt = Single Lt
    let lte = Single Lte
    let gt = Single Gt
    let plus = Single Plus
    let minus = Single Minus
    let gte = Single Gte
    let ``and`` = Single And
    let ``or`` = Single Or
    let ``in`` = Single In
    let accessor = Single Acc
    let comma = Multi Comma

type UnaryOpToken =
    | Not

    with
    override this.ToString() =
        match this with
        | Not -> "NOT"

type UpdateExpressionVerb =
    | Set
    | Add
    | Remove
    | Delete

    with
    override this.ToString() =
        match this with
        | Set -> "SET"
        | Add -> "ADD"
        | Remove -> "REMOVE"
        | Delete -> "DELETE"

module UpdateExpressionVerb =
    let tryParse v = 
        if "SET".Equals(v, System.StringComparison.OrdinalIgnoreCase) then ValueSome Set
        elif "ADD".Equals(v, System.StringComparison.OrdinalIgnoreCase) then ValueSome Add
        elif "REMOVE".Equals(v, System.StringComparison.OrdinalIgnoreCase) then ValueSome Remove
        elif "DELETE".Equals(v, System.StringComparison.OrdinalIgnoreCase) then ValueSome Delete
        else ValueNone

type SyntheticTokenInfo =
    | UpdateExpressionLabel of UpdateExpressionVerb

type Token = 
    | UnaryOp of struct (UnaryOpToken * TokenData)
    | BinaryOp of struct (BinaryOpToken * TokenData)
    | ListIndex of struct (uint * TokenData)
    | Between of TokenData
    | Open of TokenData
    | Close of TokenData
    | ExpressionAttrValue of TokenData
    | ExpressionAttrName of TokenData
    | Text of TokenData
    | Space of TokenData
    // synthetic tokens are not parsed from a string, but rather mapped in an intermediate step: post lexing, pre parsing
    | SyntheticToken of struct (SyntheticTokenInfo * TokenData)

module Token =
    let position = function
        | Text {position = position}
        | Open {position = position}
        | Close {position = position}
        | Space {position = position}
        | Between {position = position}
        | UnaryOp (_, {position = position})
        | BinaryOp (_, {position = position})
        | ListIndex (_, {position = position})
        | SyntheticToken (_, {position = position})
        | ExpressionAttrName {position = position}
        | ExpressionAttrValue {position = position} -> position

/// <summary>Match against a full regex if it appears at the specified index</summary>
let private matchAtIndex struct (rx: Regex, struct (text: string, index)) =
#if NETSTANDARD2_0
    // find match indexes over parse matches to avoid heavy (and unnecessary) substringing
    let rxMatch = rx.Match(text, index)

    if rxMatch.Success |> not
    then ValueNone
    elif rxMatch.Index <> index
    then ValueNone
    else rxMatch.Groups[0].Captures[0].Value |> ValueSome
#else
    // find match indexes over parse matches to avoid heavy (and unnecessary) substringing
    let mutable enm = rx.EnumerateMatches(text, index)
    
    if enm.MoveNext() |> not
    then ValueNone
    elif enm.Current.Index <> index then ValueNone
    elif enm.Current.Length = 0 then serverError "Infinite loop"
    else text.Substring(index, enm.Current.Length) |> ValueSome
#endif

let private token map (rx: Regex) =

    if (rx.ToString().Length = 0) then invalidOp "Regex must not be empty"

    fun (struct (_, index: int) & args) ->
        // hot path, avoid allocations if possible
        match matchAtIndex struct (rx, args) with
        | ValueNone -> ValueNone
        | ValueSome matchText ->
            struct (map { string = matchText; position = index }, index + matchText.Length)
            |> ValueSome

let private parseIndexer =
    let scanRx = Regex("""\[\s*(?<index>\d+)\s*\]""", RegexOptions.Compiled)
    // optimisation. Do not scan the entire string again to parse out index
    let parseRx = Regex("^" + scanRx.ToString(), RegexOptions.Compiled)

    let parse (data: TokenData): Token =
        let m = parseRx.Match data.string
        if m.Success |> not then invalidOp "Unexpected parsing error"

        let index = uint m.Groups["index"].Captures[0].Value
        ListIndex struct (index, data)

    token parse scanRx

let inline private addListIndex (token: TokenData) =
    struct (uint token.string, token)

let private getNextToken =
    [
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
        Regex("""\s+""", RegexOptions.Compiled) |> token Token.Space
        Regex("""=""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.eq)
        Regex("""<>""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.neq)
        Regex("""<=""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.lte)
        Regex("""<""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.lt)
        Regex(""">=""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.gte)
        Regex(""">""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.gt)
        Regex("""\+""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.plus)
        Regex("""-""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.minus)
        Regex(""",""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.comma)
        Regex("""\.""", RegexOptions.Compiled) |> token (curry Token.BinaryOp BinaryOps.accessor)
        Regex("""\(""", RegexOptions.Compiled) |> token Token.Open
        Regex("""\)""", RegexOptions.Compiled) |> token Token.Close
        Regex("""BETWEEN\b""", RegexOptions.Compiled ||| RegexOptions.IgnoreCase) |> token Token.Between
        Regex("""AND\b""", RegexOptions.Compiled ||| RegexOptions.IgnoreCase) |> token (curry Token.BinaryOp BinaryOps.``and``)
        Regex("""OR\b""", RegexOptions.Compiled ||| RegexOptions.IgnoreCase) |> token (curry Token.BinaryOp BinaryOps.``or``)
        Regex("""IN\b""", RegexOptions.Compiled ||| RegexOptions.IgnoreCase) |> token (curry Token.BinaryOp BinaryOps.``in``)
        Regex("""NOT\b""", RegexOptions.Compiled ||| RegexOptions.IgnoreCase) |> token (curry Token.UnaryOp UnaryOpToken.Not)
        Regex("""#\w+""", RegexOptions.Compiled) |> token Token.ExpressionAttrName
        Regex(""":\w+""", RegexOptions.Compiled) |> token Token.ExpressionAttrValue
        Regex("""[a-zA-Z]\w*""", RegexOptions.Compiled) |> token Token.Text
        parseIndexer
    ]
    |> fun xs d ->
        List.toSeq xs
        |> Seq.map (apply d)
        |> Maybe.traverse
        |> Collection.tryHead

let tokenize: string -> Token list =
    let rec parser' strIndex acc =
        let struct (str, index) = strIndex
        match String.length str with
        | x when x <= index -> acc
        | _ ->
            match getNextToken strIndex with
            | ValueNone -> clientError $"Cannot parse token at position {sndT strIndex}. \"{fstT strIndex}\""
            | ValueSome struct (token, next) -> parser' struct (str, next) (token::acc)

    flip tpl 0
    >> flip parser' []
    >> List.rev