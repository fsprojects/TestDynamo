
namespace DynamoDbInMemory.Model.Compiler

open System.Runtime.CompilerServices
open DynamoDbInMemory.Model.Compiler.Lexer
open DynamoDbInMemory.Model
open DynamoDbInMemory.Utils
open DynamoDbInMemory
open DynamoDbInMemory.Data.Monads.Operators

type ExpressionAttrValues = Map<string, AttributeValue>
type ExpressionAttrNames = Map<string, string>

type ExpressionParams =
    { expressionAttrValues: ExpressionAttrValues
      expressionAttrNames: ExpressionAttrNames  
      forwards: bool
      lastEvaluatedKey: struct (AttributeValue * AttributeValue voption) voption }

[<Struct; IsReadOnly>]
type QueryParams =
    { logger: Logger
      expressionParams: ExpressionParams }

type private IndexSearch = QueryParams -> Index -> Partition voption
type private PartitionSearch = QueryParams -> Partition -> PartitionBlock seq

/// <summary>
/// Compile a query from an AST. Queries can only contain a small subset
/// of the dynamodb expression syntax and can only use Partition and Sort keys
/// Queries open indexes
///
/// The result of a query can filter an index 
/// </summary>
[<RequireQualifiedAccess>]
module QueryExpressionCompiler =
    
    [<Struct; IsReadOnly>]
    type private KeyValues =
        { partitionKey: IndexSearch voption
          sortKey: PartitionSearch voption
          exprAttrNameKeys: (Map<string, string> -> KeyValues -> KeyValues) list }

        with
        static member empty = { partitionKey = ValueNone; sortKey = ValueNone; exprAttrNameKeys = [] }

    let private isSortKey (key: KeyConfig) keyName =
        KeyConfig.sortKeyName key
        ?|> ((=)keyName)
        |> ValueOption.defaultValue false

    let private setPartitionKey acc ast =
        match acc.partitionKey with
        | ValueSome _ -> clientError "Found multiple partition key queries"
        | ValueNone -> {acc with partitionKey = ValueSome ast }

    let private setSortKey acc sk =

        match acc.sortKey with
        | ValueSome _ -> clientError "Found multiple sort key queries"
        | ValueNone -> {acc with sortKey = ValueSome sk }

    let private setAliasedKey acc builder =
        {acc with exprAttrNameKeys = builder::acc.exprAttrNameKeys }

    let private getExpressionAttrValue expressionAttrValue expressionAttrValues =
        match MapUtils.tryFind expressionAttrValue expressionAttrValues with
        | ValueSome x -> x
        | ValueNone -> clientError $"Cannot find expression attribute value {expressionAttrValue}"

    let private sortKeyEq expressionAttrValue (inputs: QueryParams) =
        let p = getExpressionAttrValue expressionAttrValue inputs.expressionParams.expressionAttrValues |> ValueSome
        Partition.subset p p true inputs.expressionParams.forwards

    let private sortKeyLt expressionAttrValue (inputs: QueryParams) =
        let p = getExpressionAttrValue expressionAttrValue inputs.expressionParams.expressionAttrValues |> ValueSome
        Partition.subset ValueNone p false inputs.expressionParams.forwards

    let private sortKeyLte expressionAttrValue (inputs: QueryParams) =
        let p = getExpressionAttrValue expressionAttrValue inputs.expressionParams.expressionAttrValues |> ValueSome
        Partition.subset ValueNone p true inputs.expressionParams.forwards

    let private sortKeyGt expressionAttrValue (inputs: QueryParams) =
        let p = getExpressionAttrValue expressionAttrValue inputs.expressionParams.expressionAttrValues |> ValueSome
        Partition.subset p ValueNone false inputs.expressionParams.forwards

    let private sortKeyGte expressionAttrValue (inputs: QueryParams) =
        let p = getExpressionAttrValue expressionAttrValue inputs.expressionParams.expressionAttrValues |> ValueSome
        Partition.subset p ValueNone true inputs.expressionParams.forwards

    let private sortKeyBetween low high (inputs: QueryParams) =
        let pLow = getExpressionAttrValue low inputs.expressionParams.expressionAttrValues
        let pHigh = getExpressionAttrValue high inputs.expressionParams.expressionAttrValues
        Partition.subset (ValueSome pLow) (ValueSome pHigh) true inputs.expressionParams.forwards

    let private chooseSortKey = function
        | Eq -> sortKeyEq
        | Lt -> sortKeyLt
        | Lte -> sortKeyLte
        | Gt -> sortKeyGt
        | Gte -> sortKeyGte
        | Plus -> clientError "+ symbol is not supported in queries"
        | Minus -> clientError "- symbol is not supported in queries"
        | Acc -> clientError ". or [] symbol is not supported in queries"
        | Neq -> clientError "<> symbol is not supported in queries"
        | In -> clientError "IN operation is not supported in queries"
        | And -> clientError "AND operation is only supported when querying a single partition key value AND a single sort key value, or as part of a BETWEEN operation"
        | Or -> clientError "OR operation is not supported in queries"
        | WhiteSpace -> clientError "Invalid query expression"

    let private begins_with keyName expressionAttrValueName =
        let itemValue =
            PartitionBlock.peek
            >> Item.attributes
            >> (MapUtils.tryFind keyName
                >> ValueOption.defaultWith (fun _ -> clientError $"Missing expected sort key {keyName}"))

        let getSubstring =
            MapUtils.tryFind expressionAttrValueName
            >> ValueOption.defaultWith (fun _ -> clientError $"Expression attribute value {expressionAttrValueName} not defined")

        let startsWith = function
            | struct (String x, String y) -> x.StartsWith(y)
            | struct (Binary x, Binary y) when x.Length < y.Length -> false
            | struct (Binary x, Binary y) -> Comparison.compareArrays 0 (y.Length - 1) x y
            | x, y -> clientError $"Cannot use begins_with function on a {AttributeValue.getType x} and {AttributeValue.getType y}"

        fun (query: QueryParams) (partition: Partition) ->

            let substringAttr = getSubstring query.expressionParams.expressionAttrValues

            Partition.subset
            <| ValueSome substringAttr
            <| ValueNone
            <| true
            <| true
            <| partition
            |> Seq.takeWhile (fun x ->
                let str = itemValue x
                startsWith struct (str, substringAttr))

    let rec private findKeyExpressions (key: KeyConfig) (ast: AstNode) acc =
        match ast with

        | AstNode.BinaryOperator (op, (l, AstNode.Accessor (AccessorType.ExpressionAttrName alias))) ->
            MapUtils.tryFind alias
            >> ValueOption.defaultWith (fun _ -> clientError $"Could not find expression attribute name {alias}")
            >> AccessorType.Attribute >> AstNode.Accessor >> tpl l >> tpl op >> AstNode.BinaryOperator
            >> findKeyExpressions key
            |> setAliasedKey acc

        | AstNode.BinaryOperator (op, (AstNode.Accessor (AccessorType.ExpressionAttrName alias), r)) ->
            MapUtils.tryFind alias
            >> ValueOption.defaultWith (fun _ -> clientError $"Could not find expression attribute name {alias}")
            >> AccessorType.Attribute >> AstNode.Accessor >> flip tpl r >> tpl op >> AstNode.BinaryOperator
            >> findKeyExpressions key
            |> setAliasedKey acc

        | AstNode.Between (AstNode.Accessor (AccessorType.ExpressionAttrName alias), lowHigh) ->
            MapUtils.tryFind alias
            >> ValueOption.defaultWith (fun _ -> clientError $"Could not find expression attribute name {alias}")
            >> AccessorType.Attribute >> AstNode.Accessor >> flip tpl lowHigh >> AstNode.Between
            >> findKeyExpressions key
            |> setAliasedKey acc

        | AstNode.Call (method, ValueSome (BinaryOperator (op, (AstNode.Accessor (AccessorType.ExpressionAttrName alias), rArg)))) ->
            MapUtils.tryFind alias
            >> ValueOption.defaultWith (fun _ -> clientError $"Could not find expression attribute name {alias}")
            >> AccessorType.Attribute >> AstNode.Accessor >> flip tpl rArg >> tpl op >> BinaryOperator >> ValueSome >> tpl method >> AstNode.Call
            >> findKeyExpressions key
            |> setAliasedKey acc

        | AstNode.EmptyParenthesis -> clientError "Unexpected token \"()\""

        | AstNode.BinaryOperator (Single And, (l, r)) ->
            findKeyExpressions key l acc
            |> findKeyExpressions key r

        | AstNode.BinaryOperator (Single Eq, (AstNode.ExpressionAttrValue pl, AstNode.Accessor (AccessorType.Attribute attr)))
        | AstNode.BinaryOperator (Single Eq, (AstNode.Accessor (AccessorType.Attribute attr), AstNode.ExpressionAttrValue pl)) when attr = KeyConfig.partitionKeyName key ->
            let pk (inputs: QueryParams) index =
                MapUtils.tryFind pl inputs.expressionParams.expressionAttrValues
                |> ValueOption.defaultWith (fun _ -> clientError $"Expression attribute value {pl} not defined")
                |> flip (Index.getPartition inputs.logger) index

            setPartitionKey acc pk

        | AstNode.BinaryOperator (Single op, (AstNode.ExpressionAttrValue pl, AstNode.Accessor (AccessorType.Attribute attr)))
        | AstNode.BinaryOperator (Single op, (AstNode.Accessor (AccessorType.Attribute attr), AstNode.ExpressionAttrValue pl)) when isSortKey key attr ->
            chooseSortKey op pl
            |> setSortKey acc

        | AstNode.Between (AstNode.Accessor (AccessorType.Attribute attr), (AstNode.ExpressionAttrValue low, AstNode.ExpressionAttrValue high)) when isSortKey key attr ->
            sortKeyBetween low high
            |> setSortKey acc

        | AstNode.Call ("begins_with", ValueSome (BinaryOperator (Multi Comma, (AstNode.Accessor (AccessorType.Attribute attr), AstNode.ExpressionAttrValue pl)))) when isSortKey key attr ->

            let sortKey =
                KeyConfig.sortKeyName key
                |> ValueOption.defaultWith (fun _ -> clientError $"Invalid key configuration")    // should not happen

            begins_with sortKey pl |> setSortKey acc

        | AstNode.BinaryOperator (Single Eq, (AstNode.ExpressionAttrValue _, AstNode.Accessor (AccessorType.Attribute attr)))
        | AstNode.BinaryOperator (Single Eq, (AstNode.Accessor (AccessorType.Attribute attr), AstNode.ExpressionAttrValue _))
        | AstNode.Between (AstNode.Accessor (AccessorType.Attribute attr), (AstNode.ExpressionAttrValue _, AstNode.ExpressionAttrValue _))
        | AstNode.Call ("begins_with", ValueSome (BinaryOperator (Multi Comma, (AstNode.Accessor (AccessorType.Attribute attr), AstNode.ExpressionAttrValue _)))) ->
            clientError $"Invalid query {ast}. Found non key attribute {attr}"

        | AstNode.Synthetic (AccessorPath _)
        | AstNode.Synthetic (CsvList _)
        | AstNode.Synthetic (IndividualDeleteUpdate _)
        | AstNode.Synthetic (IndividualAddUpdate _)
        | AstNode.Synthetic (IndividualRemoveUpdate _)
        | AstNode.Synthetic (IndividualSetUpdate _)
        | AstNode.Synthetic (Projections _)
        | AstNode.Between _
        | AstNode.Call _
        | AstNode.ExpressionAttrValue _
        | AstNode.UnaryOperator _
        | AstNode.Update _
        | AstNode.Updates _
        | AstNode.Accessor _
        | AstNode.BinaryOperator _ -> clientError $"Invalid query {ast}"

    let private defaultSortFilter (inputs: QueryParams) =
        let sk = inputs.expressionParams.lastEvaluatedKey ?>>= sndT
        Partition.scan sk inputs.expressionParams.forwards

    let compile (key: KeyConfig) (ast: AstNode) =

        let queryKeys = findKeyExpressions key ast KeyValues.empty

        fun (inputs: QueryParams) (index: Index) ->
            let queryKeys =
                queryKeys.exprAttrNameKeys
                |> List.fold (fun s f -> f inputs.expressionParams.expressionAttrNames s) queryKeys

            let partition =
                let applyInputs =
                    apply index
                    >> apply inputs.expressionParams.expressionAttrValues
                    ??|> Seq.singleton
                    >> ValueOption.defaultValue Seq.empty

                queryKeys.partitionKey
                |> ValueOption.defaultWith (fun _ -> clientError "Query does not contain partition key condition")
                <| inputs
                <| index
                |> function
                    | ValueNone & x-> Logger.logFn0 "Partition not found" inputs.logger x
                    | ValueSome _ & x -> Logger.logFn1 "Using partition %A" inputs.logger x

            queryKeys.sortKey
            |> ValueOption.defaultValue defaultSortFilter
            <| inputs
            |> ValueSome
            <|? partition
            |> ValueOption.defaultValue Seq.empty