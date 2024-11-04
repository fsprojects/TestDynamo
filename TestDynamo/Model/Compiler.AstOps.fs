/// <summary>
/// Individual operations compiled from specific AST nodes
/// </summary>
[<RequireQualifiedAccess>]
module TestDynamo.Model.Compiler.AstOps

open TestDynamo.Data.Monads
open TestDynamo.Data.BasicStructures
open TestDynamo.Model
open TestDynamo.Model.Compiler
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Model.Compiler.Lexer

type private AstCompiler = AstNode -> CompilerOutput


let private noValidator = Validator.unresolve >> Ok

module Value =
    let unexpectedListIndex i: CompilerOutput = NonEmptyList.singleton $"Unexpected token \"[{i}]\"" |> Error

    let expressionAttributeValueExpression settings name: CompilerOutput =
        GetOps.expressionAttrValue name
        |> Writer.retn
        |> flip tpl (Validator.expressionAttributeValue settings name)
        |> Ok

    let private accessorErr1 = NonEmptyList.singleton "Invalid expression, expected form: x.y or x[123]" |> Error
    let private accessorErr2 = NonEmptyList.singleton "Invalid expression, expected form: x.y or x[123]" |> Error
    let private accessorErr3 = NonEmptyList.singleton "Invalid expression, expected form: x.y or x[123]" |> Error
    let accessorExpression: CodeGenSettings -> (AstNode -> CompilerOutput) -> ExpressionPartCompiler =

        let rec expandAccessorPath acc = Collection.foldBack (folder |> curry) (Ok acc)
        and folder = function
            | struct (Error _ & e, _) -> e
            | Ok acc, AstNode.Accessor x -> x::acc |> Ok
            | Ok acc, AstNode.Synthetic (AccessorPath x) -> x@acc |> Ok
            | Ok acc, AstNode.BinaryOperator (BinaryOpToken.Single Acc, (l, r)) -> expandAccessorPath acc [l; r]
            | Ok _, AstNode.Synthetic (CsvList _)
            | Ok _, AstNode.Synthetic (IndividualSetUpdate _)
            | Ok _, AstNode.Synthetic (IndividualRemoveUpdate _)
            | Ok _, AstNode.Synthetic (IndividualAddUpdate _)
            | Ok _, AstNode.Synthetic (IndividualDeleteUpdate _)
            | Ok _, AstNode.Synthetic (Projections _)
            | Ok _, AstNode.BinaryOperator _
            | Ok _, AstNode.Between _
            | Ok _, AstNode.Call _
            | Ok _, AstNode.EmptyParenthesis
            | Ok _, AstNode.UnaryOperator _
            | Ok _, AstNode.Update _
            | Ok _, AstNode.Updates _
            | Ok _, AstNode.ExpressionAttrValue _ -> accessorErr1

        let preProcessor =
            expandAccessorPath []
            >> Result.map (
                AccessorPath >> Synthetic >> List.singleton)

        // The compiler just returns the input.
        // The input will be the Synthetic AccessorPath as compiled by the AccessorPath compiler
        let compiler = flip apply
        ExpressionPartCompiler.build1 "Accessor" preProcessor (Resolved >> Ok) compiler

    let private accessorPathExpression' astPreProcessor = 

        let buildDescription = function
            | AstNode.Accessor x -> UnresolvedPath [x] |> MaybeLazyResult.retn
            | AstNode.Synthetic (AccessorPath x) -> UnresolvedPath x |> MaybeLazyResult.retn
            | AstNode.Synthetic (CsvList _)
            | AstNode.Synthetic (IndividualSetUpdate _)
            | AstNode.Synthetic (IndividualRemoveUpdate _)
            | AstNode.Synthetic (IndividualAddUpdate _)
            | AstNode.Synthetic (IndividualDeleteUpdate _)
            | AstNode.Synthetic (Projections _)
            | AstNode.Between _
            | AstNode.Call _
            | AstNode.BinaryOperator _
            | AstNode.EmptyParenthesis
            | AstNode.UnaryOperator _
            | AstNode.Update _
            | AstNode.Updates _
            | AstNode.ExpressionAttrValue _ -> accessorErr2 |> MaybeLazyResult.fromResult

        let rec compileFromAccessorTypes' = function
            | struct (x, []) -> x
            | root, AccessorType.RawAttribute attr::tail
            | root, AccessorType.Attribute attr::tail ->
                GetOps.attribute struct (root, attr)
                |> ValueSome
                |> flip tpl tail
                |> compileFromAccessorTypes'
            | root, AccessorType.ListIndex i::tail ->
                GetOps.listIndex struct (root, (int i))
                |> ValueSome
                |> flip tpl tail
                |> compileFromAccessorTypes'
            | root, AccessorType.ExpressionAttrName attr::tail ->
                GetOps.expressionAttrName struct (root, attr)
                |> ValueSome
                |> flip tpl tail
                |> compileFromAccessorTypes'

        let compileFromAccessorTypes =
            curry compileFromAccessorTypes' ValueNone
            >> ValueOption.defaultValue GetOps.rootItem

        let buildReader' = compileFromAccessorTypes >> Writer.retn

        let buildWriter' settings =
            tplDouble
            >> mapFst (MutateOps.Accessor.mutation settings)
            >> mapSnd compileFromAccessorTypes
            >> uncurry Writer.create

        let doNothing = Writer.retn (asLazy ValueNone) |> asLazy
        let buildReaderWriter =
            function
            | AstNode.Synthetic (AccessorPath p) -> flip buildWriter' p
            | AstNode.Accessor p -> flip buildWriter' [p]
            | AstNode.Synthetic (CsvList _)
            | AstNode.Synthetic (IndividualSetUpdate _)
            | AstNode.Synthetic (IndividualRemoveUpdate _)
            | AstNode.Synthetic (IndividualDeleteUpdate _)
            | AstNode.Synthetic (IndividualAddUpdate _)
            | AstNode.Synthetic (Projections _)
            | AstNode.Between _
            | AstNode.Call _
            | AstNode.BinaryOperator _
            | AstNode.EmptyParenthesis
            | AstNode.UnaryOperator _
            | AstNode.Update _
            | AstNode.Updates _
            | AstNode.ExpressionAttrValue _ -> doNothing
            |> flip

        fun settings ->
            ExpressionPartCompiler.buildRoot "AccessorPath" astPreProcessor buildDescription (buildReaderWriter settings) settings

    let private toAccessorPath = function
        | [Synthetic (AccessorPath [_])] & x -> x |> Ok
        | [AstNode.Accessor x] -> [Synthetic (AccessorPath [x])] |> Ok
        | _ -> accessorErr3

    let accessorPathExpression = accessorPathExpression' Ok
    let rootAttributeExpression = accessorPathExpression' toAccessorPath
    let rootExpressionAttributeNameExpression = rootAttributeExpression

let private lAndRCannotBeSamePathOrExpAttrValue name =
    let err = NonEmptyList.singleton $"LHS and RHS of operation {name} cannot be the same value" |> Error
    let ok = Ok ()

    function
    | struct (ResolvedPath x, ResolvedPath y) when x = y -> err
    | struct (ResolvedExpressionAttrValue struct (x, _), ResolvedExpressionAttrValue struct (y, _)) when x = y -> err
    | _ -> ok

module BinaryOps =
    let private statelessBinaryOp lAndRCanBeEqual opDescription name =
        let ok = Ok () |> asLazy
        let invalidAccessor = NonEmptyList.singleton $"Invalid expression, expected form: x {name} y" |> Error
        let validator =
            if lAndRCanBeEqual
            then ok
            else lAndRCannotBeSamePathOrExpAttrValue name
            >> Result.map (asLazy opDescription)

        ExpressionPartCompiler.build2
            name
            Ok
            noValidator
            noValidator
            validator

    let private arithmeticOp valid invalid op name settings =

        let validator =
            if settings.parserSettings.updateFunctionVerbs
            then valid
            else invalid

        let err = $"Both operands of a {name} operation must be numbers"
        let arithmetic args itemData =
            match args |> mapFst (apply itemData) |> mapSnd (apply itemData) with
            | struct (ValueSome (AttributeValue.Number _), ValueSome (AttributeValue.Number _)) -> op args itemData
            | _ -> clientError err

        ExpressionPartCompiler.build2
            name
            Ok
            noValidator
            noValidator
            validator
            arithmetic
            settings

    let private statelessBooleanOp name = statelessBinaryOp true (BooleanLogic name |> Resolved) name
    let private statelessBinaryComparer name = statelessBinaryOp false (Comparer name |> Resolved) name

    let andExpression = statelessBooleanOp "and" GetOps.``and``
    let orExpression = statelessBooleanOp "or" GetOps.``or``
    let eqExpression settings =
        if settings.parserSettings.updateFunctionVerbs
        then statelessBinaryComparer "=" GetOps.eq settings
        else statelessBinaryComparer "=" GetOps.eq settings
    let neqExpression = statelessBinaryComparer "<>" GetOps.neq
    let ltExpression = statelessBinaryComparer "<" GetOps.lt
    let lteExpression = statelessBinaryComparer "<=" GetOps.lte
    let gtExpression = statelessBinaryComparer ">" GetOps.gt
    let gteExpression = statelessBinaryComparer ">=" GetOps.gte

    let notExpression =
        let name = "NOT"
        ExpressionPartCompiler.build1
            name
            Ok
            (BooleanLogic name |> Resolved |> Ok |> asLazy) 
            GetOps.not

    let private validAddition = ResolvedDescription.Arithmetic "+" |> UnresolvedDescription.Resolved |> Ok |> asLazy
    let private invalidAddition = NonEmptyList.singleton "+ operation is not supported" |> Error |> asLazy
    let addExpression = arithmeticOp validAddition invalidAddition GetOps.Arithmetic.add "+"

    let private validSubtraction = ResolvedDescription.Arithmetic "-" |> UnresolvedDescription.Resolved |> Ok |> asLazy
    let private invalidSubtraction = NonEmptyList.singleton "- operation is not supported" |> Error |> asLazy
    let subtractExpression = arithmeticOp validSubtraction invalidSubtraction GetOps.Arithmetic.subtract "-"

module DynamoDbQueryLang =
    let csvListExpression =
        ExpressionPartCompiler.buildStar
            "CsvToList"
            Ok
            noValidator
            (ResolvedCsv >> Resolved >> Ok)
            GetOps.toList

    let inExpression: CodeGenSettings -> AstCompiler -> ExpressionPartCompiler =
        // unwrap the csv with a pre compiler
        let argPreCompiler = function
            | [x; y] -> [x; AstNode.expand y |> CsvList |> AstNode.Synthetic] |> Ok
            | x -> Ok x

        let validated = Comparer "IN" |> Resolved |> Ok
        let invalid1 = NonEmptyList.singleton "Invalid expression, expected form: x IN (y, z)" |> Error
        let invalid2 = NonEmptyList.singleton "IN expression supports max 100 inputs" |> Error
        let validator = function
            | struct (ResolvedPath _, ResolvedCsv xs) when List.length xs > 100 -> invalid2
            | ResolvedPath _, ResolvedCsv _
            | ResolvedExpressionAttrValue _, ResolvedCsv _
            | BooleanLogic _, ResolvedCsv _
            | Comparer _, ResolvedCsv _
            | FunctionCall _, ResolvedCsv _ -> validated
            | _, Projection
            | _, Arithmetic _
            | _, UpdatePlaceholder _
            | _, ResolvedExpressionAttrValue _
            | _, ResolvedPath _
            | _, BooleanLogic _
            | _, Comparer _
            | _, FunctionCall _
            | Projection, _
            | Arithmetic _, _
            | ResolvedCsv _, _
            | UpdatePlaceholder _, _ -> invalid1

        ExpressionPartCompiler.build2
            "IN"
            argPreCompiler
            noValidator
            noValidator
            validator
            GetOps.listContains

    let betweenExpression =
        let name = "BETWEEN"
        ExpressionPartCompiler.build3
            name
            Ok
            noValidator
            noValidator
            noValidator
            (Comparer name |> Resolved |> Ok |> asLazy)
            GetOps.between

module Call =

    let invalidFunction name: CompilerOutput = NonEmptyList.singleton $"Unexpected call \"{name}\"" |> Error

    let size =
        let name = "size"
        let ok = FunctionCall name |> Resolved |> Ok
        let err = NonEmptyList.singleton $"Invalid expression, expected form: {name}(x)" |> Error

        let validator = function
            | ResolvedPath _
            | ResolvedExpressionAttrValue _
            | FunctionCall _ -> ok
            | ResolvedCsv _
            | Comparer _
            | Arithmetic _
            | UpdatePlaceholder _
            | Projection
            | BooleanLogic _ -> err

        ExpressionPartCompiler.build1
            name
            Ok
            validator
            GetOps.Functions.size

    let contains =
        let name = "contains"
        let ok = FunctionCall name |> Resolved |> Ok |> asLazy
        let err = NonEmptyList.singleton $"{name} expression args must be attributes or expression attribute values" |> Error

        let argValidator = function
            | ResolvedPath _ & x
            | ResolvedExpressionAttrValue _ & x -> x |> Resolved |> Ok
            | Comparer _
            | Arithmetic _
            | ResolvedCsv _
            | BooleanLogic _
            | FunctionCall _
            | Projection
            | UpdatePlaceholder _ -> err

        let validate =
            struct (lAndRCannotBeSamePathOrExpAttrValue name, ok)
            |> ReaderResult.traverseTpl
            |> ReaderResult.map sndT

        ExpressionPartCompiler.build2
            name
            Ok
            argValidator
            argValidator
            validate
            GetOps.Functions.contains

    let begins_with =
        let name = "begins_with"
        let err = NonEmptyList.singleton $"Invalid right operand for {name}" |> Error
        let ok = FunctionCall name |> Resolved

        let validate =
            lAndRCannotBeSamePathOrExpAttrValue name
            |> ReaderResult.map (asLazy ok)

        let validateR (x: ResolvedDescription): Result<UnresolvedDescription, NonEmptyList<string>> =
            match x with
            | ResolvedPath _ & x
            | ResolvedExpressionAttrValue (_, String _) & x
            | ResolvedExpressionAttrValue (_, Binary _) & x -> Resolved x |> Ok
            | Comparer _
            | Arithmetic _
            | ResolvedCsv _
            | BooleanLogic _
            | FunctionCall _
            | UpdatePlaceholder _
            | Projection
            | ResolvedExpressionAttrValue _ -> err

        ExpressionPartCompiler.build2
            name
            Ok
            noValidator
            validateR 
            validate
            GetOps.Functions.begins_with

    let if_not_exists =
        let name = "if_not_exists"
        let err = NonEmptyList.singleton $"Invalid left operand for {name}" |> Error
        let ok = FunctionCall name |> Resolved |> Ok |> asLazy

        let validateL (x: ResolvedDescription) =
            match x with
            | ResolvedPath _ & x -> Resolved x |> Ok
            | Comparer _
            | Arithmetic _
            | ResolvedCsv _
            | BooleanLogic _
            | FunctionCall _
            | UpdatePlaceholder _
            | Projection
            | ResolvedExpressionAttrValue _ -> err

        let validateR (x: ResolvedDescription) =
            match x with
            | Arithmetic _ & x
            | ResolvedPath _ & x
            | ResolvedExpressionAttrValue _ & x -> Resolved x |> Ok
            | Comparer _
            | ResolvedCsv _
            | BooleanLogic _
            | FunctionCall _
            | Projection
            | UpdatePlaceholder _ -> err

        ExpressionPartCompiler.build2
            name
            Ok
            validateL
            validateR 
            ok
            GetOps.Functions.if_not_exists

    let list_append =
        let name = "list_append"
        let err = NonEmptyList.singleton $"Invalid left operand for {name}" |> Error
        let ok = FunctionCall name |> Resolved |> Ok |> asLazy

        let validateArg (x: ResolvedDescription) =
            match x with
            | ResolvedPath _ & x -> Resolved x |> Ok
            | ResolvedExpressionAttrValue _ & x -> Resolved x |> Ok
            | Comparer _
            | Arithmetic _
            | ResolvedCsv _
            | BooleanLogic _
            | FunctionCall _
            | Projection
            | UpdatePlaceholder _ -> err

        let argValueValidator lr itemData =
            match lr |> mapFst (apply itemData) |> mapSnd (apply itemData) with
            | ValueNone, ValueNone
            | ValueSome (AttributeList _), ValueNone
            | ValueNone, ValueSome (AttributeList _)
            | ValueSome (AttributeList _), ValueSome (AttributeList _) -> GetOps.Functions.list_append lr itemData
            | _ -> clientError "Arguments to list_append function must be lists"

        ExpressionPartCompiler.build2
            name
            Ok
            validateArg
            validateArg
            ok
            argValueValidator

    let attribute_type =
        let name = "attribute_type"
        let ok = FunctionCall name |> Resolved |> Ok |> asLazy
        let err = NonEmptyList.singleton $"Right operand for {name} must be an expression attribute value" |> Error

        let resolveRhs = function
            | ResolvedExpressionAttrValue struct (_, String "S") & x
            | ResolvedExpressionAttrValue struct (_, String "N") & x
            | ResolvedExpressionAttrValue struct (_, String "B") & x
            | ResolvedExpressionAttrValue struct (_, String "BOOL") & x
            | ResolvedExpressionAttrValue struct (_, String "L") & x
            | ResolvedExpressionAttrValue struct (_, String "M") & x
            | ResolvedExpressionAttrValue struct (_, String "SS") & x
            | ResolvedExpressionAttrValue struct (_, String "NS") & x
            | ResolvedExpressionAttrValue struct (_, String "BS") & x
            | ResolvedExpressionAttrValue struct (_, String "NULL") & x -> noValidator x
            | ResolvedExpressionAttrValue struct (_, String x) -> NonEmptyList.singleton $"Invalid attribute type parameter \"{x}\"" |> Error
            | ResolvedExpressionAttrValue struct (_, x) -> NonEmptyList.singleton $"Invalid attribute type parameter \"{x}\"" |> Error
            | Projection
            | Comparer _
            | Arithmetic _
            | ResolvedCsv _
            | FunctionCall _
            | BooleanLogic _
            | ResolvedPath _
            | UpdatePlaceholder _ -> err

        ExpressionPartCompiler.build2
            name
            Ok
            noValidator
            resolveRhs
            ok
            GetOps.Functions.attribute_type

    let private attr_exts name =
        let ok = FunctionCall name |> Resolved |> Ok
        let err = NonEmptyList.singleton $"Operand for {name} must be an attribute" |> Error

        function
            | ResolvedPath _ -> ok
            | _ -> err
        |> ExpressionPartCompiler.build1 name Ok

    let attribute_exists = attr_exts "attribute_exists" GetOps.Functions.attribute_exists
    let attribute_not_exists = attr_exts "attribute_not_exists" GetOps.Functions.attribute_not_exists

module Updates =

    module private AggregatedUpdate =

        let private describe = UpdatePlaceholder >> Resolved >> Ok >> asLazy

        let private combine =
            let getNothing = ValueNone |> asLazy |> asLazy

            Writer.traverse
            >> Writer.map getNothing

        let private preProcessor asIndividual (nodes: AstNode list) =
            Seq.collect AstNode.expand nodes
            |> Seq.map (asIndividual >> Result.map AstNode.Synthetic)
            |> Result.traverse

        let compiler verb asSynthetic =
            ExpressionPartCompiler.buildStarWithWriter
                (verb.ToString())
                (preProcessor asSynthetic)
                noValidator
                (describe verb)
                combine

    module private All =

        let private describe = UpdatePlaceholder ValueNone |> Resolved |> Ok |> asLazy

        let private combine =
            let getNothing = ValueNone |> asLazy |> asLazy

            Writer.traverse
            >> Writer.map getNothing

        let compiler =
            ExpressionPartCompiler.buildStarWithWriter
                "UPDATE"
                Ok
                noValidator
                describe
                combine

    module private Set =

        let private invalid = "Expected SET expression in the form x = y" |> NonEmptyList.singleton
        let private invalid1 = invalid |> Error
        let private invalid2 = invalid |> Error

        let private describe = UpdatePlaceholder (ValueSome Set) |> Resolved |> Ok |> asLazy

        let private returnNothing1 = flip Writer.create (asLazy ValueNone)
        let private returnNothing2 = Writer.retn (asLazy ValueNone)

        let private asSynthetic = function
            | AstNode.BinaryOperator (Single Eq, (l, r) & lr) -> IndividualSetUpdate lr |> Ok
            | _ -> invalid2

        let compiler =
            AggregatedUpdate.compiler (ValueSome Add) asSynthetic

        let private validateL = function
            | ResolvedPath _ & x -> Resolved x |> Ok
            | _ -> invalid1

        let private execute struct (l: ExpressionReaderWriter, r: ExpressionReaderWriter): ExpressionReaderWriter =
            match struct (Writer.execute l, Writer.execute r) with
            | ([mutation], _), (_, getter) ->
                { mutation with valueGetter = getter }
                |> returnNothing1
            | _ ->
                // validation should prevent this case
                assert false
                returnNothing2

        let clauseCompiler =
            ExpressionPartCompiler.build2WithWriter
                "SET CLAUSE"
                Ok
                validateL
                noValidator
                describe
                execute

    module private Remove =

        let private invalid = "Expected REMOVE expression in the form REMOVE x, x.y, z[1]" |> NonEmptyList.singleton
        let private invalid1 = invalid |> Error

        let private returnNothing1 = asLazy ValueNone
        let private returnNothing2 = flip Writer.create (asLazy ValueNone)
        let private returnNothing3 = Writer.retn (asLazy ValueNone)
        let private asSynthetic = IndividualRemoveUpdate >> Ok

        let compiler =
            AggregatedUpdate.compiler (ValueSome Remove) asSynthetic

        let private validate =
            let ok = UpdatePlaceholder (ValueSome Remove) |> Resolved |> Ok
            function
            | ResolvedPath _ & x -> ok
            | _ -> invalid1

        let private execute (arg: ExpressionReaderWriter): ExpressionReaderWriter =
            match Writer.execute arg with
            | [mutation], _ ->
                { mutation with valueGetter = returnNothing1 }
                |> returnNothing2
            | _ ->
                // validation should prevent this case
                assert false
                returnNothing3

        let clauseCompiler =
            ExpressionPartCompiler.build1WithWriter
                "REMOVE CLAUSE"
                Ok
                validate
                execute

    module private Add =

        let private invalid = "Expected ADD expression in the form ADD x y" |> NonEmptyList.singleton
        let private invalid1 = invalid |> Error
        let private invalid2 = invalid |> Error

        let private valid = UpdatePlaceholder (ValueSome Add) |> Resolved |> Ok

        let private describe = (ValueSome Add) |> UpdatePlaceholder |> Resolved |> Ok |> asLazy

        let private returnNothing1 = flip Writer.create (asLazy ValueNone)
        let private returnNothing2 = Writer.retn (asLazy ValueNone)

        let private asSynthetic = function
            | AstNode.BinaryOperator (Single WhiteSpace, (l, r) & lr) -> IndividualAddUpdate lr |> Ok
            | _ -> invalid1

        let compiler =
            AggregatedUpdate.compiler (ValueSome Add) asSynthetic

        let private isAttribute =
            let notAnAttribute = NonEmptyList.singleton "LHS of ADD update expression must be an attribute" |> Error
            let notARootAttribute = NonEmptyList.singleton "ADD update expression can only be used on top-level attributes, not nested attributes" |> Error
            function
            | ResolvedPath path & x when ValidatedPath.length path = 1 -> Resolved x |> Ok
            | ResolvedPath _ & x -> notARootAttribute
            | _ -> invalid2

        let private validateRhs = function
            | ResolvedPath _
            | ResolvedExpressionAttrValue _
            | Arithmetic _
            | FunctionCall _ -> valid
            | ResolvedCsv _
            | BooleanLogic _
            | Comparer _
            | Projection
            | UpdatePlaceholder _ -> invalid2

        let return0 = AttributeValue.Number 0M |> ValueSome |> asLazy

        let private mutate (struct (_, arg1) & args) itemData =
            match args |> mapFst (apply itemData) |> mapSnd (apply itemData) with
            // special case for add a numer to nothing
            | ValueNone, ValueSome (AttributeValue.Number _) -> GetOps.Arithmetic.add struct (return0, arg1) itemData
            | ValueNone, _
            | _, ValueNone -> clientError "Invalid ADD update expression. Attribute has no value"
            | ValueSome (AttributeValue.Number _), ValueSome (AttributeValue.Number _) -> GetOps.Arithmetic.add args itemData
            | ValueSome (AttributeValue.HashSet _), ValueSome (AttributeValue.HashSet _) ->
                GetOps.HashSets.union args itemData
                ?|> ValueSome
                |> ValueOption.defaultWith (fun _ -> clientError "Invalid ADD update expression. Sets contain two different types")
            | _ -> clientError "Invalid ADD update expression. ADD is for sets and numbers only"

        let private execute args: ExpressionReaderWriter =

            match args |> mapFst Writer.execute |> mapSnd Writer.execute with
            | ([mutation], getter1), (_, getter2) ->
                { mutation with valueGetter = mutate struct (getter1, getter2) }
                |> returnNothing1
            | _ ->
                // validation should prevent this case
                assert false
                returnNothing2

        let clauseCompiler =
            ExpressionPartCompiler.build2WithWriter
                "ADD CLAUSE"
                Ok
                isAttribute
                validateRhs
                describe
                execute

    module private Delete =

        let private invalid = "Expected DELETE expression in the form DELETE x y" |> NonEmptyList.singleton
        let private invalid1 = invalid |> Error
        let private invalid2 = invalid |> Error

        let private valid = UpdatePlaceholder (ValueSome Delete) |> Resolved |> Ok

        let private describe = (ValueSome Delete) |> UpdatePlaceholder |> Resolved |> Ok |> asLazy

        let private returnNothing1 = flip Writer.create (asLazy ValueNone)
        let private returnNothing2 = Writer.retn (asLazy ValueNone)

        let private asSynthetic = function
            | AstNode.BinaryOperator (Single WhiteSpace, (l, r) & lr) -> IndividualDeleteUpdate lr |> Ok
            | _ -> invalid1

        let compiler =
            AggregatedUpdate.compiler (ValueSome Delete) asSynthetic

        let private isAttribute =
            let notAnAttribute = NonEmptyList.singleton "LHS of DELETE update expression must be an attribute" |> Error 
            let notARootAttribute = NonEmptyList.singleton "DELETE update expression can only be used on top-level attributes, not nested attributes" |> Error

            function
            | ResolvedPath path & x when ValidatedPath.length path = 1 -> Resolved x |> Ok
            | ResolvedPath _ & x -> notARootAttribute
            | _ -> invalid2

        let private validateRhs = function
            | ResolvedPath _
            | ResolvedExpressionAttrValue _
            | Arithmetic _
            | FunctionCall _ -> valid
            | ResolvedCsv _
            | BooleanLogic _
            | Comparer _
            | Projection
            | UpdatePlaceholder _ -> invalid2

        let private mutate args itemData =
            match args |> mapFst (apply itemData) |> mapSnd (apply itemData) with
            | ValueNone, _
            | _, ValueNone -> clientError "Invalid DELETE update expression. Attribute has no value"
            | ValueSome (AttributeValue.HashSet _), ValueSome (AttributeValue.HashSet _) ->
                GetOps.HashSets.xOr args itemData
                |> ValueOption.defaultWith (fun _ -> clientError "Invalid DELETE update expression. Sets contain two different types")
            | s1, s2 ->
                clientError $"Invalid DELETE update expression. DELETE is for sets only {s1} {s2}"

        let private execute args: ExpressionReaderWriter =

            match args |> mapFst Writer.execute |> mapSnd Writer.execute with
            | ([mutation], getter1), (_, getter2) ->
                { mutation with valueGetter = mutate struct (getter1, getter2) }
                |> returnNothing1
            | _ ->
                // validation should prevent this case
                assert false
                returnNothing2

        let clauseCompiler =
            ExpressionPartCompiler.build2WithWriter
                "DELETE CLAUSE"
                Ok
                isAttribute
                validateRhs
                describe
                execute

    let all = All.compiler
    let set = Set.compiler
    let setClause = Set.clauseCompiler
    let add = Add.compiler
    let addClause = Add.clauseCompiler
    let remove = Remove.compiler
    let removeClause = Remove.clauseCompiler
    let delete = Delete.compiler
    let deleteClause = Delete.clauseCompiler

module Projections =

    let rec private unwrap = function
        | AstNode.Synthetic (Projections x) -> unwrap x
        | x -> x

    let private expand =
        Seq.map unwrap >> Seq.collect AstNode.expand >> List.ofSeq >> Ok

    let private invalid = "Expected projection expression in the form \"w, x.y, z[0]\"" |> NonEmptyList.singleton
    let private invalid1 = invalid |> Error

    let private isAttribute =
        let notAnAttribute = NonEmptyList.singleton "LHS of DELETE update expression must be an attribute" |> Error 

        function
        | ResolvedPath _ & x -> Resolved x |> Ok
        | _ -> invalid1

    let private describe = ResolvedDescription.Projection |> Resolved |> Ok |> asLazy

    let private returnNothing = ValueNone |> asLazy |> asLazy

    let projectionExpression =
        ExpressionPartCompiler.buildStar
            "PROJECT"
            expand
            isAttribute
            describe
            returnNothing

module Errors =
    let emptyParenthesis: CompilerOutput = NonEmptyList.singleton "Unexpected token \"()\"" |> Error
    let unexpectedCsv: CompilerOutput = NonEmptyList.singleton "Unexpected token \",\"" |> Error
    let invalidSpace: CompilerOutput = NonEmptyList.singleton "Invalid expression" |> Error
