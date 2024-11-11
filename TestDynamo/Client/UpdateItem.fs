module TestDynamo.Client.UpdateItem

open System.Runtime.CompilerServices
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Client
open TestDynamo.Client.Shared
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open TestDynamo.Client.ItemMapper
open TestDynamo.Client.Query
open Microsoft.Extensions.Logging

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue
type MList<'a> = System.Collections.Generic.List<'a>

let mapReturnValues (returnValues: ReturnValue) =
    returnValues
    |> CSharp.toOption
    ?|> (function
        | x when x.Value = ReturnValue.NONE.Value -> Basic None
        | x when x.Value = ReturnValue.ALL_OLD.Value -> Basic AllOld
        | x when x.Value = ReturnValue.ALL_NEW.Value -> AllNew
        | x when x.Value = ReturnValue.UPDATED_OLD.Value -> UpdatedOld
        | x when x.Value = ReturnValue.UPDATED_NEW.Value -> UpdatedNew
        | x -> clientError $"Invalid ReturnValues {x}")
    |> ValueOption.defaultValue (Basic None)

let private dictionaryHasValue (d: Dictionary<_, _>) = d <> null && d.Count > 0

[<Struct; IsReadOnly>]
type private AttributeUpdateClause =
    { expression: string
      actualAttributeName: string
      name: string
      value: struct (string * DynamoAttributeValue) voption }

[<Struct; IsReadOnly>]
type private AttributeUpdateExpression =
    { expression: string
      clauses: AttributeUpdateClause list }

    with
    static member concat =
        Seq.map (fun (x: AttributeUpdateExpression) -> struct (x.expression, x))
        >> Collection.unzip
        >> fun struct (expr, xs) ->
            { expression = expr |> Str.join " "; clauses = List.collect _.clauses xs }

    static member asOutput (logger: ILogger) (reqUpdateExpression: string) = function
        | x when x.clauses = [] -> ValueNone
        | x ->
            if System.String.IsNullOrEmpty(reqUpdateExpression) |> not
            then clientError "Legacy AttributeUpdates parameter cannot be used with an UpdateExpression"

            x.clauses
            |> Seq.fold (fun struct (names, values) x ->
                struct (
                    Map.add x.name x.actualAttributeName names,
                    x.value
                    ?|> (
                        mapSnd (attributeFromDynamodb "$")
                        >> uncurry (flip3To1 Map.add values))
                    ?|? values)) struct (Map.empty, Map.empty)
            |> tpl x.expression
            |> ValueSome

let private attributeUpdate setValue expr struct (name, value) (update: KeyValuePair<string, AttributeValueUpdate>) =

    { expression = sprintf expr name value
      actualAttributeName = update.Key
      name = name
      value = if setValue then ValueSome struct (value, update.Value.Value) else ValueNone }

let private putAttributeUpdate = attributeUpdate true "%s = %s"
let private addAttributeUpdate = attributeUpdate true "%s %s"
let private deleteAttributeUpdate = flip tpl "" >> attributeUpdate false "%s%s"

let private combineUpdates name (updates: AttributeUpdateClause seq) =
    updates
    |> Seq.fold (fun struct (expressions, clauses) x ->
        struct (x.expression::expressions, x::clauses)) struct ([], [])
    |> mapFst (Str.join "," >> sprintf "%s %s" name)
    |> fun struct (expr, xs) -> { expression = expr; clauses = xs }

let private buildFromAttributeUpdates': Dictionary<string,AttributeValueUpdate> -> AttributeUpdateExpression =

    CSharp.orEmpty
    >> Collection.groupBy (fun x ->
        let v = (CSharp.mandatory "AttributeValueUpdate.Value" x.Value)
        let v = (CSharp.mandatory "AttributeValueUpdate.Value.Action" v.Action)
        let v = (CSharp.mandatory "AttributeValueUpdate.Value.Action.Value" v.Value)
        v.ToUpper())
    >> Seq.collect (function
        | "PUT", x -> Seq.map Choice1Of3 x
        | "ADD", x -> Seq.map Choice2Of3 x
        | "DELETE", x -> Seq.map Choice3Of3 x
        | x, _ -> clientError $"Unknown attribute update value \"{x}\"")
    >> Collection.zip (NameValueEnumerator.infiniteNames "p")
    >> Seq.map (fun struct (nameValue, x) ->
        match x with
        | Choice1Of3 x -> putAttributeUpdate nameValue x |> tpl "SET"
        | Choice2Of3 x -> addAttributeUpdate nameValue x |> tpl "ADD"
        | Choice3Of3 x -> deleteAttributeUpdate (fstT nameValue) x |> tpl "REMOVE")
    >> Collection.groupBy fstT
    >> Collection.mapSnd (Seq.map sndT)
    >> Seq.map (fun struct (k, vs) -> combineUpdates k vs)
    >> AttributeUpdateExpression.concat

let private buildFromAttributeUpdates (req: UpdateItemRequest) =
    req.AttributeUpdates
    |> buildFromAttributeUpdates'
    |> flip1To3 AttributeUpdateExpression.asOutput req.UpdateExpression

let inputs logger (req: UpdateItemRequest) =
    // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

    let struct (filterExpression, struct (addNames, addValues)) =
        buildUpdateConditionExpression req.ConditionalOperator (CSharp.emptyStringToNull req.ConditionExpression |> CSharp.toOption) req.Expected
        ?|> mapFst ValueSome
        ?|? struct (ValueNone, struct (id, id))

    let struct (updateExpression, struct (names, values)) =
        buildFromAttributeUpdates req logger
        ?|> mapFst ValueSome
        ?|? struct (CSharp.emptyStringToNull req.UpdateExpression |> CSharp.toOption, struct (Map.empty, Map.empty))

    { key = ItemMapper.itemFromDynamodb "$" req.Key
      updateExpression = updateExpression
      conditionExpression =
          { conditionExpression = filterExpression
            tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
            returnValues = mapReturnValues req.ReturnValues
            expressionAttrNames =
                req.ExpressionAttributeNames
                |> expressionAttrNames
                |> MapUtils.concat2 names
                |> Maybe.expectSome
                |> addNames
            expressionAttrValues =
                req.ExpressionAttributeValues
                |> expressionAttrValues
                |> MapUtils.concat2 values
                |> Maybe.expectSome
                |> addValues } } : UpdateItemArgs

let inline private newDict () = Dictionary<_, _>()
let output databaseId items =

    let output = Shared.amazonWebServiceResponse<UpdateItemResponse>()
    output.Attributes <-
        items
        ?|> itemToDynamoDb
        |> ValueOption.defaultWith newDict
    output