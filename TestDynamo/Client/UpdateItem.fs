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
      valueLabel: string
      value: DynamoAttributeValue }

let private attributeUpdatesWarning =
    $"AttributeUpdates is deprecated. You can supress this warning with the {nameof Settings.SupressErrorsAndWarnings}.{nameof Settings.SupressErrorsAndWarnings.DeprecatedFeatures} setting"

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

            if not Settings.SupressErrorsAndWarnings.DeprecatedFeatures
            then logger.LogWarning attributeUpdatesWarning

            x.clauses
            |> Seq.fold (fun struct (names, values) x ->
                struct (Map.add x.name x.actualAttributeName names, Map.add x.valueLabel (attributeFromDynamodb "$" x.value) values)) struct (Map.empty, Map.empty)
            |> tpl x.expression
            |> ValueSome

let private attributeUpdate expr struct (name, value) (update: KeyValuePair<string, AttributeValueUpdate>) =

    { expression = sprintf expr name value
      actualAttributeName = update.Key
      name = name
      valueLabel = value
      value = update.Value.Value }

let private putAttributeUpdate = attributeUpdate "%s = %s"
let private addAttributeUpdate = attributeUpdate "%s %s"
let private deleteAttributeUpdate = attributeUpdate "%s %s" 

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
    >> Collection.zip (NameValueEnumerator.infiniteNames())
    >> Seq.map (fun struct (nameValue, x) ->
        match x with
        | "PUT", x -> Seq.map (putAttributeUpdate nameValue) x |> combineUpdates "SET"
        | "ADD", x -> Seq.map (addAttributeUpdate nameValue) x |> combineUpdates "ADD"
        | "DELETE", x -> Seq.map (deleteAttributeUpdate nameValue) x |> combineUpdates "DELETE"
        | x, _ -> clientError $"Unknown attribute update value \"{x}\"")
    >> AttributeUpdateExpression.concat

let private buildFromAttributeUpdates (req: UpdateItemRequest) =
    req.AttributeUpdates
    |> buildFromAttributeUpdates'
    |> flip1To3 AttributeUpdateExpression.asOutput req.UpdateExpression

let inputs1 logger (req: UpdateItemRequest) =
    // ReturnValuesOnConditionCheckFailure https://aws.amazon.com/blogs/database/handle-conditional-write-errors-in-high-concurrency-scenarios-with-amazon-dynamodb/

    if dictionaryHasValue req.Expected then notSupported "Legacy Expected parameter is not supported"
    if req.ConditionalOperator <> null then notSupported "Legacy ConditionalOperator parameter is not supported"

    let struct (updateExpression, struct (names, values)) =
        buildFromAttributeUpdates req logger
        ?|> mapFst ValueSome
        ?|? struct (CSharp.emptyStringToNull req.UpdateExpression |> CSharp.toOption, struct (Map.empty, Map.empty))
        
    { key = ItemMapper.itemFromDynamodb "$" req.Key
      updateExpression = updateExpression
      conditionExpression =
          { conditionExpression = req.ConditionExpression |> filterExpression
            tableName = req.TableName |> CSharp.mandatory "TableName is mandatory"
            returnValues = mapReturnValues req.ReturnValues
            expressionAttrNames =
                req.ExpressionAttributeNames
                |> expressionAttrNames
                |> MapUtils.concat2 names
                // should not be clashes, keys are guids
                |> Maybe.expectSome 
            expressionAttrValues =
                req.ExpressionAttributeValues
                |> expressionAttrValues
                |> MapUtils.concat2 values
                // should not be clashes, keys are guids
                |> Maybe.expectSome } } : UpdateItemArgs

let inputs2 logger struct (
    tableName: string,
    key: Dictionary<string, DynamoAttributeValue>,
    updates: Dictionary<string, AttributeValueUpdate>,
    returnValue: ReturnValue) =

    UpdateItemRequest (tableName, key, updates, returnValue) |> inputs1 logger

let inputs3 logger struct (
    tableName: string,
    key: Dictionary<string, DynamoAttributeValue>,
    updates: Dictionary<string, AttributeValueUpdate>) =

    UpdateItemRequest (tableName, key, updates) |> inputs1 logger

let inline private newDict () = Dictionary<_, _>()
let output databaseId items =

    let output = Shared.amazonWebServiceResponse<UpdateItemResponse>()
    output.Attributes <-
        items
        ?|> itemToDynamoDb
        |> ValueOption.defaultWith newDict
    output