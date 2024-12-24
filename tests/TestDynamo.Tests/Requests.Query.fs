module Tests.Requests.Queries

open System
open System.Linq
open System.Text.RegularExpressions
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open TestDynamo.Model
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open Tests.Utils

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue

type QueryBuilder =
    { randomizeExprAttrNames: Random voption
      tableName: string voption
      select: Select voption
      returnValues: ReturnValue voption
      indexName: string voption
      limit: int voption
      filterExpression: string voption
      conditionExpression: string voption
      keyConditionExpression: string voption
      projectionExpression: string voption
      attributesToGet: string list voption
      updateKey: Map<string, AttributeValue> voption
      updateExpression: string voption
      attributeUpdates: Map<string, AttributeValueUpdate> voption
      forwards: bool voption
      expressionAttrValues: Map<string, AttributeValue> voption
      expressionAttrNames: Map<string, string> voption }

    with

    static member empty randomizeExprAttrNames =
        { randomizeExprAttrNames = randomizeExprAttrNames
          tableName = ValueNone
          indexName = ValueNone
          updateKey = ValueNone
          attributesToGet = ValueNone
          select = ValueNone
          returnValues = ValueNone
          conditionExpression =  ValueNone
          limit = ValueNone
          attributeUpdates = ValueNone
          filterExpression = ValueNone
          keyConditionExpression = ValueNone
          projectionExpression =  ValueNone
          updateExpression = ValueNone
          forwards = ValueNone
          expressionAttrValues = ValueNone
          expressionAttrNames = ValueNone }

    static member setTableName name x = { x with tableName = ValueSome name }: QueryBuilder

    static member setLimit limit x = { x with limit =  ValueSome limit }: QueryBuilder

    static member setIndexName name x = { x with indexName = ValueSome name }: QueryBuilder

    static member setFilterExpression exp x = { x with filterExpression = ValueSome exp }: QueryBuilder

    static member removeFilterExpression x = { x with filterExpression = ValueNone }: QueryBuilder

    static member getFilterExpression x = x.filterExpression

    static member setConditionExpression exp x = { x with conditionExpression = ValueSome exp }: QueryBuilder

    static member setKeyConditionExpression exp x = { x with keyConditionExpression = ValueSome exp }: QueryBuilder

    static member addExpressionAttributeValue name value x =
        let vs = x.expressionAttrValues ?|? Map.empty |> Map.add name value
        { x with expressionAttrValues =  ValueSome vs }: QueryBuilder

    static member setAttributesToGet attr x = { x with attributesToGet =  ValueSome attr }: QueryBuilder

    static member setUpdateKey key x = { x with updateKey =  ValueSome key }: QueryBuilder

    static member setUpdateExpression exp x = { x with updateExpression =  ValueSome exp }: QueryBuilder

    static member setProjectionExpression exp x = { x with projectionExpression =  ValueSome exp }: QueryBuilder

    static member setForwards forwards x = { x with forwards = ValueSome forwards }: QueryBuilder

    static member setSelect s x =
        { x with select = s |> ValueSome }: QueryBuilder

    static member setReturnValues r x =
        { x with returnValues =  r |> ValueSome }: QueryBuilder

    static member setExpressionAttrValues k v x =
        let map = x.expressionAttrValues |> ValueOption.defaultValue Map.empty
        { x with expressionAttrValues = Map.add k v map |> ValueSome }: QueryBuilder

    static member setExpressionAttrName placeholder replaced x =
        let map = x.expressionAttrNames |> ValueOption.defaultValue Map.empty
        { x with expressionAttrNames = Map.add placeholder replaced map |> ValueSome }: QueryBuilder

    static member private doNotReplace =
        [|
            "begins_with"
            "attribute_exists"
            "attribute_not_exists"
            "attribute_type"
            "contains"
            "size"
            "if_not_exists"
            "list_append"
            "ADD"
            "SET"
            "DELETE"
            "REMOVE"
        |]

    static member replaceSomeTokens (random: Random) str =
        let replacable = Regex.Match((if str = null then "" else str), @"\b(?<!#|:)(?<token>\w+)\b")
        match replacable.Success with
        | false -> struct (str, List.empty)
        | true when replacable.Groups["token"].Captures.Count = 0 -> struct (str, List.empty)
        | true ->
            let captures =
                replacable.Groups["token"].Captures
                |> Seq.filter (fun _ -> random.Next() % 2 = 0)
                |> Seq.filter (_.Value >> flip Array.contains QueryBuilder.doNotReplace >> not)
                |> Seq.filter (_.Value >> TestDynamo.Model.Compiler.ReservedWords.isReservedWord >> not)
                |> Seq.map (fun capture -> struct ($"#randomReplace{IncrementingId.next()}", capture))
                |> List.ofSeq

            let str =
                captures
                |> List.fold (fun (s: string) struct (alias, capture) ->
                    s.Substring(0, capture.Index)
                        + alias
                        + s.Substring(capture.Index + capture.Length)) str

            let replacements = captures |> List.map (mapSnd _.Value)
            struct (str, replacements)

    static member addSomeAliasesToQ (output: QueryRequest) random =
        let dict () =
            match output.ExpressionAttributeNames with
            | null ->
                output.ExpressionAttributeNames <- Dictionary<_, _>()
                output.ExpressionAttributeNames
            | x -> x

        let replace =
            QueryBuilder.replaceSomeTokens random
            >> mapSnd (List.fold (fun _ struct (k, v) -> dict().Add(k, v)) ())
            >> fstT

        output.KeyConditionExpression <- replace output.KeyConditionExpression
        output.FilterExpression <- replace output.FilterExpression
        output.ProjectionExpression <- replace output.ProjectionExpression

    static member addSomeAliasesToU (output: UpdateItemRequest) random =
        let dict () =
            match output.ExpressionAttributeNames with
            | null ->
                output.ExpressionAttributeNames <- Dictionary<_, _>()
                output.ExpressionAttributeNames
            | x -> x

        let replace =
            QueryBuilder.replaceSomeTokens random
            >> mapSnd (List.fold (fun _ struct (k, v) -> dict().Add(k, v)) ())
            >> fstT

        output.UpdateExpression <- replace output.UpdateExpression

    static member queryRequest x =
        let output = QueryRequest()

        match x.conditionExpression with | ValueSome _ -> invalidOp "conditionExpression" | ValueNone -> ()
        match x.returnValues with | ValueSome _ -> invalidOp "returnValues" | ValueNone -> ()
        match x.attributeUpdates with | ValueSome _ -> invalidOp "attributeUpdates" | ValueNone -> ()
        match x.updateExpression with | ValueSome _ -> invalidOp "updateExpression" | ValueNone -> ()
        match x.updateKey with | ValueSome _ -> invalidOp "updateKey" | ValueNone -> ()

        match x.forwards with
        | ValueSome x -> output.ScanIndexForward <- x
        | ValueNone -> ()

        output.Select <- Maybe.Null.fromOption x.select
        output.TableName <- Maybe.Null.fromOption x.tableName
        output.IndexName <- Maybe.Null.fromOption x.indexName
        output.FilterExpression <- Maybe.Null.fromOption x.filterExpression
        x.limit ?|> (fun l -> output.Limit <- l) |> ValueOption.defaultValue ()
        output.KeyConditionExpression <- Maybe.Null.fromOption x.keyConditionExpression
        output.ProjectionExpression <- Maybe.Null.fromOption x.projectionExpression
        output.AttributesToGet <- x.attributesToGet ?|> Enumerable.ToList |> Maybe.Null.fromOption
        output.ExpressionAttributeNames <- x.expressionAttrNames ?|> kvpToDictionary |> Maybe.Null.fromOption

        output.ExpressionAttributeValues <-
            x.expressionAttrValues
            ?|> itemToDynamoDb
            |> Maybe.Null.fromOption

        x.randomizeExprAttrNames
        ?|> (QueryBuilder.addSomeAliasesToQ output)
        |> ValueOption.defaultValue ()

        output

    static member addSomeAliasesToS (output: ScanRequest) random =
        let r = QueryBuilder.replaceSomeTokens random

        match r output.FilterExpression with
        | _, [] -> ()
        | filterExpr, newAttr1 ->
            output.FilterExpression <- filterExpr
            let d =
                match output.ExpressionAttributeNames with
                | null ->
                    output.ExpressionAttributeNames <- Dictionary<_, _>()
                    output.ExpressionAttributeNames
                | x -> x

            newAttr1
            |> List.fold (fun _ struct (k, v) -> d.Add(k, v)) ()

    static member scanRequest x =
        let output = ScanRequest()

        match x.conditionExpression with | ValueSome _ -> invalidOp "conditionExpression" | ValueNone -> ()
        match x.returnValues with | ValueSome _ -> invalidOp "returnValues" | ValueNone -> ()
        match x.forwards with | ValueSome _ -> invalidOp "forwards" | ValueNone -> ()
        match x.attributeUpdates with | ValueSome _ -> invalidOp "attributeUpdates" | ValueNone -> ()
        match x.updateExpression with | ValueSome _ -> invalidOp "updateExpression" | ValueNone -> ()
        match x.keyConditionExpression with | ValueSome _ -> invalidOp "keyConditionExpression" | ValueNone -> ()
        match x.updateKey with | ValueSome _ -> invalidOp "updateKey" | ValueNone -> ()

        output.Select <- Maybe.Null.fromOption x.select
        output.TableName <- Maybe.Null.fromOption x.tableName
        output.IndexName <- Maybe.Null.fromOption x.indexName
        output.FilterExpression <- Maybe.Null.fromOption x.filterExpression
        output.ProjectionExpression <- Maybe.Null.fromOption x.projectionExpression
        output.AttributesToGet <- x.attributesToGet ?|> Enumerable.ToList |> Maybe.Null.fromOption
        x.limit ?|> (fun l -> output.Limit <- l) |> ValueOption.defaultValue ()
        output.ExpressionAttributeNames <- x.expressionAttrNames ?|> kvpToDictionary |> Maybe.Null.fromOption
        output.ExpressionAttributeValues <-
            x.expressionAttrValues
            ?|> itemToDynamoDb
            |> Maybe.Null.fromOption

        x.randomizeExprAttrNames
        ?|> (QueryBuilder.addSomeAliasesToS output)
        |> ValueOption.defaultValue ()

        output

    static member updateRequest x =
        let output = UpdateItemRequest()

        match x.forwards with | ValueSome _ -> invalidOp "forwards" | ValueNone -> ()
        match x.select with | ValueSome _ -> invalidOp "select" | ValueNone -> ()
        match x.indexName with | ValueSome _ -> invalidOp "indexName" | ValueNone -> ()
        match x.filterExpression with | ValueSome _ -> invalidOp "filterExpression" | ValueNone -> ()
        match x.keyConditionExpression with | ValueSome _ -> invalidOp "keyConditionExpression" | ValueNone -> ()
        match x.projectionExpression with | ValueSome _ -> invalidOp "projectionExpression" | ValueNone -> ()
        match x.attributesToGet with | ValueSome _ -> invalidOp "attributesToGet" | ValueNone -> ()
        match x.forwards with | ValueSome _ -> invalidOp "forwards" | ValueNone -> ()
        match x.limit with | ValueSome _ -> invalidOp "limit" | ValueNone -> ()

        output.TableName <- Maybe.Null.fromOption x.tableName
        output.ExpressionAttributeNames <- x.expressionAttrNames ?|> kvpToDictionary |> Maybe.Null.fromOption
        output.UpdateExpression <- Maybe.Null.fromOption x.updateExpression
        output.ConditionExpression <- Maybe.Null.fromOption x.conditionExpression
        output.AttributeUpdates <- ValueOption.map kvpToDictionary x.attributeUpdates |> Maybe.Null.fromOption

        output.ExpressionAttributeValues <-
            x.expressionAttrValues
            ?|> itemToDynamoDb
            |> Maybe.Null.fromOption

        output.Key <-
            Maybe.expectSome x.updateKey
            |> itemToDynamoDb

        output.ReturnValues <- Maybe.Null.fromOption x.returnValues

        x.randomizeExprAttrNames
        ?|> (QueryBuilder.addSomeAliasesToU output)
        |> ValueOption.defaultValue ()

        output
