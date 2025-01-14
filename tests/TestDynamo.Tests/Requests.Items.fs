module Tests.Items

open System
open System.Collections.Generic
open System.IO
open System.Linq
open System.Text
open System.Text.Json
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open TestDynamo
open Tests.Table
open Tests.Utils
open TestDynamo.Utils

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue

type ItemBuilder =
    { tName: string voption
      attrs: Map<string, DynamoAttributeValue>
      condition: string voption
      // OLD/NEW
      returnValues: string voption }
    with

    static member empty =
        { tName = ValueNone
          attrs = Map.empty
          returnValues = ValueNone
          condition = ValueNone }

    static member withTableName name x = { x with tName = ValueSome name } : ItemBuilder
    static member withCondition condition x = { x with condition = ValueSome condition } : ItemBuilder
    static member tableName x = x.tName |> ValueOption.defaultWith (fun _ -> invalidOp "missing table name")

    static member buildAttribute label value =
        match struct (label, value) with
        | struct ("S", value) ->
            let attrV = DynamoAttributeValue()
            attrV.S <- value
            attrV
        | "N", value ->
            let attrV = DynamoAttributeValue()
            attrV.N <- value
            attrV
        | "B", value ->
            let attrV = DynamoAttributeValue()
            let data = Convert.FromBase64String value
            attrV.B <- new MemoryStream(data)
            attrV
        | "UTF8", value ->
            value
            |> Encoding.UTF8.GetBytes
            |> Convert.ToBase64String
            |> ItemBuilder.buildAttribute "B"
        | "BOOL", "true" ->
            let attrV = DynamoAttributeValue()
            maybeSetProperty "IsBOOLSet" attrV true
            attrV.BOOL <- true
            maybeSetProperty "IsBOOLSet" attrV true
            attrV
        | "BOOL", "false" ->
            let attrV = DynamoAttributeValue()
            maybeSetProperty "IsBOOLSet" attrV true
            attrV.BOOL <- false
            maybeSetProperty "IsBOOLSet" attrV true
            attrV
        | "NULL", _ ->
            let attrV = DynamoAttributeValue()
            attrV.NULL <- true
            attrV
        | "SS", set ->
            let attrV = DynamoAttributeValue()
            maybeSetProperty "IsSSSet" attrV true
            attrV.SS <- JsonSerializer.Deserialize<List<string>> set
            maybeSetProperty "IsSSSet" attrV true
            attrV
        | "NS", set ->
            let attrV = DynamoAttributeValue()
            maybeSetProperty "IsNSSet" attrV true
            attrV.NS <- JsonSerializer.Deserialize<List<string>> set
            maybeSetProperty "IsNSSet" attrV true
            attrV
        | "BS", set ->
            let attrV = DynamoAttributeValue()
            maybeSetProperty "IsBSSet" attrV true
            attrV.BS <-
                JsonSerializer.Deserialize<List<string>> set
                |> Seq.map (ItemBuilder.buildAttribute "UTF8" >> _.B)
                |> Enumerable.ToList
            maybeSetProperty "IsBSSet" attrV true
            attrV
        | "L", list ->
            let attrV = DynamoAttributeValue()
            maybeSetProperty "IsLSet" attrV true
            attrV.L <-
                JsonSerializer.Deserialize<List<List<string>>> list
                |> Seq.map (fun x -> struct (x[0], x[1]))
                |> Seq.map (uncurry ItemBuilder.buildAttribute)
                |> Enumerable.ToList
            maybeSetProperty "IsLSet" attrV true
            attrV
        | "M", map ->
            let attrV = DynamoAttributeValue()
            maybeSetProperty "IsMSet" attrV true
            attrV.M <-
                JsonSerializer.Deserialize<List<List<string>>> map
                |> Seq.map (fun x -> struct (x[0], struct (x[1], x[2])))
                |> MapUtils.fromTuple
                |> Map.map (fun _ -> uncurry ItemBuilder.buildAttribute)
                |> kvpToDictionary
            maybeSetProperty "IsMSet" attrV true
            attrV
        | x -> invalidOp $"unknown {x}"

    static member withAttrs attrs x =
        let attrs = Seq.fold (fun s (x: KeyValuePair<string, DynamoAttributeValue>) -> Map.add x.Key x.Value s) x.attrs attrs
        { x with attrs = attrs }: ItemBuilder

    static member withReturnValues returnValues x =
        { x with returnValues = ValueSome returnValues }: ItemBuilder

    static member buildItemAttribute name ``type`` =
        ItemBuilder.buildAttribute name ``type``
        |> attributeFromDynamodb

    static member addAttribute name attr x =
        { x with attrs = Map.add name attr x.attrs }: ItemBuilder

    static member withRawAttribute name value x =
        let attr = attributeToDynamoDb value
        { x with attrs = Map.add name attr x.attrs }: ItemBuilder

    static member withAttribute name ``type`` value x =
        let attr = ItemBuilder.buildAttribute ``type`` value
        { x with attrs = Map.add name attr x.attrs }: ItemBuilder

    static member withNullAttribute name  x =
        let attr = ItemBuilder.buildAttribute "NULL" "null"
        { x with attrs = Map.add name attr x.attrs }: ItemBuilder

    static member withSetAttribute name ``type`` (vals: string list) x =
        let attr = ItemBuilder.buildAttribute ``type`` (JsonSerializer.Serialize vals)
        { x with attrs = Map.add name attr x.attrs }: ItemBuilder

    static member withListAttribute name (vals: struct (string * string) list) x =
        let attr = ItemBuilder.buildAttribute "L" (vals |> Seq.map (fun struct (t, v) -> [t; v]) |> JsonSerializer.Serialize)
        { x with attrs = Map.add name attr x.attrs }: ItemBuilder

    static member withMapAttribute name (vals: struct (string * struct (string * string)) list) x =
        let attr = ItemBuilder.buildAttribute "M" (vals |> Seq.map (fun struct (k, struct (t, v)) -> [k; t; v]) |> JsonSerializer.Serialize)
        { x with attrs = Map.add name attr x.attrs }: ItemBuilder

    static member getAttribute name (x: ItemBuilder) =
        Map.find name x.attrs |> attributeFromDynamodb

    static member attributes (x: ItemBuilder) =
        x.attrs
        |> Map.map (asLazy attributeFromDynamodb)

    static member dynamoDbAttributes (x: ItemBuilder) =
        x.attrs
        |> MapUtils.toSeq
        |> Seq.map tplToKvp
        |> Enumerable.ToDictionary

    static member asPutReq (x: ItemBuilder) =
        let request = PutItemRequest()
        request.TableName <- ItemBuilder.tableName x
        request.Item <- ItemBuilder.dynamoDbAttributes x
        request.ConditionExpression <- x.condition |> Maybe.Null.fromOption
        request.ReturnValues <-
            match x.returnValues with
            | ValueNone -> null
            | ValueSome x when x.ToLower() = "new" -> ReturnValue.ALL_NEW
            | ValueSome x when x.ToLower() = "old" -> ReturnValue.ALL_OLD
            | ValueSome x -> invalidOp x

        request

    static member asDeleteReq (x: ItemBuilder) =
        let request = DeleteItemRequest()
        request.TableName <- ItemBuilder.tableName x
        request.Key <- ItemBuilder.dynamoDbAttributes x
        request.ConditionExpression <- x.condition |> Maybe.Null.fromOption
        request.ReturnValues <-
            match x.returnValues with
            | ValueNone -> null
            | ValueSome x when x.ToLower() = "new" -> ReturnValue.ALL_NEW
            | ValueSome x when x.ToLower() = "old" -> ReturnValue.ALL_OLD
            | ValueSome x -> invalidOp x

        request
