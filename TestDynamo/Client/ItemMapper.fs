module TestDynamo.Client.ItemMapper

open System
open System.Collections.Generic
open System.IO
open System.Linq
open TestDynamo
open TestDynamo.Utils
open TestDynamo.Model

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue
type MList<'a> = List<'a>

let rec attributeFromDynamodb name (attr: DynamoAttributeValue) =
    if attr = null
    then AttributeValue.Null
    else
    match struct (
        attr.IsBOOLSet,
        attr.S,
        attr.N,
        attr.B,
        attr.NULL,
        attr.IsMSet,
        attr.IsLSet,
        attr.IsSSSet,
        attr.IsNSSet,
        attr.IsBSSet
        ) with
    | true, null, null, null, false, false, false, false, false, false -> attr.BOOL |> AttributeValue.Boolean
    | false, str, null, null, false, false, false, false, false, false when str <> null -> str |> AttributeValue.String
    | false, null, num, null, false, false, false, false, false, false when num <> null -> num |> Decimal.Parse |> AttributeValue.Number
    | false, null, null, bin, false, false, false, false, false, false when bin <> null ->
        bin.ToArray() |> AttributeValue.Binary
    | false, null, null, null, true, false, false, false, false, false -> AttributeValue.Null
    | false, null, null, null, false, true, false, false, false, false ->
        if attr.M = null then clientError "Map data not set"
        itemFromDynamodb name attr.M |> AttributeValue.HashMap
    | false, null, null, null, false, false, true, false, false, false ->
        CSharp.sanitizeSeq attr.L 
        |> Seq.mapi (fun i -> attributeFromDynamodb $"{name}[{i}]") |> Array.ofSeq |> CompressedList |> AttributeValue.AttributeList
    | false, null, null, null, false, false, false, true, false, false ->
        CSharp.sanitizeSeq attr.SS
        |> Seq.map String
        |> AttributeSet.create
        |> HashSet
    | false, null, null, null, false, false, false, false, true, false ->

        CSharp.sanitizeSeq attr.NS
        |> Seq.map (Decimal.Parse >> Number)
        |> AttributeSet.create
        |> HashSet
    | false, null, null, null, false, false, false, false, false, true ->

        CSharp.sanitizeSeq attr.BS
        |> Seq.map (fun (b: MemoryStream) -> b.ToArray() |> Binary)
        |> AttributeSet.create
        |> HashSet
    | pp -> clientError $"Unknown attribute type for \"{name}\""

and private mapAttribute name (attr: KeyValuePair<string, DynamoAttributeValue>) =
    struct (attr.Key, attributeFromDynamodb $"{name}.{attr.Key}" attr.Value)

and itemFromDynamodb name x = x |> Seq.map (mapAttribute name) |> MapUtils.fromTuple

let rec attributeToDynamoDb = function
    | AttributeValue.String x ->
        let attr = DynamoAttributeValue()
        attr.S <- x
        attr
    | AttributeValue.Number x ->
        let attr = DynamoAttributeValue()
        attr.N <- x.ToString()
        attr
    | AttributeValue.Binary x ->
        let attr = DynamoAttributeValue()
        attr.B <- new MemoryStream(x)
        attr
    | AttributeValue.Boolean x ->
        let attr = DynamoAttributeValue()
        attr.BOOL <- x
        attr.IsBOOLSet <- true
        attr
    | AttributeValue.Null ->
        let attr = DynamoAttributeValue()
        attr.NULL <- true
        attr
    | AttributeValue.HashMap x ->
        let attr = DynamoAttributeValue()
        attr.M <- itemToDynamoDb x
        attr.IsMSet <- true
        attr
    | AttributeValue.HashSet x ->
        let attr = DynamoAttributeValue()
        match AttributeSet.getSetType x with
        | AttributeType.Binary ->
            attr.BS <-
                AttributeSet.asBinarySeq x
                |> Seq.map (fun x -> new MemoryStream(x))
                |> Enumerable.ToList
            attr.IsBSSet <- true

        | AttributeType.String ->
            attr.SS <-
                AttributeSet.asStringSeq x
                |> Enumerable.ToList
            attr.IsSSSet <- true

        | AttributeType.Number ->
            attr.NS <-
                AttributeSet.asNumberSeq x
                |> Seq.map _.ToString()
                |> Enumerable.ToList
            attr.IsNSSet <- true

        | x -> clientError $"Unknown set type {x}"

        attr
    | AttributeValue.AttributeList xs ->
        let attr = DynamoAttributeValue()
        attr.L <- xs |> AttributeListType.asSeq |> Seq.map attributeToDynamoDb |> Enumerable.ToList
        attr.IsLSet <- true
        attr

and itemToDynamoDb =
    CSharp.toDictionary (fun (x: string) -> x) attributeToDynamoDb