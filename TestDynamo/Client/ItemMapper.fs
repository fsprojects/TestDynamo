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
    then AttributeValue.createNull()
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
    | true, null, null, null, false, false, false, false, false, false -> attr.BOOL |> AttributeValue.createBoolean
    | false, str, null, null, false, false, false, false, false, false when str <> null -> str |> AttributeValue.createString
    | false, null, num, null, false, false, false, false, false, false when num <> null -> num |> Decimal.Parse |> AttributeValue.createNumber
    | false, null, null, bin, false, false, false, false, false, false when bin <> null ->
        bin.ToArray() |> AttributeValue.createBinary
    | false, null, null, null, true, false, false, false, false, false -> AttributeValue.createNull()
    | false, null, null, null, false, true, false, false, false, false ->
        if attr.M = null then clientError "Map data not set"
        itemFromDynamodb name attr.M |> AttributeValue.createHashMap
    | false, null, null, null, false, false, true, false, false, false ->
        CSharp.sanitizeSeq attr.L 
        |> Seq.mapi (fun i -> attributeFromDynamodb $"{name}[{i}]") |> Array.ofSeq |> CompressedList |> AttributeValue.createAttributeList
    | false, null, null, null, false, false, false, true, false, false ->
        CSharp.sanitizeSeq attr.SS
        |> Seq.map (AttributeValue.createString)
        |> AttributeSet.create
        |> AttributeValue.createHashSet
    | false, null, null, null, false, false, false, false, true, false ->

        CSharp.sanitizeSeq attr.NS
        |> Seq.map (Decimal.Parse >> AttributeValue.createNumber)
        |> AttributeSet.create
        |> AttributeValue.createHashSet
    | false, null, null, null, false, false, false, false, false, true ->

        CSharp.sanitizeSeq attr.BS
        |> Seq.map (fun (b: MemoryStream) -> b.ToArray() |> AttributeValue.createBinary)
        |> AttributeSet.create
        |> AttributeValue.createHashSet
    | pp -> clientError $"Unknown attribute type for \"{name}\""

and private mapAttribute name (attr: KeyValuePair<string, DynamoAttributeValue>) =
    struct (attr.Key, attributeFromDynamodb $"{name}.{attr.Key}" attr.Value)

and itemFromDynamodb name x = x |> Seq.map (mapAttribute name) |> MapUtils.fromTuple

let rec attributeToDynamoDb attr =
    match attr |> AttributeValue.value with
    | WorkingAttributeValue.StringX x ->
        let attr = DynamoAttributeValue()
        attr.S <- x
        attr
    | WorkingAttributeValue.NumberX x ->
        let attr = DynamoAttributeValue()
        attr.N <- x.ToString()
        attr
    | WorkingAttributeValue.BinaryX x ->
        let attr = DynamoAttributeValue()
        attr.B <- new MemoryStream(x)
        attr
    | WorkingAttributeValue.BooleanX x ->
        let attr = DynamoAttributeValue()
        attr.BOOL <- x
        attr.IsBOOLSet <- true
        attr
    | WorkingAttributeValue.NullX ->
        let attr = DynamoAttributeValue()
        attr.NULL <- true
        attr
    | WorkingAttributeValue.HashMapX x ->
        let attr = DynamoAttributeValue()
        attr.M <- itemToDynamoDb x
        attr.IsMSet <- true
        attr
    | WorkingAttributeValue.HashSetX x ->
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
    | WorkingAttributeValue.AttributeListX xs ->
        let attr = DynamoAttributeValue()
        attr.L <- xs |> AttributeListType.asSeq |> Seq.map attributeToDynamoDb |> Enumerable.ToList
        attr.IsLSet <- true
        attr

and itemToDynamoDb =
    CSharp.toDictionary (fun (x: string) -> x) attributeToDynamoDb