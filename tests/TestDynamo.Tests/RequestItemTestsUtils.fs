namespace TestDynamo.Tests

open System
open System.Text
open System.Threading.Tasks
open Amazon.DynamoDBv2
open TestDynamo
open TestDynamo.Data.BasicStructures
open TestDynamo.Utils
open Tests.Items
open Tests.Table
open Tests.Utils
open Xunit
open Xunit.Abstractions
open TestDynamo.Model

type TableDescription =
    { name: string
      hasSk: bool
      indexHasSk: bool
      streamsEnabled: bool
      isBinaryTable: bool }

    with
    static member create isBinaryTable hasSk indexHasSk name streamsEnabled =
        { hasSk = hasSk
          indexHasSk = indexHasSk
          name = name
          streamsEnabled = streamsEnabled
          isBinaryTable = isBinaryTable }

type Tables =
    { ``table, pBytes, sBytes, IpBytes, IsBytes``: TableDescription
      ``table, p: String, s: Number, Ip: Number, Is: String``: TableDescription
      ``table, p: String, Ip: Number, Is: String``: TableDescription
      ``table, p: String, s: Number, Ip: Number``: TableDescription
      ``table, p: String, Ip: Number``: TableDescription }

    with
    static member get hasSortKey indexHasSortKey t =
        match struct (hasSortKey, indexHasSortKey) with
        | true, true -> t.``table, p: String, s: Number, Ip: Number, Is: String``
        | true, false -> t.``table, p: String, s: Number, Ip: Number``
        | false, true -> t.``table, p: String, Ip: Number, Is: String``
        | false, false -> t.``table, p: String, Ip: Number``

    static member getByStreamsEnabled streamsEnabled t =
        [
            t.``table, p: String, s: Number, Ip: Number, Is: String``
            t.``table, p: String, Ip: Number, Is: String``
            t.``table, p: String, s: Number, Ip: Number``
            t.``table, p: String, Ip: Number``
            t.``table, pBytes, sBytes, IpBytes, IsBytes``
        ]
        |> Seq.filter (fun x -> x.streamsEnabled = streamsEnabled)
        |> Seq.head

type TableItem =
    { tablePk: string
      tableSk: decimal
      indexPk: decimal
      indexSk: string
      binaryData: string
      boolData: bool }

    with
    static member asItemBuilder x =
        ItemBuilder.empty
        |> ItemBuilder.withAttribute "TablePk" "S" x.tablePk
        |> ItemBuilder.withAttribute "TableSk" "N" (x.tableSk.ToString())
        |> ItemBuilder.withAttribute "IndexPk" "N" (x.indexPk.ToString())
        |> ItemBuilder.withAttribute "IndexSk" "S" x.indexSk
        |> ItemBuilder.withAttribute "TablePk_Copy" "S" x.tablePk
        |> ItemBuilder.withAttribute "TableSk_Copy" "N" (x.tableSk.ToString())
        |> ItemBuilder.withAttribute "IndexPk_Copy" "N" (x.indexPk.ToString())
        |> ItemBuilder.withAttribute "IndexSk_Copy" "S" x.indexSk
        |> ItemBuilder.withAttribute "BinaryData" "UTF8" x.binaryData
        |> ItemBuilder.withAttribute "BoolData" "BOOL" (x.boolData.ToString().ToLower())
        |> ItemBuilder.withNullAttribute "NullData"
        |> ItemBuilder.withAttribute "LongerString" "S" "This is a longer string"
        |> ItemBuilder.withListAttribute "ListData" [
            struct ("S", x.tablePk)
            struct ("N", x.tableSk.ToString())
        ]
        |> ItemBuilder.withMapAttribute "MapData" [
            struct ("IndexPk", struct ("N", x.indexPk.ToString()))
            struct ("IndexSk", struct ("S", x.indexSk))
        ]
        |> ItemBuilder.withSetAttribute "SSData" "SS" [
            x.tablePk
            x.tablePk + "-Set data 2"
        ]
        |> ItemBuilder.withSetAttribute "NSData" "NS" [
            x.tableSk.ToString()
            (x.tableSk + 0.5M).ToString()
        ]
        |> ItemBuilder.withSetAttribute "BSData" "BS" [
            x.binaryData
            Encoding.UTF8.GetBytes x.binaryData |> Convert.ToBase64String
        ]

    static member asItem = TableItem.asItemBuilder >> ItemBuilder.dynamoDbAttributes >> (itemFromDynamodb "$")

    static member asAttributes = TableItem.asItemBuilder >> ItemBuilder.dynamoDbAttributes

module RequestItemTestUtils =

    let private aBunchOfNormalItems =
        let tableKeys =
            [1..1000]  // will be filtered by zip
            |> Seq.collect (fun pk ->
                [-2..2..2]
                |> Seq.map (tpl (pk.ToString())))

        let indexKeys =
            [-1..1]
            |> Seq.collect (fun x -> [x;x])
            |> Seq.collect (fun pk ->
                [29..1..31]
                |> Seq.map _.ToString()
                |> Seq.collect (fun x -> [x;x])
                |> Seq.map (tpl pk))

        Seq.zip tableKeys indexKeys
        |> Seq.mapi (fun i (struct (tPk, tSk), struct (iPk, iSk)) ->
            { tablePk = tPk
              tableSk = decimal tSk
              indexPk = decimal iPk
              indexSk = iSk
              binaryData = i.ToString()
              boolData = (i % 2 = 0) })
        |> List.ofSeq

    let aBunchOfBinaryItems =
        aBunchOfNormalItems
        |> List.map (fun t ->
            ItemBuilder.empty
            |> ItemBuilder.withAttribute "TablePk" "B" (Encoding.UTF8.GetBytes t.tablePk |> Convert.ToBase64String)
            |> ItemBuilder.withAttribute "TableSk" "B" (BitConverter.GetBytes (double t.tableSk + 1.0) |> Convert.ToBase64String)
            |> ItemBuilder.withAttribute "IndexPk" "B" (BitConverter.GetBytes (double t.indexPk + 1.0) |> Convert.ToBase64String)
            |> ItemBuilder.withAttribute "IndexSk" "B" (Encoding.UTF8.GetBytes t.indexSk |> Convert.ToBase64String))

    let binaryTablePk = ItemBuilder.getAttribute "TablePk"
    let binaryIndexPk = ItemBuilder.getAttribute "IndexPk"
    let binaryTableSk = ItemBuilder.getAttribute "TableSk"
    let binaryIndexSk = ItemBuilder.getAttribute "IndexSk"

    let groupedBinaryItemsWithSk isTable =
        let pk = if isTable then binaryTablePk else binaryIndexPk
        let sk = if isTable then binaryTableSk else binaryIndexSk

        aBunchOfBinaryItems
        |> Seq.groupBy pk
        |> Seq.map structTpl
        |> Seq.sortBy fstT
        |> Collection.mapSnd (
            Seq.map (fun x -> struct (sk x, x))
            >> (mapSnd sk |> Seq.sortBy >> List.ofSeq))
        |> List.ofSeq

    let groupedItems hasTableSort =
        let table =
            aBunchOfNormalItems
            |> Seq.groupBy _.tablePk
            |> Seq.map structTpl
            |> Seq.sortBy fstT
            |> Collection.mapSnd (
                Seq.map (fun x -> struct (x.tableSk, x))
                >> Seq.sortBy fstT
                >> if hasTableSort then id else Seq.last >> Seq.singleton
                >> List.ofSeq)
            |> List.ofSeq

        let index =
            aBunchOfNormalItems
            |> Seq.groupBy _.indexPk
            |> Seq.map structTpl
            |> Seq.sortBy fstT
            |> Collection.mapSnd (
                Seq.map (fun x -> struct (x.indexSk, x))
                >> Seq.sortBy fstT
                >> List.ofSeq)
            |> List.ofSeq

        struct (table, index)

    let allItems =
        groupedItems
        >> fstT
        >> Seq.collect (sndT >> Seq.map sndT)

    let groupedBinaryItems =

        let table =
            aBunchOfBinaryItems
            |> Seq.groupBy (fun x ->
                let y = binaryTablePk x
                y)
            |> Seq.map structTpl
            |> Seq.sortBy fstT
            |> Collection.mapSnd (
                Seq.map (fun x -> struct (binaryTableSk x, x))
                >> Seq.sortBy fstT
                >> List.ofSeq)
            |> List.ofSeq

        let index =
            aBunchOfBinaryItems
            |> Seq.groupBy binaryIndexPk
            |> Seq.map structTpl
            |> Seq.sortBy fstT
            |> Collection.mapSnd (
                Seq.map (fun x -> struct (binaryIndexSk x, x))
                >> Seq.sortBy fstT
                >> List.ofSeq)
            |> List.ofSeq

        struct (table, index)

    let randomSort (r: Random) (x: 'a seq): 'a seq =
        Seq.map (fun x -> struct (r.Next(), x)) x
        |> Seq.sortBy fstT
        |> Seq.map sndT

    let private assertLength l (xs: 'a seq) =
        seq {
            let mutable total = 0
            for x in xs do
                total <- total + 1
                yield x

            assert (total = l)
        }

    let randomSeqItems count filter (r: Random) (x: 'a seq): 'a seq =
        randomSort r x
        |> Seq.filter filter
        |> assertLength count
        |> Seq.take count

    let randomPartitions' count filter mapper hasSk r =
        groupedItems hasSk
        |> mapper
        |> randomSeqItems filter count r
        |> Collection.mapSnd (Collection.mapSnd (TableItem.asItem))

    let randomBinaryPartition' filter mapper r =
        groupedBinaryItems
        |> mapper
        |> randomSeqItems 1 filter r
        |> Seq.head
        |> mapSnd (Collection.mapSnd (ItemBuilder.attributes))

    let randomFilteredPartitions filter count: bool -> Random -> struct (string * struct (decimal * Map<string,AttributeValue>) seq) seq =
        randomPartitions' filter count fstT
    let randomFilteredPartition filter hasSk =
        randomPartitions' filter 1 fstT hasSk >> Seq.head
    let randomFilteredIndexPartition filter = randomPartitions' filter 1 sndT false >> Seq.head

    let randomPartitions count: bool -> Random -> struct (string * struct (decimal * Map<string,AttributeValue>) seq) seq =
        randomPartitions' (fun _ -> true) count fstT
    let randomPartition hasSk =
        randomPartitions' (fun _ -> true) 1 fstT hasSk >> Seq.head
    let randomIndexPartition = randomPartitions' (fun _ -> true) 1 sndT false >> Seq.head

    let randomBinaryPartition: Random -> struct (AttributeValue * struct (AttributeValue * Map<string,AttributeValue>) seq) =
        randomBinaryPartition' (fun _ -> true) fstT
    let randomBinaryIndexPartition: Random -> struct (AttributeValue * struct (AttributeValue * Map<string,AttributeValue>) seq) =
        randomBinaryPartition' (fun _ -> true) sndT

    let randomItem hasSk r =
        randomPartition hasSk r
        |> mapSnd Seq.head

    let randomFilteredItem predicate hasSk r =
        randomFilteredPartition predicate hasSk r
        |> mapSnd Seq.head

    let randomIndexItem: Random -> struct (struct (decimal * string) * Map<string,AttributeValue> seq) =
        randomIndexPartition
        >> mapSnd (
            Seq.groupBy fstT
            >> Seq.map structTpl
            >> Seq.filter (fun struct (_, x) -> Seq.length x > 1)
            >> Seq.head
            >> mapSnd (Seq.map sndT))
        >> fun struct (pk, struct (sk, vals)) -> struct (struct (pk, sk), vals)

    let aBunchOfNormalItemsData =
        aBunchOfNormalItems
        |> List.map TableItem.asItemBuilder

    let newTables (client: IAmazonDynamoDB) =

        let table description isBinaryTable hasSk indexHasSk streamsEnabled req =
            let name = sprintf "%s_%s_%i" "QueryAndScanTestUtils" description (uniqueId())
            let table =
                name
                |> req
                |> TableBuilder.req
                |> client.CreateTableAsync

            task {
                // act
                let! _ = table
                return {hasSk = hasSk; indexHasSk = indexHasSk; name = name; streamsEnabled = streamsEnabled; isBinaryTable = isBinaryTable}
            }

        let ``table, pBytes, sBytes, IpBytes, IsBytes`` =
            fun t ->
                TableBuilder.empty
                |> TableBuilder.withTableName t
                |> TableBuilder.withAttribute "TablePk" "B"
                |> TableBuilder.withAttribute "TableSk" "B"
                |> TableBuilder.withAttribute "IndexPk" "B"
                |> TableBuilder.withAttribute "IndexSk" "B"
                |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" (ValueSome "IndexSk") true
                |> TableBuilder.withStreamAction (CreateOrDelete.Create ())
            |> table "Bytes" true true true true

        let ``table, p: String, s: Number, Ip: Number, Is: String`` =
            fun t ->
                TableBuilder.empty
                |> TableBuilder.withTableName t
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "TableSk" "N"
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> TableBuilder.withAttribute "IndexSk" "S"
                |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" (ValueSome "IndexSk") true
                |> TableBuilder.withStreamAction (CreateOrDelete.Create ())
            |> table "TpTsIpIs" false true true true

        let ``table, p: String, Ip: Number, Is: String`` =
            fun t ->
                TableBuilder.empty
                |> TableBuilder.withTableName t
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> TableBuilder.withAttribute "IndexSk" "S"
                |> TableBuilder.withKeySchema "TablePk" ValueNone
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" (ValueSome "IndexSk") true
                |> TableBuilder.withStreamAction (CreateOrDelete.Create ())
            |> table "TpIpIs" false false true true

        let ``table, p: String, s: Number, Ip: Number`` =
            fun t ->
                TableBuilder.empty
                |> TableBuilder.withTableName t
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "TableSk" "N"
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> TableBuilder.withKeySchema "TablePk" (ValueSome "TableSk")
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" ValueNone true
                |> TableBuilder.withStreamAction (CreateOrDelete.Create ())
            |> table "TpTsIp" false true false true

        let ``table, p: String, Ip: Number`` =
            fun t ->
                TableBuilder.empty
                |> TableBuilder.withTableName t
                |> TableBuilder.withAttribute "TablePk" "S"
                |> TableBuilder.withAttribute "IndexPk" "N"
                |> TableBuilder.withKeySchema "TablePk" ValueNone
                |> TableBuilder.withSimpleGsi "TheIndex" "IndexPk" ValueNone true
            |> table "TpIp" false false false false

        let tables =
            task {
                let! t1 = ``table, pBytes, sBytes, IpBytes, IsBytes``
                let! t2 = ``table, p: String, s: Number, Ip: Number, Is: String``
                let! t3 = ``table, p: String, Ip: Number, Is: String``
                let! t4 = ``table, p: String, s: Number, Ip: Number``
                let! t5 = ``table, p: String, Ip: Number``

                let x =
                  { ``table, pBytes, sBytes, IpBytes, IsBytes`` = t1
                    ``table, p: String, s: Number, Ip: Number, Is: String`` = t2
                    ``table, p: String, Ip: Number, Is: String`` = t3
                    ``table, p: String, s: Number, Ip: Number`` = t4
                    ``table, p: String, Ip: Number`` = t5 }

                return x
            }

        let sixtyFiveNormalItemsData' tableName hasSk =
            allItems hasSk
            |> Seq.map (TableItem.asItemBuilder >>  ItemBuilder.withTableName tableName >> ItemBuilder.asPutReq)

        let aBunchOfBinaryItems' tableName =
            aBunchOfBinaryItems
            |> Seq.map (ItemBuilder.withTableName tableName >> ItemBuilder.asPutReq)

        task {
            let! tbls = tables
            let ts =
                [ tbls.``table, p: String, Ip: Number``
                  tbls.``table, p: String, s: Number, Ip: Number``
                  tbls.``table, p: String, Ip: Number, Is: String``
                  tbls.``table, p: String, s: Number, Ip: Number, Is: String`` ]
                |> List.map (fun t -> struct (t.name, t.hasSk)) 

            let! _ =
                ts
                |> Seq.collect (
                    uncurry sixtyFiveNormalItemsData'
                    >> Seq.map client.PutItemAsync)
                |> Collection.concat2 (aBunchOfBinaryItems' tbls.``table, pBytes, sBytes, IpBytes, IsBytes``.name |> Seq.map client.PutItemAsync)
                |> Task.WhenAll

            return tbls
        }

    let sharedTestData: ITestOutputHelper voption -> Task<Tables> =
        let collector = OutputCollector()
        let execution =
            task {
                let start = DateTimeOffset.Now
                use client = buildClient collector
                let client = client.Client

                let! t = newTables client
                return struct (t, DateTimeOffset.Now - start)
            }

        fun output ->
            task {
                let! struct (t, time) = execution

                match output with
                | ValueSome x ->
                    collector.Emit x
                    x.WriteLine($"Executed in {time}")
                | ValueNone -> ()

                return t
            }

    let attrsToString =
        Seq.map (MapUtils.toSeq >> Collection.mapSnd (_.ToString()) >> printFullMap) >> printFullList

    let assertModelItems struct (expected: Map<string, AttributeValue> seq, actual: Map<string, AttributeValue> seq, assertOrder) =

        let comparable = (if assertOrder then id else Seq.sort) >> List.ofSeq

        let ex = expected |> comparable
        let ac = actual |> comparable

        let printableEx = expected |> attrsToString
        let printableAc = actual |> attrsToString
        Assert.True((ex = ac), $"expected:\n{printableEx}\n\nactual:\n{printableAc}")

    let assertItems struct (expected, actual, assertOrder) =
        assertModelItems (expected, actual |> Seq.map (itemFromDynamodb "$"), assertOrder)

    let assertDynamoDbItems struct (expected, actual, assertOrder) =
        assertModelItems (expected |> Seq.map (itemFromDynamodb "$"), actual |> Seq.map (itemFromDynamodb "$"), assertOrder)

    let addMapWithProp prop (x: DynamoAttributeValue) =
        x.M <- Dictionary<string, DynamoAttributeValue>()
        x.IsMSet <- true
        x.M[prop] <- DynamoAttributeValue()
        x.M[prop]

    let addMapWith0Prop = addMapWithProp "0"

    let addListWith0Element (x: DynamoAttributeValue): DynamoAttributeValue =
        x.L <- MList<DynamoAttributeValue>()
        x.IsLSet <- true
        x.L.Add(DynamoAttributeValue())
        x.L[0]

    let buildDeeplyNestedMap (): struct (DynamoAttributeValue * DynamoAttributeValue) =
        let mapHead = DynamoAttributeValue()
        let mapTail = mapHead |> addMapWith0Prop |> addListWith0Element |> addMapWith0Prop
        mapTail.S <- "EndMap"
        struct (mapHead, mapTail)

    let buildDeeplyNestedList (): struct (DynamoAttributeValue * DynamoAttributeValue) =
        let listHead = DynamoAttributeValue()
        let listTail = listHead |> addListWith0Element |> addMapWith0Prop |> addListWith0Element
        listTail.S <- "EndList"
        struct (listHead, listTail)