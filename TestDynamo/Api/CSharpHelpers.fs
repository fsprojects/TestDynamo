namespace TestDynamo.Api

open System.Runtime.InteropServices
open Microsoft.Extensions.Logging
open TestDynamo
open TestDynamo.Model
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils
open System.Runtime.CompilerServices

type TableBuilder =
    { data: CreateTableData
      database: FSharp.Database }

    with

    member this.AddTable([<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        this.database.AddTable (Maybe.Null.toOption logger) this.data |> ignoreTyped<TableDetails>

    member private this.WithIndex isLocal name (partitionKey: struct (string * string)) (sortKey: System.Nullable<struct (string * string)>) projectionAttributes projectKeysOnly =

        let sortKey = Maybe.Null.valToOption sortKey

        let projectionAttributes =
            let partitionKey = fstT partitionKey
            let sortKey = ValueOption.map fstT sortKey

            match struct (projectKeysOnly, projectionAttributes |> Maybe.Null.toOption ?|> List.ofSeq, sortKey) with
            | false, xs, _ -> xs 
            | true, ValueNone, ValueNone -> ValueSome [partitionKey] 
            | true, ValueNone, ValueSome sk -> ValueSome [partitionKey; sk] 
            | true, ValueSome ([pk] & attr), ValueNone when pk = partitionKey -> ValueSome attr 
            | true, ValueSome ([pk; sk] & attr), ValueSome sortKey when pk = partitionKey && sk = sortKey -> ValueSome attr
            | true, _, _ -> invalidOp $"If {nameof projectKeysOnly} is used and {nameof projectionAttributes} are not null, then the {nameof projectionAttributes} must match the {nameof partitionKey} and {nameof sortKey}. Alternatively, set {nameof projectionAttributes} to null" 

        let attrs =
            (mapSnd AttributeType.parse partitionKey)::this.data.tableConfig.attributes
            |> (ValueOption.map (mapSnd AttributeType.parse >> Collection.prependL) sortKey ?|? id)

        let idx =
            { data =
                { keys = struct (fstT partitionKey, sortKey ?|> fstT)

                  projectionCols = projectionAttributes
                  projectionsAreKeys = projectKeysOnly }
              isLocal = isLocal }: IndexConfigInput

        {this with
            data.tableConfig.indexes = Map.add name idx this.data.tableConfig.indexes
            data.tableConfig.attributes = attrs }

    member this.WithGlobalSecondaryIndex(
        name,
        partitionKey: struct (string * string),
        [<Optional; DefaultParameterValue(System.Nullable<struct (string * string)>())>] sortKey: System.Nullable<struct (string * string)>,
        [<Optional; DefaultParameterValue(null: string seq)>] projectionAttributes: string seq,
        [<Optional; DefaultParameterValue(false)>] projectKeysOnly: bool) =

        this.WithIndex false name partitionKey sortKey projectionAttributes projectKeysOnly

    member this.WithStreamsEnabled(enabled: bool) = { this with data.createStream = enabled }

    member this.WithLocalSecondaryIndex(
        name,
        [<Optional; DefaultParameterValue(System.Nullable<struct (string * string)>())>] sortKey: System.Nullable<struct (string * string)>,
        [<Optional; DefaultParameterValue(null: string seq)>] projectionAttributes: string seq,
        [<Optional; DefaultParameterValue(false)>] projectKeysOnly: bool) =

        let pkName = fstT this.data.tableConfig.primaryIndex
        let pkType = this.data.tableConfig.attributes |> Seq.filter (fstT >> ((=)pkName)) |> Seq.map sndT |> Seq.head |> AttributeType.describe 

        this.WithIndex true name struct (pkName, pkType) sortKey projectionAttributes projectKeysOnly

    member this.WithDeletionProtection() =
        {this with data.tableConfig.addDeletionProtection = true }

    static member Create(
        database: FSharp.Database,
        name: string,
        partitionKey: struct (string * string),
        sortKey: System.Nullable<struct (string * string)>) =

        let sortKey = Maybe.Null.valToOption sortKey        
        let attrs =
            [
                ValueSome partitionKey
                sortKey
            ]
            |> Maybe.traverse
            |> Seq.map (mapSnd AttributeType.parse)
            |> List.ofSeq

        { database = database
          data =
            { tableConfig =
                { attributes = attrs
                  name = name
                  primaryIndex = struct (fstT partitionKey, sortKey ?|> fstT)
                  indexes = Map.empty
                  addDeletionProtection = false }
              createStream = false } }

[<Struct; IsReadOnly>]
type ListBuilder private(itemsReversed: AttributeValue list voption) =

    private new (itemsReversed: AttributeValue list) = ListBuilder(ValueSome itemsReversed)

    member this.AsAttributeList() = itemsReversed ?|? [] |> Seq.rev |> Array.ofSeq |> CompressedList |> AttributeList

    member this.AppendNull() = ListBuilder(AttributeValue.Null::(itemsReversed  ?|? []))

    member this.Append(value: string) = ListBuilder((AttributeValue.String value)::(itemsReversed  ?|? []))

    member this.Append(value: decimal) = ListBuilder((AttributeValue.Number value)::(itemsReversed  ?|? []))

    member this.Append(value: bool) = ListBuilder((AttributeValue.Boolean value)::(itemsReversed  ?|? []))

    member this.Append(value: byte array) =
        ListBuilder((AttributeValue.Binary (Array.copy value))::(itemsReversed  ?|? []))

    member this.Append(value: ListBuilder) =
        ListBuilder(value.AsAttributeList()::(itemsReversed  ?|? []))

    member this.Append(value: MapBuilder) =
        ListBuilder(value.AsMap()::(itemsReversed  ?|? []))

    member this.AppendSet(setValues: string seq) =
        let setValues = Seq.map AttributeValue.String setValues |> AttributeSet.create |> HashSet
        ListBuilder(setValues::(itemsReversed  ?|? []))

    member this.AppendSet(setValues: decimal seq) =
        let setValues = Seq.map AttributeValue.Number setValues |> AttributeSet.create |> HashSet
        ListBuilder(setValues::(itemsReversed  ?|? []))

    member this.AppendSet(setValues: byte array seq) =
        let setValues = Seq.map (Array.copy >> AttributeValue.Binary) setValues |> AttributeSet.create |> HashSet
        ListBuilder(setValues::(itemsReversed  ?|? []))

and
    [<Struct; IsReadOnly>]
    MapBuilder private(item: Map<string, AttributeValue> voption) =

        private new (item: Map<string, AttributeValue>) = MapBuilder(ValueSome item)

        with

        member this.AsRawMap() = item ?|? Map.empty

        member this.AsMap() = item ?|? Map.empty |> HashMap

        member this.Null(name: string) = MapBuilder(Map.add name AttributeValue.Null (item  ?|? Map.empty))

        member this.Attribute(name: string, value: string) = MapBuilder(Map.add name (AttributeValue.String value) (item  ?|? Map.empty))

        member this.Attribute(name: string, value: decimal) = MapBuilder(Map.add name (AttributeValue.Number value) (item  ?|? Map.empty))

        member this.Attribute(name: string, value: byte array) =
            MapBuilder(Map.add name (AttributeValue.Binary (Array.copy value)) (item  ?|? Map.empty))

        member this.Attribute(name: string, value: bool) = MapBuilder(Map.add name (AttributeValue.Boolean value) (item  ?|? Map.empty))

        member this.Attribute(name: string, value: MapBuilder) = MapBuilder(Map.add name (value.AsMap()) (item  ?|? Map.empty))

        member this.Attribute(name: string, value: ListBuilder) =
            MapBuilder(Map.add name (value.AsAttributeList()) (item  ?|? Map.empty))

        member this.AttributeSet(name: string, setValues: string seq) =
            let setValues = Seq.map AttributeValue.String setValues |> AttributeSet.create |> HashSet
            MapBuilder(Map.add name setValues (item  ?|? Map.empty))

        member this.AttributeSet(name: string, setValues: decimal seq) =
            let setValues = Seq.map AttributeValue.Number setValues |> AttributeSet.create |> HashSet
            MapBuilder(Map.add name setValues (item  ?|? Map.empty))

        member this.AttributeSet(name: string, setValues: byte array seq) =
            let setValues = Seq.map (Array.copy >> AttributeValue.Binary) setValues |> AttributeSet.create |> HashSet
            MapBuilder(Map.add name setValues (item  ?|? Map.empty))

type ItemBuilder =
    { tableName: string
      item: MapBuilder
      database: FSharp.Database }

    with

    static member Create(database: FSharp.Database, name: string) =
        { tableName = name
          item = MapBuilder()
          database = database }

    member this.Null(name: string) = { this with item = this.item.Null(name) }
    member this.Attribute(name: string, value: string) = { this with item = this.item.Attribute(name, value) }
    member this.Attribute(name: string, value: decimal) = { this with item = this.item.Attribute(name, value) }
    member this.Attribute(name: string, value: byte array) = { this with item = this.item.Attribute(name, value) }
    member this.Attribute(name: string, value: bool) = { this with item = this.item.Attribute(name, value) }
    member this.Attribute(name: string, value: MapBuilder) = { this with item = this.item.Attribute(name, value) }
    member this.Attribute(name: string, value: ListBuilder) = { this with item = this.item.Attribute(name, value) }
    member this.Attribute(name: string, value: string seq) = { this with item = this.item.AttributeSet(name, value) }
    member this.Attribute(name: string, value: decimal seq) = { this with item = this.item.AttributeSet(name, value) }
    member this.Attribute(name: string, value: byte array seq) = { this with item = this.item.AttributeSet(name, value) }

    member this.AddItem([<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        { item = this.item.AsRawMap()
          conditionExpression = ConditionAndProject<_>.empty this.tableName None }
        |> this.database.Put (Maybe.Null.toOption logger)
        |> ignoreTyped<Map<string, AttributeValue> voption>
