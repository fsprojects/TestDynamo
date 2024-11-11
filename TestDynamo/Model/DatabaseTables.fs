namespace TestDynamo.Model

open TestDynamo.Model.ExpressionExecutors.Fetch
open Microsoft.FSharp.Core
open System.Runtime.CompilerServices
open Amazon.DynamoDBv2
open TestDynamo.Data.BasicStructures
open TestDynamo.Utils
open TestDynamo
open TestDynamo.Data.Monads.Operators

type BasicReturnValues =
    | None
    | AllOld

type UpdateReturnValues =
    | Basic of BasicReturnValues
    | AllNew
    | UpdatedNew
    | UpdatedOld

type ConditionAndProject<'returnValues> =
    { tableName: string
      conditionExpression: string voption
      expressionAttrValues: Map<string, AttributeValue>
      expressionAttrNames: Map<string, string>
      returnValues: 'returnValues }

    with
    static member empty tableName returnValues =
        { tableName = tableName
          conditionExpression = ValueNone
          expressionAttrValues = Map.empty
          expressionAttrNames = Map.empty
          returnValues = returnValues }

module ConditionAndProject =
    let fromTable name =
        { tableName = name
          conditionExpression = ValueNone
          expressionAttrValues = Map.empty
          expressionAttrNames = Map.empty
          returnValues = BasicReturnValues.None }

    let map f conditionAndProject =
        { tableName = conditionAndProject.tableName
          conditionExpression = conditionAndProject.conditionExpression
          expressionAttrValues = conditionAndProject.expressionAttrValues
          expressionAttrNames = conditionAndProject.expressionAttrNames
          returnValues = f conditionAndProject.returnValues }

type UpdateItemArgs =
    { key: Map<string, AttributeValue>
      // document model uses update for everything. If item only has keys, this will be None
      updateExpression: string voption
      conditionExpression: ConditionAndProject<UpdateReturnValues> }
    with member this.tableName = this.conditionExpression.tableName 

type PutItemArgs<'item> =
    { item: 'item
      conditionExpression: ConditionAndProject<BasicReturnValues> }
    with member this.tableName = this.conditionExpression.tableName

type DeleteItemArgs<'item> =
    { conditionExpression: ConditionAndProject<BasicReturnValues>
      key: 'item }
    with member this.tableName = this.conditionExpression.tableName

type GetItemArgs =
    { conditionExpression: ConditionAndProject<SelectTypes<string voption>>
      maxPageSizeBytes: int
      keys: Map<string, AttributeValue> array }
    with
    member this.tableName = this.conditionExpression.tableName

type GetItemResponse =
    { notProessedKeys: Map<string, AttributeValue> array
      results: Map<string, AttributeValue> array }

[<Struct; IsReadOnly>]
type DatabaseId =
    { regionId: string }
    with
    override this.ToString() = this.regionId

    static member defaultId = {regionId = TestDynamo.Settings.DefaultRegion}

type StreamDataType =
    | KeysOnly
    | NewAndOldImages
    | NewImage
    | OldImage

    override this.ToString() =
        match this with
        | KeysOnly -> StreamViewType.KEYS_ONLY.Value
        | NewAndOldImages -> StreamViewType.NEW_AND_OLD_IMAGES.Value
        | NewImage -> StreamViewType.NEW_IMAGE.Value
        | OldImage -> StreamViewType.OLD_IMAGE.Value

    static member tryParse x =
        match x with
        | x when x = StreamViewType.KEYS_ONLY.Value -> StreamDataType.KeysOnly |> ValueSome
        | x when x = StreamViewType.NEW_IMAGE.Value -> StreamDataType.NewImage |> ValueSome
        | x when x = StreamViewType.OLD_IMAGE.Value -> StreamDataType.OldImage |> ValueSome
        | x when x = StreamViewType.NEW_AND_OLD_IMAGES.Value -> StreamDataType.NewAndOldImages |> ValueSome
        | _ -> ValueNone

type DatabaseSynchronizationPacket<'packet> =
    { data: 'packet
      tableName: string

      /// <summary>
      /// The path that this packet has travelled before reaching
      /// the current database. Is used to drop packets when then have come
      /// full circle (back to the originating database) 
      /// </summary>
      synchronizationPacketPath: NonEmptyList<DatabaseId>
      correlationId: int }

/// <summary>
/// A wrapper around a collection of database tables
/// </summary>
[<Struct; IsReadOnly>]
type DatabaseTables =
    private
    | H of Map<string, Table>

module DatabaseTables =

    let empty = H Map.empty

    let logOperation x = DatabaseLogger.logOperation "Tables" x

    let private tableNotFound name (H tables) =
        let tableNames =
            match tables.Keys |> Seq.map (sprintf " * %s") |> Str.join "\n" with
            | "" -> "Database has no tables created"
            | x -> $"Available tables\n{x}"

        clientError $"No table defined for {name}. {tableNames}"

    let private put' executePut (req: PutItemArgs<'item>) =
        fun logger -> function
        | H tables & tbls ->
            match MapUtils.tryFind req.tableName tables with
            | ValueNone -> tableNotFound req.tableName tbls
            | ValueSome table ->

                let tables' = Map.remove req.tableName tables
                executePut logger req.item table
                |> mapSnd (
                    flip (Map.add req.tableName) tables'
                    >> H)
        |> logOperation "PUT ITEM"

    let put = put' Table.put
    let putReplicated = put' Table.putReplicated

    let private delete' executeDelete (req: DeleteItemArgs<'item>) =
        fun logger -> function
        | H tables ->
            match MapUtils.tryFind req.tableName tables with
            | ValueNone -> clientError $"Table \"{req.tableName}\" not found"
            | ValueSome table ->
                let tables' = Map.remove req.tableName tables
                executeDelete logger req.key table
                |> mapSnd (
                    flip (Map.add req.tableName) tables'
                    >> H)
        |> logOperation "DELETE ITEM"

    let delete = delete' (flip Table.delete false)
    let deleteReplicated = delete' Table.deleteReplicated

    type TableName = string
    type Keys = Map<string, AttributeValue>
    let get (struct (tableName: TableName, keys: Keys)) =
        fun logger -> function
        | H tables ->
            match MapUtils.tryFind tableName tables with
            | ValueNone -> clientError $"Table \"{tableName}\" not found"
            | ValueSome table -> Table.get keys table
        |> logOperation "GET ITEM"

    let tryDeleteTable name =
        fun logger -> function
        | H tables & ts ->
            match MapUtils.tryFind name tables with
            | ValueNone -> struct (ValueNone, ts)
            | ValueSome table ->
                Logger.log1 "Deleting table %O" name logger

                MapUtils.tryFind name tables
                ?|> (
                    Table.hasDeletionProtection
                        |?? (fun _ -> clientError $"Cannot delete table {name} with deletion protection enabled")
                        |.. asLazy tables
                    >> Map.remove name
                    >> H
                    >> tpl (ValueSome table))
                |> ValueOption.defaultValue struct (ValueNone, ts)
        |> logOperation "DELETE TABLE"

    let private describableTable: Table -> Describable<Table> = Logger.describable (Table.describe >> sprintf "Adding table\n%O") 

    let addClonedTable name (table: Table) =
        fun logger -> function
        | H tables ->
            match Map.containsKey name tables with
            | true -> clientError $"""Table "{name}" has already been added"""
            | false ->
                table
                |> Table.reId
                |> Logger.logDescribable1 describableTable logger
                |> flip (Map.add name) tables
                |> H
        |> logOperation "ADD TABLE"

    let addTable (args: TableConfig) (logger: Logger) =
        addClonedTable args.name (Table.empty logger args) logger

    let tryGetTable name =
        function
        | H tables -> MapUtils.tryFind name tables

    let listTables = function
        | H x -> Map.keys x

    let getTable name tables =
        tryGetTable name tables
        |> ValueOption.defaultWith (fun _ ->
            if System.String.IsNullOrEmpty name
            then clientError $"Table name not specified"
            else tableNotFound name tables)

    /// <returns>None if the table was not found</returns>
    let tryUpdateTable name (req: UpdateTableData) =
        fun logger -> function
        | H tables ->
            MapUtils.tryFind name tables
            ?|> (fun table ->
                let t = Table.updateTable req logger table
                Map.add name t tables
                |> H
                |> tpl t)
        |> logOperation "UPDATE TABLE"

    let updateTable =
        fun name ->
            tryUpdateTable name
            >>>> function
                | ValueSome x -> x
                | ValueNone -> clientError $"Cannot find table {name}"
