namespace TestDynamo.Api.FSharp

open System.Runtime.InteropServices
open TestDynamo.Utils
open TestDynamo.Model

/// <summary>
/// An Item, optimized for and IDE debugger 
/// </summary>
type DebugItem(item: Item) =

    member _.InternalItem = item

    member this.Item
        with get attributeName = item[attributeName]

    override _.ToString() = item.ToString()

    member _.TryGetAttribute(attributeName, [<Out>] value: byref<AttributeValue>) =
        let attr =
            Item.attributes item
            |> MapUtils.tryFind attributeName

        match attr with
        | ValueSome x ->
            value <- x
            true
        | ValueNone -> false

    interface IEnumerable<struct(string * AttributeValue)> with
        member this.GetEnumerator() =
            Item.attributes item
            |> MapUtils.toSeq
            |> _.GetEnumerator()

    interface System.Collections.IEnumerable with
        member this.GetEnumerator() =
            (this :> IEnumerable<struct(string * AttributeValue)>).GetEnumerator()

/// <summary>
/// An Index with all of its Items, optimized for and IDE debugger 
/// </summary>
type DebugIndex =
    { Name: string
      Values: DebugItem seq }

/// <summary>
/// A Table with all of its Indexes, optimized for and IDE debugger 
/// </summary>
type DebugTable =
    { Name: string
      Values: DebugItem seq
      Indexes: DebugIndex seq }

type IDebugIndex =
    abstract Name: string

    /// <summary>
    /// Get values which have attributes matching all of the input filters
    /// An empty filter list will return all values
    /// </summary>
    abstract GetValues: filterAttributes: struct (string * AttributeValue) seq -> DebugItem seq

/// <summary>
/// An Index with all of its Items, optimised for programatic access 
/// </summary>
type LazyDebugIndex(name: string, index: Index) =

    interface IDebugIndex with
        member this.GetValues filters = this.GetValues filters
        member this.Name = Index.getName index |> ValueOption.defaultValue Index.primaryIndexName

    member _.Name = name

    /// <summary>
    /// Get all of the values on this index
    /// </summary>
    member _.GetValues () =
        index
        |> Index.scan ValueNone true true
        |> Seq.sortWith LazyDebugIndex.comparePartitions
        |> Seq.collect (Partition.scan ValueNone true)
        |> Seq.collect PartitionBlock.toList
        |> Seq.map DebugItem

    /// <summary>
    /// Return all items from this index
    /// </summary>
    member this.EagerLoad() =
        { Name = this.Name
          Values = this.GetValues() |> List.ofSeq }

    static member comparePartitions x y = Partition.partitionSortComparer.Compare(x, y)
    static member private returnTrue = asLazy true

    /// <summary>
    /// Get values which have attributes matching all of the input filters
    /// An empty filter list will return all values
    /// </summary>
    member this.GetValues filters =

        let filter =
            Seq.fold (fun s struct (k, v) ->
                let inline item (x: DebugItem) = x.InternalItem
                let filter =
                    item
                    >> Item.attributes
                    >> MapUtils.tryFind k
                    >> ValueOption.map ((=)v)
                    >> ValueOption.defaultValue false

                fun x -> s x && filter x) LazyDebugIndex.returnTrue filters
            |> Seq.filter

        this.GetValues() |> filter

/// <summary>
/// An Table with all of its Indexes, optimised for programatic access 
/// </summary>
type LazyDebugTable(name: string, table: Table) =

    interface IDebugIndex with
        member this.GetValues filters = this.GetValues filters
        member this.Name = Table.name table

    member _.Name = name
    
    member _.HasDeletionProtection = Table.hasDeletionProtection table

    member this.GetValues () = this.GetValues Seq.empty

    member _.GetValues filters =
        let idx = LazyDebugIndex(Index.primaryIndexName, Table.getIndex ValueNone table |> Maybe.expectSome)
        idx.GetValues filters

    member this.GetValues (filters, index: string) =
        let idx: LazyDebugIndex = this.GetIndex index
        idx.GetValues filters

    member _.GetIndexes () =
        table
        |> Table.listGlobalSecondaryIndexes
        |> Seq.sort
        |> Seq.map (fun x -> LazyDebugIndex(x, Table.getIndex (ValueSome x) table |> Maybe.expectSome))

    member this.GetIndex name =
        this.GetIndexes()
        |> Seq.filter (_.Name >> ((=) name))
        |> Collection.tryHead
        |> ValueOption.defaultWith (fun _ -> invalidArg (nameof name) $"Invalid index name \"{name}\"")

    member this.EagerLoad() =
        { Name = this.Name
          Values = this.GetValues() |> List.ofSeq
          Indexes = this.GetIndexes() |> Seq.map _.EagerLoad() |> List.ofSeq }
