namespace TestDynamo.Api

open System
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Amazon.DynamoDBv2
open TestDynamo
open TestDynamo.Api.FSharp
open TestDynamo.Model
open TestDynamo.Data.Monads.Operators
open TestDynamo.Utils
open Microsoft.Extensions.Logging

type FsDb = Api.FSharp.Database

/// <summary>
/// A mutable Database object containing Tables and Streams
/// </summary>
type Database (db: FsDb, [<Optional; DefaultParameterValue(false)>] disposeUnderlyingDatabase: bool) =

    static let formatLogger = CSharp.toOption ??|> Either1

    static let describeRequiredTable (db: Api.FSharp.Database) (name: string) =
        db.TryDescribeTable ValueNone name |> ValueOption.defaultWith (fun _ -> clientError $"Table {name} not found on database {db.Id}")

    new() = new Database(new FsDb(), true)
    new(cloneData: Api.FSharp.DatabaseCloneData) = new Database(new FsDb(cloneData = cloneData), true)
    new(logger: ILogger) = new Database(new FsDb(logger), true)
    new(logger: ILogger, cloneData: Api.FSharp.DatabaseCloneData) = new Database(new FsDb(logger, cloneData), true)
    new(databaseId: Model.DatabaseId) = new Database(new FsDb(databaseId), true)
    new(databaseId: Model.DatabaseId, logger: ILogger) = new Database(new FsDb(databaseId, logger), true)

    override _.ToString() = db.ToString()

    override _.GetHashCode() = db.GetHashCode()

    override _.Equals(obj) =
        match obj with
        | :? Database as other -> other.CoreDb = db
        | :? FsDb as other -> other = db
        | _ -> false

    static member (==) (x: Database, y: Database) = x.Equals(y)
    
    interface IDisposable with member this.Dispose() = this.Dispose()

    member _.Dispose() = if disposeUnderlyingDatabase then db.Dispose()

    /// <summary>
    /// Get a list of DebugTables. All Tables, Indexes and Items will be enumerated  
    /// </summary>
    member _.DebugTables = db.DebugTables |> Seq.ofList

    /// <summary>Might be null</summary>
    member _.DefaultLogger = db.DefaultLogger |> CSharp.fromOption

    member _.Id = db.Id

    /// <summary>
    /// The actual database.
    /// This class is just a thin wrapper around the CoreDb
    /// </summary>
    member _. CoreDb = db

    /// <summary>
    /// Get a table for debugging purposes
    /// </summary>
    member _.GetTable name =
        LazyDebugTable(name, (describeRequiredTable db name).table)

    /// <summary>
    /// Create a table builder, which can accumulate properties and settings
    /// and then execute an AddTable request against this database
    /// </summary>
    member _.TableBuilder(
        name: string,
        partitionKey: struct (string * string),
        [<Optional; DefaultParameterValue(System.Nullable<struct (string * string)>())>] sortKey: System.Nullable<struct (string * string)>) =

        TableBuilder.Create(db, name, partitionKey, sortKey)

    /// <summary>
    /// Create an item builder, which can accumulate attributes
    /// and then execute a Put request against this database
    /// </summary>
    member _.ItemBuilder(tableName) = ItemBuilder.Create(db, tableName)

    /// <summary>
    /// Add a callback which can subscribe to a Table Stream
    /// </summary>
    member this.SubscribeToStream(table, streamConfig, subscriber: System.Func<DatabaseSynchronizationPacket<TableCdcPacket>, CancellationToken, ValueTask<Unit>>, [<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.SubscribeToStream (CSharp.toOption logger) table streamConfig (fun data c -> subscriber.Invoke(data, c))

    /// <summary>
    /// Add a callback which can subscribe to a Table Stream
    /// </summary>
    member this.SubscribeToStream(
        table,
        behaviour,
        subscriber: System.Func<DatabaseSynchronizationPacket<TableCdcPacket>, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger) =

        let streamDataType =
            streamViewType
            |> CSharp.toOption
            ?|> (_.Value >> StreamDataType.tryParse >> Maybe.expectSomeErr "Invalid stream view type%s" "")
            ?|? StreamDataType.NewAndOldImages

        db.SubscribeToStream (CSharp.toOption logger) table (behaviour, streamDataType) (fun data c -> subscriber.Invoke(data, c) |> Io.normalizeVt)

    /// <summary>
    /// Add a callback which can subscribe to a Table Stream
    /// </summary>
    member this.SubscribeToStream(
        table,
        subscriber: System.Func<DatabaseSynchronizationPacket<TableCdcPacket>, CancellationToken, ValueTask>,
        [<Optional; DefaultParameterValue(null: StreamViewType)>] streamViewType: StreamViewType,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        this.SubscribeToStream(table, SubscriberBehaviour.defaultOptions, subscriber, streamViewType, logger)

    /// <summary>
    /// PUT an Item into the Database  
    /// </summary>
    member _.Put(args, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.Put (CSharp.toOption logger) args

    /// <summary>
    /// Update the behaviour of a stream subscriber or an entire stream  
    /// </summary>
    member _.SetStreamBehaviour(tableName, subscriberId, behaviour, [<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.SetStreamBehaviour (CSharp.toOption logger) tableName subscriberId behaviour

    /// <summary>
    /// Update a table
    /// </summary>
    member _.UpdateTable(name, req, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.UpdateTable (CSharp.toOption logger) name req

    /// <summary>
    /// Delete a table  
    /// </summary>
    member _.DeleteTable(name, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.DeleteTable (CSharp.toOption logger) name

    /// <summary>
    /// Try to delete a table
    /// 
    /// Returns a task which will resolve when all stream subscribers are finished processing
    /// The task will not throw errors. Call AwaitAllSubscribers to consume any errors
    /// </summary>
    member _.TryDeleteTable(name, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.TryDeleteTable (CSharp.toOption logger) name

    /// <summary>
    /// Get the stream config for a table  
    /// </summary>
    member _.StreamsEnabled tableName = db.StreamsEnabled tableName

    /// <summary>
    /// DELETE an Item
    /// </summary>
    member _.Delete(args: DeleteItemArgs<_>, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.Delete (CSharp.toOption logger) args

    /// <summary>
    /// CLEAR table
    /// </summary>
    member _.ClearTable(tableName, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.ClearTable (CSharp.toOption logger) tableName

    /// <summary>
    /// UPDATE an Item  
    /// </summary>
    member _.Update(args: UpdateItemArgs, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.Update (CSharp.toOption logger) args

    /// <summary>
    /// Execute transact writes  
    /// </summary>
    member _.TransactWrite(args: Database.TransactWrite.TransactWrites, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.TransactWrite (CSharp.toOption logger) args

    /// <summary>
    /// GET an Item  
    /// </summary>
    member _.Get(args: GetItemArgs, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.Get (CSharp.toOption logger) args

    /// <summary>
    /// Get table details
    /// </summary>
    member _.TryDescribeTable(name, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.TryDescribeTable (CSharp.toOption logger) name

    /// <summary>
    /// Get table details
    /// </summary>
    member _.DescribeTable(name, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.DescribeTable (CSharp.toOption logger) name

    /// <summary>
    /// Get table details
    /// </summary>
    member _.ListTables(args, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.ListTables (CSharp.toOption logger) args

    /// <summary>
    /// Execute multiple GET requests transactionally  
    /// </summary>
    member _.Gets(args: GetItemArgs seq, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.Gets (CSharp.toOption logger) args

    /// <summary>
    /// GET Items from the database  
    /// </summary>
    member _.Query(req: ExpressionExecutors.Fetch.FetchInput, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.Query (CSharp.toOption logger) req

    /// <summary>
    /// Get an Index if it exists  
    /// </summary>
    member _.TryGetStream tableName = db.TryGetStream tableName

    /// <summary>
    /// Add a new table  
    /// </summary>
    member _.AddTable(args, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.AddTable (CSharp.toOption logger) args

    /// <summary>
    /// Import some clone data. Cloned tables must not exist in the system already 
    /// </summary>
    member _.Import(args, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.Import (CSharp.toOption logger) args

    member _.Print () = db.Print ()

    /// <summary>
    /// Add a table clone from an existing table  
    /// </summary>
    member _.AddClonedTable(name, table, [<Optional; DefaultParameterValue(null: ILogger)>] logger) = db.AddClonedTable (CSharp.toOption logger) name table

    /// <summary>
    /// Build a clone of this Database which contains data and stream config
    /// Does not clone subscription callbacks to streams
    /// </summary>
    /// <param name="globalLogger">
    /// The logger to use in the newly created database
    /// </param>
    member _.Clone([<Optional; DefaultParameterValue(null: ILogger)>] globalLogger: ILogger) =
        CSharp.toOption globalLogger
        ?|> db.Clone
        ?|>? db.Clone

    /// <summary>
    /// Build some data which can be used to clone this Database at a later state.
    /// The clone data is immutable and can be used to clone multiple other Databases
    /// Any Database created from this clone data will contain the data and stream config from this Database
    /// It will not contain subscription callbacks to streams
    /// </summary>
    member this.BuildCloneData () = db.BuildCloneData()

    /// <summary>
    /// Return a task that will be completed when all subscribers have been completed and the system is at rest
    /// The returned task will re-throw any errors thrown by Stream subscriber callbacks
    /// </summary>
    member _.AwaitAllSubscribers([<Optional; DefaultParameterValue(null: ILogger)>] logger, [<Optional; DefaultParameterValue(CancellationToken())>] c) = db.AwaitAllSubscribers (CSharp.toOption logger) c
