﻿namespace TestDynamo.Api

open System
open System.Runtime.InteropServices
open Microsoft.FSharp.Core
open TestDynamo
open TestDynamo.Utils
open TestDynamo.Data.Monads.Operators
open Microsoft.Extensions.Logging

#nowarn "3390"

type FsGlobalDb = TestDynamo.Api.FSharp.GlobalDatabase
type DatabaseCloneData = TestDynamo.Api.FSharp.DatabaseCloneData

/// <summary>
/// A mutable Global Database object containing Databases (regions), Tables and Streams
/// 
/// Allows database tables to be replicated between Databases
/// Databases are created automatically when requested
/// </summary>
[<AllowNullLiteral>]
type GlobalDatabase (db: FsGlobalDb, [<Optional; DefaultParameterValue(false)>] disposeUnderlyingDatabase: bool) =

    new(
        initialDatabases: Api.FSharp.GlobalDatabaseClone,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let db =
            Maybe.Null.toOption logger
            ?|> fun x -> new FsGlobalDb(initialDatabases, x)
            ?|>? fun _ -> new FsGlobalDb(initialDatabases)

        new GlobalDatabase(db, true)

    new([<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =
        let db =
            Maybe.Null.toOption logger
            ?|> fun x -> new FsGlobalDb(x)
            ?|>? fun _ -> new FsGlobalDb()

        new GlobalDatabase(db, true)

    new(
        cloneData: DatabaseCloneData,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger: ILogger) =

        let db =
            Maybe.Null.toOption logger
            ?|> fun x -> new FsGlobalDb(x)
            ?|>? fun _ -> new FsGlobalDb()

        new GlobalDatabase(db, true)

    /// <summary>
    /// The actual database.
    /// This class is just a thin wrapper around the CoreDb
    /// </summary>
    member _.CoreDb = db

    interface IDisposable with member this.Dispose() = this.Dispose()

    member _.Dispose() = if disposeUnderlyingDatabase then db.Dispose()

    member _.DefaultLogger = db.DefaultLogger

    /// <summary>
    /// Get a list of DebugTables. All Databases, Tables, Indexes and Items will be enumerated  
    /// </summary>
    member _.DebugTables = db.DebugTables

    /// <summary>
    /// Get a database in a specific region.
    /// If the Database does not exist it will be created  
    /// </summary>
    member _.GetDatabase(databaseId, [<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        new Database(db.GetDatabase (Maybe.Null.toOption logger) databaseId)

    /// <summary>
    /// Try to get a database in a specific region.
    /// If the Database does not exist nothing will be returned  
    /// </summary>
    member _.TryGetDatabase(databaseId) =
        db.TryGetDatabase databaseId
        ?|> fun x -> new Database(x) 

    /// <summary>
    /// List all databases that have been created so far  
    /// </summary>
    member _.GetDatabases () = 
        db.GetDatabases()
        |> Map.map (fun _ x -> new Database(x))

    /// <summary>
    /// Describe a global table as a list of linked tables
    /// Optionally omit tables which replicate TO a specific DB 
    /// </summary>
    member _.TryDescribeGlobalTable(dbId, table, [<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.TryDescribeGlobalTable (Maybe.Null.toOption logger) dbId table

    member this.ListGlobalTables(
        databaseId,
        [<Optional; DefaultParameterValue(null: string)>] start: string,
        [<Optional; DefaultParameterValue(System.Nullable<int>())>] limit,
        [<Optional; DefaultParameterValue(null: ILogger)>] logger) =

        db.ListGlobalTables
            (Maybe.Null.toOption logger)
            struct (
                databaseId,
                Maybe.Null.toOption start,
                Maybe.Null.valToOption limit ?|? System.Int32.MaxValue)

    /// <summary>
    /// Describe a global table as a list of linked tables
    /// Optionally omit tables which replicate TO a specific DB 
    /// </summary>
    member this.DescribeGlobalTable(dbId, table, [<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.TryDescribeGlobalTable (Maybe.Null.toOption logger) dbId table

    member this.IsGlobalTable([<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.IsGlobalTable  (Maybe.Null.toOption logger)

    /// <param name="rootsOnly">
    /// If true, will only list replications which describe a paren => child operation.
    /// If false, will include all replications, half of which will be child => parent operations
    /// </param>
    member _.ListReplications (rootsOnly, [<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.ListReplications  (Maybe.Null.toOption logger) rootsOnly

    /// <summary>
    /// Build a clone of this GlobalDatabase which contains Databases with data, stream config and replication settings
    /// Does not clone subscription callbacks to streams
    /// </summary>
    member _.Clone([<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.Clone  (Maybe.Null.toOption logger)

    /// <summary>
    /// Build some data which can be used to clone this GlobalDatabase at a later state.
    /// The clone data is immutable and can be used to clone multiple other GlobalDatabases
    /// Any GlobalDatabase created from this clone data will contain the data, stream config and replication settings from this GlobalDatabase
    /// It will not contain subscription callbacks to streams
    /// </summary>
    member _.BuildCloneData([<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.BuildCloneData  (Maybe.Null.toOption logger)

    /// <summary>
    /// Change how a replication propagates it's data
    /// </summary>
    member this.UpdateReplication(replicationId, behaviour, twoWayUpdate, [<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.UpdateReplication
            (Maybe.Null.toOption logger)
            replicationId
            behaviour
            twoWayUpdate

    /// <summary>
    /// Update a table. This can be used to add replications
    /// </summary>
    member this.UpdateTable(dbId, tableName, args, [<Optional; DefaultParameterValue(null: ILogger)>] logger) =
        db.UpdateTable
            dbId
            (Maybe.Null.toOption logger)
            tableName
            args

    /// <summary>
    /// Return a task that will be completed when all subscribers have been completed and the system is at rest
    /// The returned task will throw any errors encountered in subscribers
    /// </summary>
    member _.AwaitAllSubscribers(
        [<Optional; DefaultParameterValue(null: ILogger)>] logger,
        [<Optional; DefaultParameterValue(System.Threading.CancellationToken())>] c) =

        db.AwaitAllSubscribers (Maybe.Null.toOption logger) c
