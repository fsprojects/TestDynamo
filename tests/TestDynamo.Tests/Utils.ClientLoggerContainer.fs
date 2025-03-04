module Tests.ClientLoggerContainer

open System
open Amazon
open Amazon.DynamoDBv2
open TestDynamo
open TestDynamo.Data.Monads.Operators
open Microsoft.Extensions.Logging

type Database = TestDynamo.Api.FSharp.Database

type TestDynamoClientBuilder() =
    static member Create(db, id, logger) = TestDynamoClient.createGlobalClient<AmazonDynamoDBClient> (ValueSome logger) true (ValueSome id) ValueNone false (ValueSome db)
    static member Create(db, id) = TestDynamoClient.createGlobalClient<AmazonDynamoDBClient> (ValueNone) true (ValueSome id) ValueNone false (ValueSome db)
    static member Create(db, logger) = TestDynamoClient.createClient<AmazonDynamoDBClient> (ValueSome logger) true ValueNone false (ValueSome db)
    static member Create(db) = TestDynamoClient.createClient<AmazonDynamoDBClient> (ValueNone) true ValueNone false (ValueSome db)
    static member Create(logger) = TestDynamoClient.createClient<AmazonDynamoDBClient> (ValueSome logger) true ValueNone false ValueNone

/// <summary>A wrapper around a database, client and logger which will dispose of items correctly</summary>
type ClientContainer private (host: Database, client: AmazonDynamoDBClient, logger: ILogger, disposeLogger: bool, disposeHost: bool) =

    let disposableLogger =
        match disposeLogger, box logger with
        | false, _ -> ValueNone
        | true, (:? IDisposable as l) -> ValueSome l
        | _ -> invalidOp "Logger must be disposable"

    do
        TestDynamo.TestDynamoClient.attach (ValueSome logger) host ValueNone false client

    new(logger: ILogger, disposeLogger) =
        new ClientContainer(new Database(), new AmazonDynamoDBClient(region = RegionEndpoint.GetBySystemName(Settings.DefaultRegion)), logger, disposeLogger, true)

    new(host: Database, logger: ILogger, disposeLogger) =
        new ClientContainer(host, new AmazonDynamoDBClient(region = RegionEndpoint.GetBySystemName(Settings.DefaultRegion)), logger, disposeLogger, false)

    member this.Host = host
    member this.Client = client

    interface IDisposable with
        member this.Dispose() =
            client.Dispose()
            disposableLogger ?|> _.Dispose() |> ignore
            if disposeHost then host.Dispose()