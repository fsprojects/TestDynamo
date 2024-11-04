module Tests.Loggers

open System
open System.IO
open DynamoDbInMemory.Utils
open Microsoft.Extensions.Logging
open Xunit.Abstractions

type DynamoAttributeValue = Amazon.DynamoDBv2.Model.AttributeValue

type ITestLogger =
    inherit ILogger
    inherit IDisposable
    abstract member TestLog: state: 'TState -> exn: Exception -> formatter: Func<'TState, Exception, string> -> string

type SimpleLogger(output: ITestOutputHelper, level: LogLevel) =
    new(output) = new SimpleLogger(output, SimpleLogger.defaultLevel)

    member this.TestLog state ``exception`` (formatter: Func<_, exn, string>) =
        let txt = formatter.Invoke(state, ``exception``)
        output.WriteLine(txt)
        txt

    member this.Dispose() = ()

    static member defaultLevel = LogLevel.Debug

    interface ITestLogger with
        member this.Dispose() = this.Dispose()
        member this.BeginScope(state) = null
        member this.IsEnabled(logLevel) = logLevel >= level
        member this.TestLog state ``exception`` formatter =
            this.TestLog state ``exception`` formatter
        member this.Log(logLevel, eventId, state, ``exception``, formatter) =
            (this :> ITestLogger).TestLog state ``exception`` formatter |> ignoreTyped<string>

type RecordingLogger(logger: ITestLogger) =

    let mutable record = false
    let mutable recorded = System.Collections.Generic.List<struct (DateTime * string)>()
    member this.Record x = record <- x

    /// <summary>Not thread safe</summary>
    member this.ClearRecord() = recorded <- System.Collections.Generic.List<_>()

    /// <summary>Not thread safe</summary>
    member this.ClearRecordBefore i =
        if i >= recorded.Count then this.ClearRecord()
        elif i <= 0 then ()
        else recorded.RemoveRange(0, i)

    member this.Recorded () = recorded :> System.Collections.Generic.IReadOnlyList<_>

    member this.RecordedStrings() = this.Recorded() |> Seq.map sndT |> Array.ofSeq :> System.Collections.Generic.IReadOnlyList<_>

    member this.Dispose() = logger.Dispose()

    interface ITestLogger with
        member this.Dispose() = this.Dispose()
        member this.BeginScope(state) = logger.BeginScope(state)
        member this.IsEnabled(logLevel) = logger.IsEnabled(logLevel)
        member this.TestLog state ``exception`` formatter =
            let txt = logger.TestLog state ``exception`` formatter
            if record then recorded.Add(struct(DateTime.UtcNow, txt))
            txt
        member this.Log(logLevel, eventId, state, ``exception``, formatter) =
            (this :> ITestLogger).TestLog state ``exception`` formatter |> ignoreTyped<string>

type DisablingLogger(logger: ITestLogger) =

    let mutable disabled = false
    let ensureEnabled () =
        if disabled then invalidOp "Logger is disabled"

    member this.Disable () = disabled <- true
    member this.Dispose() =
        logger.Dispose()
        this.Disable()

    interface ITestLogger with
        member this.Dispose() = this.Dispose()
        member this.BeginScope(state) =
            ensureEnabled()
            logger.BeginScope(state)
        member this.IsEnabled(logLevel) =
            ensureEnabled()
            logger.IsEnabled(logLevel)
        member this.TestLog state ``exception`` formatter =
            ensureEnabled()
            logger.TestLog state ``exception`` formatter
        member this.Log(logLevel, eventId, state, ``exception``, formatter) =
            (this :> ITestLogger).TestLog state ``exception`` formatter |> ignoreTyped<string>

type TestLogger(output: ITestOutputHelper, level: LogLevel) =

    let core = new SimpleLogger(output, level)
    let recorder = new RecordingLogger(core)
    let disabler = new DisablingLogger(recorder)
    let logger = disabler :> ITestLogger

    new(output) = new TestLogger(output, SimpleLogger.defaultLevel)

    member this.Disable () = disabler.Disable()

    member this.Record x = recorder.Record x

    /// <summary>Not thread safe</summary>
    member this.ClearRecord() = recorder.ClearRecord()

    member this.Recorded () = recorder.Recorded()
    member this.RecordedStrings() = recorder.RecordedStrings()
    member this.Dispose() = disabler.Dispose()

    member this.BeginScope(state) =
        logger.BeginScope(state)
    member this.IsEnabled(logLevel) =
        logger.IsEnabled(logLevel)
    member this.TestLog state ``exception`` formatter =
        logger.TestLog state ``exception`` formatter

    interface ITestLogger with
        member this.Dispose() = this.Dispose()
        member this.BeginScope(state) =
            this.BeginScope(state)
        member this.IsEnabled(logLevel) =
            this.IsEnabled(logLevel)
        member this.TestLog state ``exception`` formatter =
            this.TestLog state ``exception`` formatter
        member this.Log(logLevel, eventId, state, ``exception``, formatter) =
            (this :> ITestLogger).TestLog state ``exception`` formatter |> ignoreTyped<string>

type GenericWriter(writer: StreamWriter) =
    interface ITestOutputHelper with
        member this.WriteLine(message) =
            writer.WriteLine(message)

        member this.WriteLine(format, args) =
            writer.WriteLine(format, args)

type FileWriter(fileName: string) =

    let mutable file: FileStream = null
    let mutable fileWriter: StreamWriter = null
    let mutable writer: ITestOutputHelper = null
    do
        if File.Exists fileName then File.Delete fileName
        file <- File.OpenWrite fileName
        fileWriter <- new StreamWriter(file)
        fileWriter.AutoFlush <- true
        writer <- GenericWriter(fileWriter)

    interface IDisposable with
        member this.Dispose() =
            fileWriter.Dispose()
            file.Dispose()

    interface ITestOutputHelper with
        member this.WriteLine(message) =
            writer.WriteLine(message)

        member this.WriteLine(format, args) =
            writer.WriteLine(format, args)