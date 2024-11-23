namespace TestDynamo

open System
open System.Text.RegularExpressions
open TestDynamo.Utils
open Microsoft.Extensions.Logging
open System.Runtime.CompilerServices

/// <summary>
/// A wrapper around ToString for memory efficient logging
/// </summary>
[<Struct; IsReadOnly>]
type Describable<'a> =
    { table: 'a
      describe: 'a -> string }
    with
    override this.ToString() = this.describe this.table

type Logger =
    private
    | L of struct (struct (string * int) * ILogger)

[<RequireQualifiedAccess>]
module Logger =
    let notAnILogger =
        { new ILogger with  
            override this.Log(logLevel, eventId, state, ``exception``, formatter) = ()
            override this.IsEnabled(logLevel) = false
            override this.BeginScope(state) = null }

    let inline describable<'a> describe (x: 'a) = {describe = describe; table = x}
    let inline undescribe {table = x} = x

    let create struct (id, iLogger) =
        struct (struct ($"{id,3} > {{0}}", id), iLogger) |> L

    let correlate id (L struct (struct (template, settings), iLogger)) =
        let debugdTemplate = $"{id,3}/{template}"
        L struct (struct (debugdTemplate, settings), iLogger)

    let isEnabled level (L (_, l)) = l.IsEnabled level

    let id (L struct (struct (_, id), _)) = id

    let internal internalLogger (L struct (_, x)) = x 

    let private indentRegex = Regex(@"\>", RegexOptions.Compiled)

    let scope (L ((_, logger) & x) & l) =
        let d = logger.BeginScope(0)
        let scopedL =
            if Settings.Logging.UseDefaultLogFormatting
            then mapFstFst (fun (x: string) -> indentRegex.Replace(x, ">  ", 1)) x |> L
            else l
        struct (
            scopedL,
            if d = null then Disposable.nothingToDispose else d)

    let log0 msg (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Information
        then t.LogInformation(String.Format(gFormat, arg0 = msg))

    let log1 format obj (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Information
        then t.LogInformation(String.Format(gFormat, arg0 = sprintf format obj))

    let log2 format obj1 obj2 (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Information
        then t.LogInformation(String.Format(gFormat, arg0 = sprintf format obj1 obj2))

    let logDescribable1 (f: 'a -> Describable<'a>) logger x =
        log1 "%O" (f x) logger
        x

    let logDescribable2 arg2 (f: _ -> Describable<_>) logger x =
        log1 "%O" (f struct (x, arg2)) logger
        x

    let logFn0 msg logger x =
        log1 "%s" msg logger
        x

    let logFn1 format logger x =
        log1 format x logger
        x

    let logFn2 format arg1 logger x =
        log2 format arg1 x logger
        x

    let trace0 message (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Trace
        then t.LogTrace(String.Format(gFormat, arg0 = message))

    let trace1 format obj (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Trace
        then t.LogTrace(String.Format(gFormat, arg0 = sprintf format obj))

    let trace2 format obj1 obj2 (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Trace
        then t.LogTrace(String.Format(gFormat, arg0 = sprintf format obj1 obj2))

    let traceFn0 msg logger x =
        trace0 msg logger
        x

    let traceFn1 format logger x =
        trace1 format x logger
        x

    let traceFn2 format arg1 logger x =
        trace2 format arg1 x logger
        x

    let traceDescribable1 (f: 'a -> Describable<'a>) logger x =
        trace1 "%O" (f x) logger
        x

    let traceDescribable2 arg2 (f: _ -> Describable<_>) logger x =
        trace1 "%O" (f struct (x, arg2)) logger
        x

    let debug0 message (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Debug
        then t.LogTrace(String.Format(gFormat, arg0 = message))

    let debug1 format obj (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Debug
        then t.LogTrace(String.Format(gFormat, arg0 = sprintf format obj))

    let debug2 format obj1 obj2 (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Debug
        then t.LogTrace(String.Format(gFormat, arg0 = sprintf format obj1 obj2))

    let debugDescribable1 (f: 'a -> Describable<'a>) logger x =
        debug1 "%O" (f x) logger
        x

    let debugDescribable2 arg2 (f: _ -> Describable<_>) logger x =
        debug1 "%O" (f struct (x, arg2)) logger
        x

    let debugFn0 msg logger x =
        debug0 msg logger
        x

    let debugFn1 format logger x =
        debug1 format x logger
        x

    let debugFn2 format arg1 logger x =
        debug2 format arg1 x logger
        x

    let warning0 message (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Warning
        then t.LogWarning(String.Format(gFormat, arg0 = message))

    let warning1 format obj (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Warning
        then t.LogWarning(String.Format(gFormat, arg0 = sprintf format obj))

    let warning2 format obj1 obj2 (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Warning
        then t.LogWarning(String.Format(gFormat, arg0 = sprintf format obj1 obj2))

    let warningDescribable1 (f: 'a -> Describable<'a>) logger x =
        warning1 "%O" (f x) logger
        x

    let warningDescribable2 arg2 (f: _ -> Describable<_>) logger x =
        warning1 "%O" (f struct (x, arg2)) logger
        x

    let warningFn0 msg logger x =
        warning0 msg logger
        x

    let warningFn1 format logger x =
        warning1 format x logger
        x

    let warningFn2 format arg1 logger x =
        warning2 format arg1 x logger
        x

    let error0 message (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Error
        then t.LogError(String.Format(gFormat, arg0 = message))

    let error1 format obj (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Error
        then t.LogError(String.Format(gFormat, arg0 = sprintf format obj))

    let error2 format obj1 obj2 (L ((gFormat, _), t)) =
        if t.IsEnabled LogLevel.Error
        then t.LogError(String.Format(gFormat, arg0 = sprintf format obj1 obj2))

    let errorDescribable1 (f: 'a -> Describable<'a>) logger x =
        error1 "%O" (f x) logger
        x

    let errorDescribable2 arg2 (f: _ -> Describable<_>) logger x =
        error1 "%O" (f struct (x, arg2)) logger
        x

    let errorFn0 msg logger x =
        error0 msg logger
        x

    let errorFn1 format logger x =
        error1 format x logger
        x

    let errorFn2 format arg1 logger x =
        error2 format arg1 x logger
        x

    let empty = struct (0, notAnILogger) |> create
