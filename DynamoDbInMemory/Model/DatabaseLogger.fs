module DynamoDbInMemory.Model.DatabaseLogger

open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open DynamoDbInMemory.Utils
open DynamoDbInMemory
open DynamoDbInMemory.Data.Monads.Operators

let buildLogger =

    let mutable reqId = 100

    fun globalLogger localLogger ->
        let id = Interlocked.Increment(&reqId)

        localLogger
        ?|> ValueSome
        |> ValueOption.defaultValue globalLogger
        ?|> (tpl id >> Logger.create)
        |> ValueOption.defaultValue Logger.empty

let private isAsync =
    let rec isAsync (t: System.Type) =

        if typeof<Task>.IsAssignableFrom t then true
        elif t = typeof<ValueTask> then true
        elif t.IsConstructedGenericType && t.GetGenericTypeDefinition() = typedefof<ValueTask<_>> then true
        elif t.IsConstructedGenericType
           && (t.GetGenericTypeDefinition() = typedefof<System.ValueTuple<_, _>> || t.GetGenericTypeDefinition() = typedefof<System.Tuple<_, _>>)
        then t.GetGenericArguments() |> Seq.filter isAsync |> Seq.isEmpty |> not
        // test for funcs that return ValueTasks. Sometimes this is actually required
        // elif t.IsConstructedGenericType
        //    && t.GetGenericTypeDefinition() = typedefof<FSharp.Core.FSharpFunc<_, _>>
        // then t.GetGenericArguments() |> Seq.last |> isAsync
        else false

    let inline k (t: System.Type) = t        
    memoize ValueNone k isAsync >> sndT

let private throwIfAsync<'a> logger (context: string) (operation: string) =

    let errMsg1 = "Invalid test type for throwIfAsync. Test type cannot be generic"
    let errMsg2 = "Using logOperation for async result"
    let detailFormat = "logOperation Context: \"{0}\", Operation: \"{1}\", root type: {2}"

    let t = typeof<'a>
    Debug.Assert(not t.IsGenericType || t.IsConstructedGenericType, errMsg1, detailFormat, context, operation, t)
    Debug.Assert(isAsync t |> not, errMsg2, detailFormat, context, operation, t)

let logOperation context operation (f: Logger -> 'a -> 'b) logger input =
#if DEBUG
    // test to ensure that, if a log operation returns something async
    // it will not go through this logger. Not a perfect solution. Only recursively checks tuples of length 2 
    throwIfAsync<'b> logger context operation
#endif

    Logger.log2 "BEGIN: [%s] %s" context operation logger
    let struct (l2, disposeScope) = Logger.scope logger
    use _ = disposeScope
    let x = f l2 input
    Logger.log0 "END" logger
    x

let logOperationAsync context operation (f: Logger -> 'c -> ValueTask<'d>) logger input =
    Logger.log2 "BEGIN: [%s] %s" context operation logger

    let struct (l2, disposeScope) = Logger.scope logger
    Io.onError
        (fun _ ->
            f l2 input
            |%|> (fun x ->
                Logger.log0 "END" logger
                disposeScope.Dispose()
                x))
        (fun _ -> disposeScope.Dispose())

/// <summary>Log an operation where the result is async, but the state modification is sync</summary>
let logOperationHalfAsync context operation (f: Logger -> 'c -> struct (ValueTask<'d> * 'e)) logger input =

    let struct (l2, disposeScope) = Logger.scope logger
    Logger.log2 "BEGIN: [%s] %s" context operation logger
    let inline dispose _ = disposeScope.Dispose()
    let inline ``dispose, end and return`` x =
        Logger.log0 "END" logger
        disposeScope.Dispose()
        x
    
    try
        f l2 input
        |> mapFst (fun x ->
            Io.onError (asLazy x) dispose
            |%|> ``dispose, end and return``)
    with
    | _ ->
        disposeScope.Dispose()
        reraise()